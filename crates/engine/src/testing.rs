//! Test support for generators, shared by this crate's tests and, behind
//! the `testing` feature, by Core and domain packs.
//!
//! A generator is checked against fixtures (`Fixtures`): data, not code.
//! `check_pack` runs every generator of a pack against its fixtures and
//! fails when a generator has none, so a new generator cannot skip the
//! contract. Fixture cases must not sit exactly on a threshold of generator
//! (a horizon edge, a quantisation step), or the clock check will flake.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use kuki_extension::{
    DecisionContext, DecisionGenerator, EventQuery, EventRecord, GenError, LinkRecord,
    ObjectRecord, OntologyView, Proposal, ScopeSpec,
    config::{GeneratorSettings, OrgDecisionConfig},
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    evaluator::check,
    pack::{DecisionTypeDef, Pack},
};

/// The organisation every fixture runs under.
pub const ORG: Uuid = Uuid::from_u128(7);

/// An ontology that holds objects and nothing else. Links and events are empty until a
/// generator needs them.
pub struct FakeView(pub Vec<ObjectRecord>);

#[async_trait]
impl OntologyView for FakeView {
    async fn object(&self, id: Uuid) -> Result<Option<ObjectRecord>, GenError> {
        Ok(self.0.iter().find(|o| o.id == id).cloned())
    }
    async fn objects_of_type(
        &self,
        t: &str,
        _u: Option<Uuid>,
    ) -> Result<Vec<ObjectRecord>, GenError> {
        Ok(self
            .0
            .iter()
            .filter(|o| o.type_name == t)
            .cloned()
            .collect())
    }
    async fn links(&self, _o: Uuid, _t: Option<&str>) -> Result<Vec<LinkRecord>, GenError> {
        Ok(vec![])
    }
    async fn events(&self, _q: &EventQuery) -> Result<Vec<EventRecord>, GenError> {
        Ok(vec![])
    }
}

/// An object version valid since a fixed past date, in no org unit.
pub fn object(id: u128, type_name: &str, attrs: Value) -> ObjectRecord {
    ObjectRecord {
        id: Uuid::from_u128(id),
        type_name: type_name.into(),
        type_version: 1,
        org_unit_id: None,
        external_ids: json!({}),
        attrs,
        valid_from: Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap(),
        valid_to: None,
    }
}

/// A context for `generator_id` over `objects`, with `params` as its configured settings.
pub fn context(
    generator_id: &str,
    as_of: DateTime<Utc>,
    params: Value,
    objects: Vec<ObjectRecord>,
) -> DecisionContext {
    let mut cfg = OrgDecisionConfig::default();
    cfg.generators.insert(
        generator_id.to_owned(),
        GeneratorSettings {
            params,
            ..Default::default()
        },
    );
    DecisionContext::new(
        ORG,
        as_of,
        generator_id,
        Arc::new(cfg),
        Arc::new(FakeView(objects)),
    )
}

/// One situation to run a generator against.
#[derive(Clone)]
pub struct Case {
    pub name: &'static str,
    pub as_of: DateTime<Utc>,
    pub params: Value,
    pub objects: Vec<ObjectRecord>,
}

/// What a generator is checked with.
pub struct Fixtures {
    pub cases: Vec<Case>,
    /// Settings the generator must refuse with `GenError::InvalidData`.
    pub bad_params: Vec<Value>,
}

async fn run(
    g: &dyn DecisionGenerator,
    case: &Case,
    as_of: DateTime<Utc>,
    objects: Vec<ObjectRecord>,
) -> Result<Vec<Proposal>, String> {
    let ctx = context(g.id(), as_of, case.params.clone(), objects);
    g.evaluate(&ctx, &ScopeSpec::All)
        .await
        .map_err(|e| format!("evaluation failed: {e:?}"))
}

/// Runs the generator contract. The error names the generator, the case and the rule broken.
pub async fn check_generator(
    g: &dyn DecisionGenerator,
    decision_type: &DecisionTypeDef,
    fixtures: &Fixtures,
) -> Result<(), String> {
    let id = g.id();
    if decision_type.name != g.decision_type() {
        return Err(format!(
            "{id}: emits `{}` but was checked against `{}`",
            g.decision_type(),
            decision_type.name
        ));
    }
    let schema = jsonschema::validator_for(&decision_type.schema)
        .map_err(|e| format!("{id}: the decision type's schema is unusable: {e}"))?;
    if fixtures.cases.is_empty() {
        return Err(format!("{id}: the fixtures have no cases"));
    }

    let mut proposed_anything = false;
    for case in &fixtures.cases {
        let at = |what: String| format!("{id} / {}: {what}", case.name);
        let first = run(g, case, case.as_of, case.objects.clone())
            .await
            .map_err(at)?;

        let again = run(g, case, case.as_of, case.objects.clone())
            .await
            .map_err(at)?;
        if again != first {
            return Err(at(
                "gives different output when run twice on the same input".into(),
            ));
        }

        let mut reversed = case.objects.clone();
        reversed.reverse();
        let shuffled = run(g, case, case.as_of, reversed).await.map_err(at)?;
        if shuffled != first {
            return Err(at("depends on the order the view returns objects in".into()));
        }

        let later = run(
            g,
            case,
            case.as_of + Duration::minutes(1),
            case.objects.clone(),
        )
        .await
        .map_err(at)?;
        if later != first {
            return Err(at(
                "changes when the clock moves one minute; nothing in a candidate may drift with the clock"
                    .into(),
            ));
        }

        let mut keys = std::collections::BTreeSet::new();
        for p in &first {
            check(ORG, g.decision_type(), p, &schema).map_err(|reason| {
                at(format!(
                    "proposal `{}` fails the orchestrator's checks: {reason}",
                    p.candidate.dedup_key.as_str()
                ))
            })?;
            if !keys.insert(p.candidate.dedup_key.as_str().to_owned()) {
                return Err(at(format!(
                    "two proposals share the dedup key `{}`",
                    p.candidate.dedup_key.as_str()
                )));
            }
        }
        proposed_anything |= !first.is_empty();
    }
    if !proposed_anything {
        return Err(format!(
            "{id}: no case produces a proposal, so the contract checks nothing"
        ));
    }

    let as_of = fixtures.cases[0].as_of;
    for bad in &fixtures.bad_params {
        let ctx = context(id, as_of, bad.clone(), vec![]);
        match g.evaluate(&ctx, &ScopeSpec::All).await {
            Err(GenError::InvalidData(_)) => {}
            other => {
                return Err(format!(
                    "{id}: should refuse the bad parameters {bad} with InvalidData, got {other:?}"
                ));
            }
        }
    }
    Ok(())
}

/// Checks the pack and every generator in it. A generator with no fixtures fails, and so do
/// fixtures for a generator the pack does not have (a rename that left them behind).
pub async fn check_pack(
    pack: &Pack,
    fixtures: &BTreeMap<&'static str, Fixtures>,
) -> Result<(), String> {
    pack.validate().map_err(|e| e.to_string())?;
    for g in &pack.generators {
        let Some(f) = fixtures.get(g.id()) else {
            return Err(format!(
                "generator `{}` has no contract fixtures; add some",
                g.id()
            ));
        };
        let d = pack
            .decision_types
            .iter()
            .find(|d| d.name == g.decision_type())
            .expect("a validated pack declares its generators' decision types");
        check_generator(g.as_ref(), d, f).await?;
    }
    for id in fixtures.keys() {
        if !pack.generators.iter().any(|g| g.id() == *id) {
            return Err(format!(
                "fixtures exist for `{id}`, which is not a generator of pack `{}`",
                pack.name
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use kuki_types::{Candidate, ConfidenceKind, DedupKey, Factor, FactorValue, Rationale, Tier};

    use super::*;
    use crate::pack::fakes;

    /// Which rule a test generator breaks.
    #[derive(Clone, Copy, PartialEq)]
    enum Flaw {
        None,
        ClockDrift,
        ViewOrder,
        BadPayload,
    }

    struct Quirk(Flaw);

    #[async_trait]
    impl DecisionGenerator for Quirk {
        fn id(&self) -> &'static str {
            "quirk.t0"
        }
        fn version(&self) -> semver::Version {
            semver::Version::new(1, 0, 0)
        }
        fn decision_type(&self) -> &'static str {
            "quirk.t"
        }
        fn tier(&self) -> Tier {
            Tier::T0
        }
        fn requires(&self) -> &'static [kuki_extension::Capability] {
            &[]
        }
        fn scope(&self, _t: &kuki_extension::Trigger, _c: &DecisionContext) -> ScopeSpec {
            ScopeSpec::All
        }
        async fn evaluate(
            &self,
            ctx: &DecisionContext,
            _s: &ScopeSpec,
        ) -> Result<Vec<Proposal>, GenError> {
            let mut objects = ctx.view.objects_of_type("thing", None).await?;
            if self.0 != Flaw::ViewOrder {
                objects.sort_by_key(|o| o.id);
            }
            Ok(objects
                .into_iter()
                .map(|o| {
                    let key = o.id.to_string();
                    let summary = if self.0 == Flaw::ClockDrift {
                        format!("due at {}", ctx.as_of.timestamp())
                    } else {
                        "the thing is due".to_owned()
                    };
                    let payload = if self.0 == Flaw::BadPayload {
                        json!({})
                    } else {
                        json!({ "n": 1 })
                    };
                    Proposal {
                        candidate: Candidate {
                            decision_type: "quirk.t".into(),
                            dedup_key: DedupKey::new(
                                ctx.organisation_id,
                                "quirk.t",
                                &[key.as_str()],
                            ),
                            org_unit_id: None,
                            payload,
                            rationale: Rationale {
                                summary,
                                factors: vec![Factor {
                                    label: "f".into(),
                                    value: FactorValue::Text("v".into()),
                                    unit: None,
                                    weight: None,
                                    direction: None,
                                    evidence: vec![],
                                }],
                                confidence: 0.9,
                                confidence_kind: ConfidenceKind::DataQuality,
                                counterfactual: None,
                                narrative: None,
                            },
                            urgency: 0.5,
                            impact: None,
                            evidence: vec![],
                        },
                        input_digest: "a".repeat(64),
                    }
                })
                .collect())
        }
    }

    fn quirk_type() -> DecisionTypeDef {
        DecisionTypeDef {
            name: "quirk.t",
            pack: "quirk",
            schema_ref: "quirk/t.json",
            default_action: None,
            schema: json!({
                "type": "object",
                "required": ["n"],
                "properties": { "n": { "type": "integer" } }
            }),
        }
    }

    fn fixtures() -> Fixtures {
        Fixtures {
            cases: vec![Case {
                name: "two things",
                as_of: Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap(),
                params: json!({}),
                objects: vec![object(1, "thing", json!({})), object(2, "thing", json!({}))],
            }],
            bad_params: vec![],
        }
    }

    async fn verdict(flaw: Flaw, f: &Fixtures) -> Result<(), String> {
        check_generator(&Quirk(flaw), &quirk_type(), f).await
    }

    #[tokio::test]
    async fn a_clean_generator_passes() {
        assert_eq!(verdict(Flaw::None, &fixtures()).await, Ok(()));
    }

    #[tokio::test]
    async fn a_clock_drifting_generator_is_caught() {
        let e = verdict(Flaw::ClockDrift, &fixtures()).await.unwrap_err();
        assert!(e.contains("clock"), "{e}");
    }

    #[tokio::test]
    async fn a_view_order_dependent_generator_is_caught() {
        let e = verdict(Flaw::ViewOrder, &fixtures()).await.unwrap_err();
        assert!(e.contains("order"), "{e}");
    }

    #[tokio::test]
    async fn a_payload_that_breaks_the_schema_is_caught_by_the_orchestrators_own_check() {
        let e = verdict(Flaw::BadPayload, &fixtures()).await.unwrap_err();
        assert!(e.contains("orchestrator") && e.contains("payload"), "{e}");
    }

    #[tokio::test]
    async fn bad_params_that_are_accepted_are_caught() {
        let mut f = fixtures();
        f.bad_params = vec![json!({ "anything": 1 })];
        let e = verdict(Flaw::None, &f).await.unwrap_err();
        assert!(e.contains("bad parameters"), "{e}");
    }

    #[tokio::test]
    async fn fixtures_that_propose_nothing_are_refused() {
        let mut f = fixtures();
        f.cases[0].objects.clear();
        let e = verdict(Flaw::None, &f).await.unwrap_err();
        assert!(e.contains("checks nothing"), "{e}");
    }

    #[tokio::test]
    async fn a_generator_without_fixtures_fails_the_pack_check() {
        let p = fakes::pack(
            "p",
            vec![fakes::decision_type("p.t", "p", "p/t.json")],
            vec![fakes::generator("p.g.t0", "p.t")],
        );
        let e = check_pack(&p, &BTreeMap::new()).await.unwrap_err();
        assert!(
            e.contains("p.g.t0") && e.contains("no contract fixtures"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn fixtures_for_a_generator_the_pack_lacks_fail_the_pack_check() {
        let p = fakes::pack("p", vec![], vec![]);
        let stale = BTreeMap::from([("ghost.t0", fixtures())]);
        let e = check_pack(&p, &stale).await.unwrap_err();
        assert!(e.contains("ghost.t0"), "{e}");
    }
}
