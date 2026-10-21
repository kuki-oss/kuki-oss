//! Test support for generators, shared by this crate's tests and, behind the `testing` feature,
//! by Core and domain packs.
//!
//! A generator is checked against fixtures (`Fixtures`): data, not code. `check_pack` runs every
//! generator of a pack against its fixtures and fails when a generator has none, so a new
//! generator cannot skip the contract. Fixture cases must not sit exactly on a threshold of the
//! generator (a horizon edge, a quantisation step) or on a query bound, or the clock check will
//! flake.

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

fn valid_at(from: DateTime<Utc>, to: Option<DateTime<Utc>>, at: DateTime<Utc>) -> bool {
    from <= at && to.is_none_or(|t| t > at)
}

/// An in-memory ontology that answers as of `as_of`, like the real view: objects and links not
/// valid at the clock, and events not yet recorded, do not exist. Events come back ordered by
/// `(occurred_at, id)`; objects and links come back in the order given, so a generator that
/// depends on their order is caught by the contract's reversal check.
pub struct FakeView {
    pub as_of: DateTime<Utc>,
    pub objects: Vec<ObjectRecord>,
    pub links: Vec<LinkRecord>,
    pub events: Vec<EventRecord>,
}

#[async_trait]
impl OntologyView for FakeView {
    async fn object(&self, id: Uuid) -> Result<Option<ObjectRecord>, GenError> {
        Ok(self
            .objects
            .iter()
            .find(|o| o.id == id && valid_at(o.valid_from, o.valid_to, self.as_of))
            .cloned())
    }
    async fn objects_of_type(
        &self,
        t: &str,
        _u: Option<Uuid>,
    ) -> Result<Vec<ObjectRecord>, GenError> {
        Ok(self
            .objects
            .iter()
            .filter(|o| o.type_name == t && valid_at(o.valid_from, o.valid_to, self.as_of))
            .cloned()
            .collect())
    }
    async fn links(&self, o: Uuid, t: Option<&str>) -> Result<Vec<LinkRecord>, GenError> {
        Ok(self
            .links
            .iter()
            .filter(|l| {
                (l.src_id == o || l.dst_id == o)
                    && t.is_none_or(|t| l.type_name == t)
                    && valid_at(l.valid_from, l.valid_to, self.as_of)
            })
            .cloned()
            .collect())
    }
    async fn events(&self, q: &EventQuery) -> Result<Vec<EventRecord>, GenError> {
        let mut out: Vec<EventRecord> = self
            .events
            .iter()
            .filter(|e| {
                e.subject_id == q.subject_id
                    && e.recorded_at <= self.as_of
                    && q.type_name.as_deref().is_none_or(|t| e.type_name == t)
                    && q.occurred_from.is_none_or(|f| e.occurred_at >= f)
                    && q.occurred_to.is_none_or(|t| e.occurred_at < t)
            })
            .cloned()
            .collect();
        out.sort_by_key(|e| (e.occurred_at, e.id));
        Ok(out)
    }
}

fn since_october() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap()
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
        valid_from: since_october(),
        valid_to: None,
    }
}

/// A link between two objects, valid since a fixed past date.
pub fn link(id: u128, type_name: &str, src: u128, dst: u128, attrs: Value) -> LinkRecord {
    LinkRecord {
        id: Uuid::from_u128(id),
        type_name: type_name.into(),
        src_id: Uuid::from_u128(src),
        dst_id: Uuid::from_u128(dst),
        attrs,
        valid_from: since_october(),
        valid_to: None,
    }
}

/// An event about `subject`, recorded the moment it occurred.
pub fn event(
    id: u128,
    subject: u128,
    type_name: &str,
    occurred: DateTime<Utc>,
    payload: Value,
) -> EventRecord {
    EventRecord {
        id: Uuid::from_u128(id),
        type_name: type_name.into(),
        type_version: 1,
        subject_id: Uuid::from_u128(subject),
        occurred_at: occurred,
        recorded_at: occurred,
        payload,
        supersedes: None,
        source: "test".into(),
    }
}

/// A context for `generator_id` over `objects` alone, with `params` as its configured settings.
pub fn context(
    generator_id: &str,
    as_of: DateTime<Utc>,
    params: Value,
    objects: Vec<ObjectRecord>,
) -> DecisionContext {
    context_with(generator_id, as_of, params, objects, vec![], vec![])
}

/// A context over objects, links and events.
pub fn context_with(
    generator_id: &str,
    as_of: DateTime<Utc>,
    params: Value,
    objects: Vec<ObjectRecord>,
    links: Vec<LinkRecord>,
    events: Vec<EventRecord>,
) -> DecisionContext {
    let mut cfg = OrgDecisionConfig::default();
    cfg.generators.insert(
        generator_id.to_owned(),
        GeneratorSettings {
            params,
            ..Default::default()
        },
    );
    let view = FakeView {
        as_of,
        objects,
        links,
        events,
    };
    DecisionContext::new(ORG, as_of, generator_id, Arc::new(cfg), Arc::new(view))
}

/// One situation to run a generator against.
#[derive(Clone)]
pub struct Case {
    pub name: &'static str,
    pub as_of: DateTime<Utc>,
    pub params: Value,
    pub objects: Vec<ObjectRecord>,
    pub links: Vec<LinkRecord>,
    pub events: Vec<EventRecord>,
}

impl Default for Case {
    /// Empty, at the Unix epoch: build a case with `..Case::default()`.
    fn default() -> Self {
        Self {
            name: "",
            as_of: Utc.timestamp_opt(0, 0).unwrap(),
            params: json!({}),
            objects: vec![],
            links: vec![],
            events: vec![],
        }
    }
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
    links: Vec<LinkRecord>,
) -> Result<Vec<Proposal>, String> {
    let ctx = context_with(
        g.id(),
        as_of,
        case.params.clone(),
        objects,
        links,
        case.events.clone(),
    );
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
        let first = run(
            g,
            case,
            case.as_of,
            case.objects.clone(),
            case.links.clone(),
        )
        .await
        .map_err(at)?;

        let again = run(
            g,
            case,
            case.as_of,
            case.objects.clone(),
            case.links.clone(),
        )
        .await
        .map_err(at)?;
        if again != first {
            return Err(at(
                "gives different output when run twice on the same input".into(),
            ));
        }

        let mut objects = case.objects.clone();
        objects.reverse();
        let mut links = case.links.clone();
        links.reverse();
        let shuffled = run(g, case, case.as_of, objects, links).await.map_err(at)?;
        if shuffled != first {
            return Err(at(
                "depends on the order the view returns objects or links in".into(),
            ));
        }

        let later = run(
            g,
            case,
            case.as_of + Duration::minutes(1),
            case.objects.clone(),
            case.links.clone(),
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
                ..Case::default()
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

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap()
    }

    #[tokio::test]
    async fn the_fake_view_shows_only_what_exists_at_the_clock() {
        let mut ended = object(1, "thing", json!({}));
        ended.valid_to = Some(at() - Duration::hours(1));
        let mut not_yet = object(2, "thing", json!({}));
        not_yet.valid_from = at() + Duration::hours(1);
        let live = object(3, "thing", json!({}));
        let mut late = event(1, 3, "e", at() - Duration::hours(2), json!({}));
        late.recorded_at = at() + Duration::hours(1);
        let seen = event(2, 3, "e", at() - Duration::hours(1), json!({}));
        let view = FakeView {
            as_of: at(),
            objects: vec![ended, not_yet, live],
            links: vec![],
            events: vec![late, seen],
        };

        let objects = view.objects_of_type("thing", None).await.unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].id, Uuid::from_u128(3));
        assert!(view.object(Uuid::from_u128(1)).await.unwrap().is_none());
        assert!(view.object(Uuid::from_u128(2)).await.unwrap().is_none());

        let q = EventQuery {
            subject_id: Uuid::from_u128(3),
            type_name: None,
            occurred_from: None,
            occurred_to: None,
        };
        let events = view.events(&q).await.unwrap();
        assert_eq!(
            events.len(),
            1,
            "an event recorded after the clock is unknown"
        );
        assert_eq!(events[0].id, Uuid::from_u128(2));
    }

    #[tokio::test]
    async fn links_are_found_from_either_end_and_filtered_by_type_and_time() {
        let mut ended = link(3, "assigned", 1, 2, json!({}));
        ended.valid_to = Some(at() - Duration::hours(1));
        let view = FakeView {
            as_of: at(),
            objects: vec![],
            links: vec![
                link(1, "assigned", 1, 2, json!({})),
                link(2, "owns", 1, 2, json!({})),
                ended,
            ],
            events: vec![],
        };
        let from = |o: u128, t: Option<&'static str>| {
            let view = &view;
            async move { view.links(Uuid::from_u128(o), t).await.unwrap() }
        };
        assert_eq!(from(1, None).await.len(), 2, "the ended link is gone");
        assert_eq!(
            from(2, None).await.len(),
            2,
            "found from the destination too"
        );
        assert_eq!(from(1, Some("owns")).await.len(), 1);
        assert!(from(9, None).await.is_empty());
    }

    #[tokio::test]
    async fn events_come_back_in_time_then_id_order_within_the_query_range() {
        let t = |h: i64| at() - Duration::hours(h);
        let view = FakeView {
            as_of: at(),
            objects: vec![],
            links: vec![],
            events: vec![
                event(3, 1, "e", t(1), json!({})),
                event(2, 1, "e", t(3), json!({})),
                event(1, 1, "e", t(3), json!({})),
                event(4, 1, "other", t(2), json!({})),
            ],
        };
        let q = |ty: Option<&str>, from, to| EventQuery {
            subject_id: Uuid::from_u128(1),
            type_name: ty.map(str::to_owned),
            occurred_from: from,
            occurred_to: to,
        };
        let ids =
            |es: Vec<EventRecord>| -> Vec<u128> { es.iter().map(|e| e.id.as_u128()).collect() };

        assert_eq!(
            ids(view.events(&q(None, None, None)).await.unwrap()),
            [1, 2, 4, 3]
        );
        assert_eq!(
            ids(view.events(&q(Some("e"), None, None)).await.unwrap()),
            [1, 2, 3]
        );
        assert_eq!(
            ids(view.events(&q(None, Some(t(3)), Some(t(1)))).await.unwrap()),
            [1, 2, 4],
            "from is inclusive and to is exclusive"
        );
    }
}
