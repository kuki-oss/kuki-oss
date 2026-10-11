//! `deadline.at_risk.t0`: any object type with a due date and (optionally) a status.
//!
//! The organisation says which type, which attributes and how far ahead to look:
//!
//! ```json
//! { "object_type": "case", "due_attr": "due_at",
//!   "status_attr": "status", "open_statuses": ["open", "in_progress"],
//!   "horizon_hours": 72 }
//! ```
//!
//! It proposes a `work.escalate` decision for every open object that is overdue or
//! due within the horizon. Plain arithmetic over what the ontology holds: no
//! inference, no model.
//!
//! Stability rule: the candidate must not contain anything that drifts with the
//! clock (hours remaining, "as of" times). The store treats any change to the
//! body or rationale as a material revision, so a drifting field would re-notify
//! the reviewer on every run. The only value that moves is urgency, and it is
//! quantised to steps of 0.05 so that it revises on real change.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use kuki_extension::{
    Capability, DecisionContext, DecisionGenerator, GenError, ObjectRecord, Proposal, ScopeSpec,
    Trigger,
};
use kuki_types::{
    Candidate, ConfidenceKind, DedupKey, EvidenceKind, EvidenceRef, Factor, FactorValue,
    InputDigestBuilder, Rationale, Tier,
};
use serde::Deserialize;
use serde_json::{Value, json};

pub const ID: &str = "deadline.at_risk.t0";
pub const DECISION_TYPE: &str = "work.escalate";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    object_type: String,
    #[serde(default = "default_due_attr")]
    due_attr: String,
    #[serde(default)]
    status_attr: Option<String>,
    /// Values of `status_attr` that mean "still open". Required when `status_attr` is set.
    #[serde(default)]
    open_statuses: Vec<String>,
    #[serde(default = "default_horizon")]
    horizon_hours: i64,
}

fn default_due_attr() -> String {
    "due_at".to_owned()
}

fn default_horizon() -> i64 {
    72
}

impl Params {
    fn from_value(v: Value) -> Result<Self, GenError> {
        let p: Params = serde_json::from_value(v)
            .map_err(|e| GenError::InvalidData(format!("{ID} params: {e}")))?;
        if p.object_type.trim().is_empty() {
            return Err(GenError::InvalidData(format!(
                "{ID} params: object_type must not be empty"
            )));
        }
        if p.horizon_hours <= 0 || p.horizon_hours > 24 * 365 {
            return Err(GenError::InvalidData(format!(
                "{ID} params: horizon_hours must be between 1 and 8760"
            )));
        }
        if p.status_attr.is_some() && p.open_statuses.is_empty() {
            return Err(GenError::InvalidData(format!(
                "{ID} params: open_statuses is required when status_attr is set"
            )));
        }
        Ok(p)
    }
}

pub struct DeadlineAtRisk;

/// Urgency in [0.4, 1.0]: 1.0 once overdue, rising linearly from 0.4 at the edge
/// of the horizon. Quantised to 0.05 so that small clock movement does not revise
/// the recommendation.
fn urgency(remaining_secs: i64, horizon_secs: i64) -> f64 {
    if remaining_secs <= 0 {
        return 1.0;
    }
    let fraction = (remaining_secs as f64 / horizon_secs as f64).clamp(0.0, 1.0);
    let raw = 0.4 + 0.6 * (1.0 - fraction);
    (raw * 20.0).round() / 20.0
}

fn due_at(obj: &ObjectRecord, attr: &str) -> Option<DateTime<Utc>> {
    let s = obj.attrs.get(attr)?.as_str()?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

fn is_open(obj: &ObjectRecord, p: &Params) -> bool {
    match &p.status_attr {
        None => true,
        Some(attr) => obj
            .attrs
            .get(attr)
            .and_then(Value::as_str)
            .is_some_and(|s| p.open_statuses.iter().any(|o| o == s)),
    }
}

#[async_trait]
impl DecisionGenerator for DeadlineAtRisk {
    fn id(&self) -> &'static str {
        ID
    }

    fn version(&self) -> semver::Version {
        semver::Version::new(1, 0, 0)
    }

    fn decision_type(&self) -> &'static str {
        DECISION_TYPE
    }

    fn tier(&self) -> Tier {
        Tier::T0
    }

    fn requires(&self) -> &'static [Capability] {
        &[]
    }

    fn scope(&self, _trigger: &Trigger, _ctx: &DecisionContext) -> ScopeSpec {
        ScopeSpec::All
    }

    async fn evaluate(
        &self,
        ctx: &DecisionContext,
        scope: &ScopeSpec,
    ) -> Result<Vec<Proposal>, GenError> {
        let params_json = ctx.config.params(ID);
        let p = Params::from_value(params_json.clone())?;
        let horizon = Duration::hours(p.horizon_hours);

        let mut objects = ctx.view.objects_of_type(&p.object_type, None).await?;
        objects.sort_by_key(|o| o.id);

        let mut out = Vec::new();
        for obj in objects {
            if let ScopeSpec::Entities(ids) = scope
                && !ids.contains(&obj.id)
            {
                continue;
            }
            if !is_open(&obj, &p) {
                continue;
            }
            // An object without a readable due date has nothing to be late against.
            let Some(due) = due_at(&obj, &p.due_attr) else {
                continue;
            };
            let remaining = due - ctx.as_of;
            if remaining > horizon {
                continue;
            }
            let overdue = remaining <= Duration::zero();
            let due_text = due.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            let id_text = obj.id.to_string();

            let evidence = EvidenceRef {
                kind: EvidenceKind::Object,
                id: id_text.clone(),
                // The version's own timestamp, not the evaluation clock, so it does not drift.
                as_of: obj.valid_from,
                note: None,
            };
            let summary = if overdue {
                format!(
                    "{} is overdue (due {due_text}) and still open",
                    p.object_type
                )
            } else {
                format!("{} is due {due_text} and still open", p.object_type)
            };
            let status = p
                .status_attr
                .as_ref()
                .and_then(|a| obj.attrs.get(a))
                .cloned()
                .unwrap_or(Value::Null);

            let candidate = Candidate {
                decision_type: DECISION_TYPE.to_owned(),
                dedup_key: DedupKey::new(ctx.organisation_id, DECISION_TYPE, &[id_text.as_str()]),
                org_unit_id: obj.org_unit_id,
                payload: json!({
                    "objectId": id_text,
                    "objectType": p.object_type,
                    "dueAt": due_text,
                    "overdue": overdue,
                    "status": status,
                }),
                rationale: Rationale {
                    summary,
                    factors: vec![Factor {
                        label: "Due date".to_owned(),
                        value: FactorValue::Text(due_text.clone()),
                        unit: None,
                        weight: None,
                        direction: None,
                        evidence: vec![evidence.clone()],
                    }],
                    // Arithmetic over recorded values: as certain as the data is.
                    confidence: 1.0,
                    confidence_kind: ConfidenceKind::DataQuality,
                    counterfactual: None,
                    narrative: None,
                },
                urgency: urgency(remaining.num_seconds(), horizon.num_seconds()),
                impact: None,
                evidence: vec![evidence],
            };

            // Everything the candidate depends on, and nothing that depends on the clock.
            let input_digest = InputDigestBuilder::new()
                .add("object_id", &json!(obj.id))
                .add("valid_from", &json!(obj.valid_from))
                .add("due_at", &json!(due_text))
                .add("status", &status)
                .add("params", &params_json)
                .finish();

            out.push(Proposal {
                candidate,
                input_digest,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone;
    use kuki_extension::{
        EventQuery, EventRecord, LinkRecord, OntologyView,
        config::{GeneratorSettings, OrgDecisionConfig},
    };
    use uuid::Uuid;

    use super::*;

    struct FakeView(Vec<ObjectRecord>);

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

    fn clock() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap()
    }

    fn case(id: u128, due: Option<DateTime<Utc>>, status: &str) -> ObjectRecord {
        let mut attrs = json!({ "status": status });
        if let Some(d) = due {
            attrs["due_at"] = json!(d.to_rfc3339());
        }
        ObjectRecord {
            id: Uuid::from_u128(id),
            type_name: "case".into(),
            type_version: 1,
            org_unit_id: None,
            external_ids: json!({}),
            attrs,
            valid_from: Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap(),
            valid_to: None,
        }
    }

    fn params() -> Value {
        json!({ "object_type": "case", "status_attr": "status",
                "open_statuses": ["open", "in_progress"], "horizon_hours": 72 })
    }

    fn ctx_at(as_of: DateTime<Utc>, params: Value, objects: Vec<ObjectRecord>) -> DecisionContext {
        let mut cfg = OrgDecisionConfig::default();
        cfg.generators.insert(
            ID.to_owned(),
            GeneratorSettings {
                params,
                ..Default::default()
            },
        );
        DecisionContext::new(
            Uuid::from_u128(7),
            as_of,
            ID,
            Arc::new(cfg),
            Arc::new(FakeView(objects)),
        )
    }

    async fn run(ctx: &DecisionContext) -> Vec<Proposal> {
        DeadlineAtRisk.evaluate(ctx, &ScopeSpec::All).await.unwrap()
    }

    #[tokio::test]
    async fn an_overdue_open_object_gets_a_decision_at_full_urgency() {
        let c = ctx_at(
            clock(),
            params(),
            vec![case(1, Some(clock() - Duration::hours(4)), "open")],
        );
        let out = run(&c).await;
        assert_eq!(out.len(), 1);
        let cand = &out[0].candidate;
        assert_eq!(cand.decision_type, "work.escalate");
        assert_eq!(cand.urgency, 1.0);
        assert_eq!(cand.payload["overdue"], json!(true));
        assert!(
            cand.dedup_key
                .as_str()
                .ends_with(&Uuid::from_u128(1).to_string())
        );
        cand.validate().unwrap();
        assert_eq!(out[0].input_digest.len(), 64);
    }

    #[tokio::test]
    async fn urgency_rises_as_the_deadline_nears() {
        let c = ctx_at(
            clock(),
            params(),
            vec![
                case(1, Some(clock() + Duration::hours(36)), "open"),
                case(2, Some(clock() + Duration::hours(6)), "open"),
            ],
        );
        let out = run(&c).await;
        let far = out
            .iter()
            .find(|p| p.candidate.payload["objectId"] == json!(Uuid::from_u128(1).to_string()))
            .unwrap();
        let near = out
            .iter()
            .find(|p| p.candidate.payload["objectId"] == json!(Uuid::from_u128(2).to_string()))
            .unwrap();
        assert!((far.candidate.urgency - 0.7).abs() < 1e-9);
        assert!(near.candidate.urgency > far.candidate.urgency);
        assert_eq!(far.candidate.payload["overdue"], json!(false));
    }

    #[tokio::test]
    async fn objects_outside_the_horizon_closed_or_undated_are_skipped() {
        let c = ctx_at(
            clock(),
            params(),
            vec![
                case(1, Some(clock() + Duration::hours(100)), "open"),
                case(2, Some(clock() - Duration::hours(1)), "done"),
                case(3, None, "open"),
            ],
        );
        assert!(run(&c).await.is_empty());
    }

    #[tokio::test]
    async fn an_unparseable_due_date_is_skipped_not_fatal() {
        let mut bad = case(1, None, "open");
        bad.attrs["due_at"] = json!("next tuesday");
        let ok = case(2, Some(clock() - Duration::hours(1)), "open");
        let out = run(&ctx_at(clock(), params(), vec![bad, ok])).await;
        assert_eq!(out.len(), 1);
    }

    #[tokio::test]
    async fn without_a_status_attribute_every_dated_object_counts() {
        let p = json!({ "object_type": "case" });
        let c = ctx_at(
            clock(),
            p,
            vec![case(1, Some(clock() - Duration::hours(1)), "done")],
        );
        assert_eq!(run(&c).await.len(), 1);
    }

    #[tokio::test]
    async fn an_entity_scope_limits_the_run() {
        let c = ctx_at(
            clock(),
            params(),
            vec![
                case(1, Some(clock() - Duration::hours(1)), "open"),
                case(2, Some(clock() - Duration::hours(1)), "open"),
            ],
        );
        let out = DeadlineAtRisk
            .evaluate(&c, &ScopeSpec::Entities(vec![Uuid::from_u128(2)]))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].candidate.payload["objectId"],
            json!(Uuid::from_u128(2).to_string())
        );
    }

    #[tokio::test]
    async fn a_minute_of_clock_movement_changes_nothing() {
        let objs = vec![case(1, Some(clock() + Duration::hours(36)), "open")];
        let a = run(&ctx_at(clock(), params(), objs.clone())).await;
        let b = run(&ctx_at(clock() + Duration::minutes(1), params(), objs)).await;
        assert_eq!(
            a, b,
            "a drifting field would revise the recommendation on every run"
        );
    }

    #[tokio::test]
    async fn output_order_does_not_depend_on_view_order() {
        let a = vec![
            case(1, Some(clock() - Duration::hours(1)), "open"),
            case(2, Some(clock() - Duration::hours(1)), "open"),
        ];
        let mut b = a.clone();
        b.reverse();
        assert_eq!(
            run(&ctx_at(clock(), params(), a)).await,
            run(&ctx_at(clock(), params(), b)).await
        );
    }

    #[tokio::test]
    async fn bad_params_are_reported() {
        for p in [
            json!({}),
            json!({ "object_type": "case", "horizon_hours": 0 }),
            json!({ "object_type": "case", "status_attr": "status" }),
            json!({ "object_type": "case", "surprise": 1 }),
        ] {
            let c = ctx_at(clock(), p, vec![]);
            assert!(matches!(
                DeadlineAtRisk.evaluate(&c, &ScopeSpec::All).await,
                Err(GenError::InvalidData(_))
            ));
        }
    }

    #[test]
    fn it_declares_no_capabilities() {
        assert!(DeadlineAtRisk.requires().is_empty());
        assert_eq!(DeadlineAtRisk.tier(), Tier::T0);
    }
}
