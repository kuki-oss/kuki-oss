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

    fn scope(&self, trigger: &Trigger, _ctx: &DecisionContext) -> ScopeSpec {
        if trigger.entities.is_empty() {
            ScopeSpec::All
        } else {
            ScopeSpec::Entities(trigger.entities.clone())
        }
    }

    async fn evaluate(
        &self,
        ctx: &DecisionContext,
        scope: &ScopeSpec,
    ) -> Result<Vec<Proposal>, GenError> {
        let params_json = ctx.config.params(ID);
        let p = Params::from_value(params_json.clone())?;
        let horizon = Duration::hours(p.horizon_hours);

        let mut objects = if let ScopeSpec::Entities(ids) = scope {
            // Event-driven run: read only what changed. Ids of other object types are ignored.
            let mut found = Vec::new();
            for id in ids.iter().collect::<std::collections::BTreeSet<_>>() {
                if let Some(o) = ctx.view.object(*id).await?
                    && o.type_name == p.object_type
                {
                    found.push(o);
                }
            }
            found
        } else {
            ctx.view.objects_of_type(&p.object_type, None).await?
        };
        objects.sort_by_key(|o| o.id);

        let mut out = Vec::new();
        for obj in objects {
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
pub(crate) mod fixture {
    //! The deadline generator's contract fixtures and builders, shared with the pack test.

    use chrono::{DateTime, Duration, TimeZone, Utc};
    use kuki_extension::ObjectRecord;
    use serde_json::{Value, json};

    use crate::testing::{Case, Fixtures, object};

    pub fn clock() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap()
    }

    pub fn case(id: u128, due: Option<DateTime<Utc>>, status: &str) -> ObjectRecord {
        let mut attrs = json!({ "status": status });
        if let Some(d) = due {
            attrs["due_at"] = json!(d.to_rfc3339());
        }
        object(id, "case", attrs)
    }

    pub fn params() -> Value {
        json!({ "object_type": "case", "status_attr": "status",
                "open_statuses": ["open", "in_progress"], "horizon_hours": 72 })
    }

    pub fn fixtures() -> Fixtures {
        let h = Duration::hours;
        let mut unreadable = case(6, None, "open");
        unreadable.attrs["due_at"] = json!("next tuesday");
        Fixtures {
            cases: vec![
                // 36 h and 6 h sit mid-step of the 0.05 urgency quantisation, so a minute of
                // clock movement cannot cross a boundary.
                Case {
                    name: "overdue and approaching",
                    as_of: clock(),
                    params: params(),
                    objects: vec![
                        case(1, Some(clock() - h(4)), "open"),
                        case(2, Some(clock() + h(36)), "open"),
                        case(3, Some(clock() + h(6)), "open"),
                    ],
                },
                Case {
                    name: "noise around one real decision",
                    as_of: clock(),
                    params: params(),
                    objects: vec![
                        case(4, Some(clock() - h(1)), "done"),
                        case(5, None, "open"),
                        unreadable,
                        case(7, Some(clock() + h(100)), "open"),
                        case(8, Some(clock() - h(1)), "open"),
                    ],
                },
                Case {
                    name: "no status attribute",
                    as_of: clock(),
                    params: json!({ "object_type": "case" }),
                    objects: vec![case(9, Some(clock() - h(1)), "done")],
                },
            ],
            bad_params: vec![
                json!({}),
                json!({ "object_type": "case", "horizon_hours": 0 }),
                json!({ "object_type": "case", "status_attr": "status" }),
                json!({ "object_type": "case", "surprise": 1 }),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::fixture::{case, clock, params};
    use super::*;
    use crate::testing::context;

    fn ctx_at(as_of: DateTime<Utc>, params: Value, objects: Vec<ObjectRecord>) -> DecisionContext {
        context(ID, as_of, params, objects)
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

    #[test]
    fn it_declares_no_capabilities() {
        assert!(DeadlineAtRisk.requires().is_empty());
        assert_eq!(DeadlineAtRisk.tier(), Tier::T0);
    }

    #[test]
    fn the_scope_follows_the_trigger() {
        let ctx = ctx_at(clock(), params(), vec![]);
        let trig = |entities| Trigger {
            organisation_id: Uuid::nil(),
            topic: "t".into(),
            entities,
        };
        assert_eq!(DeadlineAtRisk.scope(&trig(vec![]), &ctx), ScopeSpec::All);
        assert_eq!(
            DeadlineAtRisk.scope(&trig(vec![Uuid::from_u128(1)]), &ctx),
            ScopeSpec::Entities(vec![Uuid::from_u128(1)])
        );
    }

    #[tokio::test]
    async fn scoped_entities_of_another_type_are_ignored() {
        let mut other = case(5, Some(clock() - Duration::hours(1)), "open");
        other.type_name = "task".into();
        let c = ctx_at(clock(), params(), vec![other]);
        let out = DeadlineAtRisk
            .evaluate(&c, &ScopeSpec::Entities(vec![Uuid::from_u128(5)]))
            .await
            .unwrap();
        assert!(out.is_empty());
    }
}
