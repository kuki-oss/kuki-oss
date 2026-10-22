//! `signal.anomaly.t0`: a reading far from the subject's own recent history.
//!
//! The organisation says which objects to watch and which events carry the readings:
//!
//! ```json
//! { "object_type": "sensor", "event_type": "reading", "value_field": "value",
//!   "min_history": 8, "history": 30, "z_threshold": 3.0 }
//! ```
//!
//! For each object it takes the numeric `value_field` of its `event_type` events, oldest first.
//! When the latest reading is at least `z_threshold` sample standard deviations from the mean of
//! the `history` readings before it, it proposes a `signal.investigate` decision. Plain
//! arithmetic over what the ontology holds: no inference, no model.
//!
//! Stability rule: nothing here depends on the clock. The baseline is the last `history`
//! readings, counted rather than timed, and urgency and confidence are quantised to 0.05, so a
//! run revises the recommendation only when the data changes. One open decision per subject:
//! its dedup key is the subject, so a newer anomalous reading revises it and a return to normal
//! clears it.
//!
//! A reading another event supersedes (a correction) is ignored, and so is any event whose value
//! is not a finite number.

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use kuki_extension::{
    Capability, DecisionContext, DecisionGenerator, EventQuery, EventRecord, GenError, Proposal,
    ScopeSpec, Trigger,
};
use kuki_types::{
    Candidate, ConfidenceKind, DedupKey, EvidenceKind, EvidenceRef, Factor, FactorValue,
    InputDigestBuilder, Rationale, Tier,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

pub const ID: &str = "signal.anomaly.t0";
pub const DECISION_TYPE: &str = "signal.investigate";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    object_type: String,
    event_type: String,
    #[serde(default = "default_value_field")]
    value_field: String,
    /// Baseline readings needed before the latest one can be judged.
    #[serde(default = "default_min_history")]
    min_history: usize,
    /// At most this many readings before the latest form the baseline.
    #[serde(default = "default_history")]
    history: usize,
    #[serde(default = "default_z")]
    z_threshold: f64,
    /// The smallest spread a baseline is treated as having, so a flat history cannot divide by
    /// zero: any move off it counts.
    #[serde(default = "default_min_spread")]
    min_spread: f64,
}

fn default_value_field() -> String {
    "value".to_owned()
}
fn default_min_history() -> usize {
    8
}
fn default_history() -> usize {
    30
}
fn default_z() -> f64 {
    3.0
}
fn default_min_spread() -> f64 {
    1e-6
}

impl Params {
    fn from_value(v: Value) -> Result<Self, GenError> {
        let p: Params = serde_json::from_value(v)
            .map_err(|e| GenError::InvalidData(format!("{ID} params: {e}")))?;
        let bad = |what: &str| Err(GenError::InvalidData(format!("{ID} params: {what}")));
        if p.object_type.trim().is_empty()
            || p.event_type.trim().is_empty()
            || p.value_field.trim().is_empty()
        {
            return bad("object_type, event_type and value_field must not be empty");
        }
        if p.min_history < 3 {
            return bad("min_history must be at least 3");
        }
        if p.history < p.min_history || p.history > 1000 {
            return bad("history must be between min_history and 1000");
        }
        if !(p.z_threshold > 0.0 && p.z_threshold <= 100.0) {
            return bad("z_threshold must be above 0 and at most 100");
        }
        if !(p.min_spread > 0.0 && p.min_spread <= 1e6) {
            return bad("min_spread must be above 0 and at most 1000000");
        }
        Ok(p)
    }
}

pub struct SignalAnomaly;

struct Reading {
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
    value: f64,
}

/// The usable readings, in the order the view returned them (oldest first). Events another event
/// supersedes are dropped; so is anything without a finite number at `field`.
fn readings(events: &[EventRecord], field: &str) -> Vec<Reading> {
    let superseded: BTreeSet<Uuid> = events.iter().filter_map(|e| e.supersedes).collect();
    events
        .iter()
        .filter(|e| !superseded.contains(&e.id))
        .filter_map(|e| {
            let value = e.payload.get(field)?.as_f64()?;
            value.is_finite().then_some(Reading {
                event_id: e.id,
                occurred_at: e.occurred_at,
                recorded_at: e.recorded_at,
                value,
            })
        })
        .collect()
}

/// Mean and sample standard deviation. The caller guarantees at least two values.
fn mean_and_spread(xs: &[f64]) -> (f64, f64) {
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let variance = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    (mean, variance.sqrt())
}

fn round4(x: f64) -> f64 {
    (x * 1e4).round() / 1e4
}

fn quantise(x: f64) -> f64 {
    (x * 20.0).round() / 20.0
}

/// 0.5 at the threshold, rising to 1.0 at twice the threshold.
fn urgency(z_abs: f64, threshold: f64) -> f64 {
    let excess = ((z_abs - threshold) / threshold).clamp(0.0, 1.0);
    quantise(0.5 + 0.5 * excess)
}

/// 0.6, plus up to 0.4 for how much of the allowed history the baseline uses.
fn confidence(baseline: usize, history: usize) -> f64 {
    quantise(0.6 + 0.4 * (baseline as f64 / history as f64).min(1.0))
}

#[async_trait]
impl DecisionGenerator for SignalAnomaly {
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

        let mut subjects = if let ScopeSpec::Entities(ids) = scope {
            // Event-driven run: read only what changed. Ids of other object types are ignored.
            let mut found = Vec::new();
            for id in ids.iter().collect::<BTreeSet<_>>() {
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
        subjects.sort_by_key(|o| o.id);

        let mut out = Vec::new();
        for obj in subjects {
            let events = ctx
                .view
                .events(&EventQuery {
                    subject_id: obj.id,
                    type_name: Some(p.event_type.clone()),
                    occurred_from: None,
                    occurred_to: None,
                })
                .await?;
            let all = readings(&events, &p.value_field);
            let Some((latest, before)) = all.split_last() else {
                continue;
            };
            if before.len() < p.min_history {
                continue;
            }
            let baseline = &before[before.len().saturating_sub(p.history)..];
            let values: Vec<f64> = baseline.iter().map(|r| r.value).collect();
            let (mean, spread) = mean_and_spread(&values);
            let z = (latest.value - mean) / spread.max(p.min_spread);
            if !z.is_finite() || z.abs() < p.z_threshold {
                continue;
            }

            let direction = if z > 0.0 { "above" } else { "below" };
            let (latest_v, mean_v, spread_v, z_v) = (
                round4(latest.value),
                round4(mean),
                round4(spread),
                round4(z),
            );
            let id_text = obj.id.to_string();
            let latest_at = latest
                .occurred_at
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

            let reading_evidence = EvidenceRef {
                kind: EvidenceKind::Event,
                id: latest.event_id.to_string(),
                // The ledger time of the reading itself, not the evaluation clock.
                as_of: latest.recorded_at,
                note: None,
            };
            let subject_evidence = EvidenceRef {
                kind: EvidenceKind::Object,
                id: id_text.clone(),
                as_of: obj.valid_from,
                note: None,
            };

            let candidate = Candidate {
                decision_type: DECISION_TYPE.to_owned(),
                dedup_key: DedupKey::new(ctx.organisation_id, DECISION_TYPE, &[id_text.as_str()]),
                org_unit_id: obj.org_unit_id,
                payload: json!({
                    "subjectId": id_text,
                    "subjectType": p.object_type,
                    "eventType": p.event_type,
                    "valueField": p.value_field,
                    "latest": latest_v,
                    "latestEventId": latest.event_id.to_string(),
                    "latestAt": latest_at,
                    "baselineMean": mean_v,
                    "baselineSpread": spread_v,
                    "zScore": z_v,
                    "readings": baseline.len(),
                    "direction": direction,
                }),
                rationale: Rationale {
                    summary: format!(
                        "latest {} reading {latest_v} is {:.1} standard deviations {direction} \
                         its recent mean {mean_v}",
                        p.event_type,
                        z.abs()
                    ),
                    factors: vec![
                        Factor {
                            label: "Latest reading".to_owned(),
                            value: FactorValue::Text(latest_v.to_string()),
                            unit: None,
                            weight: None,
                            direction: None,
                            evidence: vec![reading_evidence.clone()],
                        },
                        Factor {
                            label: "Recent mean".to_owned(),
                            value: FactorValue::Text(mean_v.to_string()),
                            unit: None,
                            weight: None,
                            direction: None,
                            evidence: vec![],
                        },
                        Factor {
                            label: "Deviation".to_owned(),
                            value: FactorValue::Text(format!(
                                "{:.1} standard deviations (threshold {})",
                                z.abs(),
                                p.z_threshold
                            )),
                            unit: None,
                            weight: None,
                            direction: None,
                            evidence: vec![],
                        },
                    ],
                    confidence: confidence(baseline.len(), p.history),
                    confidence_kind: ConfidenceKind::DataQuality,
                    counterfactual: None,
                    narrative: None,
                },
                urgency: urgency(z.abs(), p.z_threshold),
                impact: None,
                evidence: vec![reading_evidence, subject_evidence],
            };

            let used: Vec<Value> = baseline
                .iter()
                .chain(std::iter::once(latest))
                .map(|r| json!([r.event_id, r.value]))
                .collect();
            let input_digest = InputDigestBuilder::new()
                .add("subject_id", &json!(obj.id))
                .add("valid_from", &json!(obj.valid_from))
                .add("readings", &Value::Array(used))
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
    //! The generator's contract fixtures, shared with the pack test.

    use chrono::{DateTime, Duration, TimeZone, Utc};
    use kuki_extension::EventRecord;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::testing::{Case, Fixtures, event, object};

    const STEADY: [f64; 10] = [10.0, 10.2, 9.8, 10.1, 9.9, 10.0, 10.2, 9.8, 10.1, 9.9];

    pub fn clock() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap()
    }

    pub fn params() -> Value {
        json!({ "object_type": "sensor", "event_type": "reading", "value_field": "value",
                "min_history": 8, "history": 30, "z_threshold": 3.0 })
    }

    /// Ten steady readings, then `last`.
    pub fn steady_then(last: f64) -> Vec<f64> {
        STEADY.iter().copied().chain([last]).collect()
    }

    /// One reading an hour of `subject`, the last an hour before the clock. Ids count up from
    /// `first_id`.
    pub fn readings(subject: u128, first_id: u128, values: &[f64]) -> Vec<EventRecord> {
        let n = values.len() as i64;
        values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                event(
                    first_id + i as u128,
                    subject,
                    "reading",
                    clock() - Duration::hours(n - i as i64),
                    json!({ "value": v }),
                )
            })
            .collect()
    }

    /// A reading that corrects `replaces`.
    pub fn correction(id: u128, subject: u128, replaces: u128, value: f64) -> EventRecord {
        let mut e = event(
            id,
            subject,
            "reading",
            clock() - Duration::minutes(30),
            json!({ "value": value }),
        );
        e.supersedes = Some(Uuid::from_u128(replaces));
        e
    }

    pub fn fixtures() -> Fixtures {
        let sensor = |id| object(id, "sensor", json!({}));

        let mut spike_events = readings(1, 100, &steady_then(25.0));
        spike_events.extend(readings(2, 200, &steady_then(10.0)));
        spike_events.extend(readings(3, 300, &[10.0, 10.2, 9.8, 10.1, 50.0]));
        spike_events.extend(readings(4, 400, &steady_then(2.0)));

        let mut corrected = readings(5, 500, &steady_then(25.0));
        corrected.push(correction(599, 5, 510, 10.0));

        let mut future = readings(6, 600, &STEADY);
        // Recorded after the clock, so it does not exist yet.
        future.push(event(
            610,
            6,
            "reading",
            clock() + Duration::hours(2),
            json!({ "value": 99.0 }),
        ));

        let mut noise = readings(7, 700, &steady_then(10.0));
        noise.push(event(
            711,
            7,
            "other",
            clock() - Duration::minutes(30),
            json!({ "value": 99.0 }),
        ));
        noise.push(event(
            712,
            7,
            "reading",
            clock() - Duration::minutes(20),
            json!({ "value": "n/a" }),
        ));

        let case = |name, objects: Vec<_>, events| Case {
            name,
            as_of: clock(),
            params: params(),
            objects,
            events,
            ..Case::default()
        };
        Fixtures {
            cases: vec![
                case(
                    "a spike, a drop, a quiet one and one with too little history",
                    vec![sensor(1), sensor(2), sensor(3), sensor(4)],
                    spike_events,
                ),
                case(
                    "a correction replaces the spike",
                    vec![sensor(5)],
                    corrected,
                ),
                case(
                    "a reading recorded later is unknown",
                    vec![sensor(6)],
                    future,
                ),
                case(
                    "other event types and non-numeric values are ignored",
                    vec![sensor(7)],
                    noise,
                ),
            ],
            bad_params: vec![
                json!({}),
                json!({ "object_type": "sensor" }),
                json!({ "object_type": "", "event_type": "reading" }),
                json!({ "object_type": "sensor", "event_type": "reading", "z_threshold": 0 }),
                json!({ "object_type": "sensor", "event_type": "reading", "min_history": 2 }),
                json!({ "object_type": "sensor", "event_type": "reading",
                        "min_history": 8, "history": 5 }),
                json!({ "object_type": "sensor", "event_type": "reading", "min_spread": 0 }),
                json!({ "object_type": "sensor", "event_type": "reading", "surprise": 1 }),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use kuki_extension::ObjectRecord;

    use super::fixture::{clock, correction, params, readings, steady_then};
    use super::*;
    use crate::testing::{context_with, object};

    fn sensor(id: u128) -> ObjectRecord {
        object(id, "sensor", json!({}))
    }

    async fn run(p: Value, objects: Vec<ObjectRecord>, events: Vec<EventRecord>) -> Vec<Proposal> {
        let ctx = context_with(ID, clock(), p, objects, vec![], events);
        SignalAnomaly.evaluate(&ctx, &ScopeSpec::All).await.unwrap()
    }

    #[tokio::test]
    async fn a_spike_is_flagged_with_the_numbers_behind_it() {
        let out = run(
            params(),
            vec![sensor(1)],
            readings(1, 100, &steady_then(25.0)),
        )
        .await;
        assert_eq!(out.len(), 1);
        let c = &out[0].candidate;
        assert_eq!(c.decision_type, "signal.investigate");
        assert_eq!(c.urgency, 1.0);
        assert_eq!(c.payload["direction"], "above");
        assert_eq!(c.payload["latest"], json!(25.0));
        assert_eq!(c.payload["readings"], json!(10));
        assert_eq!(
            c.payload["subjectId"],
            json!(Uuid::from_u128(1).to_string())
        );
        assert_eq!(
            c.payload["latestEventId"],
            json!(Uuid::from_u128(110).to_string())
        );
        assert!(
            c.dedup_key
                .as_str()
                .ends_with(&Uuid::from_u128(1).to_string())
        );
        c.validate().unwrap();
        assert_eq!(out[0].input_digest.len(), 64);
    }

    #[tokio::test]
    async fn a_drop_is_flagged_below() {
        let out = run(
            params(),
            vec![sensor(1)],
            readings(1, 100, &steady_then(2.0)),
        )
        .await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].candidate.payload["direction"], "below");
    }

    #[tokio::test]
    async fn too_little_history_stays_quiet() {
        let few = readings(1, 100, &[10.0, 10.2, 9.8, 10.1, 50.0]);
        assert!(run(params(), vec![sensor(1)], few).await.is_empty());
    }

    #[tokio::test]
    async fn a_corrected_reading_is_ignored() {
        let mut events = readings(1, 100, &steady_then(25.0));
        assert_eq!(
            run(params(), vec![sensor(1)], events.clone()).await.len(),
            1
        );
        events.push(correction(199, 1, 110, 10.0));
        assert!(run(params(), vec![sensor(1)], events).await.is_empty());
    }

    #[tokio::test]
    async fn a_flat_history_flags_any_move_and_nothing_else() {
        let mut flat = vec![5.0; 8];
        flat.push(5.0);
        assert!(
            run(params(), vec![sensor(1)], readings(1, 100, &flat))
                .await
                .is_empty()
        );
        let mut moved = vec![5.0; 8];
        moved.push(5.5);
        assert_eq!(
            run(params(), vec![sensor(1)], readings(1, 100, &moved))
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn the_baseline_is_the_last_history_readings_only() {
        let mut values = vec![0.0; 20];
        for _ in 0..5 {
            values.extend([10.0, 10.1]);
        }
        values.push(10.5);
        let events = readings(1, 100, &values);

        // The whole run is the baseline: it is wide, so 10.5 is unremarkable.
        assert!(
            run(params(), vec![sensor(1)], events.clone())
                .await
                .is_empty()
        );
        // Only the recent, tight readings: 10.5 is far outside them.
        let mut narrow = params();
        narrow["history"] = json!(10);
        assert_eq!(run(narrow, vec![sensor(1)], events).await.len(), 1);
    }

    #[tokio::test]
    async fn an_entity_scope_limits_the_run() {
        let mut events = readings(1, 100, &steady_then(25.0));
        events.extend(readings(2, 200, &steady_then(25.0)));
        let ctx = context_with(
            ID,
            clock(),
            params(),
            vec![sensor(1), sensor(2)],
            vec![],
            events,
        );
        let out = SignalAnomaly
            .evaluate(&ctx, &ScopeSpec::Entities(vec![Uuid::from_u128(2)]))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].candidate.payload["subjectId"],
            json!(Uuid::from_u128(2).to_string())
        );
    }

    #[test]
    fn it_declares_no_capabilities() {
        assert!(SignalAnomaly.requires().is_empty());
        assert_eq!(SignalAnomaly.tier(), Tier::T0);
    }
}
