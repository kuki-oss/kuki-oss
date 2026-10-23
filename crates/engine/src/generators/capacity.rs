//! `capacity.conflict.t0`: a resource that has been given more work than it can take.
//!
//! The organisation says which objects are resources, which are work, how they are
//! linked and where the numbers live:
//!
//! ```json
//! { "resource_type": "room", "work_type": "booking", "link_type": "assigned_to",
//!   "capacity_attr": "capacity", "load_attr": "load",
//!   "status_attr": "status", "open_statuses": ["open"],
//!   "warn_at": 0.9, "full_urgency_at": 1.5 }
//! ```
//!
//! For every resource it adds up the load of the open work linked to it (in either
//! direction, by `link_type`, as of the clock`) and compares it with the resource's
//! capacity. At `warn_at` of the capacity or more it proposes a `capacity.rebalance`
//! decision; above the capacity the payload says `overloaded`. Plain arithmetic over
//! what the ontology holds: no inference, no model. Capacity is one number, not per
//! period.
//!
//! Work linked to several resources counts in full towards each, and one linked twice
//! to the same resource counts once. Work without a readable load is left out and
//! counted in the payload (`unsized`), so the reviewer knows the total may be low. A
//! resource without a readable, positive capacity is skipped.
//!
//! Stability rule: nothing here depends on the clock. Urgency is quantised to 0.05, so
//! a run revises the recommendation only when the assignment changes.
//!
//! An event-driven run covers the resources named in the scope and the resources linked
//! to any work named in it.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
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
use uuid::Uuid;

pub const ID: &str = "capacity.conflcit.t0";
pub const DECISION_TYPE: &str = "capacity.rebalance";

/// At most this many of the largest assigned items are cited as evidence.
const EVIDENCE_WORK: usize = 5;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    resource_type: String,
    work_type: String,
    link_type: String,
    #[serde(default = "default_capacity_attr")]
    capacity_attr: String,
    #[serde(default = "default_load_attr")]
    load_attr: String,
    #[serde(default)]
    status_attr: Option<String>,
    /// Values of `status_attr` that mean "still open". Required when `status_attr` is set.
    #[serde(default)]
    open_statuses: Vec<String>,
    /// The share of capacity at which the early warning starts.
    #[serde(default = "default_warn_at")]
    warn_at: f64,
    /// The share of capacity at which urgency reaches 1.0.
    #[serde(default = "default_full_urgency_at")]
    full_urgency_at: f64,
}

fn default_capacity_attr() -> String {
    "capacity".to_owned()
}
fn default_load_attr() -> String {
    "load".to_owned()
}
fn default_warn_at() -> f64 {
    0.9
}
fn default_full_urgency_at() -> f64 {
    1.5
}

impl Params {
    fn from_value(v: Value) -> Result<Self, GenError> {
        let p: Params = serde_json::from_value(v)
            .map_err(|e| GenError::InvalidData(format!("{ID} params: {e}")))?;
        let bad = |what: &str| Err(GenError::InvalidData(format!("{ID} params: {what}")));
        if [
            &p.resource_type,
            &p.work_type,
            &p.link_type,
            &p.capacity_attr,
            &p.load_attr,
        ]
        .iter()
        .any(|s| s.trim().is_empty())
        {
            return bad("the types, the link type and the attribute names must not be empty");
        }
        if !(p.warn_at > 0.0 && p.warn_at <= 1.0) {
            return bad("warn_at must be above 0 and at most 1");
        }
        if !(p.full_urgency_at > p.warn_at && p.full_urgency_at <= 100.0) {
            return bad("full_urgency_at must be above warn_at and at most 100");
        }
        if p.status_attr.is_some() && p.open_statuses.is_empty() {
            return bad("open_statuses is required when status_attr is set");
        }
        Ok(p)
    }
}

pub struct CapacityConflict;

struct Work {
    id: Uuid,
    load: f64,
    valid_from: DateTime<Utc>,
}

fn number(obj: &ObjectRecord, attr: &str) -> Option<f64> {
    obj.attrs.get(attr)?.as_f64().filter(|v| v.is_finite())
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

fn round4(x: f64) -> f64 {
    (x * 1e4).round() / 1e4
}

/// Urgency in [0.4, 1.0]: 0.4 at `warn_at`, rising linearly to 1.0 at `full_at`. Quantised to
/// 0.05 so that a small change in load does not revise the recommendation.
fn urgency(ratio: f64, warn_at: f64, full_at: f64) -> f64 {
    if ratio >= full_at {
        return 1.0;
    }
    let fraction = ((ratio - warn_at) / (full_at - warn_at)).clamp(0.0, 1.0);
    ((0.4 + 0.6 * fraction) * 20.0).round() / 20.0
}

#[async_trait]
impl DecisionGenerator for CapacityConflict {
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

        // The resources to judge, by id so the order never depends on the view.
        let mut resources: BTreeMap<Uuid, ObjectRecord> = BTreeMap::new();
        if let ScopeSpec::Entities(ids) = scope {
            for id in ids.iter().collect::<BTreeSet<_>>() {
                let Some(o) = ctx.view.object(*id).await? else {
                    continue;
                };
                if o.type_name == p.resource_type {
                    resources.insert(o.id, o);
                } else if o.type_name == p.work_type {
                    // A changed work item re-evaluates the resources it is assigned to.
                    for l in ctx.view.links(o.id, Some(&p.link_type)).await? {
                        let other = if l.src_id == o.id { l.dst_id } else { l.src_id };
                        if let Some(r) = ctx.view.object(other).await?
                            && r.type_name == p.resource_type
                        {
                            resources.insert(r.id, r);
                        }
                    }
                }
            }
        } else {
            for o in ctx.view.objects_of_type(&p.resource_type, None).await? {
                resources.insert(o.id, o);
            }
        }

        let mut out = Vec::new();
        for res in resources.into_values() {
            let Some(capacity) = number(&res, &p.capacity_attr).filter(|c| *c > 0.0) else {
                continue;
            };

            let linked = ctx.view.links(res.id, Some(&p.link_type)).await?;
            let work_ids: BTreeSet<Uuid> = linked
                .iter()
                .map(|l| {
                    if l.src_id == res.id {
                        l.dst_id
                    } else {
                        l.src_id
                    }
                })
                .filter(|other| *other != res.id)
                .collect();

            let mut works: Vec<Work> = Vec::new();
            let mut unsized_count = 0usize;
            for wid in work_ids {
                let Some(w) = ctx.view.object(wid).await? else {
                    continue;
                };
                if w.type_name != p.work_type || !is_open(&w, &p) {
                    continue;
                }
                match number(&w, &p.load_attr).filter(|l| *l >= 0.0) {
                    Some(load) => works.push(Work {
                        id: w.id,
                        load,
                        valid_from: w.valid_from,
                    }),
                    None => unsized_count += 1,
                }
            }

            // Summed in id order, so the total is the same on every run.
            let total: f64 = works.iter().map(|w| w.load).sum();
            let ratio = total / capacity;
            if ratio < p.warn_at {
                continue;
            }
            let overloaded = ratio > 1.0;
            let (capacity_v, load_v, ratio_v) = (round4(capacity), round4(total), round4(ratio));
            let percent = format!("{:.0}%", ratio * 100.0);
            let res_id = res.id.to_string();

            let mut biggest: Vec<&Work> = works.iter().collect();
            biggest.sort_by(|a, b| {
                b.load
                    .partial_cmp(&a.load)
                    .unwrap_or(Ordering::Equal)
                    .then(a.id.cmp(&b.id))
            });
            biggest.truncate(EVIDENCE_WORK);
            let work_evidence: Vec<EvidenceRef> = biggest
                .iter()
                .map(|w| EvidenceRef {
                    kind: EvidenceKind::Object,
                    id: w.id.to_string(),
                    // The version's own timestamp, not the evaluation clock.
                    as_of: w.valid_from,
                    note: None,
                })
                .collect();
            let resource_evidence = EvidenceRef {
                kind: EvidenceKind::Object,
                id: res_id.clone(),
                as_of: res.valid_from,
                note: None,
            };

            let summary = if overloaded {
                format!(
                    "{} is assigned {load_v} against a capacity of {capacity_v}",
                    p.resource_type
                )
            } else {
                format!(
                    "{} is at {percent} of its capacity ({load_v} of {capacity_v})",
                    p.resource_type
                )
            };

            let mut evidence = vec![resource_evidence.clone()];
            evidence.extend(work_evidence.iter().cloned());
            let candidate = Candidate {
                decision_type: DECISION_TYPE.to_owned(),
                dedup_key: DedupKey::new(ctx.organisation_id, DECISION_TYPE, &[res_id.as_str()]),
                org_unit_id: res.org_unit_id,
                payload: json!({
                    "resourceId": res_id,
                    "resourceType": p.resource_type,
                    "capacity": capacity_v,
                    "load": load_v,
                    "ratio": ratio_v,
                    "overloaded": overloaded,
                    "workCount": works.len(),
                    "unsized": unsized_count,
                }),
                rationale: Rationale {
                    summary,
                    factors: vec![
                        Factor {
                            label: "Assigned load".to_owned(),
                            value: FactorValue::Text(load_v.to_string()),
                            unit: None,
                            weight: None,
                            direction: None,
                            evidence: work_evidence,
                        },
                        Factor {
                            label: "Capacity".to_owned(),
                            value: FactorValue::Text(capacity_v.to_string()),
                            unit: None,
                            weight: None,
                            direction: None,
                            evidence: vec![resource_evidence],
                        },
                        Factor {
                            label: "Utilisation".to_owned(),
                            value: FactorValue::Text(percent),
                            unit: None,
                            weight: None,
                            direction: None,
                            evidence: vec![],
                        },
                    ],
                    // Arithmetic over recorded values: as certain as the data is.
                    confidence: 1.0,
                    confidence_kind: ConfidenceKind::DataQuality,
                    counterfactual: None,
                    narrative: None,
                },
                urgency: urgency(ratio, p.warn_at, p.full_urgency_at),
                impact: None,
                evidence,
            };

            let assigned: Vec<Value> = works
                .iter()
                .map(|w| json!([w.id, w.valid_from, w.load]))
                .collect();
            let input_digest = InputDigestBuilder::new()
                .add("resource_id", &json!(res.id))
                .add("valid_from", &json!(res.valid_from))
                .add("capacity", &json!(capacity))
                .add("work", &Value::Array(assigned))
                .add("unsized", &json!(unsized_count))
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
    //! The generator's contract fixtures and builders, shared with the pack test.

    use chrono::{DateTime, Duration, TimeZone, Utc};
    use kuki_extension::{LinkRecord, ObjectRecord};
    use serde_json::{Value, json};

    use crate::testing::{Case, Fixtures, link, object};

    pub fn clock() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap()
    }

    pub fn params() -> Value {
        json!({ "resource_type": "room", "work_type": "booking", "link_type": "assigned_to",
                "capacity_attr": "capacity", "load_attr": "load",
                "status_attr": "status", "open_statuses": ["open"],
                "warn_at": 0.9, "full_urgency_at": 1.5 })
    }

    pub fn room(id: u128, capacity: f64) -> ObjectRecord {
        object(id, "room", json!({ "capacity": capacity }))
    }

    pub fn booking(id: u128, load: f64, status: &str) -> ObjectRecord {
        object(id, "booking", json!({ "load": load, "status": status }))
    }

    /// Booking `booking` is assigned to room `room`.
    pub fn assign(id: u128, booking: u128, room: u128) -> LinkRecord {
        link(id, "assigned_to", booking, room, json!({}))
    }

    pub fn fixtures() -> Fixtures {
        let case = |name, objects, links| Case {
            name,
            as_of: clock(),
            params: params(),
            objects,
            links,
            ..Case::default()
        };

        // Room 1 is quiet (the closed booking does not count), room 2 is in the warning band
        // (a booking linked another way, and one with no load, do not change the sum), room 3
        // is overloaded (a note is not a booking). Rooms 4 and 5 have no usable capacity.
        let unsized_booking = object(109, "booking", json!({ "status": "open" }));
        let note = object(110, "note", json!({ "load": 99.0, "status": "open" }));
        let mixed = case(
            "quiet, warning, overloaded, and noise around them",
            vec![
                room(1, 40.0),
                room(2, 40.0),
                room(3, 40.0),
                room(4, 0.0),
                object(5, "room", json!({})),
                booking(101, 28.0, "open"),
                booking(102, 20.0, "open"),
                booking(103, 18.0, "open"),
                booking(104, 30.0, "open"),
                booking(105, 22.0, "open"),
                booking(106, 50.0, "open"),
                booking(107, 100.0, "done"),
                booking(108, 100.0, "open"),
                unsized_booking,
                note,
                booking(111, 50.0, "open"),
            ],
            vec![
                assign(1, 101, 1),
                assign(2, 102, 2),
                assign(3, 103, 2),
                assign(4, 104, 3),
                assign(5, 105, 3),
                assign(6, 106, 4),
                assign(7, 107, 1),
                link(8, "mentions", 108, 2, json!({})),
                assign(9, 109, 2),
                assign(10, 110, 3),
                assign(11, 111, 5),
            ],
        );

        // One booking in two rooms counts for both; a duplicate link counts once.
        let shared = case(
            "work in two resources, and a duplicated link",
            vec![room(11, 10.0), room(12, 10.0), booking(201, 9.5, "open")],
            vec![
                assign(20, 201, 11),
                assign(21, 201, 12),
                assign(22, 201, 11),
            ],
        );

        let mut ended = assign(30, 301, 21);
        ended.valid_to = Some(clock() - Duration::hours(1));
        let past = case(
            "an assignment that has ended does not count",
            vec![room(21, 10.0), booking(301, 20.0, "open")],
            vec![ended],
        );

        Fixtures {
            cases: vec![mixed, shared, past],
            bad_params: vec![
                json!({}),
                json!({ "resource_type": "room", "work_type": "booking" }),
                json!({ "resource_type": "", "work_type": "booking", "link_type": "assigned_to" }),
                json!({ "resource_type": "room", "work_type": "booking",
                        "link_type": "assigned_to", "warn_at": 0 }),
                json!({ "resource_type": "room", "work_type": "booking",
                        "link_type": "assigned_to", "warn_at": 1.5 }),
                json!({ "resource_type": "room", "work_type": "booking",
                        "link_type": "assigned_to", "warn_at": 0.9, "full_urgency_at": 0.8 }),
                json!({ "resource_type": "room", "work_type": "booking",
                        "link_type": "assigned_to", "status_attr": "status" }),
                json!({ "resource_type": "room", "work_type": "booking",
                        "link_type": "assigned_to", "surprise": 1 }),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use kuki_extension::LinkRecord;

    use super::fixture::{assign, booking, clock, params, room};
    use super::*;
    use crate::testing::{context_with, link, object};

    async fn run(p: Value, objects: Vec<ObjectRecord>, links: Vec<LinkRecord>) -> Vec<Proposal> {
        let ctx = context_with(ID, clock(), p, objects, links, vec![]);
        CapacityConflict
            .evaluate(&ctx, &ScopeSpec::All)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_resource_near_its_capacity_gets_an_early_warning() {
        let out = run(
            params(),
            vec![
                room(1, 40.0),
                booking(2, 20.0, "open"),
                booking(3, 18.0, "open"),
            ],
            vec![assign(1, 2, 1), assign(2, 3, 1)],
        )
        .await;
        assert_eq!(out.len(), 1);
        let c = &out[0].candidate;
        assert_eq!(c.decision_type, "capacity.rebalance");
        assert_eq!(c.payload["overloaded"], json!(false));
        assert_eq!(c.payload["capacity"], json!(40.0));
        assert_eq!(c.payload["load"], json!(38.0));
        assert_eq!(c.payload["ratio"], json!(0.95));
        assert_eq!(c.payload["workCount"], json!(2));
        assert_eq!(c.payload["unsized"], json!(0));
        assert_eq!(c.urgency, 0.45);
        assert!(
            c.dedup_key
                .as_str()
                .ends_with(&Uuid::from_u128(1).to_string())
        );
        c.validate().unwrap();
        assert_eq!(out[0].input_digest.len(), 64);
    }

    #[tokio::test]
    async fn a_resource_over_its_capacity_is_overloaded_and_more_urgent() {
        let out = run(
            params(),
            vec![
                room(1, 40.0),
                booking(2, 30.0, "open"),
                booking(3, 22.0, "open"),
            ],
            vec![assign(1, 2, 1), assign(2, 3, 1)],
        )
        .await;
        assert_eq!(out.len(), 1);
        let c = &out[0].candidate;
        assert_eq!(c.payload["overloaded"], json!(true));
        assert_eq!(c.urgency, 0.8);
        assert!(
            c.rationale.summary.contains("52"),
            "{}",
            c.rationale.summary
        );
    }

    #[tokio::test]
    async fn below_the_warning_level_is_quiet() {
        let out = run(
            params(),
            vec![room(1, 40.0), booking(2, 28.0, "open")],
            vec![assign(1, 2, 1)],
        )
        .await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn the_warning_level_is_a_setting() {
        let objects = vec![room(1, 40.0), booking(2, 28.0, "open")];
        let links = vec![assign(1, 2, 1)];
        assert!(
            run(params(), objects.clone(), links.clone())
                .await
                .is_empty()
        );

        let mut early = params();
        early["warn_at"] = json!(0.6);
        assert_eq!(run(early, objects, links).await.len(), 1);
    }

    #[tokio::test]
    async fn closed_work_other_links_and_other_types_do_not_count() {
        let out = run(
            params(),
            vec![
                room(1, 10.0),
                booking(2, 20.0, "done"),
                booking(3, 20.0, "open"),
                object(4, "note", json!({ "load": 20.0, "status": "open" })),
            ],
            vec![
                assign(1, 2, 1),
                link(2, "mentions", 3, 1, json!({})),
                assign(3, 4, 1),
            ],
        )
        .await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn work_without_a_load_is_left_out_and_reported() {
        let out = run(
            params(),
            vec![
                room(1, 10.0),
                booking(2, 9.5, "open"),
                object(3, "booking", json!({ "status": "open" })),
            ],
            vec![assign(1, 2, 1), assign(2, 3, 1)],
        )
        .await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].candidate.payload["unsized"], json!(1));
        assert_eq!(out[0].candidate.payload["workCount"], json!(1));
    }

    #[tokio::test]
    async fn shared_work_counts_for_each_resource_and_a_duplicate_link_counts_once() {
        let out = run(
            params(),
            vec![room(1, 10.0), room(2, 10.0), booking(3, 9.5, "open")],
            vec![assign(1, 3, 1), assign(2, 3, 2), assign(3, 3, 1)],
        )
        .await;
        assert_eq!(out.len(), 2);
        for p in &out {
            assert_eq!(p.candidate.payload["load"], json!(9.5));
        }
    }

    #[tokio::test]
    async fn a_resource_without_a_usable_capacity_is_skipped() {
        let out = run(
            params(),
            vec![
                object(1, "room", json!({})),
                room(2, 0.0),
                room(3, -5.0),
                booking(4, 50.0, "open"),
            ],
            vec![assign(1, 4, 1), assign(2, 4, 2), assign(3, 4, 3)],
        )
        .await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn an_entity_scope_covers_the_named_resources_and_those_of_named_work() {
        let objects = vec![
            room(1, 40.0),
            room(2, 40.0),
            booking(3, 50.0, "open"),
            booking(4, 50.0, "open"),
        ];
        let links = vec![assign(1, 3, 1), assign(2, 4, 2)];
        let ctx = context_with(ID, clock(), params(), objects, links, vec![]);
        let scoped = |ids: Vec<u128>| {
            let ctx = &ctx;
            async move {
                CapacityConflict
                    .evaluate(
                        ctx,
                        &ScopeSpec::Entities(ids.into_iter().map(Uuid::from_u128).collect()),
                    )
                    .await
                    .unwrap()
            }
        };
        let id_of = |p: &Proposal| p.candidate.payload["resourceId"].clone();

        let by_work = scoped(vec![3]).await;
        assert_eq!(by_work.len(), 1);
        assert_eq!(id_of(&by_work[0]), json!(Uuid::from_u128(1).to_string()));

        let by_resource = scoped(vec![2]).await;
        assert_eq!(by_resource.len(), 1);
        assert_eq!(
            id_of(&by_resource[0]),
            json!(Uuid::from_u128(2).to_string())
        );

        assert!(scoped(vec![99]).await.is_empty());
    }

    #[test]
    fn it_declares_no_capabilities() {
        assert!(CapacityConflict.requires().is_empty());
        assert_eq!(CapacityConflict.tier(), Tier::T0);
    }
}
