//! Decision types that ship with the engine. They are installed per organisation by
//! an operator (privileged connection); `kuki_app` cannot write the registry.

use serde_json::json;
use sqlx::PgConnection;
use std::sync::Arc;
use uuid::Uuid;

use crate::generators::{
    anomaly::SignalAnomaly, capacity::CapacityConflict, deadline::DeadlineAtRisk,
};
pub use crate::pack::DecisionTypeDef;
use crate::pack::Pack;

pub const PACK: &str = "kuki.base";

/// `work.escalate`: the payload `deadline.at_risk.t0` produces.
fn work_escalate() -> DecisionTypeDef {
    DecisionTypeDef {
        name: "work.escalate",
        pack: PACK,
        schema_ref: "kuki.base/work.escalate.json",
        default_action: Some("builtin:flag_review"),
        schema: json!({
            "type": "object",
            "required": ["objectId", "objectType", "dueAt", "overdue"],
            "properties": {
                "objectId":   { "type": "string", "minLength": 1 },
                "objectType": { "type": "string", "minLength": 1 },
                "dueAt":      { "type": "string", "minLength": 1 },
                "overdue":    { "type": "boolean" },
                "status":     {}
            },
            "additionalProperties": false
        }),
    }
}

/// `signal.investigate`: the payload `signal.anomaly.t0` produces.
fn signal_investigate() -> DecisionTypeDef {
    DecisionTypeDef {
        name: "signal.investigate",
        pack: PACK,
        schema_ref: "kuki.base/signal.investigate.json",
        default_action: Some("builtin:flag_review"),
        schema: json!({
            "type": "object",
            "required": ["subjectId", "subjectType", "eventType", "valueField", "latest",
                         "latestEventId", "latestAt", "baselineMean", "baselineSpread",
                         "zScore", "readings", "direction"],
            "properties": {
                "subjectId":      { "type": "string", "minLength": 1 },
                "subjectType":    { "type": "string", "minLength": 1 },
                "eventType":      { "type": "string", "minLength": 1 },
                "valueField":     { "type": "string", "minLength": 1 },
                "latest":         { "type": "number" },
                "latestEventId":  { "type": "string", "minLength": 1 },
                "latestAt":       { "type": "string", "minLength": 1 },
                "baselineMean":   { "type": "number" },
                "baselineSpread": { "type": "number" },
                "zScore":         { "type": "number" },
                "readings":       { "type": "integer", "minimum": 1 },
                "direction":      { "enum": ["above", "below"] }
            },
            "additionalProperties": false
        }),
    }
}

/// `capacity.rebalance`: the payload `capacity.conflict.t0` produces.
fn capacity_rebalance() -> DecisionTypeDef {
    DecisionTypeDef {
        name: "capacity.rebalance",
        pack: PACK,
        schema_ref: "kuki.base/capacity.rebalance.json",
        default_action: Some("builtin:flag_review"),
        schema: json!({
            "type": "object",
            "required": ["resourceId", "resourceType", "capacity", "load", "ratio",
                         "overloaded", "workCount", "unsized"],
            "properties": {
                "resourceId":   { "type": "string", "minLength": 1 },
                "resourceType": { "type": "string", "minLength": 1 },
                "capacity":     { "type": "number", "exclusiveMinimum": 0 },
                "load":         { "type": "number", "minimum": 0 },
                "ratio":        { "type": "number", "minimum": 0 },
                "overloaded":   { "type": "boolean" },
                "workCount":    { "type": "integer", "minimum": 0 },
                "unsized":      { "type": "integer", "minimum": 0 }
            },
            "additionalProperties": false
        }),
    }
}

pub fn decision_types() -> Vec<DecisionTypeDef> {
    vec![work_escalate(), signal_investigate(), capacity_rebalance()]
}

/// The pack kuki-oss ships: domain-neutral decision types and
/// the generators that produce them.
pub fn base_pack() -> Pack {
    Pack {
        name: PACK,
        version: semver::Version::new(0, 1, 0),
        decision_types: decision_types(),
        generators: vec![
            Arc::new(DeadlineAtRisk),
            Arc::new(SignalAnomaly),
            Arc::new(CapacityConflict),
        ],
    }
}

pub async fn install_decision_types(c: &mut PgConnection, org: Uuid) -> Result<(), sqlx::Error> {
    crate::pack::install(c, org, &decision_types()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validator() -> jsonschema::Validator {
        jsonschema::validator_for(&work_escalate().schema).unwrap()
    }

    #[test]
    fn the_schema_accepts_what_the_deadline_generator_produces() {
        let v = validator();
        for status in [json!(null), json!("open")] {
            let payload = json!({
                "objectId": "11111111-1111-1111-1111-111111111111", "objectType": "case",
                "dueAt": "2026-10-12T09:00:00Z", "overdue": false, "status": status
            });
            assert!(v.is_valid(&payload), "{payload}");
        }
    }

    #[test]
    fn the_schema_rejects_missing_and_unknown_fields() {
        let v = validator();
        assert!(!v.is_valid(&json!({ "objectId": "x" })));
        assert!(!v.is_valid(&json!({
            "objectId": "x", "objectType": "case", "dueAt": "d", "overdue": true, "extra": 1
        })));
    }

    #[test]
    fn escalations_are_flagged_for_review_on_acceptance() {
        let d = decision_types()
            .into_iter()
            .find(|d| d.name == "work.escalate")
            .unwrap();
        assert_eq!(d.default_action, Some("builtin:flag_review"));
    }

    #[test]
    fn every_default_action_fits_the_database_rule() {
        // Mirrors the CHECK on decision_types.default_action_type.
        for d in decision_types() {
            let Some(a) = d.default_action else { continue };
            let mut chars = a.chars();
            assert!(
                a.len() <= 128
                    && chars.next().is_some_and(|c| c.is_ascii_lowercase())
                    && chars.all(|c| c.is_ascii_lowercase()
                        || c.is_ascii_digit()
                        || "_.:-".contains(c)),
                "{}: {a}",
                d.name
            );
        }
    }

    #[test]
    fn the_base_pack_is_consistent_and_named_by_the_constant() {
        let p = base_pack();
        assert_eq!(p.name, PACK);
        assert_eq!(p.validate(), Ok(()));
    }

    #[tokio::test]
    async fn every_generator_in_the_base_pack_meets_the_generator_contract() {
        use std::collections::BTreeMap;

        use crate::generators::{anomaly, capacity, deadline};

        let fixtures = BTreeMap::from([
            (deadline::ID, deadline::fixture::fixtures()),
            (anomaly::ID, anomaly::fixture::fixtures()),
            (capacity::ID, capacity::fixture::fixtures()),
        ]);
        crate::testing::check_pack(&base_pack(), &fixtures)
            .await
            .unwrap();
    }
}
