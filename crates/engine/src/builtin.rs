//! Decision types that ship with the engine. They are installed per organisation by
//! an operator (privileged connection); `kuki_app` cannot write the registry.

use kuki_store::engine_reads::register_decision_type;
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

pub const PACK: &str = "kuki.base";

pub struct DecisionTypeDef {
    pub name: &'static str,
    pub pack: &'static str,
    pub schema_ref: &'static str,
    /// What accepting a decision of this type queues, if anything.
    /// Constant per type, so it cannot make a candidate drift between
    /// runs. `builtin:` names are served by `kuki_actions::BuiltinRouter`;
    /// any other name needs a registered webhook endpoint.
    pub default_action: Option<&'static str>,
    pub schema: Value,
}

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

pub fn decision_types() -> Vec<DecisionTypeDef> {
    vec![work_escalate()]
}

pub async fn install_decision_types(c: &mut PgConnection, org: Uuid) -> Result<(), sqlx::Error> {
    for d in decision_types() {
        register_decision_type(c, org, d.name, d.pack, d.schema_ref, &d.schema, d.default_action)
            .await?;
    }
    Ok(())
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
}
