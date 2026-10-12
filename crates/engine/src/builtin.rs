//! Decision types that ship with the engine. They are installed per organisation by
//! an operator (privileged connection); `kuki_app` cannot write the registry.

use kuki_store::engine_reads::register_decision_type;
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

pub const PACK: &str = "kuki.core";

pub struct DecisionTypeDef {
    pub name: &'static str,
    pub pack: &'static str,
    pub schema_ref: &'static str,
    pub schema: Value,
}

/// `work.escalate`: the payload `deadline.at_risk.t0` produces.
fn work_escalate() -> DecisionTypeDef {
    DecisionTypeDef {
        name: "work.escalate",
        pack: PACK,
        schema_ref: "kuki.core/work.escalate.json",
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
        register_decision_type(c, org, d.name, d.pack, d.schema_ref, &d.schema).await?;
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
}
