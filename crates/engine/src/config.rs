//! Loading and checking an organisation's decision configuration.

use kuki_extension::config::OrgDecisionConfig;
use kuki_store::engine_reads::decision_config;
use serde_json::Value;
use sqlx::PgConnection;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("decision config is invalid: {0}")]
    Invalid(String),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

const KNOWN_KEYS: [&str; 3] = ["review_budget", "min_confidence_for_display", "generators"];

/// `None`, JSON null and `{}` all mean "nothing configured", which is the default
/// (no generator enabled). Unknown top-level keys are rejected so a typo cannot
/// silently disable a generator.
pub fn parse_config(raw: Option<Value>) -> Result<OrgDecisionConfig, ConfigError> {
    let value = match raw {
        None | Some(Value::Null) => return Ok(OrgDecisionConfig::default()),
        Some(v) => v,
    };
    let Some(obj) = value.as_object() else {
        return Err(ConfigError::Invalid("expected a JSON object".into()));
    };
    if let Some(k) = obj.keys().find(|k| !KNOWN_KEYS.contains(&k.as_str())) {
        return Err(ConfigError::Invalid(format!("unknown key `{k}`")));
    }
    let cfg: OrgDecisionConfig =
        serde_json::from_value(value).map_err(|e| ConfigError::Invalid(e.to_string()))?;
    let c = cfg.min_confidence_for_display;
    if !c.is_finite() || !(0.0..=1.0).contains(&c) {
        return Err(ConfigError::Invalid(format!(
            "min_confidence_for_display {c} is not in [0,1]"
        )));
    }
    Ok(cfg)
}

/// Reads and checks the config for `org`. The connection must already be inside `with_org_tx`
/// for that organisation; for any other organisation the store returns nothing and this is
/// the default.
pub async fn load_config(
    c: &mut PgConnection,
    org: Uuid,
) -> Result<OrgDecisionConfig, ConfigError> {
    parse_config(decision_config(c, org).await?)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn nothing_configured_is_the_default() {
        for raw in [None, Some(Value::Null), Some(json!({}))] {
            let c = parse_config(raw).unwrap();
            assert_eq!(c, OrgDecisionConfig::default());
            assert!(!c.enabled("deadline.at_risk.t0"));
        }
    }

    #[test]
    fn a_listed_generator_is_enabled_with_its_params() {
        let c = parse_config(Some(json!({
            "generators": { "deadline.at_risk.t0": { "params": { "object_type": "case" } } }
        })))
        .unwrap();
        assert!(c.enabled("deadline.at_risk.t0"));
        assert_eq!(c.params("deadline.at_risk.t0")["object_type"], "case");
    }

    #[test]
    fn unknown_top_level_keys_are_rejected() {
        let e = parse_config(Some(json!({ "generaters": {} }))).unwrap_err();
        assert!(e.to_string().contains("generaters"));
    }

    #[test]
    fn a_non_object_is_rejected() {
        assert!(parse_config(Some(json!([1]))).is_err());
        assert!(parse_config(Some(json!("x"))).is_err());
    }

    #[test]
    fn confidence_floor_must_be_a_probability() {
        assert!(parse_config(Some(json!({ "min_confidence_for_display": 1.5 }))).is_err());
        assert!(parse_config(Some(json!({ "min_confidence_for_display": 0.5 }))).is_ok());
    }
}
