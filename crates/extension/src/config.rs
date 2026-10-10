use std::collections::BTreeMap;

use kuki_types::InputDigestBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorMode {
    /// Output is surfaced to users.
    #[default]
    Active,
    /// Runs and records output, never surfaced. Keeps the baseline measurable (invariant I7)
    Shadow,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GeneratorSettings {
    pub enabled: bool,
    pub mode: GeneratorMode,
    pub params: Value,
}

impl Default for GeneratorSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: GeneratorMode::Active,
            params: json!({}),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReviewBudget {
    pub per_org_unit_per_day: u32,
}

impl Default for ReviewBudget {
    fn default() -> Self {
        Self {
            per_org_unit_per_day: 20,
        }
    }
}

/// The per-organisation decision configuration. Versioned by content: `config_hash` goes into
/// every recommendation's provenance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct OrgDecisionConfig {
    pub review_budget: ReviewBudget,
    pub min_confidence_for_display: f64,
    /// Keyed by generator id, for example `inventory.replenish.t0`.
    pub generators: BTreeMap<String, GeneratorSettings>,
}

impl Default for OrgDecisionConfig {
    fn default() -> Self {
        Self {
            review_budget: ReviewBudget::default(),
            min_confidence_for_display: 0.30,
            generators: BTreeMap::new(),
        }
    }
}

impl OrgDecisionConfig {
    pub fn settings(&self, generator_id: &str) -> Option<&GeneratorSettings> {
        self.generators.get(generator_id)
    }

    /// A generator with no entry is not enabled. Pack manifests supply defaults upstream of this.
    pub fn enabled(&self, generator_id: &str) -> bool {
        self.settings(generator_id).is_some_and(|s| s.enabled)
    }

    pub fn mode(&self, generator_id: &str) -> GeneratorMode {
        self.settings(generator_id)
            .map(|s| s.mode)
            .unwrap_or_default()
    }

    pub fn params(&self, generator_id: &str) -> Value {
        self.settings(generator_id)
            .map(|s| s.params.clone())
            .unwrap_or_else(|| json!({}))
    }

    /// Stable content hash (domain-generated SHA-256 over canonical JSON, via kuki-types).
    pub fn config_hash(&self) -> String {
        let value = serde_json::to_value(self).expect("config always serialises");
        InputDigestBuilder::new()
            .add("org_decision_config", &value)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_readme() {
        let c = OrgDecisionConfig::default();
        assert_eq!(c.review_budget.per_org_unit_per_day, 20);
        assert_eq!(c.min_confidence_for_display, 0.30);
        assert!(
            !c.enabled("inventory.replenish.t0"),
            "no entry means not enabled"
        );
    }

    #[test]
    fn a_partial_document_fills_in_defaults() {
        let c: OrgDecisionConfig = serde_json::from_value(json!({
            "generators": { "inventory.replenish.t1": { "mode": "shadow", "params": { "service_level_z": 1.65 } } }
        }))
        .unwrap();
        assert_eq!(c.review_budget.per_org_unit_per_day, 20);
        assert!(
            c.enabled("inventory.replenish.t1"),
            "a listed generator defaults to enabled"
        );
        assert_eq!(c.mode("inventory.replenish.t1"), GeneratorMode::Shadow);
        assert_eq!(c.params("inventory.replenish.t1")["service_level_z"], 1.65);
        assert_eq!(c.params("unknown"), json!({}));
    }

    #[test]
    fn hash_is_stable_and_tracks_content() {
        let a = OrgDecisionConfig::default();
        let mut b = a.clone();
        assert_eq!(a.config_hash(), b.config_hash());
        assert_eq!(a.config_hash().len(), 64);
        b.min_confidence_for_display = 0.5;
        assert_ne!(a.config_hash(), b.config_hash());
    }
}
