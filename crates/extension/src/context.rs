use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use kuki_types::{DedupKey, InputDigestBuilder};
use serde_json::json;
use uuid::Uuid;

use crate::{config::OrgDecisionConfig, inference::InferenceClient, view::OntologyView};

/// What caused an evaluation: an outbox topic with the entities it touched, or a schedule tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trigger {
    pub organisation_id: Uuid,
    pub topic: String,
    pub entities: Vec<Uuid>,
}

/// Which entities a generator wants re-evaluated for a trigger. Keeps evaluation incremental.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScopeSpec {
    All,
    Entities(Vec<Uuid>),
}

/// An existing pending recommendation, so a generator can see what dedup will do with its output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRef {
    pub recommendation_id: Uuid,
    pub dedup_key: DedupKey,
    pub version: i32,
}

/// Everything a generator may read, for one generator at one clock. Generators MUST NOT read the
/// clock, query stores, or use unseeded randomness (spec 7.1)
pub struct DecisionContext {
    pub organisation_id: Uuid,
    /// The evaluation clock. Never `Utc::now()` inside a generator.
    pub as_of: DateTime<Utc>,
    /// Derived from (organisation, as_of, generator id). See `derive_seed`.
    pub seed: u64,
    pub config: Arc<OrgDecisionConfig>,
    pub view: Arc<dyn OntologyView>,
    pub pending: Vec<PendingRef>,
    /// T3 and above only. `None` in Kuki OSS.
    pub inference: Option<Arc<dyn InferenceClient>>,
}

impl DecisionContext {
    pub fn new(
        organisation_id: Uuid,
        as_of: DateTime<Utc>,
        generator_id: &str,
        config: Arc<OrgDecisionConfig>,
        view: Arc<dyn OntologyView>,
    ) -> Self {
        Self {
            organisation_id,
            as_of,
            seed: derive_seed(organisation_id, as_of, generator_id),
            config,
            view,
            pending: vec![],
            inference: None,
        }
    }

    pub fn with_pending(mut self, pending: Vec<PendingRef>) -> Self {
        self.pending = pending;
        self
    }

    pub fn with_inference(mut self, inference: Option<Arc<dyn InferenceClient>>) -> Self {
        self.inference = inference;
        self
    }
}

/// First 8 bytes of the framed digest of (organisation, as_of, generator id).
pub fn derive_seed(organisation_id: Uuid, as_of: DateTime<Utc>, generator_id: &str) -> u64 {
    let hex = InputDigestBuilder::new()
        .add("organisation_id", &json!(organisation_id.to_string()))
        .add(
            "as_of",
            &json!(as_of.to_rfc3339_opts(SecondsFormat::Nanos, true)),
        )
        .add("generator_id", &json!(generator_id))
        .finish();
    u64::from_str_radix(&hex[..16], 16).expect("a hex digest parses")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_seed_is_deterministic_and_depends_on_every_input() {
        let (org, t) = (Uuid::nil(), Utc::now());
        let base = derive_seed(org, t, "inventory.replenish.t0");
        assert_eq!(base, derive_seed(org, t, "inventory.replenish.t0"));
        assert_ne!(base, derive_seed(org, t, "inventory.replenish.t1"));
        assert_ne!(
            base,
            derive_seed(
                org,
                t + chrono::Duration::seconds(1),
                "inventory.replenish.t0"
            )
        );
        assert_ne!(
            base,
            derive_seed(Uuid::from_u128(1), t, "inventory.replenish.t0")
        );
    }
}
