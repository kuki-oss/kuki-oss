//! Autonomous Execution Governor
//!
//! TODO(KUKI-v3): This is just a MINIMAL stub, not the full Trust Ladder specified
//! in Kuki v3 documentation §6. Only Gate 0 (sandbox short-circuit) is built. The
//! four real gates (opt-in, confidence threshold, trust accrual, reversibility
//! ceiling) depend on kuki_action_types / WriteBackHandler / the
//! auto_execution_enabled_types + trust_threshold_count columns on
//! merchant_kuki_config, none of which exist. Until they exist, this gate always
//! returns Pending for every merchant, sandbox or not, so nothing auto-executes
//! today regardless of confidence or trust. When v3's registries are built, replace
//! this stub with the real Gate 1→2→3→4 sequence from Kuki v3's §6.3, keeping Gate 0
//! first and unconditional.

use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use crate::types::{KukiRecommendation, RecommendationStatus};

pub struct AutoExecutionGate;

impl AutoExecutionGate {
    /// Called after a generator emits a KukiRecommendation, before it is
    /// written to kuki_recommendations. Returns the status the row should
    /// be persisted with.
    pub async fn evaluate(
        &self,
        rec: &KukiRecommendation,
        merchant_kind: &str,
    ) -> RecommendationStatus {
        // GATE 0: sandbox-short-circuit (categorical, not configurable)
        // A sandbox merchant must NEVER reach AutoExecuted, regardless of
        // confidence or trust values. THIS IS CHECKED BEFORE ANYTHING ELSE
        if merchant_kind == "sandbox" {
            return RecommendationStatus::Pending;
        }

        // TODO(KUKI-v3): full Trust Ladder not yet implemented (see TODO above)
        let _ = rec;
        RecommendationStatus::Pending
    }
}

/// Fetches a merchant's `kind` column. Small, self-contained query so this
/// module doesn't require MerchantContext(generator.rs) to carry `kind` yet
pub async fn fetch_merchant_kind(pool: &PgPool, merchant_id: Uuid) -> Result<String> {
    let kind: (String,) = sqlx::query_as("SELECT kind FROM merchants WHERE id = $1")
        .bind(merchant_id)
        .fetch_one(pool)
        .await
        .context("Failed to fetch merchant kind")?;
    Ok(kind.0)
}
