use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::error::ExtensionError;

#[derive(Clone, Debug, PartialEq)]
pub struct PriorRequest {
    pub organisation_id: Uuid,
    /// Example `lead_time_days`.
    pub signal: String,
    /// Coarse segment descriptor. Never an identifier of a single entity
    pub segment: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Prior {
    pub mean: f64,
    /// Weight in pseudo-observations for partial pooling with the organisation's own data.
    pub strength: f64,
    pub contributors: u32,
}

#[async_trait]
pub trait PriorProvider: Send + Sync {
    /// True when priors come from the Network Plane.
    fn is_networked(&self) -> bool {
        false
    }
    /// `None` means no prior is available (including below `k_min`): use local defaults.
    async fn prior(&self, request: &PriorRequest) -> Result<Option<Prior>, ExtensionError>;
}

/// Kuki OSS default: local defaults only, no network.
pub struct NoPriors;

#[async_trait]
impl PriorProvider for NoPriors {
    async fn prior(&self, _request: &PriorRequest) -> Result<Option<Prior>, ExtensionError> {
        Ok(None)
    }
}
