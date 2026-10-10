use async_trait::async_trait;
use serde_json::Value;

use crate::error::ExtensionError;

#[derive(Clone, Debug, PartialEq)]
pub struct InferenceRequest {
    /// Registry name of the model, example, `inventory.demand.lgbm`.
    pub model: String,
    pub features: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InferenceResponse {
    pub output: Value,
    /// Content hash of the model artefact that produced the output. Goes into provenance.
    pub model_artefact: String,
    /// Per-feature attributions (SHAP) when the model provides them.
    pub explanation: Option<Value>,
}

/// Core's client to the inference service. Every call is logged with the artefact hash.
#[async_trait]
pub trait InferenceClient: Send + Sync {
    async fn predict(
        &self,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, ExtensionError>;
}
