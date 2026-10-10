//! Optional cognition endpoint. It may map, extract, explain and translate.
//! It never computes a quantity, threshold, ranking or confidence (invariant I2).
use async_trait::async_trait;
use serde_json::Value;

use crate::error::ExtensionError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LlmTask {
    MapColumns,
    ExtractDocument,
    Explain,
    TranslateQuery,
}

/// How sensitive the input is. The provider config caps what each endpoint may receive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum DataClass {
    Public,
    Internal,
    Confidential,
    Restricted,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LlmRequest {
    pub task: LlmTask,
    pub data_class: DataClass,
    pub system: String,
    pub input: String,
    /// When set, the output must conform to this JSON schema.
    pub json_schema: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LlmResponse {
    pub text: String,
    pub model: String,
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    fn enabled(&self) -> bool;
    async fn complete(&self, request: &LlmRequest) -> Result<LlmResponse, ExtensionError>;
}

/// The default: cognition is off.
pub struct DisabledLlm;

#[async_trait]
impl LlmProvider for DisabledLlm {
    fn enabled(&self) -> bool {
        false
    }
    async fn complete(&self, _request: &LlmRequest) -> Result<LlmResponse, ExtensionError> {
        Err(ExtensionError::Disabled("llm"))
    }
}
