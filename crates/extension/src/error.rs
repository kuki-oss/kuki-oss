use thiserror::Error;

/// Failures of the service-style extension points (policy, audit, priors, LLM, inference).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExtensionError {
    #[error("{0} is disabled")]
    Disabled(&'static str),
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("internal error: {0}")]
    Internal(String),
}

/// Failures a generator may report. The orchestrator logs the generator id and skips its output.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GenError {
    #[error("required input is missing: {0}")]
    MissingInput(String),
    #[error("invalid input data: {0}")]
    InvalidData(String),
    #[error("inference is required but not configured")]
    InferenceUnavailable,
    #[error(transparent)]
    Extension(#[from] ExtensionError),
    #[error("internal error: {0}")]
    Internal(String),
}
