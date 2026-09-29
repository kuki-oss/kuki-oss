use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Not found")]
    NotFound,
    #[error("Internal server error: {0}")]
    Database(sqlx::Error),
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Internal server error")]
    InternalServerError,
    #[error("Unauthorized")]
    Unauthorized,
}

// Replaces derive-generated `#[from]` conversion. Runs the moment
// a raw sqlx::Error becomes an AppError, i.e, every `?` on a sqlx
// call anywhere in the query modules. Logging here means the real
// error is captured centrally, without touching any individual
// resolver or query function
impl From<sqlx::Error> for AppError {
  fn from(e: sqlx::Error) -> Self {
    tracing::error!(error = ?e, "database error");
    AppError::Database(e)
  }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, self.to_string()),
            AppError::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            AppError::Database(_) | AppError::InternalServerError => {
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
            }
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}

/// SurrealDB-specific error type. Implements std::error::Error via
/// thiserror, so anyhow::Context works directly on any
/// Result<_, SurrealError> without a `.map_err(anyhow::anyhow!(e))`
/// wrapper at call sites
#[derive(Debug, Error)]
pub enum SurrealError {
    #[error("SurrealDB connection failed: {0}")]
    Connect(String),
    #[error("SurrealDB authentication failed: {0}")]
    Auth(String),
    #[error("SurrealDB namespace/database selection failed: {0}")]
    UseNsDb(String),
    #[error("SurrealDB query failed: {0}")]
    Query(String),
    #[error("SurrealDB response did not match expected shape: {0}")]
    Decode(String),
}
