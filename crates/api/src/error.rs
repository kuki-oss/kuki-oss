use std::borrow::Cow;

use async_graphql::ErrorExtensions;
use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// CLOSED registry (spec 3.3). Identical to GraphQL `ErrorCode` and OpenAPI `ProblemCode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    Unauthenticated,
    Forbidden,
    ValidationFailed,
    RateLimited,
    Conflict,
    NotFound,
    Internal,
}

impl ErrorCode {
    pub const ALL: [ErrorCode; 7] = [
        Self::Unauthenticated,
        Self::Forbidden,
        Self::ValidationFailed,
        Self::RateLimited,
        Self::Conflict,
        Self::NotFound,
        Self::Internal,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::Forbidden => "FORBIDDEN",
            Self::ValidationFailed => "VALIDATION_FAILED",
            Self::RateLimited => "RATE_LIMITED",
            Self::Conflict => "CONFLICT",
            Self::NotFound => "NOT_FOUND",
            Self::Internal => "INTERNAL",
        }
    }
}

/// OPEN enum: clients must tolerate unknown values. Identical to GraphQL `ErrorReason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum ErrorReason {
    CostLimitExceeded,
    DepthLimitExceeded,
    PageSizeExceeded,
    PersistedQueryRequired,
    StaleVersion,
    InvalidStateTransition,
}

impl ErrorReason {
    pub const ALL: [ErrorReason; 6] = [
        Self::CostLimitExceeded,
        Self::DepthLimitExceeded,
        Self::PageSizeExceeded,
        Self::PersistedQueryRequired,
        Self::StaleVersion,
        Self::InvalidStateTransition,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CostLimitExceeded => "COST_LIMIT_EXCEEDED",
            Self::DepthLimitExceeded => "DEPTH_LIMIT_EXCEEDED",
            Self::PageSizeExceeded => "PAGE_SIZE_EXCEEDED",
            Self::PersistedQueryRequired => "PERSISTED_QUERY_REQUIRED",
            Self::StaleVersion => "STALE_VERSION",
            Self::InvalidStateTransition => "INVALID_STATE_TRANSITION",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct FieldError {
    pub pointer: String,
    pub message: String,
}

#[derive(Debug)]
pub struct ApiError {
    code: ErrorCode,
    status: StatusCode,
    reason: Option<ErrorReason>,
    retry_after_secs: Option<u64>,
    field_errors: Vec<FieldError>,
    detail: Cow<'static, str>,
    /// Logged, never rendered.
    source: Option<String>,
}

impl ApiError {
    fn new(code: ErrorCode, status: StatusCode, detail: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code,
            status,
            reason: None,
            retry_after_secs: None,
            field_errors: vec![],
            detail: detail.into(),
            source: None,
        }
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }
    pub fn status(&self) -> StatusCode {
        self.status
    }
    pub fn reason(&self) -> Option<ErrorReason> {
        self.reason
    }

    /// One message for every authentication failure.
    pub fn unauthenticated() -> Self {
        Self::new(
            ErrorCode::Unauthenticated,
            StatusCode::UNAUTHORIZED,
            "Missing or invalid credentials.",
        )
    }
    pub fn forbidden(scope: &str) -> Self {
        Self::new(
            ErrorCode::Forbidden,
            StatusCode::FORBIDDEN,
            format!("Missing required scope `{scope}`."),
        )
    }
    pub fn validation(detail: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorCode::ValidationFailed, StatusCode::BAD_REQUEST, detail)
    }
    /// VALIDATION_FAILED with HTTP 413 (the registry is closed, so there is no extra code).
    pub fn payload_too_large(limit_bytes: usize) -> Self {
        Self::new(
            ErrorCode::ValidationFailed,
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("Request body exceeds {limit_bytes} bytes."),
        )
    }
    pub fn rate_limited(retry_after_secs: u64) -> Self {
        let mut e = Self::new(
            ErrorCode::RateLimited,
            StatusCode::TOO_MANY_REQUESTS,
            "Rate limit exceeded.",
        );
        e.retry_after_secs = Some(retry_after_secs);
        e
    }
    pub fn conflict(reason: Option<ErrorReason>, detail: impl Into<Cow<'static, str>>) -> Self {
        let mut e = Self::new(ErrorCode::Conflict, StatusCode::CONFLICT, detail);
        e.reason = reason;
        e
    }
    pub fn not_found(what: &str) -> Self {
        Self::new(
            ErrorCode::NotFound,
            StatusCode::NOT_FOUND,
            format!("{what} not found."),
        )
    }
    /// INTERNAL with HTTP 503: readiness failures and dependency outages.
    pub fn unavailable(detail: &'static str) -> Self {
        Self::new(ErrorCode::Internal, StatusCode::SERVICE_UNAVAILABLE, detail)
    }
    /// INTERNAL with HTTP 504: the request outlived the server's time limit.
    pub fn timeout(limit_secs: u64) -> Self {
        Self::new(
            ErrorCode::Internal,
            StatusCode::GATEWAY_TIMEOUT,
            format!(
                "The request exceeded the {limit_secs} second time limit. Retry it; \
                 ingest calls resume under the same Idempotency-Key."
            ),
        )
    }
    pub fn internal(cause: impl std::fmt::Display) -> Self {
        let mut e = Self::new(
            ErrorCode::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
            "An internal error occurred.",
        );
        e.source = Some(cause.to_string());
        e
    }
    pub fn with_reason(mut self, r: ErrorReason) -> Self {
        self.reason = Some(r);
        self
    }
    pub fn with_field_errors(mut self, v: Vec<FieldError>) -> Self {
        self.field_errors = v;
        self
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        Self::internal(e)
    }
}

fn no_errors(v: &&[FieldError]) -> bool {
    v.is_empty()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Problem<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    title: &'static str,
    status: u16,
    detail: &'a str,
    code: ErrorCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<ErrorReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_seconds: Option<u64>,
    #[serde(skip_serializing_if = "no_errors")]
    errors: &'a [FieldError],
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Some(cause) = &self.source {
            tracing::error!(%cause, code = self.code.as_str(), "request failed");
        }
        let body = Problem {
            kind: "about:blank",
            title: self.status.canonical_reason().unwrap_or("Error"),
            status: self.status.as_u16(),
            detail: self.detail.as_ref(),
            code: self.code,
            reason: self.reason,
            retry_after_seconds: self.retry_after_secs,
            errors: &self.field_errors,
        };
        let mut res = (self.status, Json(&body)).into_response();
        let h = res.headers_mut();
        h.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        if let Some(s) = self.retry_after_secs {
            h.insert(header::RETRY_AFTER, HeaderValue::from(s));
        }
        if self.code == ErrorCode::Unauthenticated {
            h.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        res
    }
}

impl From<ApiError> for async_graphql::Error {
    fn from(e: ApiError) -> Self {
        if let Some(src) = &e.source {
            // The cause stays server-side; clients only see the generic detail.
            tracing::error!(error = ?src, "graphql resolver failed");
        }
        async_graphql::Error::new(e.detail.to_string()).extend_with(|_, ext| {
            ext.set("code", e.code.as_str());
            if let Some(r) = e.reason {
                ext.set("reason", r.as_str());
            }
            if let Some(s) = e.retry_after_secs {
                ext.set("retryAfterSeconds", s);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const SDL: &str = include_str!("../../../api/schema/kuki.graphql");

    fn sdl_enum(name: &str) -> Vec<String> {
        let start = SDL
            .find(&format!("enum {name} {{"))
            .unwrap_or_else(|| panic!("enum {name} missing from SDL"));
        let body = &SDL[start..];
        let body = &body[body.find('{').unwrap() + 1..];
        let body = &body[..body.find('}').unwrap()];
        let mut v: Vec<String> = body
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('"') && !l.starts_with('#'))
            .map(String::from)
            .collect();
        v.sort();
        v
    }

    #[test]
    fn registries_match_the_contract() {
        let mut code: Vec<String> = ErrorCode::ALL.iter().map(|c| c.as_str().into()).collect();
        code.sort();
        assert_eq!(code, sdl_enum("ErrorCode"));
        let mut reason: Vec<String> = ErrorReason::ALL.iter().map(|c| c.as_str().into()).collect();
        reason.sort();
        assert_eq!(reason, sdl_enum("ErrorReason"));
    }

    #[test]
    fn serde_names_match_as_str() {
        for c in ErrorCode::ALL {
            assert_eq!(serde_json::to_value(c).unwrap(), c.as_str());
        }
        for r in ErrorReason::ALL {
            assert_eq!(serde_json::to_value(r).unwrap(), r.as_str());
        }
    }

    async fn body(res: Response) -> serde_json::Value {
        serde_json::from_slice(&axum::body::to_bytes(res.into_body(), 4096).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn problem_details_follow_the_contract() {
        let res = ApiError::rate_limited(7).into_response();
        assert_eq!(res.status(), 429);
        assert_eq!(res.headers()[header::RETRY_AFTER], "7");
        assert_eq!(
            res.headers()[header::CONTENT_TYPE],
            "application/problem+json"
        );
        let v = body(res).await;
        assert_eq!(
            (
                v["code"].as_str(),
                v["retryAfterSeconds"].as_u64(),
                v["type"].as_str()
            ),
            (Some("RATE_LIMITED"), Some(7), Some("about:blank"))
        );

        assert_eq!(ApiError::validation("x").status(), 400);
        assert_eq!(ApiError::payload_too_large(10).status(), 413);
        assert_eq!(
            ApiError::payload_too_large(10).code(),
            ErrorCode::ValidationFailed
        );
        let v =
            body(ApiError::conflict(Some(ErrorReason::StaleVersion), "x").into_response()).await;
        assert_eq!(v["reason"], "STALE_VERSION");
    }

    #[tokio::test]
    async fn internal_errors_never_leak_their_cause() {
        let res = ApiError::internal("password=hunter2 refused").into_response();
        assert_eq!(res.status(), 500);
        assert!(!body(res).await.to_string().contains("hunter2"));
    }
}
