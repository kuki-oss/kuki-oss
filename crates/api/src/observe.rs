//! Request ids and one structured log line per request.
//!
//! Every response carries `X-Request-Id`: the caller's, when it is 1-128 visible ASCII
//! characters, otherwise a fresh UUID. Handlers and anything they log run inside a span that
//! carries the id, so a client's id leads straight to the server's log lines. The route
//! template is logged, never the raw path or query, which can hold identifiers and cursors.

use std::time::Instant;

use axum::{
    extract::{MatchedPath, Request},
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use tracing::Instrument;
use uuid::Uuid;

const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// In the request extensions, for handlers that want it.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

fn valid_request_id(s: &str) -> bool {
    (1..=128).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_graphic())
}

pub async fn request_context(mut req: Request, next: Next) -> Response {
    let id = req
        .headers()
        .get(REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|s| valid_request_id(s))
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    req.extensions_mut().insert(RequestId(id.clone()));

    let method = req.method().clone();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".into());
    let span = tracing::info_span!("request", request_id = %id, %method, %route);

    let start = Instant::now();
    let mut res = next.run(req).instrument(span.clone()).await;
    let status = res.status().as_u16();
    let latency_ms = start.elapsed().as_millis() as u64;

    span.in_scope(|| match (status, route.starts_with("/v1/health/")) {
        (500.., _) => tracing::error!(status, latency_ms, "request failed"),
        (_, true) => tracing::debug!(status, latency_ms, "request"),
        _ => tracing::info!(status, latency_ms, "request"),
    });

    if let Ok(v) = HeaderValue::from_str(&id) {
        res.headers_mut().insert(REQUEST_ID, v);
    }
    res
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        extract::Extension,
        http::{Request, StatusCode},
        routing::get,
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    fn app() -> Router {
        Router::new()
            .route(
                "/x",
                get(|Extension(id): Extension<RequestId>| async move { id.0 }),
            )
            .layer(axum::middleware::from_fn(request_context))
    }

    /// Returns (response header, id the handler saw).
    async fn call(id: Option<&str>) -> (String, String) {
        let mut req = Request::builder().uri("/x");
        if let Some(id) = id {
            req = req.header("x-request-id", id);
        }
        let res = app()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let header = res.headers()["x-request-id"].to_str().unwrap().to_owned();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        (header, String::from_utf8(body.to_vec()).unwrap())
    }

    #[test]
    fn request_id_rules() {
        assert!(valid_request_id("a"));
        assert!(valid_request_id(&"x".repeat(128)));
        assert!(!valid_request_id(""));
        assert!(!valid_request_id(&"x".repeat(129)));
        assert!(!valid_request_id("has space"));
        assert!(!valid_request_id("tab\there"));
    }

    #[tokio::test]
    async fn an_id_is_generated_when_none_is_sent() {
        let (header, seen) = call(None).await;
        assert!(Uuid::parse_str(&header).is_ok());
        assert_eq!(header, seen);
    }

    #[tokio::test]
    async fn a_good_id_is_echoed_and_visible_to_handlers() {
        let (header, seen) = call(Some("req-abc.123")).await;
        assert_eq!(
            (header.as_str(), seen.as_str()),
            ("req-abc.123", "req-abc.123")
        );
    }

    #[tokio::test]
    async fn a_bad_id_is_replaced() {
        for bad in ["has space", &"x".repeat(129), ""] {
            let (header, seen) = call(Some(bad)).await;
            assert!(Uuid::parse_str(&header).is_ok(), "{bad:?}");
            assert_eq!(header, seen);
        }
    }
}
