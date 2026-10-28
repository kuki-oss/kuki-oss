//! HTTP hardening: CORS, a request timeout and response headers.
//!
//! - CORS is off unless `KUKI_CORS_ORIGINS` lists exact origins. There is no wildcard and no
//!   credentials: authentication is a bearer token, never a cookie.
//! - `KUKI_REQUEST_TIMEOUT_SECONDS` (default 30, 0 disables) bounds a request. The WebSocket
//!   upgrade is exempt so subscriptions are not cut off. A request that times out is dropped;
//!   ingest commits per chunk and resumes under the same idempotency key, so a retry is safe.
//! - Every response says `nosniff` and `no-store`: all of it is per-tenant data.
//!
//! The timeout is a middleware that reads `RequestTimeout` from the request extensions, as the
//! rate limiter does, so `routes::router` stays free of configuration and tests run unlimited.

use std::time::Duration;

use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue, Method, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::{config::ApiConfig, error::ApiError};

pub const DEFAULT_TIMEOUT_SECS: u64 = 30;
const MAX_TIMEOUT_SECS: u64 = 3600;
const MAX_ORIGINS: usize = 50;

const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const RATELIMIT_LIMIT: HeaderName = HeaderName::from_static("ratelimit-limit");
const RATELIMIT_REMAINING: HeaderName = HeaderName::from_static("ratelimit-remaining");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardeningConfig {
    /// Lower-cased exact origins, e.g. `https://app.example.com`. Empty: CORS is off.
    pub cors_origins: Vec<String>,
    /// `None`: no limit.
    pub request_timeout: Option<Duration>,
}

/// An origin is `http(s)://host[:port]`: no path, query, userinfo or wildcard.
fn valid_origin(s: &str) -> bool {
    let Some(host) = s
        .strip_prefix("https://")
        .or_else(|| s.strip_prefix("http://"))
    else {
        return false;
    };
    s.len() <= 253
        && host.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '[')
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']'))
}

impl HardeningConfig {
    pub fn parse(origins: Option<&str>, timeout: Option<&str>) -> Result<Self, String> {
        let mut cors_origins: Vec<String> = Vec::new();
        for raw in origins
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let origin = raw.to_ascii_lowercase();
            if !valid_origin(&origin) {
                return Err(format!(
                    "KUKI_CORS_ORIGINS: `{raw}` is not an origin such as https://app.example.com \
                     (scheme, host and optional port; no path and no wildcard)"
                ));
            }
            if !cors_origins.contains(&origin) {
                cors_origins.push(origin);
            }
        }
        if cors_origins.len() > MAX_ORIGINS {
            return Err(format!(
                "KUKI_CORS_ORIGINS lists more than {MAX_ORIGINS} origins"
            ));
        }

        let secs = match timeout {
            None => DEFAULT_TIMEOUT_SECS,
            Some(s) => s
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|n| *n <= MAX_TIMEOUT_SECS)
                .ok_or(
                    "KUKI_REQUEST_TIMEOUT_SECONDS must be a whole number from 0 to 3600 \
                     (0 disables the limit)",
                )?,
        };
        Ok(Self {
            cors_origins,
            request_timeout: (secs > 0).then(|| Duration::from_secs(secs)),
        })
    }

    pub fn from_env() -> Result<Self, String> {
        let origins = ApiConfig::read_setting("KUKI_CORS_ORIGINS").map_err(|e| e.to_string())?;
        let timeout =
            ApiConfig::read_setting("KUKI_REQUEST_TIMEOUT_SECONDS").map_err(|e| e.to_string())?;
        Self::parse(origins.as_deref(), timeout.as_deref())
    }

    /// `None` when no origin is allowed: then no CORS header is ever sent.
    pub fn cors_layer(&self) -> Option<CorsLayer> {
        if self.cors_origins.is_empty() {
            return None;
        }
        let origins: Vec<HeaderValue> = self
            .cors_origins
            .iter()
            .filter_map(|o| HeaderValue::from_str(o).ok())
            .collect();
        Some(
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(origins))
                .allow_methods([Method::GET, Method::POST])
                .allow_headers([
                    header::AUTHORIZATION,
                    header::CONTENT_TYPE,
                    IDEMPOTENCY_KEY,
                    REQUEST_ID,
                ])
                .expose_headers([
                    RATELIMIT_LIMIT,
                    RATELIMIT_REMAINING,
                    header::RETRY_AFTER,
                    REQUEST_ID,
                ])
                .max_age(Duration::from_secs(600)),
        )
    }
}

/// The request time limit, put in the request extensions by `serve`.
#[derive(Clone, Copy, Debug)]
pub struct RequestTimeout(pub Duration);

pub async fn timeout(req: Request, next: Next) -> Response {
    let limit = req.extensions().get::<RequestTimeout>().map(|t| t.0);
    let upgrade = req.headers().contains_key(header::UPGRADE);
    match limit {
        Some(limit) if !upgrade => match tokio::time::timeout(limit, next.run(req)).await {
            Ok(res) => res,
            Err(_) => ApiError::timeout(limit.as_secs().max(1)).into_response(),
        },
        _ => next.run(req).await,
    }
}

pub async fn secure_headers(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    let h = res.headers_mut();
    h.entry(header::X_CONTENT_TYPE_OPTIONS)
        .or_insert(HeaderValue::from_static("nosniff"));
    h.entry(header::CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-store"));
    res
}

#[cfg(test)]
mod tests {
    use axum::{Extension, Router, body::Body, http::Request, middleware::from_fn, routing::get};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    fn cfg(origins: &str) -> HardeningConfig {
        HardeningConfig::parse(Some(origins), None).unwrap()
    }

    async fn call(app: Router, req: Request<Body>) -> Response {
        app.oneshot(req).await.unwrap()
    }

    fn plain(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn with_origin(origin: &str) -> Request<Body> {
        Request::builder()
            .uri("/x")
            .header("origin", origin)
            .body(Body::empty())
            .unwrap()
    }

    fn app() -> Router {
        Router::new().route("/x", get(|| async { "ok" }))
    }

    #[test]
    fn origins_are_parsed_strictly() {
        let c = cfg(" https://App.example.com , http://localhost:3000,https://app.example.com,");
        assert_eq!(
            c.cors_origins,
            ["https://app.example.com", "http://localhost:3000"]
        );
        assert!(cfg("").cors_origins.is_empty());
        assert!(
            HardeningConfig::parse(None, None)
                .unwrap()
                .cors_layer()
                .is_none()
        );
        assert!(c.cors_layer().is_some());

        for bad in [
            "*",
            "null",
            "https://*.example.com",
            "https://a.com/",
            "https://a.com/path",
            "https://a.com?x=1",
            "https://user@a.com",
            "ftp://a.com",
            "http://",
            "a.com",
        ] {
            assert!(HardeningConfig::parse(Some(bad), None).is_err(), "{bad}");
        }
        let many: Vec<String> = (0..=MAX_ORIGINS)
            .map(|i| format!("https://h{i}.example"))
            .collect();
        assert!(HardeningConfig::parse(Some(&many.join(",")), None).is_err());
    }

    #[test]
    fn the_timeout_setting_defaults_disables_and_rejects_nonsense() {
        let secs = |t: Option<&str>| {
            HardeningConfig::parse(None, t)
                .unwrap()
                .request_timeout
                .map(|d| d.as_secs())
        };
        assert_eq!(secs(None), Some(DEFAULT_TIMEOUT_SECS));
        assert_eq!(secs(Some("0")), None);
        assert_eq!(secs(Some(" 5 ")), Some(5));
        for bad in ["x", "-1", "3601", "1.5", ""] {
            assert!(HardeningConfig::parse(None, Some(bad)).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn an_allowed_origin_is_echoed_and_others_get_nothing() {
        let app = app().layer(cfg("https://app.example.com").cors_layer().unwrap());
        let ok = call(app.clone(), with_origin("https://app.example.com")).await;
        assert_eq!(
            ok.headers()["access-control-allow-origin"],
            "https://app.example.com"
        );
        assert!(
            !ok.headers()
                .contains_key("access-control-allow-credentials")
        );

        let other = call(app.clone(), with_origin("https://evil.example")).await;
        assert_eq!(
            other.status(),
            200,
            "the request is served; the browser refuses it"
        );
        assert!(!other.headers().contains_key("access-control-allow-origin"));

        let none = call(app, plain("/x")).await;
        assert!(!none.headers().contains_key("access-control-allow-origin"));
    }

    #[tokio::test]
    async fn a_preflight_lists_the_methods_and_headers_clients_need() {
        let app = app().layer(cfg("https://app.example.com").cors_layer().unwrap());
        let req = Request::builder()
            .method("OPTIONS")
            .uri("/x")
            .header("origin", "https://app.example.com")
            .header("access-control-request-method", "POST")
            .header(
                "access-control-request-headers",
                "authorization, idempotency-key, content-type",
            )
            .body(Body::empty())
            .unwrap();
        let res = call(app, req).await;
        assert!(res.status().is_success(), "{}", res.status());
        let h = res.headers();
        let text = |name: &str| h[name].to_str().unwrap().to_ascii_lowercase();
        assert!(text("access-control-allow-headers").contains("authorization"));
        assert!(text("access-control-allow-headers").contains("idempotency-key"));
        assert!(text("access-control-allow-methods").contains("post"));
        assert_eq!(h["access-control-max-age"], "600");
    }

    fn slow(ms: u64) -> Router {
        Router::new()
            .route(
                "/x",
                get(move || async move {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    "ok"
                }),
            )
            .layer(from_fn(timeout))
    }

    fn limited(app: Router, ms: u64) -> Router {
        app.layer(Extension(RequestTimeout(Duration::from_millis(ms))))
    }

    #[tokio::test]
    async fn a_slow_request_gets_a_504_problem() {
        let res = call(limited(slow(2000), 50), plain("/x")).await;
        assert_eq!(res.status(), 504);
        assert_eq!(res.headers()["content-type"], "application/problem+json");
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["code"], "INTERNAL");
        assert_eq!(v["status"], 504);

        let fast = call(limited(slow(1), 2000), plain("/x")).await;
        assert_eq!(fast.status(), 200);
    }

    #[tokio::test]
    async fn upgrades_and_unconfigured_servers_are_not_timed_out() {
        let upgrade = Request::builder()
            .uri("/x")
            .header("upgrade", "websocket")
            .body(Body::empty())
            .unwrap();
        assert_eq!(call(limited(slow(150), 50), upgrade).await.status(), 200);
        assert_eq!(call(slow(150), plain("/x")).await.status(), 200);
    }

    #[tokio::test]
    async fn responses_carry_nosniff_and_no_store_unless_set() {
        let app = Router::new()
            .route("/plain", get(|| async { "ok" }))
            .route(
                "/cached",
                get(|| async { ([(header::CACHE_CONTROL, "max-age=5")], "ok") }),
            )
            .layer(from_fn(secure_headers));
        let plain_res = call(app.clone(), plain("/plain")).await;
        assert_eq!(plain_res.headers()["x-content-type-options"], "nosniff");
        assert_eq!(plain_res.headers()["cache-control"], "no-store");
        let cached = call(app, plain("/cached")).await;
        assert_eq!(cached.headers()["cache-control"], "max-age=5");
        assert_eq!(cached.headers()["x-content-type-options"], "nosniff");
    }
}
