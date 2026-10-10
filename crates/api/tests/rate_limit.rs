mod common;

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use common::*;
use http_body_util::BodyExt;
use kuki_api::ratelimit::RateLimiter;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

/// A slow-refilling limiter, so a test sees a fixed budget.
fn limited(pool: PgPool, burst: u32) -> Router {
    app(pool).layer(axum::Extension(Arc::new(
        RateLimiter::new(burst, 0.001).unwrap(),
    )))
}

async fn get(app: &Router, uri: &str, token: Option<&str>) -> (StatusCode, HeaderMap, Value) {
    let mut b = Request::builder().uri(uri);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let res = app
        .clone()
        .oneshot(b.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (status, headers) = (res.status(), res.headers().clone());
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        headers,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn viewer(app: &Router, token: Option<&str>) -> (StatusCode, HeaderMap, Value) {
    let mut b = Request::builder()
        .method("POST")
        .uri("/v1/graphql")
        .header("content-type", "application/json");
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let body = Body::from(r#"{"query":"{ viewer { principalId } }"}"#);
    let res = app.clone().oneshot(b.body(body).unwrap()).await.unwrap();
    let (status, headers) = (res.status(), res.headers().clone());
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        headers,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn header<'a>(h: &'a HeaderMap, name: &str) -> &'a str {
    h[name].to_str().unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn each_key_has_its_own_budget_and_reports_it(admin: PgPool) {
    let org = seed_org(&admin, "rl-keys").await;
    let a = seed_key(&admin, org, &["ingest:read"]).await;
    let b = seed_key(&admin, org, &["ingest:read"]).await;
    let app = limited(app_pool(&admin).await, 2);

    let (s, h, _) = viewer(&app, Some(&a)).await;
    assert_eq!(
        (
            s,
            header(&h, "ratelimit-limit"),
            header(&h, "ratelimit-remaining")
        ),
        (StatusCode::OK, "2", "1")
    );
    let (s, h, _) = viewer(&app, Some(&a)).await;
    assert_eq!(
        (s, header(&h, "ratelimit-remaining")),
        (StatusCode::OK, "0")
    );

    let (s, h, body) = viewer(&app, Some(&a)).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["code"], "RATE_LIMITED");
    let retry: u64 = header(&h, "retry-after").parse().unwrap();
    assert!(retry >= 1);
    assert_eq!(body["retryAfterSeconds"], retry);
    assert_eq!(header(&h, "ratelimit-remaining"), "0");

    // Another key of the same organisation is unaffected.
    let (s, h, _) = viewer(&app, Some(&b)).await;
    assert_eq!(
        (s, header(&h, "ratelimit-remaining")),
        (StatusCode::OK, "1")
    );

    // Health checks need no key and are never counted.
    let (s, h, _) = get(&app, "/v1/health/live", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(h.get("ratelimit-limit").is_none());
}

#[sqlx::test(migrations = "../../migrations")]
async fn requests_without_valid_credentials_are_not_counted(admin: PgPool) {
    let org = seed_org(&admin, "rl-anon").await;
    let token = seed_key(&admin, org, &["ingest:read"]).await;
    let app = limited(app_pool(&admin).await, 1);

    for _ in 0..5 {
        let (s, h, _) = viewer(&app, Some("kuki_test_nope")).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert!(h.get("ratelimit-limit").is_none());
    }
    let (s, _, _) = viewer(&app, None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);

    let (s, _, _) = viewer(&app, Some(&token)).await;
    assert_eq!(
        s,
        StatusCode::OK,
        "the key's budget was untouched by the failures"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_limited_ingest_request_writes_nothing_and_can_be_retried(admin: PgPool) {
    let (org, item, token) = tenant(&admin, "rl-ingest").await;
    let app = limited(app_pool(&admin).await, 1);

    let (s, h, _) = send(&app, json_req(&token, "k1", vec![event(item, json!(1))])).await;
    assert_eq!(
        (s, header(&h, "ratelimit-remaining")),
        (StatusCode::ACCEPTED, "0")
    );

    let (s, _, body) = send(&app, json_req(&token, "k2", vec![event(item, json!(2))])).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["code"], "RATE_LIMITED");
    assert_eq!(count(&admin, "events", org).await, 1);
    assert_eq!(
        count(&admin, "idempotency_keys", org).await,
        1,
        "the refused request left no trace"
    );
}
