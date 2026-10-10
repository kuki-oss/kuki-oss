mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

async fn post(app: &axum::Router, token: Option<&str>, body: String) -> (StatusCode, Value) {
    let mut b = Request::builder()
        .method("POST")
        .uri("/v1/graphql")
        .header("content-type", "application/json");
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let resp = app
        .clone()
        .oneshot(b.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn gql(app: &axum::Router, token: Option<&str>, q: &str) -> (StatusCode, Value) {
    post(app, token, json!({ "query": q }).to_string()).await
}

#[sqlx::test(migrations = "../../migrations")]
async fn viewer_works_for_any_authenticated_key(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let org = common::seed_org(&admin, "acme").await;
    let tok = common::seed_key(&admin, org, &["ingest:write"]).await;

    let (s, b) = gql(
        &app,
        Some(&tok),
        "{ viewer { principalId scopes orgUnitScopePath organisation { id slug } } }",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(b["errors"].is_null(), "{b}");
    let v = &b["data"]["viewer"];
    assert!(v["principalId"].as_str().unwrap().starts_with("api_key:"));
    assert_eq!(v["scopes"], json!(["ingest:write"]));
    assert!(v["orgUnitScopePath"].is_null());
    assert_eq!(v["organisation"]["id"], org.to_string());
    assert_eq!(v["organisation"]["slug"], "acme");
}

#[sqlx::test(migrations = "../../migrations")]
async fn organisation_needs_decisions_read(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let org = common::seed_org(&admin, "acme").await;
    let ingest = common::seed_key(&admin, org, &["ingest:write"]).await;
    let reader = common::seed_key(&admin, org, &["decisions:read"]).await;

    let (s, b) = gql(&app, Some(&ingest), "{ organisation { slug } }").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["errors"][0]["extensions"]["code"], "FORBIDDEN");
    assert!(b["data"].is_null());

    let (_, b) = gql(&app, Some(&reader), "{ organisation { slug name } }").await;
    assert!(b["errors"].is_null(), "{b}");
    assert_eq!(b["data"]["organisation"]["slug"], "acme");
}

#[sqlx::test(migrations = "../../migrations")]
async fn keys_only_ever_see_their_own_organisation(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let a = common::seed_org(&admin, "acme").await;
    let _b = common::seed_org(&admin, "globex").await;
    let tok = common::seed_key(&admin, a, &["decisions:read"]).await;

    let (_, b) = gql(
        &app,
        Some(&tok),
        "{ viewer { organisation { slug } } organisation { slug } }",
    )
    .await;
    assert_eq!(b["data"]["viewer"]["organisation"]["slug"], "acme");
    assert_eq!(b["data"]["organisation"]["slug"], "acme");
}

#[sqlx::test(migrations = "../../migrations")]
async fn unauthenticated_is_a_401_problem(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let (s, b) = gql(&app, None, "{ viewer { principalId } }").await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(b["code"], "UNAUTHENTICATED");
}

#[sqlx::test(migrations = "../../migrations")]
async fn malformed_body_is_a_validation_problem(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let org = common::seed_org(&admin, "acme").await;
    let tok = common::seed_key(&admin, org, &["ingest:write"]).await;
    let (s, b) = post(&app, Some(&tok), "{".into()).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(b["code"], "VALIDATION_FAILED");
}

#[sqlx::test(migrations = "../../migrations")]
async fn syntax_errors_carry_a_code(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let org = common::seed_org(&admin, "acme").await;
    let tok = common::seed_key(&admin, org, &["ingest:write"]).await;
    let (s, b) = gql(&app, Some(&tok), "{ viewer ").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
}
