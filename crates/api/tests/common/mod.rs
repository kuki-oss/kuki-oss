#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use kuki_api::auth::KeyHasher;
use kuki_api::pg::{PgKeyStore, PgReadiness};
use kuki_api::{ApiConfig, ApiError, AppState, router};
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tower::ServiceExt;
use uuid::Uuid;

pub const PEPPER: &[u8] = b"integration-test-pepper-not-a-secret";
pub const SCHEMA: &str = r#"{"type":"object","required":["delta"],"properties":{"delta":{"type":"integer"}},"additionalProperties":false}"#;

pub struct TestHasher;

#[async_trait]
impl KeyHasher for TestHasher {
    async fn hash(&self, secret: &str) -> Result<[u8; 32], ApiError> {
        let mut m = Hmac::<Sha256>::new_from_slice(PEPPER).unwrap();
        m.update(secret.as_bytes());
        Ok(m.finalize().into_bytes().into())
    }
}

pub async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect_with((*admin.connect_options()).clone())
        .await
        .unwrap()
}

pub fn config() -> ApiConfig {
    ApiConfig::from_lookup(|k| match k {
        "KUKI_DATABASE_URL" => Some("postgres://unused".into()),
        "KUKI_BAO_ADDR" => Some("http://127.0.0.1:8200".into()),
        "KUKI_BAO_TOKEN" => Some("test-token".into()),
        "KUKI_KEY_ENV" => Some("test".into()),
        _ => None,
    })
    .unwrap()
}

pub fn app(pool: PgPool) -> axum::Router {
    router(AppState {
        config: Arc::new(config()),
        pool: pool.clone(),
        keys: Arc::new(PgKeyStore(pool.clone())),
        hasher: Arc::new(TestHasher),
        ready: Arc::new(PgReadiness(pool)),
        extensions: Arc::new(kuki_extension::Extensions::default()),
    })
}

pub async fn seed_org(admin: &PgPool, slug: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, $2, $2)")
        .bind(id)
        .bind(slug)
        .execute(admin)
        .await
        .unwrap();
    id
}

pub async fn seed_key(admin: &PgPool, org: Uuid, scopes: &[&str]) -> String {
    seed_key_in(admin, org, scopes, None).await
}

pub async fn seed_key_in(
    admin: &PgPool,
    org: Uuid,
    scopes: &[&str],
    scope_path: Option<&str>,
) -> String {
    let key_id = Uuid::new_v4().simple().to_string()[..12].to_string();
    let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let hash = TestHasher.hash(&secret).await.unwrap();
    let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
    sqlx::query(
        "INSERT INTO api_keys (id, key_id, organisation_id, name, secret_hash, scopes, org_unit_scope_path)
         VALUES (gen_random_uuid(), $1, $2, 'it', $3, $4::text[], $5::text::ltree)",
    )
    .bind(&key_id)
    .bind(org)
    .bind(hash.to_vec())
    .bind(scopes)
    .bind(scope_path)
    .execute(admin)
    .await
    .unwrap();
    format!("kuki_test_{key_id}_{secret}")
}

pub async fn seed_object_type(admin: &PgPool, org: Uuid) {
    sqlx::query(
        "INSERT INTO object_types (organisation_id, name, version, json_schema)
         VALUES ($1, 'item', 1, $2::jsonb)",
    )
    .bind(org)
    .bind(r#"{"type":"object","required":["name"],"properties":{"name":{"type":"string"}},"additionalProperties":false}"#)
    .execute(admin)
    .await
    .unwrap();
}

pub async fn seed_type(admin: &PgPool, org: Uuid) {
    sqlx::query(
        "INSERT INTO event_types (organisation_id, name, version, subject_type, json_schema)
         VALUES ($1, 'stock.delta', 1, 'item', $2::jsonb)",
    )
    .bind(org)
    .bind(SCHEMA)
    .execute(admin)
    .await
    .unwrap();
}

pub async fn seed_item(admin: &PgPool, org: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, type, type_version, external_ids, attrs, valid_from)
         VALUES ($1, $2, 'item', 1, '{}', '{}', now())",
    )
    .bind(id)
    .bind(org)
    .execute(admin)
    .await
    .unwrap();
    id
}

/// An org with an event type, one item, `ingest:write` and `ingest:read` key.
pub async fn tenant(admin: &PgPool, slug: &str) -> (Uuid, Uuid, String) {
    let org = seed_org(admin, slug).await;
    seed_type(admin, org).await;
    let item = seed_item(admin, org).await;
    let token = seed_key(admin, org, &["ingest:write", "ingest:read"]).await;
    (org, item, token)
}

pub fn event(subject: Uuid, delta: Value) -> Value {
    json!({
        "type": "stock.delta",
        "typeVersion": 1,
        "subject": { "id": subject },
        "occurredAt": chrono::Utc::now().to_rfc3339(),
        "payload": { "delta": delta }
    })
}

pub struct Req<'a> {
    pub uri: &'a str,
    pub token: Option<&'a str>,
    pub idem: Option<&'a str>,
    pub content_type: &'a str,
    pub body: String,
}

pub async fn send(app: &axum::Router, r: Req<'_>) -> (StatusCode, HeaderMap, Value) {
    let mut b = Request::builder()
        .method("POST")
        .uri(r.uri)
        .header("content-type", r.content_type);
    if let Some(t) = r.token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    if let Some(k) = r.idem {
        b = b.header("idempotency-key", k);
    }
    let res = app
        .clone()
        .oneshot(b.body(Body::from(r.body)).unwrap())
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

pub fn json_req<'a>(token: &'a str, idem: &'a str, events: Vec<Value>) -> Req<'a> {
    Req {
        uri: "/v1/events",
        token: Some(token),
        idem: Some(idem),
        content_type: "application/json",
        body: json!({ "events": events }).to_string(),
    }
}

pub async fn count(admin: &PgPool, table: &str, org: Uuid) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {table} WHERE organisation_id = $1"
    ))
    .bind(org)
    .fetch_one(admin)
    .await
    .unwrap()
}

pub async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: &str,
    idem: Option<&str>,
) -> (StatusCode, Value) {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));
    if let Some(k) = idem {
        b = b.header("idempotency-key", k);
    }
    let res = app
        .clone()
        .oneshot(b.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// POST a GraphQL query with a bearer token. Returns (status, body).
pub async fn gql(app: &axum::Router, token: &str, query: &str) -> (StatusCode, Value) {
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/graphql")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(axum::body::Body::from(
            serde_json::json!({ "query": query }).to_string(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Authenticated probe: `{ viewer { principalId } }`.
pub async fn viewer(app: &axum::Router, token: &str) -> (StatusCode, Value) {
    gql(app, token, "{ viewer { principalId } }").await
}

/// Insert an org unit (as the owner role) and return its id.
pub async fn unit(admin: &PgPool, org: Uuid, path: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO org_units (id, organisation_id, path, name) VALUES ($1, $2, $3::text::ltree, $3)")
        .bind(id)
        .bind(org)
        .bind(path)
        .execute(admin)
        .await
        .unwrap();
    id
}

/// A key restricted to a subtree. Assumes the raw key is `kuki_<env>_<key_id>_<secret>`.
pub async fn scoped_key(admin: &PgPool, org: Uuid, scopes: &[&str], unit: &str) -> String {
    let tok = seed_key(admin, org, scopes).await;
    let key_id = tok.split('_').nth(2).unwrap().to_owned();
    sqlx::query("UPDATE api_keys SET org_unit_scope_path = $2::text::ltree WHERE key_id = $1")
        .bind(key_id)
        .bind(unit)
        .execute(admin)
        .await
        .unwrap();
    tok
}
