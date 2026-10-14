mod common;

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use kuki_api::auth::KeyHasher;
use kuki_api::pg::{PgKeyStore, PgReadiness};
use kuki_api::{ApiConfig, ApiError, AppState, router};
use sha2::Sha256;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tower::ServiceExt;
use uuid::Uuid;

const PEPPER: &[u8] = b"integration-test-pepper-not-a-secret";

struct TestHasher;

#[async_trait]
impl KeyHasher for TestHasher {
    async fn hash(&self, secret: &str) -> Result<[u8; 32], ApiError> {
        let mut m = Hmac::<Sha256>::new_from_slice(PEPPER).unwrap();
        m.update(secret.as_bytes());
        Ok(m.finalize().into_bytes().into())
    }
}

/// Insert an org + key as the superuser; return the raw bearer token.
async fn seed_key(pool: &PgPool, revoked: bool, expired: bool) -> (String, Uuid) {
    let org = Uuid::new_v4();
    let key_id = Uuid::new_v4().simple().to_string()[..12].to_string();
    let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let hash = TestHasher.hash(&secret).await.unwrap();

    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, $2, 'test-org')")
        .bind(org)
        .bind(format!("test-{}", &org.simple().to_string()[..8]))
        .execute(pool)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO api_keys (id, key_id, organisation_id, name, secret_hash, scopes, \
                               revoked_at, expires_at) \
         VALUES (gen_random_uuid(), $1, $2, 'it', $3, ARRAY['ingest:write'], \
                 CASE WHEN $4 THEN now() END, \
                 CASE WHEN $5 THEN now() - interval '1 minute' END)",
    )
    .bind(&key_id)
    .bind(org)
    .bind(hash.to_vec())
    .bind(revoked)
    .bind(expired)
    .execute(pool)
    .await
    .unwrap();

    (format!("kuki_test_{key_id}_{secret}"), org)
}

/// The app connects as the unprivileged role, like production.
/// Adjust to however your store tests obtain a `kuki_app` pool.
async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
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

fn config() -> ApiConfig {
    ApiConfig::from_lookup(|k| match k {
        "KUKI_DATABASE_URL" => Some("postgres://unused".into()),
        "KUKI_BAO_ADDR" => Some("http://127.0.0.1:8200".into()),
        "KUKI_BAO_TOKEN" => Some("test-token".into()),
        "KUKI_KEY_ENV" => Some("test".into()),
        _ => None,
    })
    .unwrap()
}

fn app(pool: PgPool) -> axum::Router {
    router(AppState {
        config: Arc::new(config()),
        pool: pool.clone(),
        keys: Arc::new(PgKeyStore(pool.clone())),
        hasher: Arc::new(TestHasher),
        ready: Arc::new(PgReadiness(pool)),
        extensions: Arc::new(kuki_extension::Extensions::default()),
    })
}

async fn call(
    app: axum::Router,
    uri: &str,
    bearer: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().uri(uri);
    if let Some(b) = bearer {
        req = req.header("authorization", format!("Bearer {b}"));
    }
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[sqlx::test(migrations = "../../migrations")]
async fn valid_key_reaches_viewer(admin: PgPool) {
    let (token, _) = seed_key(&admin, false, false).await;
    let (s, body) = common::viewer(&app(app_pool(&admin).await), &token).await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        body["data"]["viewer"]["principalId"]
            .as_str()
            .unwrap()
            .starts_with("api_key:"),
        "{body}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn revoked_key_is_401(admin: PgPool) {
    let (token, _) = seed_key(&admin, true, false).await;
    let (s, _) = common::viewer(&app(app_pool(&admin).await), &token).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "../../migrations")]
async fn expired_key_is_401(admin: PgPool) {
    let (token, _) = seed_key(&admin, false, true).await;
    let (s, _) = common::viewer(&app(app_pool(&admin).await), &token).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "../../migrations")]
async fn wrong_secret_is_401(admin: PgPool) {
    let (token, _) = seed_key(&admin, false, false).await;
    let tampered = format!(
        "{}{}",
        &token[..token.len() - 1],
        if token.ends_with('0') { '1' } else { '0' }
    );
    let (s, _) = common::viewer(&app(app_pool(&admin).await), &tampered).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "../../migrations")]
async fn ready_is_200_when_migrated(admin: PgPool) {
    let (s, _) = call(app(app_pool(&admin).await), "/v1/health/ready", None).await;
    assert_eq!(s, StatusCode::OK);
}
