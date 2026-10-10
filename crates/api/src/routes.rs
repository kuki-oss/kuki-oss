//! HTTP surface: health, REST ingest and the GraphQL data plane (`POST /v1/graphql`).
use axum::{Json, Router, extract::State, routing::get};
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/health/live", get(live))
        .route("/v1/health/ready", get(ready))
        .route(
            "/v1/events",
            axum::routing::post(crate::events::post_events),
        )
        .route("/v1/backfills/{id}", get(crate::backfills::get_backfill))
        .route(
            "/v1/backfills/{id}/complete",
            axum::routing::post(crate::backfills::complete_backfill),
        )
        .route(
            "/v1/objects/batch",
            axum::routing::post(crate::objects::post_objects),
        )
        .route(
            "/v1/links/batch",
            axum::routing::post(crate::links::post_links),
        )
        .route("/v1/quarantine", get(crate::quarantine::list_quarantine))
        .route(
            "/v1/quarantine/{id}/replay",
            axum::routing::post(crate::quarantine::replay_quarantined),
        )
        .route(
            "/v1/graphql",
            axum::routing::post(crate::graphql::post_graphql)
                .layer(axum::extract::DefaultBodyLimit::max(64 * 1024)),
        )
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer(axum::middleware::from_fn(crate::ratelimit::report))
        .layer(axum::middleware::from_fn(crate::observe::request_context))
        .with_state(state)
}

/// Process is up. No dependencies checked, no auth.
async fn live() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// Safe to receive traffic: DB is reachable and fully migrated.
/// 503 (via `ApiError::unavailable`) otherwise. No auth.
async fn ready(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    state.ready.check().await?;
    Ok(Json(json!({ "status": "ready" })))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt; // oneshot

    use super::*;
    use crate::auth::testing::{MemoryKeys, TestHasher, issue_key, record};
    use crate::config::ApiConfig;
    use crate::state::Readiness;

    struct Ready(bool);

    #[async_trait::async_trait]
    impl Readiness for Ready {
        async fn check(&self) -> Result<(), ApiError> {
            if self.0 {
                Ok(())
            } else {
                Err(ApiError::unavailable("database not ready"))
            }
        }
    }

    fn app(keys: MemoryKeys, ready: bool) -> Router {
        router(AppState {
            config: Arc::new(ApiConfig::for_tests()), // key_env = "test", pepper below
            pool: sqlx::PgPool::connect_lazy("postgres://unused/unused").unwrap(),
            hasher: Arc::new(TestHasher::standard()),
            keys: Arc::new(keys),
            ready: Arc::new(Ready(ready)),
        })
    }

    async fn get_json(app: Router, uri: &str, auth: Option<&str>) -> (StatusCode, Value) {
        let mut req = Request::builder().uri(uri);
        if let Some(a) = auth {
            req = req.header("authorization", a);
        }
        let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn live_needs_no_auth() {
        let (s, body) = get_json(app(MemoryKeys::default(), true), "/v1/health/live", None).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn ready_reflects_dependency_state() {
        let (s, _) = get_json(app(MemoryKeys::default(), true), "/v1/health/ready", None).await;
        assert_eq!(s, StatusCode::OK);
        let (s, body) = get_json(app(MemoryKeys::default(), false), "/v1/health/ready", None).await;
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "INTERNAL");
    }

    async fn post_graphql(app: Router, auth: Option<&str>, query: &str) -> (StatusCode, Value) {
        let mut b = Request::builder()
            .method("POST")
            .uri("/v1/graphql")
            .header("content-type", "application/json");
        if let Some(a) = auth {
            b = b.header("authorization", a);
        }
        let body = Body::from(serde_json::json!({ "query": query }).to_string());
        let res = app.oneshot(b.body(body).unwrap()).await.unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn graphql_requires_auth() {
        let (s, body) = post_graphql(
            app(MemoryKeys::default(), true),
            None,
            "{ viewer { principalId } }",
        )
        .await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "UNAUTHENTICATED");
    }

    #[tokio::test]
    async fn graphql_viewer_returns_principal_and_scopes() {
        let (raw, id, hash) = issue_key("test", &TestHasher::standard()).await;
        let keys = MemoryKeys::default().with(&id, record(hash, &["ingest:write"]));

        // No `organisation` selection: this unit-test app has no database.
        let (s, body) = post_graphql(
            app(keys, true),
            Some(&format!("Bearer {raw}")),
            "{ viewer { principalId scopes orgUnitScopePath } }",
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        assert!(body["errors"].is_null(), "{body}");
        let v = &body["data"]["viewer"];
        assert!(v["principalId"].as_str().unwrap().starts_with("api_key:"));
        assert_eq!(v["scopes"], serde_json::json!(["ingest:write"]));
        assert!(v["orgUnitScopePath"].is_null());
    }

    #[tokio::test]
    async fn the_temporary_rest_viewer_is_gone() {
        let (raw, id, hash) = issue_key("test", &TestHasher::standard()).await;
        let keys = MemoryKeys::default().with(&id, record(hash, &["ingest:write"]));
        let req = Request::builder()
            .uri("/v1/viewer")
            .header("authorization", format!("Bearer {raw}"))
            .body(Body::empty())
            .unwrap();
        let res = app(keys, true).oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }
}
