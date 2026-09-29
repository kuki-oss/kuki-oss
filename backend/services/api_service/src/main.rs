//! GraphQL API server binary.
//!
//! Sets up the Axum HTTP server with CORS, serves the Async-GraphQL endpoint
//! at `/graphql/v2` and a GraphiQL playground at `/graphiql/v2`.

mod auth;
mod merchant;
mod sandbox;
mod sandbox_cleanup;
mod schema;
mod kuki;
mod kuki_config;
mod merchant_upload;

use common::{
    db,
    s3::build_s3_client,
};
use std::env;
use crate::{schema::{TcsSchema, jwks::JwksCache}};

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use tower_http::{
  cors::{CorsLayer, AllowOrigin, Any},
  trace::TraceLayer,
};
use axum::{
    http::{HeaderMap, Method, HeaderValue},
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) schema: TcsSchema,
    pub(crate) jwks: JwksCache,
    pub(crate) pool: sqlx::PgPool,
    pub s3_client: aws_sdk_s3::Client,
}

async fn graphql_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> impl IntoResponse {
    let request = schema::graphql_request_with_auth(
      &headers,
      req.into_inner(),
      &state.jwks,
      &state.pool,
    ).await;
    let schema = state.schema.clone();

    // Run resolver execution in its own task so a panic inside any resolver
    // is caught by tokio as a JoinError, instead of propagating  into and
    // potentially killing whatever task is serving this connection
    let response = match tokio::spawn(async move { schema.execute(request).await }).await {
      Ok(response) => response,
      Err(join_err) => {
        tracing::error!(error = ?join_err, "GraphQL resolver panicked");
        async_graphql::Response::from_errors(vec![async_graphql::ServerError::new(
          "Internal server error",
          None,
        )])
      }
    };

    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        axum::Json(response),
    )
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql/v2").finish())
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
      .with_env_filter(
        EnvFilter::try_from_env("API_RUST_LOG")
          .unwrap_or_else(|_| EnvFilter::new("info"))
      )
      .init();

    let database_url = env::var("API_DATABASE_URL").expect("API_DATABASE_URL must be set");
    let pool = db::connect(&database_url).await;

    let jwks_url = env::var("BETTER_AUTH_JWKS_URL")
        .unwrap_or_else(|_| "http://localhost:3000/api/auth/jwks".to_string());

    let s3_bucket = env::var("S3_BUCKET").expect("S3_BUCKET must be set");

    let s3_client = build_s3_client().await;

    let cleanup_pool = pool.clone();

    let state = AppState {
        schema: schema::build(pool.clone(), s3_client.clone(), s3_bucket),
        jwks: JwksCache::new(jwks_url),
        pool,
        s3_client,
    };

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            if let Err(e) = sandbox_cleanup::mutations::delete_expired_sandbox_merchants(&cleanup_pool).await {
                tracing::error!(error = ?e, "sandbox cleanup: delete_expired_sandbox_merchants failed");
            }
            if let Err(e) = sandbox_cleanup::mutations::sweep_stale_uploads(&cleanup_pool, 1).await {
                tracing::error!(error = ?e, "sandbox cleanup: sweep_stale_uploads failed");
            }
        }
    });

    let cors_origin_raw = env::var("API_CORS_ORIGIN")
        .expect("API_CORS_ORIGIN must be set");

    let cors_origins: Vec<axum::http::HeaderValue> = cors_origin_raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<axum::http::HeaderValue>()
                .unwrap_or_else(|_| panic!("API_CORS_ORIGIN contains invalid origin: {s}"))
        })
        .collect();

    if cors_origins.is_empty() {
        panic!("API_CORS_ORIGIN must contain at least one valid origin");
    }

    tracing::info!("CORS origin loaded: {:?}", cors_origins);

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(cors_origins))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
        ])
        .allow_credentials(true);

    let app = Router::new()
        .route("/graphql/v2", axum::routing::post(graphql_handler))
        .route("/graphiql/v2", get(graphiql))
        .route("/sandbox/bootstrap", axum::routing::post(sandbox::bootstrap))
        .route("/health", get(|| async {  r#"{"status":"ok"}"#  }))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state);

    let port = env::var("API_PORT").unwrap_or_else(|_| "8080".to_string());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();

    tracing::info!("api_service listening on port {}", port);
    tracing::info!("GraphQL playground: http://localhost:{}/graphiql", port);

    axum::serve(listener, app).await.unwrap();
}
