//! kuki-api: HTTP access. Config, one error type, API-key authentication, and the
//! routes to prove them (`/v1/health/*`, `/v1/graphql`). REST ingest mounts on this too.
pub mod admin;
pub mod auth;
pub mod backfills;
pub mod bao;
pub mod config;
pub mod error;
pub mod events;
pub mod graphql;
pub mod links;
pub mod objects;
pub mod observe;
pub mod pg;
pub mod quarantine;
pub mod ratelimit;
pub mod routes;
pub mod serve;
pub mod state;

pub use auth::{KeyHasher, KeyRecord, KeyStore, Principal, Scope, authenticate};
pub use config::{ApiConfig, ConfigError};
pub use error::ApiError;
pub use routes::router;
pub use serve::{ServeError, build_state, serve};
pub use state::{AppState, Readiness};
