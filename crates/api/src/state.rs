use std::sync::Arc;

use crate::{
    auth::{KeyHasher, KeyStore},
    config::ApiConfig,
    error::ApiError,
};

#[async_trait::async_trait]
pub trait Readiness: Send + Sync {
    async fn check(&self) -> Result<(), ApiError>;
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ApiConfig>,
    pub pool: sqlx::PgPool,
    pub keys: Arc<dyn KeyStore>,
    pub hasher: Arc<dyn KeyHasher>,
    pub ready: Arc<dyn Readiness>,
}
