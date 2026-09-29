use jwks::{JwksCache, MerchantClaims as Claims};
use mutation::MutationRoot;
use query::QueryRoot;

use axum::http::HeaderMap;
use sqlx::PgPool;
use uuid::Uuid;
use async_graphql::{
    Context,
    EmptySubscription,
    Schema,
    Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MerchantKind {
    Production,
    Sandbox,
}

impl MerchantKind {
    fn from_db_str(s: &str) -> Self {
        match s {
            "sandbox" => Self::Sandbox,
            // fail safe: unknown values are treated as production, xoxo
            _ => Self::Production,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MerchantAuth {
    pub id: Uuid,
    pub kind: MerchantKind,
}

pub fn require_merchant_with_kind(ctx: &Context<'_>) -> Result<MerchantAuth> {
    ctx.data::<MerchantAuth>()
        .cloned()
        .map_err(|_| async_graphql::Error::new("Unauthorized: valid token required"))
}
