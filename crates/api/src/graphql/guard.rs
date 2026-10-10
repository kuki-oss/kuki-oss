use async_graphql::{Context, Guard, Result};

use crate::auth::Principal;
use crate::error::ApiError;

/// Mirrors `@requiresScope(scope: "...")` in the contract.
pub struct RequiresScope(&'static str);

impl RequiresScope {
    pub const fn new(scope: &'static str) -> Self {
        Self(scope)
    }
}

impl Guard for RequiresScope {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let p = ctx.data::<Principal>()?;
        if p.scopes.iter().any(|s| s.as_str() == self.0) {
            Ok(())
        } else {
            Err(ApiError::forbidden("the key lacks the scope this field requires").into())
        }
    }
}
