use futures::future::BoxFuture;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

/// The tenant a request acts for. Built for the authenticated credential,
/// never from request input.
#[derive(Clone, Debug)]
pub struct OrgSession {
    pub org_id: Uuid,
    /// ltree path limiting the session to an org-unit subtree.
    /// `None` = the whole organisation.
    pub scope_path: Option<String>,
}

/// Runs `f` in a transaction with the tenant set, so row-level security
/// applies to every statement. This is the only sanctioned way to touch
/// tenant tables.
pub async fn with_org_tx<T, E, F>(pool: &PgPool, session: &OrgSession, f: F) -> Result<T, E>
where
    E: From<sqlx::Error>,
    F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, E>>,
{
    let mut tx = pool.begin().await?;
    // `true` = local to this transaction, so a pooled connection never
    // leaks a tenant.
    sqlx::query(
        "SELECT set_config('kuki.org_id', $1, true), set_config('kuki.scope_path', $2, true)",
    )
    .bind(session.org_id.to_string())
    .bind(session.scope_path.as_deref().unwrap_or(""))
    .execute(&mut *tx)
    .await?;
    let out = f(&mut tx).await?; // an error here drops `tx`, which rolls back
    tx.commit().await?;
    Ok(out)
}
