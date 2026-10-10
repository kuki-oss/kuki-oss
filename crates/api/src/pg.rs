use async_trait::async_trait;
use sqlx::{PgPool, Row};
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

use crate::auth::{KeyRecord, KeyStore};
use crate::error::ApiError;
use crate::state::Readiness;

/// The SQL function throttles the write to one per 5 minutes; this keeps hot keys from
/// even making the call, and from queueing tasks behind a busy pool.
const TOUCH_EVERY: Duration = Duration::from_secs(60);
static RECENT: LazyLock<Mutex<HashMap<Uuid, Instant>>> = LazyLock::new(Default::default);

fn is_due(recent: &mut HashMap<Uuid, Instant>, key: Uuid, now: Instant) -> bool {
    if recent
        .get(&key)
        .is_some_and(|t| now.duration_since(*t) < TOUCH_EVERY)
    {
        return false;
    }
    if recent.len() >= 4096 {
        recent.retain(|_, t| now.duration_since(*t) < TOUCH_EVERY);
    }
    recent.insert(key, now);
    true
}

pub struct PgKeyStore(pub PgPool);

#[async_trait]
impl KeyStore for PgKeyStore {
    async fn find(&self, key_id: &str) -> Result<Option<KeyRecord>, ApiError> {
        let row = sqlx::query(
            "SELECT id, organisation_id, secret_hash, scopes, \
                    org_unit_scope_path::text AS org_unit_scope_path, \
                    expires_at, revoked_at \
             FROM auth_lookup_api_key($1)",
        )
        .bind(key_id)
        .fetch_optional(&self.0)
        .await?;

        row.map(|r| {
            Ok(KeyRecord {
                id: r.try_get("id")?,
                organisation_id: r.try_get("organisation_id")?,
                secret_hash: r.try_get("secret_hash")?,
                scopes: r.try_get("scopes")?,
                org_unit_scope_path: r.try_get("org_unit_scope_path")?,
                expires_at: r.try_get("expires_at")?,
                revoked_at: r.try_get("revoked_at")?,
            })
        })
        .transpose()
        .map_err(|e: sqlx::Error| ApiError::from(e))
    }

    fn touch(&self, key: Uuid) {
        let due = RECENT
            .lock()
            .map(|mut m| is_due(&mut m, key, Instant::now()))
            .unwrap_or(false);
        if !due {
            return;
        }
        let pool = self.0.clone();
        tokio::spawn(async move {
            if let Err(e) = sqlx::query("SELECT auth_touch_api_key($1)")
                .bind(key)
                .execute(&pool)
                .await
            {
                tracing::warn!(error = %e, "could not record api key use");
            }
        });
    }
}

pub struct PgReadiness(pub PgPool);

#[async_trait]
impl Readiness for PgReadiness {
    async fn check(&self) -> Result<(), ApiError> {
        let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&self.0)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "readiness: database check failed");
                ApiError::unavailable("database unavailable")
            })?;

        if (applied as usize) < kuki_store::MIGRATOR.migrations.len() {
            return Err(ApiError::unavailable("migrations pending"));
        }
        Ok(())
    }
}

/// Ready only when every dependency is ready (Postgres, OpenBao).
pub struct AllReady(pub Vec<std::sync::Arc<dyn Readiness>>);

#[async_trait]
impl Readiness for AllReady {
    async fn check(&self) -> Result<(), ApiError> {
        for r in &self.0 {
            r.check().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn touches_are_throttled_per_key() {
        let mut m = HashMap::new();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let t0 = Instant::now();
        assert!(is_due(&mut m, a, t0));
        assert!(!is_due(&mut m, a, t0 + Duration::from_secs(59)));
        assert!(is_due(&mut m, b, t0 + Duration::from_secs(1)));
        assert!(is_due(&mut m, a, t0 + TOUCH_EVERY));
    }
}
