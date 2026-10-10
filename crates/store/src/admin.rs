//! Priviledges operations: organisations and API keys. Run these with the owner role,
//! never as `kuki_app` (which has no privileges on `api_keys`).

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

/// Needs a role that bypasses row-level security (superuser or BYPASSRLS)
pub async fn org_by_slug(c: &mut PgConnection, slug: &str) -> sqlx::Result<Option<Uuid>> {
    sqlx::query_scalar("SELECT id FROM organisations WHERE slug = $1")
        .bind(slug)
        .fetch_optional(c)
        .await
}

/// Get-or-create by slug. Returns `(id, created)`.
pub async fn org_create(pool: &PgPool, slug: &str, name: &str) -> sqlx::Result<(Uuid, bool)> {
    let mut tx = pool.begin().await?;
    if let Some(id) = org_by_slug(&mut tx, slug).await? {
        return Ok((id, false));
    }
    let id = Uuid::new_v4();
    // The insert must satisfy the org_isolation policy even for roles that do not bypass it.
    sqlx::query("SELECT set_config('kuki.org_id', $1, true)")
        .bind(id.to_string())
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(slug)
        .bind(name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((id, true))
}

pub struct NewApiKey<'a> {
    pub org: Uuid,
    pub key_id: &'a str,
    pub name: &'a str,
    pub secret_hash: &'a [u8],
    pub scopes: &'a [String],
    pub unit: Option<&'a str>,
    pub expires_at: Option<DateTime<Utc>>,
}

/// `Ok(None)` when `unit` names no org unit of that organisation. A missing organisation
/// is a foreign-key violation (SQLSTATE 23503).
pub async fn api_key_create(pool: &PgPool, k: &NewApiKey<'_>) -> sqlx::Result<Option<Uuid>> {
    let mut tx = pool.begin().await?;
    // Tenant context, so org_units is visible under RLS whatever the admin role is.
    sqlx::query("SELECT set_config('kuki.org_id', $1, true)")
        .bind(k.org.to_string())
        .execute(&mut *tx)
        .await?;
    if let Some(unit) = k.unit {
        let found: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM org_units WHERE organisation_id = $1 AND path = $2::text::ltree)",
        )
        .bind(k.org)
        .bind(unit)
        .fetch_one(&mut *tx)
        .await?;
        if !found {
            return Ok(None);
        }
    }
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO api_keys (id, key_id, organisation_id, name, secret_hash, scopes,
                               org_unit_scope_path, expires_at)
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5::text[], $6::text::ltree, $7)
         RETURNING id",
    )
    .bind(k.key_id)
    .bind(k.org)
    .bind(k.name)
    .bind(k.secret_hash)
    .bind(k.scopes)
    .bind(k.unit)
    .bind(k.expires_at)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(id))
}

/// Never includes the hash.
#[derive(Debug, Clone)]
pub struct ApiKeyRow {
    pub key_id: String,
    pub name: String,
    pub organisation_id: Uuid,
    pub org_slug: Option<String>,
    pub scopes: Vec<String>,
    pub unit: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
}

pub async fn api_key_list(pool: &PgPool, org: Option<Uuid>) -> sqlx::Result<Vec<ApiKeyRow>> {
    let rows = sqlx::query(
        "SELECT k.key_id, k.name, k.organisation_id, o.slug AS org_slug, k.scopes,
                k.org_unit_scope_path::text AS unit, k.created_at, k.expires_at, k.revoked_at, k.last_used_at
           FROM api_keys k LEFT JOIN organisations o ON o.id = k.organisation_id
          WHERE ($1::uuid IS NULL OR k.organisation_id = $1)
          ORDER BY k.created_at, k.key_id",
    )
    .bind(org)
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|r| {
            Ok(ApiKeyRow {
                key_id: r.try_get("key_id")?,
                name: r.try_get("name")?,
                organisation_id: r.try_get("organisation_id")?,
                org_slug: r.try_get("org_slug")?,
                scopes: r.try_get("scopes")?,
                unit: r.try_get("unit")?,
                created_at: r.try_get("created_at")?,
                expires_at: r.try_get("expires_at")?,
                revoked_at: r.try_get("revoked_at")?,
                last_used_at: r.try_get("last_used_at")?,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Revoked {
    Now,
    Already,
    NotFound,
}

/// Takes effect on the next request: authentication reads the row every time.
pub async fn api_key_revoke(pool: &PgPool, key_id: &str) -> sqlx::Result<Revoked> {
    let n = sqlx::query(
        "UPDATE api_keys SET revoked_at = now() WHERE key_id = $1 AND revoked_at IS NULL",
    )
    .bind(key_id)
    .execute(pool)
    .await?
    .rows_affected();
    if n == 1 {
        return Ok(Revoked::Now);
    }
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM api_keys WHERE key_id = $1)")
            .bind(key_id)
            .fetch_one(pool)
            .await?;
    Ok(if exists {
        Revoked::Already
    } else {
        Revoked::NotFound
    })
}
