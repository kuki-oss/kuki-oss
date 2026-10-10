//! Tenant-level reads. Callers run these inside `with_org_tx`, so RLS applies.

use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
pub struct OrganisationRow {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    pub region: Option<String>,
}

pub async fn organisation_get(
    c: &mut PgConnection,
    id: Uuid,
) -> Result<Option<OrganisationRow>, sqlx::Error> {
    sqlx::query_as("SELECT id, slug, name, region FROM organisations WHERE id = $1")
        .bind(id)
        .fetch_optional(c)
        .await
}

#[derive(Debug, sqlx::FromRow)]
pub struct OrgUnitRow {
    pub id: Uuid,
    pub path: String,
    pub name: String,
    pub attrs: serde_json::Value,
}

/// Keyset page ordered by `path`. `under` and `after` must already be valid ltree text.
/// The restrictive `unit_scope` policy narrows the result for subtree-scoped keys.
pub async fn org_units_page(
    c: &mut PgConnection,
    org: Uuid,
    under: Option<&str>,
    after: Option<&str>,
    limit: i64,
) -> Result<Vec<OrgUnitRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, path::text AS path, name, attrs
           FROM org_units
          WHERE organisation_id = $1
            AND ($2::text IS NULL OR path <@ $2::text::ltree)
            AND ($3::text IS NULL OR path > $3::text::ltree)
          ORDER BY path
          LIMIT $4",
    )
    .bind(org)
    .bind(under)
    .bind(after)
    .bind(limit)
    .fetch_all(c)
    .await
}
