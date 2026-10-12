//! The evaluator's change feed. See migration 0008.

use sqlx::PgConnection;
use uuid::Uuid;

/// Takes (and deletes) up to `limit` entities that have been dirty for at least `min_age_secs`,
/// oldest first, as `(organisation, entity)`. Several callers may run at once; they get disjoint rows.
pub async fn claim(
    c: &mut PgConnection,
    limit: i32,
    min_age_secs: f64,
) -> Result<Vec<(Uuid, Uuid)>, sqlx::Error> {
    sqlx::query_as("SELECT org_id, entity FROM ontology_dirty_claim($1, $2)")
        .bind(limit)
        .bind(min_age_secs)
        .fetch_all(c)
        .await
}

/// Marks entities dirty again. Needs the tenant context of `org` (inside `with_org_tx`); for any
/// other organisation it does nothing.
pub async fn mark(c: &mut PgConnection, org: Uuid, ids: &[Uuid]) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT ontology_dirty_mark($1, $2)")
        .bind(org)
        .bind(ids)
        .execute(c)
        .await?;
    Ok(())
}

/// Organisations with at least one generator configured.
pub async fn evaluator_organisations(c: &mut PgConnection) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT * FROM evaluator_organisations()")
        .fetch_all(c)
        .await
}
