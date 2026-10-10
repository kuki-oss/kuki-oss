//! Backfill lifecyclee: open (implicit) -> completing -> completed, or expired.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row, postgres::PgRow};
use uuid::Uuid;

/// Sliding window: every accepted batch pushes expiry out to now + this.
pub const DEFAULT_TTL_SECS: f64 = 7.0 * 24.0 * 3600.0;

#[derive(Debug, Clone, PartialEq)]
pub struct Backfill {
    pub id: String,
    /// open | completing | completed | expired (a lapsed `open` row reads as expired).
    pub status: String,
    pub opened_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub event_count: i64,
}

fn read(r: &PgRow) -> sqlx::Result<Backfill> {
    Ok(Backfill {
        id: r.try_get("id")?,
        status: r.try_get("status")?,
        opened_at: r.try_get("opened_at")?,
        expires_at: r.try_get("expires_at")?,
        event_count: r.try_get("event_count")?,
    })
}

/// Creates the backfill on first use and extends an open one. Returns the effective status.
/// Anything other than "open" means the caller must refuse the batch.
pub async fn backfill_open(
    c: &mut PgConnection,
    org: Uuid,
    id: &str,
    ttl_secs: f64,
) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "INSERT INTO backfills AS b (organisation_id, id, expires_at)
         VALUES ($1, $2, now() + make_interval(secs => $3))
         ON CONFLICT (organisation_id, id) DO UPDATE
           SET expires_at = CASE WHEN b.status = 'open' AND b.expires_at > now()
                                 THEN GREATEST(b.expires_at, EXCLUDED.expires_at)
                                 ELSE b.expires_at END
         RETURNING CASE WHEN b.status = 'open' AND b.expires_at <= now()
                        THEN 'expired' ELSE b.status END",
    )
    .bind(org)
    .bind(id)
    .bind(ttl_secs)
    .fetch_one(c)
    .await
}

/// The count is live while the backfill is open and frozen once completion starts.
pub async fn backfill_get(
    c: &mut PgConnection,
    org: Uuid,
    id: &str,
) -> sqlx::Result<Option<Backfill>> {
    let row = sqlx::query(
        "SELECT b.id,
                CASE WHEN b.status = 'open' AND b.expires_at <= now()
                     THEN 'expired' ELSE b.status END AS status,
                b.opened_at, b.expires_at,
                CASE WHEN b.status IN ('completing', 'completed') THEN b.event_count
                     ELSE (SELECT count(*) FROM events e
                            WHERE e.organisation_id = b.organisation_id
                              AND e.backfill_id = b.id) END AS event_count
           FROM backfills b
          WHERE b.organisation_id = $1 AND b.id = $2",
    )
    .bind(org)
    .bind(id)
    .fetch_optional(c)
    .await?;
    row.as_ref().map(read).transpose()
}

/// open -> completing, freezing the event count. `None` means nothing transitioned
/// (unknown, already completing/completed, or expired): the caller reads it with `backfill_get`.
pub async fn backfill_begin_complete(
    c: &mut PgConnection,
    org: Uuid,
    id: &str,
) -> sqlx::Result<Option<Backfill>> {
    let row = sqlx::query(
        "UPDATE backfills b
            SET status = 'completing',
                event_count = (SELECT count(*) FROM events e
                                WHERE e.organisation_id = b.organisation_id
                                  AND e.backfill_id = b.id)
          WHERE b.organisation_id = $1 AND b.id = $2
            AND b.status = 'open' AND b.expires_at > now()
        RETURNING b.id, b.status, b.opened_at, b.expires_at, b.event_count",
    )
    .bind(org)
    .bind(id)
    .fetch_optional(c)
    .await?;
    row.as_ref().map(read).transpose()
}
