//! Ingest write path. Every function runs inside `with_org_tx`, so RLS scopes it to one tenant.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum Idempotency {
    /// First sight of this key (or the old row expired). Process the request.
    Proceed,
    /// Seen before but unfinished (a crash, or a concurrent duplicate). Processing
    /// again is safe: ids derived from the key are deterministic and writes are DO NOTHING
    Resume,
    /// Finished before. Return the stored response.
    Replay { status: i32, response: Value },
    /// Same key, different request.
    Mismatch,
}

pub async fn idempotency_begin(
    c: &mut PgConnection,
    org: Uuid,
    key: &str,
    request_hash: &[u8],
) -> sqlx::Result<Idempotency> {
    // Inserts a fresh row, or reclaims an expired one. Zero rows means a live row exists.
    let claimed = sqlx::query(
        "INSERT INTO idempotency_keys (organisation_id, key, request_hash, state)
         VALUES ($1, $2, $3, 'in_progress')
         ON CONFLICT (organisation_id, key) DO UPDATE
            SET request_hash = EXCLUDED.request_hash, state = 'in_progress',
                status_code = NULL, response = NULL,
                created_at = now(), expires_at = now() + interval '24 hours'
          WHERE idempotency_keys.expires_at < now()",
    )
    .bind(org)
    .bind(key)
    .bind(request_hash)
    .execute(&mut *c)
    .await?
    .rows_affected();
    if claimed == 1 {
        return Ok(Idempotency::Proceed);
    }

    let row = sqlx::query(
        "SELECT request_hash, state, status_code, response
           FROM idempotency_keys WHERE organisation_id = $1 AND key = $2",
    )
    .bind(org)
    .bind(key)
    .fetch_one(&mut *c)
    .await?;

    let stored: Vec<u8> = row.try_get("request_hash")?;
    if stored != request_hash {
        return Ok(Idempotency::Mismatch);
    }
    let state: String = row.try_get("state")?;
    if state == "completed" {
        let status: Option<i32> = row.try_get("status_code")?;
        let response: Option<Value> = row.try_get("response")?;
        return Ok(Idempotency::Replay {
            status: status.unwrap_or(202),
            response: response.unwrap_or(Value::Null),
        });
    }
    Ok(Idempotency::Resume)
}

pub async fn idempotency_complete(
    c: &mut PgConnection,
    org: Uuid,
    key: &str,
    request_hash: &[u8],
    status: i32,
    response: &Value,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE idempotency_keys SET state = 'completed', status_code = $4, response = $5
          WHERE organisation_id = $1 AND key = $2 AND request_hash = $3",
    )
    .bind(org)
    .bind(key)
    .bind(request_hash)
    .bind(status)
    .bind(response)
    .execute(c)
    .await?;
    Ok(())
}

pub struct NewBatch<'a> {
    pub id: Uuid,
    pub org: Uuid,
    pub kind: &'a str, // events | objects | links | replay
    pub idempotency_key: &'a str,
    pub source: &'a str,
    pub backfill_id: Option<&'a str>,
}

pub async fn batch_ensure(c: &mut PgConnection, b: &NewBatch<'_>) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO ingest_batches (id, organisation_id, kind, idempotency_key, source, mode, backfill_id)
         VALUES ($1, $2, $3, $4, $5,
                 CASE WHEN $6::text IS NULL THEN 'live' ELSE 'backfill' END, $6)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(b.id)
    .bind(b.org)
    .bind(b.kind)
    .bind(b.idempotency_key)
    .bind(b.source)
    .bind(b.backfill_id)
    .execute(c)
    .await?;
    Ok(())
}

pub async fn batch_finish(
    c: &mut PgConnection,
    id: Uuid,
    accepted: i32,
    rejected: i32,
) -> sqlx::Result<()> {
    sqlx::query("UPDATE ingest_batches SET accepted_count = $2, rejected_count = $3 WHERE id = $1")
        .bind(id)
        .bind(accepted)
        .bind(rejected)
        .execute(c)
        .await?;
    Ok(())
}

pub struct Rejection<'a> {
    pub id: Uuid,
    pub batch_id: Uuid,
    pub index: i32,
    pub code: &'a str,
    pub message: &'a str,
    pub field: Option<&'a str>,
    pub raw: &'a Value,
}

pub async fn quarantine_insert(
    c: &mut PgConnection,
    org: Uuid,
    r: &Rejection<'_>,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO ingest_quarantine (id, organisation_id, batch_id, row_index, code, message, field, raw)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING",
    )
    .bind(r.id)
    .bind(org)
    .bind(r.batch_id)
    .bind(r.index)
    .bind(r.code)
    .bind(r.message)
    .bind(r.field)
    .bind(r.raw)
    .execute(c)
    .await?;
    Ok(())
}

pub struct EventType {
    pub subject_type: String,
    pub json_schema: Value,
}

pub async fn event_type_get(
    c: &mut PgConnection,
    org: Uuid,
    name: &str,
    version: i32,
) -> sqlx::Result<Option<EventType>> {
    let row = sqlx::query(
        "SELECT subject_type, json_schema FROM event_types
          WHERE organisation_id = $1 AND name = $2 AND version = $3",
    )
    .bind(org)
    .bind(name)
    .bind(version)
    .fetch_optional(c)
    .await?;
    row.map(|r| {
        Ok(EventType {
            subject_type: r.try_get("subject_type")?,
            json_schema: r.try_get("json_schema")?,
        })
    })
    .transpose()
}

pub async fn event_type_known(c: &mut PgConnection, org: Uuid, name: &str) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM event_types WHERE organisation_id = $1 AND name = $2)",
    )
    .bind(org)
    .bind(name)
    .fetch_one(c)
    .await
}

pub enum Subject<'a> {
    Id(Uuid),
    External { system: &'a str, id: &'a str },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSubject {
    pub id: Uuid,
    pub org_unit_id: Option<Uuid>,
    pub object_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Resolution {
    Found(ResolvedSubject),
    /// Also returned for objects hidden by org-unit scope: existence must not leak.
    NotFound,
    Ambiguous,
}

/// Exact match on id or external id. Fuzzy matching is never automatic (spec 5.6)
pub async fn subject_resolve(
    c: &mut PgConnection,
    org: Uuid,
    subject: &Subject<'_>,
) -> sqlx::Result<Resolution> {
    let rows = match subject {
        Subject::Id(id) => {
            sqlx::query(
                "SELECT id, org_unit_id, type FROM objects
                  WHERE organisation_id = $1 AND id = $2 AND valid_to IS NULL",
            )
            .bind(org)
            .bind(id)
            .fetch_all(c)
            .await?
        }
        Subject::External { system, id } => {
            sqlx::query(
                "SELECT id, org_unit_id, type FROM objects
                  WHERE organisation_id = $1 AND valid_to IS NULL AND external_ids @> $2
                  LIMIT 2",
            )
            .bind(org)
            .bind(json!({ *system: *id }))
            .fetch_all(c)
            .await?
        }
    };

    match rows.as_slice() {
        [] => Ok(Resolution::NotFound),
        [r] => Ok(Resolution::Found(ResolvedSubject {
            id: r.try_get("id")?,
            org_unit_id: r.try_get("org_unit_id")?,
            object_type: r.try_get("type")?,
        })),
        _ => Ok(Resolution::Ambiguous),
    }
}

pub struct NewEvent {
    pub id: Uuid,
    pub event_type: String,
    pub type_version: i32,
    pub subject_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
    pub supersedes: Option<Uuid>,
}

/// Appends a chunk. Returns how many rows were new; rows whose deterministic id already
/// exists (a retry) are skipped, not errors. Callers still count them as accepted.
pub async fn events_insert(
    c: &mut PgConnection,
    org: Uuid,
    source: &str,
    idempotency_key: &str,
    backfill_id: Option<&str>,
    events: &[NewEvent],
) -> sqlx::Result<u64> {
    if events.is_empty() {
        return Ok(0);
    }
    let ids: Vec<Uuid> = events.iter().map(|e| e.id).collect();
    let types: Vec<String> = events.iter().map(|e| e.event_type.clone()).collect();
    let versions: Vec<i32> = events.iter().map(|e| e.type_version).collect();
    let subjects: Vec<Uuid> = events.iter().map(|e| e.subject_id).collect();
    let occurred: Vec<DateTime<Utc>> = events.iter().map(|e| e.occurred_at).collect();
    let payloads: Vec<Value> = events.iter().map(|e| e.payload.clone()).collect();
    let supersedes: Vec<Option<Uuid>> = events.iter().map(|e| e.supersedes).collect();

    let res = sqlx::query(
        "INSERT INTO events (id, organisation_id, type, type_version, subject_id,
                             occurred_at, payload, supersedes, source, idempotency_key, backfill_id)
         SELECT u.id, $1, u.type, u.type_version, u.subject_id, u.occurred_at, u.payload,
                u.supersedes, $2, $3, $11
           FROM UNNEST($4::uuid[], $5::text[], $6::int[], $7::uuid[],
                       $8::timestamptz[], $9::jsonb[], $10::uuid[])
                AS u(id, type, type_version, subject_id, occurred_at, payload, supersedes)
         ON CONFLICT DO NOTHING",
    )
    .bind(org)
    .bind(source)
    .bind(idempotency_key)
    .bind(ids)
    .bind(types)
    .bind(versions)
    .bind(subjects)
    .bind(occurred)
    .bind(payloads)
    .bind(supersedes)
    .bind(backfill_id)
    .execute(c)
    .await?;
    Ok(res.rows_affected())
}

#[derive(Debug, Clone)]
pub struct QuarantineRow {
    pub id: Uuid,
    pub batch_id: Uuid,
    pub row_index: i32,
    pub received_at: DateTime<Utc>,
    pub code: String,
    pub message: String,
    pub field: Option<String>,
    pub status: String,
    pub raw: Value,
    /// Kind of the batch that rejected it: events | objects | links.
    pub kind: String,
}

const QUARANTINE_SELECT: &str =
    "SELECT q.id, q.batch_id, q.row_index, q.received_at, q.code, q.message,
        q.field, q.status, q.raw, b.kind
   FROM ingest_quarantine q JOIN ingest_batches b ON b.id = q.batch_id";

fn quarantine_from(r: &sqlx::postgres::PgRow) -> sqlx::Result<QuarantineRow> {
    Ok(QuarantineRow {
        id: r.try_get("id")?,
        batch_id: r.try_get("batch_id")?,
        row_index: r.try_get("row_index")?,
        received_at: r.try_get("received_at")?,
        code: r.try_get("code")?,
        message: r.try_get("message")?,
        field: r.try_get("field")?,
        status: r.try_get("status")?,
        raw: r.try_get("raw")?,
        kind: r.try_get("kind")?,
    })
}

/// Rows still awaiting correction, ordered by id. `after` is an exclusive keyset cursor.
pub async fn quarantine_list(
    c: &mut PgConnection,
    org: Uuid,
    batch: Option<Uuid>,
    after: Option<Uuid>,
    limit: i64,
) -> sqlx::Result<Vec<QuarantineRow>> {
    let sql = format!(
        "{QUARANTINE_SELECT}
          WHERE q.organisation_id = $1 AND q.status = 'quarantined'
            AND ($2::uuid IS NULL OR q.batch_id = $2)
            AND ($3::uuid IS NULL OR q.id > $3)
          ORDER BY q.id LIMIT $4"
    );
    let rows = sqlx::query(&sql)
        .bind(org)
        .bind(batch)
        .bind(after)
        .bind(limit)
        .fetch_all(c)
        .await?;
    rows.iter().map(quarantine_from).collect()
}

pub async fn quarantine_get(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
) -> sqlx::Result<Option<QuarantineRow>> {
    let sql = format!("{QUARANTINE_SELECT} WHERE q.organisation_id = $1 AND q.id = $2");
    let row = sqlx::query(&sql)
        .bind(org)
        .bind(id)
        .fetch_optional(c)
        .await?;
    row.as_ref().map(quarantine_from).transpose()
}

/// Returns true when this call changed the row.
pub async fn quarantine_mark_replayed(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
) -> sqlx::Result<bool> {
    let n = sqlx::query(
        "UPDATE ingest_quarantine SET status = 'replayed', replayed_at = now()
          WHERE organisation_id = $1 AND id = $2 AND status = 'quarantined'",
    )
    .bind(org)
    .bind(id)
    .execute(c)
    .await?
    .rows_affected();
    Ok(n == 1)
}

pub async fn batch_exists(c: &mut PgConnection, org: Uuid, id: Uuid) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM ingest_batches WHERE organisation_id = $1 AND id = $2)",
    )
    .bind(org)
    .bind(id)
    .fetch_one(c)
    .await
}
