//! Recommendation persistence. Every function runs inside `with_org_tx`, so row-level
//! security (organisation isolation and unit scope) applies, and every state change
//! enqueues its event in the same transaction.

use std::fmt;

use chrono::{DateTime, SecondsFormat, Utc};
use kuki_types::RecommendationStatus;
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::outbox::{self, TOPIC_RECOMMENDATIONS};

/// Urgency and confidence changes smaller than this are not a revision.
const TOLERANCE: f64 = 0.01;
/// Superseded versions kept in `revision_history`.
const HISTORY_LIMIT: usize = 20;

#[derive(Debug, sqlx::FromRow)]
pub struct RecommendationRow {
    pub id: Uuid,
    pub organisation_id: Uuid,
    pub org_unit_id: Option<Uuid>,
    pub decision_type: String,
    pub dedup_key: String,
    pub status: String,
    pub urgency: f64,
    pub confidence: f64,
    pub rationale_summary: String,
    pub body: Value,
    pub provenance: Value,
    pub revision_history: Value,
    pub resolution: Option<Value>,
    pub version: i32,
    pub created_at: DateTime<Utc>,
    pub last_confirmed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolved_by: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

const COLS: &str = "id, organisation_id, org_unit_id, decision_type, dedup_key, status, \
                    urgency::float8 AS urgency, confidence::float8 AS confidence, rationale_summary, \
                    body, provenance, revision_history, resolution, version, created_at, \
                    last_confirmed_at, updated_at, resolved_at, resolved_by, expires_at";

pub async fn get(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
) -> Result<Option<RecommendationRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {COLS} FROM recommendations WHERE organisation_id = $1 AND id = $2"
    ))
    .bind(org)
    .bind(id)
    .fetch_optional(c)
    .await
}

struct Event<'a> {
    kind: &'a str,
    id: Uuid,
    version: i32,
    status: &'a str,
    previous: Option<&'a str>,
    decision_type: &'a str,
    org_unit_id: Option<Uuid>,
    at: DateTime<Utc>,
}

impl Event<'_> {
    fn payload(&self) -> Value {
        json!({
            "type": format!("recommendation.{}", self.kind),
            "id": self.id.to_string(),
            "version": self.version,
            "status": self.status,
            "previousStatus": self.previous,
            "decisionType": self.decision_type,
            "orgUnitId": self.org_unit_id.map(|u| u.to_string()),
            "occurredAt": self.at.to_rfc3339_opts(SecondsFormat::Micros, true),
        })
    }
}

async fn emit(c: &mut PgConnection, org: Uuid, e: &Event<'_>) -> Result<(), sqlx::Error> {
    outbox::enqueue(
        c,
        org,
        TOPIC_RECOMMENDATIONS,
        &e.id.to_string(),
        &e.payload(),
    )
    .await?;
    Ok(())
}

/// What the evaluator hands over: a validated candidate, already assembled into columns.
#[derive(Clone, Debug)]
pub struct NewRecommendation {
    /// Used only when no pending row exists for the dedup key.
    pub id: Uuid,
    pub org_unit_id: Option<Uuid>,
    pub decision_type: String,
    pub dedup_key: String,
    pub urgency: f64,
    pub confidence: f64,
    pub rationale_summary: String,
    /// The stored body (spec 9.1).
    pub body: Value,
    pub provenance: Value,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Upserted {
    /// A new pending recommendation. `recommendation.created` was enqueued.
    Created { id: Uuid, version: i32 },
    /// The pending recommendation changed materially. `recommendation.updated` was enqueued.
    Revised { id: Uuid, version: i32 },
    /// Same as before: only `last_confirmed_at` and `expires_at` moved. No event.
    Unchanged { id: Uuid },
}

type ExistingRow = (Uuid, i32, Value, String, f64, f64, Value);

/// Creates, revises or confirms the pending recommendation for `new.dedup_key`.
pub async fn upsert_pending(
    c: &mut PgConnection,
    org: Uuid,
    new: &NewRecommendation,
    now: DateTime<Utc>,
) -> Result<Upserted, sqlx::Error> {
    // Two evaluators racing to create the same key: the loser retries and finds the winner's row.
    for _ in 0..3 {
        let existing: Option<ExistingRow> = sqlx::query_as(
            "SELECT id, version, body, rationale_summary, urgency::float8, confidence::float8, revision_history
               FROM recommendations
              WHERE organisation_id = $1 AND dedup_key = $2 AND status = 'pending'
                FOR UPDATE",
        )
        .bind(org)
        .bind(&new.dedup_key)
        .fetch_optional(&mut *c)
        .await?;

        let Some((id, version, body, summary, urgency, confidence, history)) = existing else {
            let created: Option<i32> = sqlx::query_scalar(
                "INSERT INTO recommendations
                   (id, organisation_id, org_unit_id, decision_type, dedup_key, status, urgency, confidence,
                    rationale_summary, body, provenance, created_at, last_confirmed_at, updated_at, expires_at)
                 VALUES ($1, $2, $3, $4, $5, 'pending', $6::float8, $7::float8, $8, $9, $10, $11, $11, $11, $12)
                 ON CONFLICT (organisation_id, dedup_key) WHERE status = 'pending' DO NOTHING
                 RETURNING version",
            )
            .bind(new.id)
            .bind(org)
            .bind(new.org_unit_id)
            .bind(&new.decision_type)
            .bind(&new.dedup_key)
            .bind(new.urgency)
            .bind(new.confidence)
            .bind(&new.rationale_summary)
            .bind(&new.body)
            .bind(&new.provenance)
            .bind(now)
            .bind(new.expires_at)
            .fetch_optional(&mut *c)
            .await?;
            if let Some(version) = created {
                let e = Event {
                    kind: "created",
                    id: new.id,
                    version,
                    status: "pending",
                    previous: None,
                    decision_type: &new.decision_type,
                    org_unit_id: new.org_unit_id,
                    at: now,
                };
                emit(c, org, &e).await?;
                return Ok(Upserted::Created {
                    id: new.id,
                    version,
                });
            }
            continue;
        };

        let material = body != new.body
            || summary != new.rationale_summary
            || (urgency - new.urgency).abs() >= TOLERANCE
            || (confidence - new.confidence).abs() >= TOLERANCE;

        if !material {
            sqlx::query(
                "UPDATE recommendations SET last_confirmed_at = $3, expires_at = $4
                  WHERE organisation_id = $1 AND id = $2",
            )
            .bind(org)
            .bind(id)
            .bind(now)
            .bind(new.expires_at)
            .execute(&mut *c)
            .await?;
            return Ok(Upserted::Unchanged { id });
        }

        let mut history = match history {
            Value::Array(a) => a,
            _ => vec![],
        };
        history.push(json!({
            "version": version,
            "replacedAt": now.to_rfc3339_opts(SecondsFormat::Micros, true),
            "urgency": urgency,
            "confidence": confidence,
            "rationaleSummary": summary,
            "body": body,
        }));
        if history.len() > HISTORY_LIMIT {
            history.drain(..history.len() - HISTORY_LIMIT);
        }

        let new_version: i32 = sqlx::query_scalar(
            "UPDATE recommendations
                SET urgency = $3::float8, confidence = $4::float8, rationale_summary = $5, body = $6,
                    provenance = $7, revision_history = $8, last_confirmed_at = $9, expires_at = $10
              WHERE organisation_id = $1 AND id = $2
          RETURNING version",
        )
        .bind(org)
        .bind(id)
        .bind(new.urgency)
        .bind(new.confidence)
        .bind(&new.rationale_summary)
        .bind(&new.body)
        .bind(&new.provenance)
        .bind(Value::Array(history))
        .bind(now)
        .bind(new.expires_at)
        .fetch_one(&mut *c)
        .await?;

        let e = Event {
            kind: "updated",
            id,
            version: new_version,
            status: "pending",
            previous: None,
            decision_type: &new.decision_type,
            org_unit_id: new.org_unit_id,
            at: now,
        };
        emit(c, org, &e).await?;
        return Ok(Upserted::Revised {
            id,
            version: new_version,
        });
    }
    Err(sqlx::Error::Protocol(
        "recommendation upsert kept losing a race for its dedup key".into(),
    ))
}

pub struct Transition<'a> {
    pub id: Uuid,
    pub to: RecommendationStatus,
    /// Optimistic concurrency: refuse if the recommendation has moved on since the caller read it.
    pub expected_version: Option<i32>,
    /// Who did it, for example `user:amina`, `system:expiry` or `policy:auto`.
    pub actor: &'a str,
    pub resolution: Option<&'a Value>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Transitioned {
    Changed {
        version: i32,
    },
    /// Already in the requested status: a retry. Nothing was written.
    Unchanged,
}

#[derive(Debug)]
pub enum TransitionError {
    /// No such recommendation, or it is outside the caller's scope (indistinguishable by design).
    NotFound,
    VersionConflict {
        current: i32,
    },
    Illegal {
        from: RecommendationStatus,
        to: RecommendationStatus,
    },
    /// The stored status is not one this code knows.
    Corrupt(String),
    Db(sqlx::Error),
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "recommendation not found"),
            Self::VersionConflict { current } => {
                write!(f, "recommendation is at version {current}")
            }
            Self::Illegal { from, to } => {
                write!(f, "cannot move from {} to {}", from.as_str(), to.as_str())
            }
            Self::Corrupt(s) => write!(f, "unknown stored status {s:?}"),
            Self::Db(e) => write!(f, "database: {e}"),
        }
    }
}

impl std::error::Error for TransitionError {}

impl From<sqlx::Error> for TransitionError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

pub async fn transition(
    c: &mut PgConnection,
    org: Uuid,
    t: &Transition<'_>,
    now: DateTime<Utc>,
) -> Result<Transitioned, TransitionError> {
    let row: Option<(String, i32, String, Option<Uuid>)> = sqlx::query_as(
        "SELECT status, version, decision_type, org_unit_id FROM recommendations
          WHERE organisation_id = $1 AND id = $2 FOR UPDATE",
    )
    .bind(org)
    .bind(t.id)
    .fetch_optional(&mut *c)
    .await?;
    let Some((status, version, decision_type, org_unit_id)) = row else {
        return Err(TransitionError::NotFound);
    };
    let from = RecommendationStatus::parse(&status)
        .ok_or_else(|| TransitionError::Corrupt(status.clone()))?;

    if from == t.to {
        return Ok(Transitioned::Unchanged);
    }
    if let Some(expected) = t.expected_version
        && expected != version {
            return Err(TransitionError::VersionConflict { current: version });
        }
    if !from.can_transition_to(t.to) {
        return Err(TransitionError::Illegal { from, to: t.to });
    }

    use RecommendationStatus::*;
    let resolves = matches!(
        t.to,
        Accepted | Rejected | Expired | Superseded | AutoExecuted
    );
    let new_version: i32 = sqlx::query_scalar(
        "UPDATE recommendations
            SET status      = $3,
                resolved_at = CASE WHEN $4 THEN $5::timestamptz ELSE resolved_at END,
                resolved_by = CASE WHEN $4 THEN $6::text        ELSE resolved_by END,
                resolution  = CASE WHEN $4 THEN $7::jsonb       ELSE resolution  END
          WHERE organisation_id = $1 AND id = $2
      RETURNING version",
    )
    .bind(org)
    .bind(t.id)
    .bind(t.to.as_str())
    .bind(resolves)
    .bind(now)
    .bind(t.actor)
    .bind(t.resolution)
    .fetch_one(&mut *c)
    .await?;

    let e = Event {
        kind: "status_changed",
        id: t.id,
        version: new_version,
        status: t.to.as_str(),
        previous: Some(from.as_str()),
        decision_type: &decision_type,
        org_unit_id,
        at: now,
    };
    emit(c, org, &e).await?;
    Ok(Transitioned::Changed {
        version: new_version,
    })
}

/// Expires pending recommendations whose `expires_at` has passed, oldest first.
/// Returns the ids that were expired by this call.
pub async fn expire_due(
    c: &mut PgConnection,
    org: Uuid,
    now: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<Uuid>, sqlx::Error> {
    let due: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM recommendations
          WHERE organisation_id = $1 AND status = 'pending' AND expires_at <= $2
          ORDER BY expires_at, id LIMIT $3",
    )
    .bind(org)
    .bind(now)
    .bind(limit)
    .fetch_all(&mut *c)
    .await?;

    let resolution = json!({ "reason": "expired" });
    let mut expired = Vec::with_capacity(due.len());
    for id in due {
        let t = Transition {
            id,
            to: RecommendationStatus::Expired,
            expected_version: None,
            actor: "system:expiry",
            resolution: Some(&resolution),
        };
        match transition(c, org, &t, now).await {
            Ok(Transitioned::Changed { .. }) => expired.push(id),
            // Someone resolved it between the SELECT and now. Nothing to do.
            Ok(Transitioned::Unchanged)
            | Err(TransitionError::NotFound | TransitionError::Illegal { .. }) => {}
            Err(TransitionError::VersionConflict { .. }) => {}
            Err(TransitionError::Corrupt(s)) => {
                return Err(sqlx::Error::Protocol(format!(
                    "recommendation {id} has unknown status {s:?}"
                )));
            }
            Err(TransitionError::Db(e)) => return Err(e),
        }
    }
    Ok(expired)
}
