//! Recommendation reads for the API layer: inbox, filters, counts and the change feed.
//! Every function runs inside `with_org_tx`, so organisation isolation and unit scope apply.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::recommendations::{COLS, RecommendationRow};

pub const DEFAULT_DISPLAY_FLOOR: f64 = 0.30;

/// `decision.min_confidence_for_display` from the organisation's config, default 0.30.
pub async fn display_floor(c: &mut PgConnection, org: Uuid) -> Result<f64, sqlx::Error> {
    let v: Option<Option<f64>> = sqlx::query_scalar::<_, Option<f64>>(
        "SELECT CASE WHEN jsonb_typeof(config->'decision'->'min_confidence_for_display') = 'number'
                     THEN (config->'decision'->>'min_confidence_for_display')::float8 END
           FROM organisations WHERE id = $1",
    )
    .bind(org)
    .fetch_optional(c)
    .await?;
    Ok(v.flatten().unwrap_or(DEFAULT_DISPLAY_FLOOR).clamp(0.0, 1.0))
}

/// What every list-like read applies: the confidence floor and an optional unit subtree.
pub struct Visible<'a> {
    pub under: Option<&'a str>,
    pub min_confidence: f64,
}

fn push_visible(qb: &mut QueryBuilder<'_, Postgres>, v: &Visible<'_>) {
    qb.push(" AND confidence::float8 >= ")
        .push_bind(v.min_confidence);
    if let Some(p) = v.under {
        // Org-wide recommendations have no unit, so a path filter excludes them.
        qb.push(" AND EXISTS (SELECT 1 FROM org_units u WHERE u.id = recommendations.org_unit_id AND u.path <@ ")
            .push_bind(p.to_owned())
            .push("::text::ltree)");
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// urgency desc, created desc, id desc (matches `recs_inbox`).
    Inbox,
    Created,
}

#[derive(Clone, Debug)]
pub enum After {
    /// `urgency` is the column's own text, so the comparison is exact.
    Inbox {
        urgency: String,
        created_at: DateTime<Utc>,
        id: Uuid,
    },
    Created {
        created_at: DateTime<Utc>,
        id: Uuid,
    },
}

pub struct ListFilter<'a> {
    /// Never empty.
    pub statuses: &'a [String],
    pub decision_types: Option<&'a [String]>,
    pub min_urgency: Option<f64>,
    pub visible: Visible<'a>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Listed {
    #[sqlx(flatten)]
    pub row: RecommendationRow,
    pub urgency_key: String,
}

pub async fn list(
    c: &mut PgConnection,
    org: Uuid,
    f: &ListFilter<'_>,
    order: Order,
    after: Option<&After>,
    limit: i64,
) -> Result<Vec<Listed>, sqlx::Error> {
    let mut qb = QueryBuilder::new(format!(
        "SELECT {COLS}, urgency::text AS urgency_key FROM recommendations WHERE organisation_id = "
    ));
    qb.push_bind(org);
    qb.push(" AND status = ANY(")
        .push_bind(f.statuses.to_vec())
        .push(")");
    if let Some(t) = f.decision_types {
        qb.push(" AND decision_type = ANY(")
            .push_bind(t.to_vec())
            .push(")");
    }
    if let Some(u) = f.min_urgency {
        qb.push(" AND urgency::float8 >= ").push_bind(u);
    }
    push_visible(&mut qb, &f.visible);

    match (order, after) {
        (_, None) => {}
        (
            Order::Inbox,
            Some(After::Inbox {
                urgency,
                created_at,
                id,
            }),
        ) => {
            qb.push(" AND (urgency, created_at, id) < (")
                .push_bind(urgency.clone())
                .push("::text::numeric, ")
                .push_bind(*created_at)
                .push(", ")
                .push_bind(*id)
                .push(")");
        }
        (Order::Created, Some(After::Created { created_at, id })) => {
            qb.push(" AND (created_at, id) < (")
                .push_bind(*created_at)
                .push(", ")
                .push_bind(*id)
                .push(")");
        }
        _ => {
            return Err(sqlx::Error::Protocol(
                "cursor does not match the order".into(),
            ));
        }
    }

    match order {
        Order::Inbox => qb.push(
            " ORDER BY recommendations.urgency DESC, recommendations.created_at DESC, recommendations.id DESC",
        ),
        Order::Created => qb.push(" ORDER BY recommendations.created_at DESC, recommendations.id DESC"),
    };
    qb.push(" LIMIT ").push_bind(limit);
    qb.build_query_as::<Listed>().fetch_all(c).await
}

/// Catch-all feed ordered by `(updated_at, id)`; `updated_after` is exclusive.
pub async fn changes(
    c: &mut PgConnection,
    org: Uuid,
    updated_after: DateTime<Utc>,
    after: Option<(DateTime<Utc>, Uuid)>,
    visible: &Visible<'_>,
    limit: i64,
) -> Result<Vec<RecommendationRow>, sqlx::Error> {
    let mut qb = QueryBuilder::new(format!(
        "SELECT {COLS} FROM recommendations WHERE organisation_id = "
    ));
    qb.push_bind(org)
        .push(" AND updated_at > ")
        .push_bind(updated_after);
    push_visible(&mut qb, visible);
    if let Some((t, id)) = after {
        qb.push(" AND (updated_at, id) > (")
            .push_bind(t)
            .push(", ")
            .push_bind(id)
            .push(")");
    }
    qb.push(" ORDER BY recommendations.updated_at, recommendations.id LIMIT ")
        .push_bind(limit);
    qb.build_query_as::<RecommendationRow>().fetch_all(c).await
}

#[derive(Debug, sqlx::FromRow)]
pub struct TypeCounts {
    pub decision_type: String,
    pub pending: i64,
    pub accepted: i64,
    pub rejected: i64,
}

/// Per decision type: pending now, and accepted or rejected since `since` (by `resolved_at`).
/// "Accepted" includes the execution states, because a person accepted all of them.
pub async fn summary(
    c: &mut PgConnection,
    org: Uuid,
    visible: &Visible<'_>,
    since: DateTime<Utc>,
) -> Result<Vec<TypeCounts>, sqlx::Error> {
    let mut qb = QueryBuilder::new(
        "SELECT decision_type,
                count(*) FILTER (WHERE status = 'pending') AS pending,
                count(*) FILTER (WHERE status IN ('accepted','executing','executed','failed') AND resolved_at >= ",
    );
    qb.push_bind(since);
    qb.push(") AS accepted, count(*) FILTER (WHERE status = 'rejected' AND resolved_at >= ");
    qb.push_bind(since);
    qb.push(") AS rejected FROM recommendations WHERE organisation_id = ")
        .push_bind(org);
    qb.push(" AND status IN ('pending','accepted','executing','executed','failed','rejected')");
    push_visible(&mut qb, visible);
    qb.push(" GROUP BY decision_type ORDER BY decision_type");
    qb.build_query_as::<TypeCounts>().fetch_all(c).await
}

#[derive(Debug, sqlx::FromRow)]
pub struct ExecutionRow {
    pub id: Uuid,
    pub action_type: String,
    pub status: String,
    pub adapter: String,
    pub attempts: i32,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub result: Option<serde_json::Value>,
}

/// Bounded, oldest first. Call only for a recommendation the caller can already see.
pub async fn executions_for(
    c: &mut PgConnection,
    org: Uuid,
    recommendation: Uuid,
) -> Result<Vec<ExecutionRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, action_type, status, adapter, attempts, created_by, created_at, finished_at, result
           FROM action_executions
          WHERE organisation_id = $1 AND recommendation_id = $2
          ORDER BY created_at, id LIMIT 100",
    )
    .bind(org)
    .bind(recommendation)
    .fetch_all(c)
    .await
}
