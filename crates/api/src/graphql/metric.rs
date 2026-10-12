//! `metric(name, subjectId, asOf)`: the latest value with as_of <= asOf (valid time).

use async_graphql::{Context, ID, Json, Object, Result, SimpleObject};
use chrono::{DateTime, Utc};
use futures::FutureExt;
use sqlx::PgPool;

use super::connection::{parse_uuid, validation};
use super::guard::RequiresScope;
use crate::auth::Principal;
use crate::error::ApiError;

#[derive(SimpleObject)]
pub struct MetricValue {
    name: String,
    version: i32,
    subject_id: ID,
    as_of: DateTime<Utc>,
    value: Json<serde_json::Value>,
    unit: Option<String>,
}

#[derive(Default)]
pub struct MetricQuery;

#[Object]
impl MetricQuery {
    /// Null when there is no such value, or its subject is outside the caller's scope.
    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn metric(
        &self,
        ctx: &Context<'_>,
        name: String,
        subject_id: ID,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<Option<MetricValue>> {
        if name.is_empty() || name.len() > 200 {
            return Err(validation("name must be 1 to 200 characters", None));
        }
        let subject = parse_uuid(&subject_id, "subjectId")?;

        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let row = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let name = name.clone();
            async move { kuki_store::metrics::metric_at(c, org, &name, subject, as_of).await }
                .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        Ok(row.map(|r| MetricValue {
            name: r.name,
            version: r.version,
            subject_id: ID(r.subject_id.to_string()),
            as_of: r.as_of,
            value: Json(r.value),
            unit: r.unit,
        }))
    }
}
