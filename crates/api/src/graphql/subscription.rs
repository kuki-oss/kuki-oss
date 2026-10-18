//! Subscriptions (spec 3.4): recommendationCreated, recommendationUpdated and
//! recommendationStatusChanged.
//!
//! Each connection holds a receiver on the process' `EventFeed` (one Kafka consumer
//! per API process). An event is cheaply filtered on organisation, type and `decisionTypes`;
//! survivors are reloaded under the subscriber's own tenant context, so row-level security
//! (unit scope), the confidence display floor and `orgUnitPath` decide what the caller may
//! see, exactly as for `recommendationChanges`. Clients receive current state, which may
//! be newer than the event; they dedupe on (id, version).
//!
//! Delivery to a connected client is at most once: a socket that falls behind skips events,
//! and a reconnecting client catches up with `recommendationChanges`.

use async_graphql::{Context, Error, InputObject, Result, SimpleObject, Subscription};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use kuki_outbox::{EventFeed, LiveEvent};
use kuki_store::{recommendation_reads as reads, recommendations::RecommendationRow, session::with_org_tx};
use kuki_types as kt;
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::broadcast::{self, error::RecvError};
use uuid::Uuid;

use super::connection::{check_ltree, validation};
use super::guard::RequiresScope;
use super::recommendations::{KukiRecommendation, KukiRecommendationStatus, stored};
use crate::{auth::Principal, error::ApiError};

#[derive(InputObject, Default)]
pub struct SubscriptionFilter {
    decision_types: Option<Vec<String>>,
    org_unit_path: Option<String>,
}

#[derive(SimpleObject)]
pub struct RecommendationStatusChange {
    recommendation: KukiRecommendation,
    previous_status: KukiRecommendationStatus,
}

#[derive(Clone, Debug, Default)]
struct Want {
    decision_types: Option<Vec<String>>,
    under: Option<String>,
}

fn validate(f: SubscriptionFilter) -> Result<Want> {
    let decision_types = match f.decision_types {
        None => None,
        Some(t) if t.is_empty() => return Err(validation("decisionTypes must not be empty", None)),
        Some(t) if t.iter().any(|x| x.is_empty() || x.len() > 200) => {
            return Err(validation("decisionTypes entries must be 1 to 200 characters", None));
        }
        Some(t) => Some(t),
    };
    if let Some(u) = &f.org_unit_path {
        check_ltree(u, "orgUnitPath")?;
    }
    Ok(Want { decision_types, under: f.org_unit_path })
}

#[derive(Debug, PartialEq, Eq)]
struct Routed {
    id: Uuid,
    previous: Option<String>,
}

/// The cheap checks, from the event alone.
fn route(ev: &LiveEvent, org: Uuid, kind: &str, want: &Want) -> Option<Routed> {
    if ev.organisation_id != org || ev.event_type != kind {
        return None;
    }
    if let Some(types) = &want.decision_types {
        let dt = ev.payload.get("decisionType").and_then(Value::as_str)?;
        if !types.iter().any(|t| t == dt) {
            return None;
        }
    }
    let id = ev
        .payload
        .get("id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())?;
    let previous = ev
        .payload
        .get("previousStatus")
        .and_then(Value::as_str)
        .map(str::to_owned);
    Some(Routed { id, previous })
}

struct Delivered {
    rec: KukiRecommendation,
    previous: Option<KukiRecommendationStatus>,
}

struct Live {
    rx: broadcast::Receiver<LiveEvent>,
    principal: Principal,
    pool: PgPool,
    kind: &'static str,
    want: Want,
}

async fn load(s: &Live, id: Uuid) -> Result<Option<RecommendationRow>, sqlx::Error> {
    let org = s.principal.session.org_id;
    let under = s.want.under.clone();
    with_org_tx::<_, sqlx::Error, _>(&s.pool, &s.principal.session, move |c| {
        async move {
            let floor = reads::display_floor(c, org).await?;
            let v = reads::Visible { under: under.as_deref(), min_confidence: floor };
            reads::get_visible(c, org, id, &v).await
        }
        .boxed()
    })
    .await
}

fn deliveries(live: Live) -> impl futures::Stream<Item = Delivered> + Send + 'static {
    futures::stream::unfold(live, |mut s| async move {
        loop {
            let ev = match s.rx.recv().await {
                Ok(ev) => ev,
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "a subscription fell behind and skipped events");
                    continue;
                }
                Err(RecvError::Closed) => return None,
            };
            let Some(r) = route(&ev, s.principal.session.org_id, s.kind, &s.want) else {
                continue;
            };
            match load(&s, r.id).await {
                // Not found, out of scope, or under the display floor: not this caller's event.
                Ok(None) => continue,
                Ok(Some(row)) => match KukiRecommendation::new(row) {
                    Ok(rec) => {
                        let previous = r
                            .previous
                            .and_then(|p| stored::<kt::RecommendationStatus>(&p).ok())
                            .map(KukiRecommendationStatus::from);
                        return Some((Delivered { rec, previous }, s));
                    }
                    Err(e) => tracing::warn!(error = %e.message, "a subscribed recommendation did not parse"),
                },
                Err(e) => tracing::warn!(error = %e, "could not load a recommendation for a subscriber"),
            }
        }
    })
}

fn open(
    ctx: &Context<'_>,
    kind: &'static str,
    filter: Option<SubscriptionFilter>,
) -> Result<BoxStream<'static, Delivered>> {
    let want = validate(filter.unwrap_or_default())?;
    let principal = ctx.data::<Principal>()?.clone();
    let pool = ctx.data::<PgPool>()?.clone();
    let feed = ctx.data_opt::<EventFeed>().ok_or_else(|| {
        Error::from(ApiError::unavailable(
            "live subscriptions are not enabled on this server (KUKI_KAFKA_BROKERS is not set)",
        ))
    })?;
    let live = Live { rx: feed.subscribe(), principal, pool, kind, want };
    Ok(deliveries(live).boxed())
}

#[derive(Default)]
pub struct SubscriptionRoot;

#[Subscription(name = "Subscription")]
impl SubscriptionRoot {
    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn recommendation_created(
        &self,
        ctx: &Context<'_>,
        filter: Option<SubscriptionFilter>,
    ) -> Result<BoxStream<'static, KukiRecommendation>> {
        Ok(open(ctx, "recommendation.created", filter)?.map(|d| d.rec).boxed())
    }

    /// Material change updated in place (spec 7.4).
    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn recommendation_updated(
        &self,
        ctx: &Context<'_>,
        filter: Option<SubscriptionFilter>,
    ) -> Result<BoxStream<'static, KukiRecommendation>> {
        Ok(open(ctx, "recommendation.updated", filter)?.map(|d| d.rec).boxed())
    }

    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn recommendation_status_changed(
        &self,
        ctx: &Context<'_>,
        filter: Option<SubscriptionFilter>,
    ) -> Result<BoxStream<'static, RecommendationStatusChange>> {
        Ok(open(ctx, "recommendation.status_changed", filter)?
            .filter_map(|d| async move {
                Some(RecommendationStatusChange { recommendation: d.rec, previous_status: d.previous? })
            })
            .boxed())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const KIND: &str = "recommendation.status_changed";

    fn ev(org: Uuid, kind: &str, payload: Value) -> LiveEvent {
        LiveEvent { organisation_id: org, event_id: "1".into(), event_type: kind.into(), payload }
    }

    #[test]
    fn route_accepts_only_matching_events() {
        let (org, id) = (Uuid::new_v4(), Uuid::new_v4());
        let payload = json!({ "id": id.to_string(), "decisionType": "inventory.replenish", "previousStatus": "pending" });
        let want = Want { decision_types: Some(vec!["inventory.replenish".into()]), under: None };

        let got = route(&ev(org, KIND, payload.clone()), org, KIND, &want).unwrap();
        assert_eq!(got, Routed { id, previous: Some("pending".into()) });

        assert!(route(&ev(Uuid::new_v4(), KIND, payload.clone()), org, KIND, &want).is_none(), "other organisation");
        assert!(route(&ev(org, "recommendation.created", payload.clone()), org, KIND, &want).is_none(), "other event type");
        let other = Want { decision_types: Some(vec!["scheduling.conflict".into()]), under: None };
        assert!(route(&ev(org, KIND, payload.clone()), org, KIND, &other).is_none(), "other decision type");
        // No filter: any decision type passes.
        assert!(route(&ev(org, KIND, payload), org, KIND, &Want::default()).is_some());
    }

    #[test]
    fn route_needs_a_uuid_id() {
        let org = Uuid::new_v4();
        for payload in [json!({}), json!({ "id": 5 }), json!({ "id": "not-a-uuid" })] {
            assert!(route(&ev(org, KIND, payload), org, KIND, &Want::default()).is_none());
        }
    }

    #[test]
    fn filters_are_validated() {
        assert!(validate(SubscriptionFilter::default()).is_ok());
        let empty = SubscriptionFilter { decision_types: Some(vec![]), org_unit_path: None };
        assert!(validate(empty).is_err());
        let bad_path = SubscriptionFilter { decision_types: None, org_unit_path: Some("not a path!".into()) };
        assert!(validate(bad_path).is_err());
        let ok = SubscriptionFilter { decision_types: Some(vec!["a.b".into()]), org_unit_path: Some("hq.east".into()) };
        assert!(validate(ok).is_ok());
    }
}
