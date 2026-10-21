//! Shadow proposals: upserts count runs and refresh the stored copy, pruning is per generator
//! and organisation, and row-level security keeps tenants apart.

use chrono::{DateTime, Duration, Utc};
use futures::FutureExt;
use kuki_store::{
    recommendations::NewRecommendation,
    session::{OrgSession, with_org_tx},
    shadow,
};
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

fn t(s: &str) -> DateTime<Utc> {
    s.parse().unwrap()
}

async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect_with((*admin.connect_options()).clone())
        .await
        .unwrap()
}

async fn seed_org(admin: &PgPool, n: u128) -> Uuid {
    let id = Uuid::from_u128(n);
    let slug = format!("org{n}");
    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, $2, $2)")
        .bind(id)
        .bind(slug)
        .execute(admin)
        .await
        .unwrap();
    id
}

fn proposal(key: &str, urgency: f64, summary: &str) -> NewRecommendation {
    NewRecommendation {
        id: Uuid::new_v4(),
        org_unit_id: None,
        decision_type: "work.escalate".into(),
        dedup_key: key.into(),
        urgency,
        confidence: 0.9,
        rationale_summary: summary.into(),
        body: json!({ "urgency": urgency, "rationale": { "confidence": 0.9 } }),
        provenance: json!({ "generator_id": "g" }),
        expires_at: None,
    }
}

async fn list_as(app: &PgPool, org: Uuid, generator: &'static str) -> Vec<shadow::ShadowRow> {
    let session = OrgSession {
        org_id: org,
        scope_path: None,
    };
    with_org_tx::<_, sqlx::Error, _>(app, &session, move |c| {
        async move { shadow::list(c, org, generator, 50).await }.boxed()
    })
    .await
    .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_rerun_counts_the_run_and_refreshes_the_stored_copy(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    let mut c = admin.acquire().await.unwrap();
    let t0 = t("2026-10-11T12:00:00Z");
    let t1 = t0 + Duration::hours(1);

    shadow::upsert(&mut c, org, "g", &proposal("k", 0.5, "first"), t0)
        .await
        .unwrap();
    shadow::upsert(&mut c, org, "g", &proposal("k", 0.8, "second"), t1)
        .await
        .unwrap();

    let rows = shadow::list(&mut c, org, "g", 50).await.unwrap();
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.seen_count, 2);
    assert_eq!(r.first_seen_at, t0, "first sighting is kept");
    assert_eq!(r.last_seen_at, t1);
    assert_eq!(r.summary, "second");
    assert!((r.urgency - 0.8).abs() < 1e-9);
    assert!((r.confidence - 0.9).abs() < 1e-9);
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_report_is_most_urgent_first_and_per_generator(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    let mut c = admin.acquire().await.unwrap();
    let now = t("2026-10-11T12:00:00Z");
    for (g, k, u) in [("g", "low", 0.2), ("g", "high", 0.9), ("other", "x", 1.0)] {
        shadow::upsert(&mut c, org, g, &proposal(k, u, k), now)
            .await
            .unwrap();
    }
    let keys: Vec<_> = shadow::list(&mut c, org, "g", 50)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.dedup_key)
        .collect();
    assert_eq!(keys, ["high", "low"]);
    assert_eq!(shadow::list(&mut c, org, "g", 1).await.unwrap().len(), 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn pruning_only_removes_old_rows_of_that_generator_and_organisation(admin: PgPool) {
    let a = seed_org(&admin, 1).await;
    let b = seed_org(&admin, 2).await;
    let mut c = admin.acquire().await.unwrap();
    let old = t("2026-10-01T00:00:00Z");
    let new = t("2026-10-11T00:00:00Z");
    let cutoff = t("2026-10-05T00:00:00Z");

    shadow::upsert(&mut c, a, "g", &proposal("old", 0.5, "x"), old)
        .await
        .unwrap();
    shadow::upsert(&mut c, a, "g", &proposal("new", 0.5, "x"), new)
        .await
        .unwrap();
    shadow::upsert(&mut c, a, "other", &proposal("old", 0.5, "x"), old)
        .await
        .unwrap();
    shadow::upsert(&mut c, b, "g", &proposal("old", 0.5, "x"), old)
        .await
        .unwrap();

    assert_eq!(shadow::prune(&mut c, a, "g", cutoff).await.unwrap(), 1);

    let left = |org, g| {
        let admin = admin.clone();
        async move {
            let mut c = admin.acquire().await.unwrap();
            shadow::list(&mut c, org, g, 50).await.unwrap().len()
        }
    };
    assert_eq!(left(a, "g").await, 1, "the new row stays");
    assert_eq!(left(a, "other").await, 1, "another generator is untouched");
    assert_eq!(left(b, "g").await, 1, "another organisation is untouched");
}

#[sqlx::test(migrations = "../../migrations")]
async fn tenants_cannot_see_or_write_each_others_shadow_rows(admin: PgPool) {
    let app = app_pool(&admin).await;
    let a = seed_org(&admin, 1).await;
    let b = seed_org(&admin, 2).await;
    let now = t("2026-10-11T12:00:00Z");
    {
        let mut c = admin.acquire().await.unwrap();
        shadow::upsert(&mut c, a, "g", &proposal("a-row", 0.5, "x"), now)
            .await
            .unwrap();
        shadow::upsert(&mut c, b, "g", &proposal("b-row", 0.5, "x"), now)
            .await
            .unwrap();
    }

    let mine: Vec<_> = list_as(&app, a, "g")
        .await
        .into_iter()
        .map(|r| r.dedup_key)
        .collect();
    assert_eq!(mine, ["a-row"]);

    // Naming another organisation inside a tenant's transaction reads nothing and writes nothing.
    let session = OrgSession {
        org_id: a,
        scope_path: None,
    };
    let foreign = with_org_tx::<_, sqlx::Error, _>(&app, &session, move |c| {
        async move { shadow::list(c, b, "g", 50).await }.boxed()
    })
    .await
    .unwrap();
    assert!(foreign.is_empty());
    let write = with_org_tx::<_, sqlx::Error, _>(&app, &session, move |c| {
        async move { shadow::upsert(c, b, "g", &proposal("x", 0.5, "x"), now).await }.boxed()
    })
    .await;
    assert!(write.is_err(), "row-level security refuses a foreign write");

    // And with no tenant at all, nothing is visible.
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM shadow_proposals")
        .fetch_one(&app)
        .await
        .unwrap();
    assert_eq!(n, 0);
}
