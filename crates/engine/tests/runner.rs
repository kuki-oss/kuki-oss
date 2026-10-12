use std::{collections::BTreeSet, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures::FutureExt;
use kuki_engine::{Evaluator, EvaluatorOptions, Leader, Registry, Runner, RunnerConfig, builtin};
use kuki_store::{
    dirty,
    session::{OrgSession, with_org_tx},
};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

fn u(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn now() -> DateTime<Utc> {
    "2026-10-11T12:00:00Z".parse().unwrap()
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

async fn seed_org(admin: &PgPool, slug: &str, decision: Value) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO organisations (id, slug, name, config) VALUES ($1, $2, $2, $3)")
        .bind(id)
        .bind(slug)
        .bind(json!({ "decision": decision }))
        .execute(admin)
        .await
        .unwrap();
    id
}

async fn deadline_org(admin: &PgPool, slug: &str) -> Uuid {
    let org = seed_org(
        admin,
        slug,
        json!({ "generators": { "deadline.at_risk.t0": { "params": {
            "object_type": "case", "due_attr": "due_at", "status_attr": "status",
            "open_statuses": ["open"], "horizon_hours": 72 } } } }),
    )
    .await;
    let mut c = admin.acquire().await.unwrap();
    builtin::install_decision_types(&mut c, org).await.unwrap();
    org
}

fn overdue(status: &str) -> Value {
    json!({ "status": status, "due_at": (now() - chrono::Duration::hours(5)).to_rfc3339() })
}

async fn put_obj(admin: &PgPool, org: Uuid, id: u128, attrs: Value, from: &str) {
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, type, type_version, attrs, valid_from)
         VALUES ($1, $2, 'case', 1, $3, $4::text::timestamptz)",
    )
    .bind(u(id))
    .bind(org)
    .bind(attrs)
    .bind(from)
    .execute(admin)
    .await
    .unwrap();
}

async fn new_version(admin: &PgPool, org: Uuid, id: u128, attrs: Value, at: &str) {
    sqlx::query(
        "UPDATE objects SET valid_to = $3::text::timestamptz
          WHERE organisation_id = $1 AND id = $2 AND valid_to IS NULL",
    )
    .bind(org)
    .bind(u(id))
    .bind(at)
    .execute(admin)
    .await
    .unwrap();
    put_obj(admin, org, id, attrs, at).await;
}

async fn dirty_set(admin: &PgPool, org: Uuid) -> BTreeSet<Uuid> {
    sqlx::query_scalar::<_, Uuid>("SELECT entity_id FROM ontology_dirty WHERE organisation_id = $1")
        .bind(org)
        .fetch_all(admin)
        .await
        .unwrap()
        .into_iter()
        .collect()
}

async fn count_status(admin: &PgPool, org: Uuid, status: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM recommendations WHERE organisation_id = $1 AND status = $2",
    )
    .bind(org)
    .bind(status)
    .fetch_one(admin)
    .await
    .unwrap()
}

fn runner(app: &PgPool) -> Runner {
    let registry = Registry::builtin(&BTreeSet::new()).unwrap();
    let ev = Arc::new(Evaluator::new(
        app.clone(),
        Arc::new(registry),
        EvaluatorOptions::default(),
    ));
    Runner::new(
        app.clone(),
        ev,
        RunnerConfig {
            debounce: Duration::ZERO,
            ..Default::default()
        },
    )
}

#[sqlx::test(migrations = "../../migrations")]
async fn writes_mark_their_entities_dirty(admin: PgPool) {
    let org = seed_org(&admin, "acme", json!({})).await;
    put_obj(&admin, org, 1, json!({}), "2026-10-01T00:00:00Z").await;
    sqlx::query(
        "INSERT INTO links (id, organisation_id, type, src_id, dst_id, valid_from)
         VALUES ($1, $2, 'assigned_to', $3, $4, now())",
    )
    .bind(u(100))
    .bind(org)
    .bind(u(2))
    .bind(u(3))
    .execute(&admin)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO events (id, organisation_id, type, type_version, subject_id, occurred_at, payload, source)
         VALUES ($1, $2, 'touched', 1, $3, now(), '{}'::jsonb, 'test')",
    )
    .bind(u(200))
    .bind(org)
    .bind(u(4))
    .execute(&admin)
    .await
    .unwrap();
    assert_eq!(
        dirty_set(&admin, org).await,
        BTreeSet::from([u(1), u(2), u(3), u(4)])
    );

    // A second version of the same entity is still one row.
    new_version(&admin, org, 1, json!({"n": 2}), "2026-10-02T00:00:00Z").await;
    assert_eq!(dirty_set(&admin, org).await.len(), 4);
}

#[sqlx::test(migrations = "../../migrations")]
async fn claiming_waits_for_the_debounce_and_consumes(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO ontology_dirty (organisation_id, entity_id, marked_at)
         VALUES ($1, $2, now() - interval '1 hour'), ($1, $3, now())",
    )
    .bind(org)
    .bind(u(1))
    .bind(u(2))
    .execute(&admin)
    .await
    .unwrap();

    let mut c = app.acquire().await.unwrap();
    assert_eq!(
        dirty::claim(&mut c, 100, 60.0).await.unwrap(),
        vec![(org, u(1))]
    );
    assert!(
        dirty::claim(&mut c, 100, 60.0).await.unwrap().is_empty(),
        "claimed rows are consumed"
    );
    assert_eq!(
        dirty::claim(&mut c, 100, 0.0).await.unwrap(),
        vec![(org, u(2))]
    );
    assert!(dirty_set(&admin, org).await.is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_app_role_reaches_the_feed_only_through_its_functions(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = seed_org(&admin, "acme", json!({})).await;
    let other = seed_org(&admin, "other", json!({})).await;

    assert!(
        sqlx::query("SELECT 1 FROM ontology_dirty")
            .fetch_all(&app)
            .await
            .is_err()
    );

    // No tenant context: nothing is marked.
    let mut c = app.acquire().await.unwrap();
    dirty::mark(&mut c, org, &[u(1)]).await.unwrap();
    drop(c);
    assert!(dirty_set(&admin, org).await.is_empty());

    let session = |org| OrgSession {
        org_id: org,
        scope_path: None,
    };
    with_org_tx::<_, sqlx::Error, _>(&app, &session(org), |c| {
        async move { dirty::mark(c, org, &[u(1)]).await }.boxed()
    })
    .await
    .unwrap();
    assert_eq!(dirty_set(&admin, org).await, BTreeSet::from([u(1)]));

    // A session for another organisation cannot mark this one's entities.
    with_org_tx::<_, sqlx::Error, _>(&app, &session(other), |c| {
        async move { dirty::mark(c, org, &[u(2)]).await }.boxed()
    })
    .await
    .unwrap();
    assert_eq!(dirty_set(&admin, org).await, BTreeSet::from([u(1)]));
}

#[sqlx::test(migrations = "../../migrations")]
async fn only_organisations_with_generators_are_listed(admin: PgPool) {
    let app = app_pool(&admin).await;
    let on = seed_org(&admin, "on", json!({ "generators": { "x": {} } })).await;
    seed_org(&admin, "empty", json!({ "generators": {} })).await;
    seed_org(&admin, "none", json!({})).await;

    let mut c = app.acquire().await.unwrap();
    assert_eq!(
        dirty::evaluator_organisations(&mut c).await.unwrap(),
        vec![on]
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn changes_drive_the_evaluator_and_a_closed_case_clears_its_recommendation(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme").await;
    put_obj(&admin, org, 1, overdue("open"), "2026-10-01T00:00:00Z").await;
    put_obj(&admin, org, 2, overdue("open"), "2026-10-01T00:00:00Z").await;
    let runner = runner(&app);

    let first = runner.drain_once_at(now()).await.unwrap();
    assert_eq!((first.claimed, first.runs, first.failed), (2, 1, 0));
    assert_eq!(count_status(&admin, org, "pending").await, 2);

    new_version(&admin, org, 1, overdue("done"), "2026-10-11T11:00:00Z").await;
    assert_eq!(dirty_set(&admin, org).await, BTreeSet::from([u(1)]));
    let second = runner.drain_once_at(now()).await.unwrap();
    assert_eq!((second.claimed, second.runs, second.failed), (1, 1, 0));
    assert_eq!(count_status(&admin, org, "expired").await, 1);
    assert_eq!(
        count_status(&admin, org, "pending").await,
        1,
        "the other case is untouched"
    );

    let idle = runner.drain_once_at(now()).await.unwrap();
    assert_eq!(idle, Default::default());
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_failed_run_puts_its_changes_back(admin: PgPool) {
    let app = app_pool(&admin).await;
    // A misspelt key makes the whole configuration unusable.
    let org = seed_org(&admin, "bad", json!({ "genertors": {} })).await;
    put_obj(&admin, org, 1, json!({}), "2026-10-01T00:00:00Z").await;

    let r = runner(&app).drain_once_at(now()).await.unwrap();
    assert_eq!((r.claimed, r.runs, r.failed), (1, 0, 1));
    assert_eq!(dirty_set(&admin, org).await, BTreeSet::from([u(1)]));
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_tick_runs_every_configured_organisation_in_full(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme").await;
    seed_org(&admin, "idle", json!({})).await;
    put_obj(&admin, org, 1, overdue("open"), "2026-10-01T00:00:00Z").await;

    let reports = runner(&app).tick_once_at(now()).await.unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].organisation_id, org);
    assert_eq!(count_status(&admin, org, "pending").await, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn only_one_instance_leads_the_tick(admin: PgPool) {
    let app = app_pool(&admin).await;
    let first = Leader::try_acquire(&app).await.unwrap();
    assert!(first.is_some());
    assert!(Leader::try_acquire(&app).await.unwrap().is_none());
}
