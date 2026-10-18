use futures::FutureExt;
use kuki_store::session::{OrgSession, with_org_tx};
use kuki_store::{webhook_deliveries as wd, webhook_subscriptions as ws};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

const URL: &str = "https://example.org/hooks/events";
const REF: &str = "kuki/webhooks/acme/events";
const CHANGED: &str = "recommendation.status_changed";
const CREATED: &str = "recommendation.created";
const UPDATED: &str = "recommendation.updated";

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

async fn seed_org(admin: &PgPool, slug: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO organisations (id, slug, name, config) VALUES ($1, $2, $2, '{}'::jsonb)",
    )
    .bind(id)
    .bind(slug)
    .execute(admin)
    .await
    .unwrap();
    id
}

fn session(org: Uuid) -> OrgSession {
    OrgSession {
        org_id: org,
        scope_path: None,
    }
}

fn types(t: &[&str]) -> Vec<String> {
    t.iter().map(|s| s.to_string()).collect()
}

fn payload(version: i32) -> Value {
    json!({ "type": CHANGED, "id": "r1", "version": version,
            "status": "accepted", "previousStatus": "pending" })
}

async fn subscribe(
    app: &PgPool,
    org: Uuid,
    name: &str,
    t: &[&str],
    enabled: bool,
) -> ws::Subscription {
    let (name, t) = (name.to_owned(), types(t));
    with_org_tx::<_, sqlx::Error, _>(app, &session(org), move |c| {
        async move { ws::upsert(c, org, &name, URL, REF, &t, enabled).await }.boxed()
    })
    .await
    .unwrap()
}

async fn fan_out(app: &PgPool, org: Uuid, event_id: &str, event_type: &str, p: Value) -> usize {
    let (e, t) = (event_id.to_owned(), event_type.to_owned());
    with_org_tx::<_, sqlx::Error, _>(app, &session(org), move |c| {
        async move { wd::enqueue_for_event(c, org, &e, &t, &p).await }.boxed()
    })
    .await
    .unwrap()
}

async fn claim(app: &PgPool, limit: i32, lease: f64) -> Vec<wd::Claimed> {
    let mut c = app.acquire().await.unwrap();
    wd::claim(&mut c, limit, lease).await.unwrap()
}

fn events(rows: &[wd::Claimed]) -> Vec<String> {
    let mut v: Vec<String> = rows.iter().map(|r| r.event_id.clone()).collect();
    v.sort();
    v
}

#[sqlx::test(migrations = "../../migrations")]
async fn subscriptions_match_on_event_type_and_enabled(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    subscribe(&app, org, "all", &[CREATED, UPDATED, CHANGED], true).await;
    subscribe(&app, org, "status", &[CHANGED], true).await;
    subscribe(&app, org, "off", &[CHANGED], false).await;

    let (changed, created) = with_org_tx::<_, sqlx::Error, _>(&app, &session(org), move |c| {
        async move {
            let names =
                |v: Vec<ws::Subscription>| v.into_iter().map(|s| s.name).collect::<Vec<_>>();
            Ok((
                names(ws::matching(c, org, CHANGED).await?),
                names(ws::matching(c, org, CREATED).await?),
            ))
        }
        .boxed()
    })
    .await
    .unwrap();
    assert_eq!(changed, ["all", "status"]);
    assert_eq!(created, ["all"]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn upsert_replaces_in_place_and_keeps_the_id(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let first = subscribe(&app, org, "ops", &[CHANGED], true).await;
    let second = subscribe(&app, org, "ops", &[CREATED, CHANGED], false).await;
    assert_eq!(first.id, second.id);
    assert_eq!(second.event_types, [CREATED, CHANGED]);
    assert!(!second.enabled);
}

#[sqlx::test(migrations = "../../migrations")]
async fn tenants_cannot_see_or_remove_each_others_subscriptions(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let app = app_pool(&admin).await;
    subscribe(&app, a, "ops", &[CHANGED], true).await;

    let (seen, removed) = with_org_tx::<_, sqlx::Error, _>(&app, &session(b), move |c| {
        async move { Ok((ws::list(c, a).await?, ws::remove(c, a, "ops").await?)) }.boxed()
    })
    .await
    .unwrap();
    assert!(seen.is_empty());
    assert!(!removed);

    let still = with_org_tx::<_, sqlx::Error, _>(&app, &session(a), move |c| {
        async move { ws::list(c, a).await }.boxed()
    })
    .await
    .unwrap();
    assert_eq!(still.len(), 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_database_rejects_bad_subscriptions(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let cases: Vec<(&str, &str, &str, Vec<String>)> = vec![
        ("Bad Name", URL, REF, types(&[CHANGED])),
        ("ok", "ftp://example.org/x", REF, types(&[CHANGED])),
        ("ok", URL, "../etc/passwd", types(&[CHANGED])),
        ("ok", URL, "/leading", types(&[CHANGED])),
        ("ok", URL, REF, vec![]),
        ("ok", URL, REF, types(&["recommendation.deleted"])),
    ];
    for (name, url, secret_ref, t) in cases {
        let (name, url, secret_ref) = (name.to_owned(), url.to_owned(), secret_ref.to_owned());
        let r = with_org_tx::<_, sqlx::Error, _>(&app, &session(org), move |c| {
            async move {
                ws::upsert(c, org, &name, &url, &secret_ref, &t, true)
                    .await
                    .map(|_| ())
            }
            .boxed()
        })
        .await;
        assert!(r.is_err());
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_event_fans_out_once_per_matching_subscription(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let app = app_pool(&admin).await;
    subscribe(&app, a, "one", &[CHANGED], true).await;
    subscribe(&app, a, "two", &[CHANGED], true).await;
    subscribe(&app, a, "created-only", &[CREATED], true).await;

    assert_eq!(fan_out(&app, a, "10", CHANGED, payload(2)).await, 2);
    // Kafka redelivers the same event: nothing more is queued.
    assert_eq!(fan_out(&app, a, "10", CHANGED, payload(2)).await, 0);
    // A different event queues again; an event nobody wants, or an organisation with no
    // subscriptions, queues nothing.
    assert_eq!(fan_out(&app, a, "11", CHANGED, payload(3)).await, 2);
    assert_eq!(fan_out(&app, a, "12", UPDATED, payload(4)).await, 0);
    assert_eq!(fan_out(&app, b, "10", CHANGED, payload(2)).await, 0);

    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM webhook_deliveries")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(n, 4);
}

#[sqlx::test(migrations = "../../migrations")]
async fn claims_are_limited_leased_and_cross_organisation(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let app = app_pool(&admin).await;
    subscribe(&app, a, "ops", &[CHANGED], true).await;
    subscribe(&app, b, "ops", &[CHANGED], true).await;
    fan_out(&app, a, "e1", CHANGED, payload(1)).await;
    fan_out(&app, a, "e2", CHANGED, payload(2)).await;
    fan_out(&app, b, "e3", CHANGED, payload(1)).await;

    // Oldest first, limited, with the subscription's address and secret reference.
    let first = claim(&app, 2, 60.0).await;
    assert_eq!(events(&first), ["e1", "e2"]);
    assert!(
        first
            .iter()
            .all(|r| r.attempts == 1 && r.url == URL && r.secret_ref == REF)
    );
    assert!(first.iter().all(|r| r.organisation_id == a));

    // The rest, from another organisation, while the first two are under a live lease.
    let second = claim(&app, 10, 60.0).await;
    assert_eq!(events(&second), ["e3"]);
    assert_eq!(second[0].organisation_id, b);
    assert!(claim(&app, 10, 60.0).await.is_empty());

    // Leases that have run out are taken over, so delivery is at least once.
    let again = claim(&app, 10, 0.0).await;
    assert_eq!(events(&again), ["e1", "e2", "e3"]);
    assert!(again.iter().all(|r| r.attempts == 2));
}

#[sqlx::test(migrations = "../../migrations")]
async fn disabled_subscriptions_are_not_delivered_until_re_enabled(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let sub = subscribe(&app, org, "ops", &[CHANGED], true).await;
    fan_out(&app, org, "e1", CHANGED, payload(1)).await;

    sqlx::query("UPDATE webhook_subscriptions SET enabled = false WHERE id = $1")
        .bind(sub.id)
        .execute(&admin)
        .await
        .unwrap();
    assert!(claim(&app, 10, 60.0).await.is_empty());

    sqlx::query("UPDATE webhook_subscriptions SET enabled = true WHERE id = $1")
        .bind(sub.id)
        .execute(&admin)
        .await
        .unwrap();
    assert_eq!(events(&claim(&app, 10, 60.0).await), ["e1"]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn finish_and_retry_only_act_on_a_running_row_of_the_named_organisation(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let app = app_pool(&admin).await;
    subscribe(&app, a, "ops", &[CHANGED], true).await;
    fan_out(&app, a, "e1", CHANGED, payload(1)).await;

    let mut c = app.acquire().await.unwrap();
    let row = wd::claim(&mut c, 10, 60.0).await.unwrap().remove(0);

    // Another organisation cannot settle it.
    assert!(
        !wd::finish(&mut c, b, row.id, wd::Ending::Delivered, None)
            .await
            .unwrap()
    );
    assert!(!wd::retry(&mut c, b, row.id, "boom", 0.0).await.unwrap());

    // A retry puts it back, hidden until its delay passes.
    assert!(
        wd::retry(&mut c, a, row.id, "HTTP 503", 3600.0)
            .await
            .unwrap()
    );
    assert!(wd::claim(&mut c, 10, 60.0).await.unwrap().is_empty());
    sqlx::query("UPDATE webhook_deliveries SET not_before = now() WHERE id = $1")
        .bind(row.id)
        .execute(&admin)
        .await
        .unwrap();
    let again = wd::claim(&mut c, 10, 60.0).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempts, 2);

    // Delivered once; a second settlement finds nothing running.
    assert!(
        wd::finish(&mut c, a, row.id, wd::Ending::Delivered, None)
            .await
            .unwrap()
    );
    assert!(
        !wd::finish(&mut c, a, row.id, wd::Ending::Delivered, None)
            .await
            .unwrap()
    );
    let (status, finished, last_error): (String, bool, Option<String>) = sqlx::query_as(
        "SELECT status, finished_at IS NOT NULL, last_error FROM webhook_deliveries WHERE id = $1",
    )
    .bind(row.id)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(status, "delivered");
    assert!(finished);
    assert_eq!(last_error.as_deref(), Some("HTTP 503"));

    // Giving up parks it as dead, with the reason.
    fan_out(&app, a, "e2", CHANGED, payload(2)).await;
    let dead = wd::claim(&mut c, 10, 60.0).await.unwrap().remove(0);
    assert!(
        wd::finish(
            &mut c,
            a,
            dead.id,
            wd::Ending::Dead,
            Some("gave up after 8 attempts")
        )
        .await
        .unwrap()
    );
    let (status, last_error): (String, Option<String>) =
        sqlx::query_as("SELECT status, last_error FROM webhook_deliveries WHERE id = $1")
            .bind(dead.id)
            .fetch_one(&admin)
            .await
            .unwrap();
    assert_eq!(status, "dead");
    assert_eq!(last_error.as_deref(), Some("gave up after 8 attempts"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_app_role_cannot_modify_deliveries_directly(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    subscribe(&app, org, "ops", &[CHANGED], true).await;
    fan_out(&app, org, "e1", CHANGED, payload(1)).await;

    for sql in [
        "UPDATE webhook_deliveries SET status = 'delivered'",
        "DELETE FROM webhook_deliveries",
    ] {
        assert!(sqlx::query(sql).execute(&app).await.is_err(), "{sql}");
    }
}
