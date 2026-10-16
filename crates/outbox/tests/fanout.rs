use futures::FutureExt;
use kuki_outbox::{Fanout, Handled, Incoming, Skip};
use kuki_store::session::{OrgSession, with_org_tx};
use kuki_store::webhook_subscriptions as ws;
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

const URL: &str = "https://example.org/hooks/events";
const REF: &str = "kuki/webhooks/acme/events";
const CHANGED: &str = "recommendation.status_changed";

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
    sqlx::query("INSERT INTO organisations (id, slug, name, config) VALUES ($1, $2, $2, '{}'::jsonb)")
        .bind(id)
        .bind(slug)
        .execute(admin)
        .await
        .unwrap();
    id
}

async fn subscribe(app: &PgPool, org: Uuid, name: &str, event_types: &[&str]) {
    let name = name.to_owned();
    let types: Vec<String> = event_types.iter().map(|s| s.to_string()).collect();
    with_org_tx::<_, sqlx::Error, _>(
        app,
        &OrgSession { org_id: org, scope_path: None },
        move |c| {
            async move { ws::upsert(c, org, &name, URL, REF, &types, true).await.map(|_| ()) }.boxed()
        },
    )
    .await
    .unwrap();
}

fn body(event_type: &str, version: i32) -> Vec<u8> {
    serde_json::to_vec(&json!({ "type": event_type, "id": "r1", "version": version })).unwrap()
}

async fn queued(admin: &PgPool) -> Vec<(Uuid, String)> {
    sqlx::query_as("SELECT organisation_id, event_id FROM webhook_deliveries ORDER BY event_id")
        .fetch_all(admin)
        .await
        .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_event_is_queued_once_per_matching_subscription_and_a_replay_adds_nothing(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let app = app_pool(&admin).await;
    subscribe(&app, a, "one", &[CHANGED]).await;
    subscribe(&app, a, "two", &[CHANGED]).await;
    let fanout = Fanout::new(app.clone());

    let (a_str, b_str) = (a.to_string(), b.to_string());
    let payload = body(CHANGED, 2);
    fn make_event<'a>(org: &'a str, payload: &'a [u8]) -> Incoming<'a> {
        Incoming {
            outbox_id: Some("10"),
            organisation_id: Some(org),
            payload: Some(payload),
        }
    }
    // SAFETY of borrows: `event` is only called while `payload` is alive.
    assert_eq!(fanout.handle(&make_event(&a_str, &payload)).await.unwrap(), Handled::Queued(2));
    // Kafka redelivers the message: nothing more is queued.
    assert_eq!(fanout.handle(&make_event(&a_str, &payload)).await.unwrap(), Handled::Queued(0));
    // The same outbox id for an organisation without subscriptions queues nothing.
    assert_eq!(fanout.handle(&make_event(&b_str, &payload)).await.unwrap(), Handled::Queued(0));

    let rows = queued(&admin).await;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(org, id)| *org == a && id == "10"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_event_type_nobody_subscribed_to_queues_nothing(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    subscribe(&app, org, "ops", &[CHANGED]).await;
    let org_str = org.to_string();
    let payload = body("recommendation.created", 1);
    let m = Incoming { outbox_id: Some("1"), organisation_id: Some(&org_str), payload: Some(&payload) };

    let handled = Fanout::new(app).handle(&m).await.unwrap();
    assert_eq!(handled, Handled::Queued(0));
    assert!(queued(&admin).await.is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
async fn unusable_messages_are_skipped_without_failing_or_queueing(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    subscribe(&app, org, "ops", &[CHANGED]).await;
    let fanout = Fanout::new(app);
    let org_str = org.to_string();
    let good = body(CHANGED, 1);
    let junk: Value = json!("not an event");
    let junk = serde_json::to_vec(&junk).unwrap();

    let cases = [
        Incoming { outbox_id: None, organisation_id: Some(&org_str), payload: Some(&good) },
        Incoming { outbox_id: Some("1"), organisation_id: None, payload: Some(&good) },
        Incoming { outbox_id: Some("1"), organisation_id: Some("nope"), payload: Some(&good) },
        Incoming { outbox_id: Some("1"), organisation_id: Some(&org_str), payload: None },
        Incoming { outbox_id: Some("1"), organisation_id: Some(&org_str), payload: Some(&junk) },
    ];
    for m in cases {
        let handled = fanout.handle(&m).await.unwrap();
        assert!(matches!(handled, Handled::Skipped(_)), "{m:?}");
    }
    assert!(matches!(
        fanout
            .handle(&Incoming { outbox_id: None, organisation_id: Some(&org_str), payload: Some(&good) })
            .await
            .unwrap(),
        Handled::Skipped(Skip::MissingHeader("kuki-outbox-id"))
    ));
    assert!(queued(&admin).await.is_empty());
}
