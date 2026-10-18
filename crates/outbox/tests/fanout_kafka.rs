use std::time::Duration;

use futures::FutureExt;
use kuki_outbox::{
    ConsumerConfig, FanoutConsumer, KafkaConfig, KafkaPublisher, Message, Publisher, ensure_topics,
};
use kuki_store::session::{OrgSession, with_org_tx};
use kuki_store::webhook_subscriptions as ws;
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

const CHANGED: &str = "recommendation.status_changed";

#[sqlx::test(migrations = "../../migrations")]
async fn events_flow_from_kafka_to_the_delivery_queue(admin: PgPool) {
    let Ok(brokers) = std::env::var("KUKI_TEST_KAFKA_BROKERS") else {
        eprintln!("KUKI_TEST_KAFKA_BROKERS is not set; skipping");
        return;
    };

    let org = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO organisations (id, slug, name, config) VALUES ($1, 'a', 'a', '{}'::jsonb)",
    )
    .bind(org)
    .execute(&admin)
    .await
    .unwrap();
    let app = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect_with((*admin.connect_options()).clone())
        .await
        .unwrap();
    with_org_tx::<_, sqlx::Error, _>(
        &app,
        &OrgSession {
            org_id: org,
            scope_path: None,
        },
        move |c| {
            async move {
                ws::upsert(
                    c,
                    org,
                    "ops",
                    "https://example.org/h",
                    "kuki/webhooks/acme/events",
                    &[CHANGED.to_string()],
                    true,
                )
                .await
                .map(|_| ())
            }
            .boxed()
        },
    )
    .await
    .unwrap();

    // A topic and group of its own, so nothing else on the broker interferes.
    let topic = format!("kuki.test.fanout.{}", Uuid::new_v4().simple());
    let kafka = KafkaConfig {
        brokers,
        client_id: "kuki-test".into(),
        partitions: 1,
        replication: 1,
    };
    ensure_topics(&kafka, &[topic.as_str()]).await.unwrap();

    let event = |version: i32| json!({ "type": CHANGED, "id": "r1", "version": version });
    let message = |id: i64, payload: Value| Message {
        id,
        organisation_id: org,
        topic: topic.clone(),
        key: "r1".into(),
        payload,
        attempts: 0,
    };
    // A good event, a payload that is no event at all, the first event again (the relay is at
    // least once), and a second good event.
    let batch = [
        message(101, event(2)),
        message(102, json!("not an event")),
        message(101, event(2)),
        message(103, event(3)),
    ];
    let published = KafkaPublisher::connect(&kafka)
        .unwrap()
        .publish(&batch)
        .await;
    assert_eq!(published.acked, 4);
    assert!(published.failure.is_none());

    let cfg = ConsumerConfig {
        group_id: format!("kuki-test-{}", Uuid::new_v4()),
        topic: topic.clone(),
        from_start: true,
    };
    let consumer = FanoutConsumer::connect(&kafka, &cfg, app.clone()).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        consumer
            .run(async {
                let _ = stopped.await;
            })
            .await
    });

    // Everything is handled once 103 (the last message) has been queued.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let ids = loop {
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT event_id FROM webhook_deliveries ORDER BY event_id")
                .fetch_all(&admin)
                .await
                .unwrap();
        if ids.len() >= 2 || tokio::time::Instant::now() > deadline {
            break ids;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();

    // The junk payload was skipped, the replay added nothing, the good events were queued.
    assert_eq!(ids, ["101", "103"]);
}
