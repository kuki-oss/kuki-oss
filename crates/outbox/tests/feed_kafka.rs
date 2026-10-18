use std::time::Duration;

use kuki_outbox::{EventFeed, KafkaConfig, KafkaPublisher, Message, Publisher, ensure_topics};
use serde_json::json;
use tokio::sync::watch;
use uuid::Uuid;

#[tokio::test]
async fn kafka_events_reach_a_receiver() {
    let Ok(brokers) = std::env::var("KUKI_TEST_KAFKA_BROKERS") else {
        eprintln!("KUKI_TEST_KAFKA_BROKERS is not set; skipping");
        return;
    };
    let topic = format!("kuki.test.feed.{}", Uuid::new_v4().simple());
    let kafka = KafkaConfig {
        brokers,
        client_id: "kuki-test".into(),
        partitions: 1,
        replication: 1,
    };
    ensure_topics(&kafka, &[topic.as_str()]).await.unwrap();

    let (stop, stopped) = watch::channel(false);
    let (feed, task) = EventFeed::start(&kafka, &topic, 16, stopped).unwrap();
    let mut rx = feed.subscribe();

    let org = Uuid::new_v4();
    let publisher = KafkaPublisher::connect(&kafka).unwrap();
    let message = Message {
        id: 7,
        organisation_id: org,
        topic: topic.clone(),
        key: "r1".into(),
        payload: json!({ "type": "recommendation.status_changed", "id": "r1", "version": 2 }),
        attempts: 0,
    };

    // The group joins asynchronously and starts at "latest", so publish until one arrives.
    let got = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            publisher.publish(std::slice::from_ref(&message)).await;
            if let Ok(Ok(e)) = tokio::time::timeout(Duration::from_millis(1500), rx.recv()).await {
                return e;
            }
        }
    })
    .await
    .expect("no event arrived");

    assert_eq!(got.event_id, "7");
    assert_eq!(got.organisation_id, org);
    assert_eq!(got.event_type, "recommendation.status_changed");

    stop.send(true).unwrap();
    task.await.unwrap();
}
