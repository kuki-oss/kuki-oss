use std::sync::Arc;
use std::time::Duration;

use kuki_outbox::{KafkaConfig, KafkaPublisher, Relay, RelayConfig, Step, ensure_topics};
use kuki_store::outbox;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test(migrations = "../../migrations")]
async fn rows_reach_kafka_in_key_order_with_their_headers(pool: PgPool) {
    let brokers = std::env::var("KUKI_TEST_KAFKA_BROKERS")
        .expect("set KUKI_TEST_KAFKA_BROKERS, for example localhost:9092");
    let cfg = KafkaConfig {
        brokers: brokers.clone(),
        client_id: "kuki-test".into(),
        partitions: 3,
        replication: 1,
    };
    let topic = format!("kuki.test.{}", Uuid::new_v4().simple());
    ensure_topics(&cfg, &[&topic]).await.unwrap();
    ensure_topics(&cfg, &[&topic]).await.unwrap(); // idempotent

    let org = Uuid::new_v4();
    let mut c = pool.acquire().await.unwrap();
    let mut outbox_ids = vec![];
    for n in 0..6 {
        let key = if n % 2 == 0 { "rec-a" } else { "rec-b" };
        outbox_ids.push(
            outbox::enqueue(&mut c, org, &topic, key, &json!({ "n": n }))
                .await
                .unwrap(),
        );
    }

    let relay = Relay::new(
        pool.clone(),
        Arc::new(KafkaPublisher::connect(&cfg).unwrap()),
        RelayConfig::default(),
    );
    assert_eq!(relay.step(&mut c).await.unwrap(), Step::Published(6));

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("kuki-test-{}", Uuid::new_v4()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&[&topic]).unwrap();

    // (key, n, outbox id header, organisation header, partition)
    let mut got: Vec<(String, i64, String, String, i32)> = vec![];
    while got.len() < 6 {
        let m = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("timed out waiting for Kafka")
            .unwrap();
        let header = |name: &str| {
            let h = m.headers().expect("headers");
            (0..h.count())
                .map(|i| h.get(i))
                .find(|h| h.key == name)
                .and_then(|h| h.value.map(|v| String::from_utf8(v.to_vec()).unwrap()))
                .unwrap_or_else(|| panic!("missing header {name}"))
        };
        let payload: serde_json::Value = serde_json::from_slice(m.payload().unwrap()).unwrap();
        got.push((
            String::from_utf8(m.key().unwrap().to_vec()).unwrap(),
            payload["n"].as_i64().unwrap(),
            header("kuki-outbox-id"),
            header("kuki-organisation-id"),
            m.partition(),
        ));
    }

    for key in ["rec-a", "rec-b"] {
        let mine: Vec<_> = got.iter().filter(|g| g.0 == key).collect();
        let n: Vec<i64> = mine.iter().map(|g| g.1).collect();
        let mut sorted = n.clone();
        sorted.sort_unstable();
        assert_eq!(n, sorted, "{key}: events for one key arrive in order");
        assert_eq!(n.len(), 3);
        assert!(
            mine.iter().all(|g| g.4 == mine[0].4),
            "{key}: one key, one partition"
        );
    }
    assert!(got.iter().all(|g| g.3 == org.to_string()));
    let mut headers: Vec<String> = got.iter().map(|g| g.2.clone()).collect();
    headers.sort();
    let mut want: Vec<String> = outbox_ids.iter().map(i64::to_string).collect();
    want.sort();
    assert_eq!(headers, want, "every message carries its outbox id");
}
