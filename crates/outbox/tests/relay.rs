use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kuki_outbox::{Message, PublishFailure, Published, Publisher, Relay, RelayConfig, Step};
use kuki_store::outbox::{self, TOPIC_RECOMMENDATIONS};
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Records what it was given. Optionally fails once at a position in the first batch it sees.
#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<i64>>,
    fail_once: Mutex<Option<(usize, bool)>>,
}

impl Recorder {
    fn failing_at(index: usize, retriable: bool) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::default(),
            fail_once: Mutex::new(Some((index, retriable))),
        })
    }
    fn seen(&self) -> Vec<i64> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl Publisher for Recorder {
    async fn publish(&self, batch: &[Message]) -> Published {
        let plan = self.fail_once.lock().unwrap().take();
        match plan {
            Some((i, retriable)) if i < batch.len() => {
                self.seen
                    .lock()
                    .unwrap()
                    .extend(batch[..i].iter().map(|m| m.id));
                Published {
                    acked: i,
                    failure: Some(PublishFailure {
                        reason: "scripted".into(),
                        retriable,
                    }),
                }
            }
            _ => {
                self.seen.lock().unwrap().extend(batch.iter().map(|m| m.id));
                Published {
                    acked: batch.len(),
                    failure: None,
                }
            }
        }
    }
}

async fn enqueue(pool: &PgPool, n: i64) -> i64 {
    let mut c = pool.acquire().await.unwrap();
    outbox::enqueue(
        &mut c,
        Uuid::new_v4(),
        TOPIC_RECOMMENDATIONS,
        &format!("rec-{}", n % 2),
        &json!({ "n": n }),
    )
    .await
    .unwrap()
}

async fn count(pool: &PgPool, filter: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM outbox WHERE {filter}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_batch_is_published_in_order_and_marked_delivered(pool: PgPool) {
    let mut want = vec![];
    for n in 0..5 {
        want.push(enqueue(&pool, n).await);
    }
    let rec = Arc::new(Recorder::default());
    let relay = Relay::new(pool.clone(), rec.clone(), RelayConfig::default());
    let mut c = pool.acquire().await.unwrap();

    assert_eq!(relay.step(&mut c).await.unwrap(), Step::Published(5));
    assert_eq!(rec.seen(), want);
    assert_eq!(count(&pool, "delivered_at IS NOT NULL").await, 5);
    assert_eq!(relay.step(&mut c).await.unwrap(), Step::Idle);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_failure_keeps_the_acknowledged_prefix_and_retries_the_rest_first(pool: PgPool) {
    let mut want = vec![];
    for n in 0..5 {
        want.push(enqueue(&pool, n).await);
    }
    let rec = Recorder::failing_at(2, true);
    let relay = Relay::new(pool.clone(), rec.clone(), RelayConfig::default());
    let mut c = pool.acquire().await.unwrap();

    let step = relay.step(&mut c).await.unwrap();
    assert!(
        matches!(step, Step::Failed { published: 2, .. }),
        "{step:?}"
    );
    assert_eq!(count(&pool, "delivered_at IS NOT NULL").await, 2);
    assert_eq!(
        count(&pool, "claimed_at IS NULL AND delivered_at IS NULL").await,
        3,
        "the rest were released"
    );

    assert_eq!(relay.step(&mut c).await.unwrap(), Step::Published(3));
    assert_eq!(rec.seen(), want, "every row was published once, in order");
    assert_eq!(count(&pool, "delivered_at IS NOT NULL").await, 5);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_poison_message_is_parked_and_the_rest_continue(pool: PgPool) {
    let mut want = vec![];
    for n in 0..4 {
        want.push(enqueue(&pool, n).await);
    }
    let rec = Recorder::failing_at(1, false);
    let relay = Relay::new(pool.clone(), rec.clone(), RelayConfig::default());
    let mut c = pool.acquire().await.unwrap();

    assert!(matches!(
        relay.step(&mut c).await.unwrap(),
        Step::Failed { published: 1, .. }
    ));
    let (dead, error): (i64, Option<String>) =
        sqlx::query_as("SELECT id, last_error FROM outbox WHERE dead_at IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dead, want[1]);
    assert_eq!(error.as_deref(), Some("scripted"));

    assert_eq!(relay.step(&mut c).await.unwrap(), Step::Published(2));
    assert_eq!(rec.seen(), [want[0], want[2], want[3]]);
    assert_eq!(relay.step(&mut c).await.unwrap(), Step::Idle);
}

#[sqlx::test(migrations = "../../migrations")]
async fn only_the_leader_publishes(pool: PgPool) {
    // As the real binary does: connect as the application role.
    let app = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|c, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *c).await?;
                Ok(())
            })
        })
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();

    for n in 0..20 {
        enqueue(&pool, n).await;
    }
    let cfg = RelayConfig {
        poll_interval: Duration::from_millis(20),
        standby_interval: Duration::from_millis(50),
        ..RelayConfig::default()
    };
    let (a, b) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
    let (stop_a, stop_b) = (
        tokio::sync::oneshot::channel::<()>(),
        tokio::sync::oneshot::channel::<()>(),
    );
    let ra = Relay::new(app.clone(), a.clone(), cfg.clone());
    let rb = Relay::new(app.clone(), b.clone(), cfg);
    let ta = tokio::spawn(async move {
        ra.run(async {
            let _ = stop_a.1.await;
        })
        .await
    });
    let tb = tokio::spawn(async move {
        rb.run(async {
            let _ = stop_b.1.await;
        })
        .await
    });

    let mut delivered = 0;
    for _ in 0..200 {
        delivered = count(&pool, "delivered_at IS NOT NULL").await;
        if delivered == 20 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(delivered, 20, "the leader drains the outbox");
    assert!(
        a.seen().is_empty() != b.seen().is_empty(),
        "exactly one relay published"
    );

    let _ = (stop_a.0.send(()), stop_b.0.send(()));
    ta.await.unwrap().unwrap();
    tb.await.unwrap().unwrap();
}
