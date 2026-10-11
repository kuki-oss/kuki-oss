use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use kuki_store::outbox;
use sqlx::{PgConnection, PgPool};

use crate::error::RelayError;
use crate::publisher::{Message, Publisher};

/// Advisory-lock key that elects the single active relay ("kukioutx").
pub const LEADER_LOCK_KEY: i64 = 0x6B75_6B69_6F75_7478;

#[derive(Clone, Debug)]
pub struct RelayConfig {
    pub batch_size: i32,
    /// How long a claimed row is reserved. A crashed relay's rows come back after this.
    pub lease: Duration,
    /// Sleep when there is nothing to publish.
    pub poll_interval: Duration,
    /// How often a non-leader checks whether the leader is gone.
    pub standby_interval: Duration,
    pub max_backoff: Duration,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            lease: Duration::from_secs(30),
            poll_interval: Duration::from_millis(250),
            standby_interval: Duration::from_secs(2),
            max_backoff: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Nothing was waiting.
    Idle,
    /// This many messages were acknowledged by the broker and marked delivered.
    Published(usize),
    /// The batch stopped early; the unacknowledged rows were released (or parked, if poison).
    Failed { published: usize, reason: String },
}

enum Exit {
    Shutdown,
    NotLeader,
}

pub struct Relay {
    pool: PgPool,
    publisher: Arc<dyn Publisher>,
    cfg: RelayConfig,
}

fn ids(batch: &[Message]) -> Vec<i64> {
    batch.iter().map(|m| m.id).collect()
}

impl Relay {
    pub fn new(pool: PgPool, publisher: Arc<dyn Publisher>, cfg: RelayConfig) -> Self {
        Self {
            pool,
            publisher,
            cfg,
        }
    }

    /// One claim, publish and mark cycle on the given connection.
    pub async fn step(&self, conn: &mut PgConnection) -> Result<Step, RelayError> {
        let rows = outbox::claim(conn, self.cfg.batch_size, self.cfg.lease.as_secs_f64()).await?;
        if rows.is_empty() {
            return Ok(Step::Idle);
        }
        let batch: Vec<Message> = rows.into_iter().map(Message::from).collect();

        let out = self.publisher.publish(&batch).await;
        if out.acked > 0 {
            outbox::mark_published(conn, &ids(&batch[..out.acked])).await?;
        }
        let Some(failure) = out.failure else {
            return Ok(Step::Published(out.acked));
        };

        // Everything after the acknowledged prefix goes back, so the failed row is retried
        // before anything newer and per-key order holds.
        let mut rest = &batch[out.acked..];
        if !failure.retriable
            && let Some((head, tail)) = rest.split_first()
        {
            outbox::mark_dead(conn, head.id, &failure.reason).await?;
            tracing::error!(outbox_id = head.id, reason = %failure.reason, "outbox message parked as dead");
            rest = tail;
        }
        if !rest.is_empty() {
            outbox::release(conn, &ids(rest)).await?;
        }
        Ok(Step::Failed {
            published: out.acked,
            reason: failure.reason,
        })
    }

    /// Runs until `shutdown` resolves. Only one relay at a time publishes; the others stand by.
    pub async fn run<F: Future<Output = ()>>(&self, shutdown: F) -> Result<(), RelayError> {
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            match self.lead(&mut shutdown).await {
                Ok(Exit::Shutdown) => return Ok(()),
                Ok(Exit::NotLeader) => {}
                Err(e) => tracing::warn!(error = %e, "outbox relay stopped, retrying"),
            }
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tokio::time::sleep(self.cfg.standby_interval) => {}
            }
        }
    }

    async fn lead<F: Future<Output = ()>>(
        &self,
        shutdown: &mut Pin<&mut F>,
    ) -> Result<Exit, RelayError> {
        // Detached: the advisory lock belongs to this session, so the connection must never go
        // back to the pool. Dropping it closes the socket, which releases the lock.
        let mut conn = self.pool.acquire().await?.detach();
        let leader: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(LEADER_LOCK_KEY)
            .fetch_one(&mut conn)
            .await?;
        if !leader {
            return Ok(Exit::NotLeader);
        }
        tracing::info!("outbox relay is the leader");

        let mut failures = 0u32;
        loop {
            let wait = match self.step(&mut conn).await? {
                Step::Published(_) => {
                    failures = 0;
                    Duration::ZERO
                }
                Step::Idle => {
                    failures = 0;
                    self.cfg.poll_interval
                }
                Step::Failed { published, reason } => {
                    failures = failures.saturating_add(1);
                    let backoff =
                        Duration::from_secs(1u64 << failures.min(5)).min(self.cfg.max_backoff);
                    tracing::warn!(published, %reason, backoff_secs = backoff.as_secs(), "outbox publish failed");
                    backoff
                }
            };
            tokio::select! {
                biased;
                _ = &mut *shutdown => return Ok(Exit::Shutdown),
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }
}
