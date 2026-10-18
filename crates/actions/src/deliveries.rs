//! Delivers recommendation events to subscribed receivers.
//!
//! Claims queued `webhook_deliveries`, signs each event with the subscription's
//! secret (read from OpenBao through the same `SecretSource` as the action adapter)
//! and POSTs it.
//!
//! Request: the event payload as JSON, with
//!   Kuki-Signature: t=<unix>,v1=<hex>   (see kuki_outbox::signing; 300 s replay window)
//!   Kuki-Event-Id: <outbox id>          stable across retries: dedupe on it
//!   Kuki-Event-Type, Kuki-Organisation-Id, Kuki-Attempt
//! Retries can reorder events, so receivers order by the payload's `version`.
//!
//! 2xx delivers. Transient failures (network, 408/425/429/5xx) back off and retry.
//! Permanent failures (other statuses, an unusable secret) and exhausted retries park
//! the row as `dead`.

use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::FutureExt;
use kuki_store::webhook_deliveries::{self as store, Claimed, Ending};
use sqlx::PgPool;
use tokio::sync::watch;

use crate::{
    adapter::Outcome,
    secrets::SecretSource,
    webhook::{Target, WebhookConfig, client, post_signed},
};

#[derive(Clone, Debug)]
pub struct DeliveryConfig {
    /// Deliveries claimed per tick, and sent concurrently.
    pub batch: i32,
    /// How long a claim is held. A worker that dies mid-delivery is taken over after this.
    pub lease: Duration,
    /// Longest one delivery may take. Must be shorter than the lease.
    pub attempt_timeout: Duration,
    pub max_attempts: i32,
    /// Delay after the first failed attempt; doubles each time, up to `backoff_cap`.
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
    /// Pause between polls when nothing was due.
    pub idle: Duration,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            batch: 32,
            lease: Duration::from_secs(120),
            attempt_timeout: Duration::from_secs(30),
            max_attempts: 12,
            backoff_base: Duration::from_secs(10),
            backoff_cap: Duration::from_secs(3600),
            idle: Duration::from_secs(1),
        }
    }
}

impl DeliveryConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch < 1 {
            return Err("batch must be at least 1".into());
        }
        if self.max_attempts < 1 {
            return Err("max_attempts must be at least 1".into());
        }
        if self.attempt_timeout.is_zero() {
            return Err("attempt_timeout must be positive".into());
        }
        if self.attempt_timeout >= self.lease {
            return Err(
                "attempt_timeout must be shorter than the lease, or a slow receiver is called twice"
                    .into(),
            );
        }
        Ok(())
    }

    fn backoff(&self, attempt: i32) -> f64 {
        let doublings = attempt.saturating_sub(1).clamp(0, 30);
        (self.backoff_base.as_secs_f64() * 2f64.powi(doublings)).min(self.backoff_cap.as_secs_f64())
    }
}

/// What one [`DeliveryWorker::tick`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeliveryReport {
    pub claimed: usize,
    pub delivered: usize,
    /// Parked: a permanent failure, or the retries ran out.
    pub dead: usize,
    pub retried: usize,
    /// Settled by someone else first (its lease had expired, or the subscription was removed).
    pub lost: usize,
    /// A database error: the lease will expire and the delivery will be taken again.
    pub errored: usize,
}

enum Step {
    Delivered,
    Dead,
    Retried,
    Lost,
}

pub struct DeliveryWorker {
    pool: PgPool,
    http: reqwest::Client,
    secrets: Arc<dyn SecretSource>,
    allow_insecure: bool,
    config: DeliveryConfig,
}

impl DeliveryWorker {
    /// `pool` connects as the application role.
    pub fn new(
        pool: PgPool,
        secrets: Arc<dyn SecretSource>,
        webhook: &WebhookConfig,
        config: DeliveryConfig,
    ) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            pool,
            http: client(webhook)?,
            secrets,
            allow_insecure: webhook.allow_insecure_http,
            config,
        })
    }

    /// Claims a batch and delivers it.
    pub async fn tick(&self) -> Result<DeliveryReport, sqlx::Error> {
        let claimed = {
            let mut c = self.pool.acquire().await?;
            store::claim(&mut c, self.config.batch, self.config.lease.as_secs_f64()).await?
        };
        let mut report = DeliveryReport {
            claimed: claimed.len(),
            ..DeliveryReport::default()
        };
        let steps = futures::future::join_all(claimed.into_iter().map(|d| async move {
            let id = d.id;
            (id, self.process(d).await)
        }))
        .await;
        for (id, step) in steps {
            match step {
                Ok(Step::Delivered) => report.delivered += 1,
                Ok(Step::Dead) => report.dead += 1,
                Ok(Step::Retried) => report.retried += 1,
                Ok(Step::Lost) => report.lost += 1,
                Err(e) => {
                    report.errored += 1;
                    tracing::error!(delivery = %id, error = %e, "delivering an event failed; it will be taken again when its lease expires");
                }
            }
        }
        Ok(report)
    }

    /// Polls until `stop` flips to true.
    pub async fn run(&self, mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            let idle = match self.tick().await {
                Ok(r) => {
                    if r.claimed > 0 {
                        tracing::info!(?r, "delivered events");
                    }
                    r.claimed == 0
                }
                Err(e) => {
                    tracing::error!(error = %e, "event delivery tick failed");
                    true
                }
            };
            if idle {
                tokio::select! {
                    _ = tokio::time::sleep(self.config.idle) => {}
                    _ = stop.changed() => {}
                }
            }
        }
    }

    async fn process(&self, d: Claimed) -> Result<Step, sqlx::Error> {
        let body = match serde_json::to_vec(&d.payload) {
            Ok(b) => b,
            Err(e) => {
                return self
                    .settle(
                        &d,
                        Ending::Dead,
                        Some(format!("cannot encode payload: {e}")),
                    )
                    .await;
            }
        };
        let headers = [
            ("Kuki-Event-Id", d.event_id.clone()),
            ("Kuki-Event-Type", d.event_type.clone()),
            ("Kuki-Organisation-Id", d.organisation_id.to_string()),
            ("Kuki-Attempt", d.attempts.to_string()),
        ];
        let target = Target {
            http: &self.http,
            url: &d.url,
            secret_ref: &d.secret_ref,
            secrets: self.secrets.as_ref(),
            allow_insecure: self.allow_insecure,
        };
        let run = AssertUnwindSafe(post_signed(&target, &body, &headers)).catch_unwind();
        let outcome = match tokio::time::timeout(self.config.attempt_timeout, run).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Outcome::Retry("the delivery panicked".into()),
            Err(_) => Outcome::Retry(format!(
                "the receiver did not answer within {} seconds",
                self.config.attempt_timeout.as_secs()
            )),
        };

        match outcome {
            Outcome::Succeeded(_) => self.settle(&d, Ending::Delivered, None).await,
            Outcome::Failed(error) => self.settle(&d, Ending::Dead, Some(error)).await,
            Outcome::Retry(error) => self.retry_or_park(&d, error).await,
        }
    }

    async fn settle(
        &self,
        d: &Claimed,
        ending: Ending,
        error: Option<String>,
    ) -> Result<Step, sqlx::Error> {
        let mut c = self.pool.acquire().await?;
        let done = store::finish(&mut c, d.organisation_id, d.id, ending, error.as_deref()).await?;
        if done && ending == Ending::Dead {
            tracing::warn!(
                delivery = %d.id,
                subscription = %d.subscription_id,
                event = %d.event_id,
                error = error.as_deref().unwrap_or(""),
                "gave up delivering an event"
            );
        }
        Ok(match (done, ending) {
            (false, _) => Step::Lost,
            (true, Ending::Delivered) => Step::Delivered,
            (true, Ending::Dead) => Step::Dead,
        })
    }

    async fn retry_or_park(&self, d: &Claimed, error: String) -> Result<Step, sqlx::Error> {
        if d.attempts >= self.config.max_attempts {
            let why = format!("gave up after {} attempts: {error}", d.attempts);
            return self.settle(d, Ending::Dead, Some(why)).await;
        }
        let mut c = self.pool.acquire().await?;
        let queued = store::retry(
            &mut c,
            d.organisation_id,
            d.id,
            &error,
            self.config.backoff(d.attempts),
        )
        .await?;
        Ok(if queued { Step::Retried } else { Step::Lost })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_is_capped() {
        let c = DeliveryConfig {
            backoff_base: Duration::from_secs(10),
            backoff_cap: Duration::from_secs(100),
            ..DeliveryConfig::default()
        };
        let got: Vec<f64> = (1..=6).map(|attempt| c.backoff(attempt)).collect();
        assert_eq!(got, [10.0, 20.0, 40.0, 80.0, 100.0, 100.0]);
    }

    #[test]
    fn configuration_rules() {
        assert!(DeliveryConfig::default().validate().is_ok());
        let bad = [
            DeliveryConfig {
                batch: 0,
                ..DeliveryConfig::default()
            },
            DeliveryConfig {
                max_attempts: 0,
                ..DeliveryConfig::default()
            },
            DeliveryConfig {
                attempt_timeout: Duration::ZERO,
                ..DeliveryConfig::default()
            },
            // A timeout as long as the lease would let a slow receiver be called twice.
            DeliveryConfig {
                attempt_timeout: Duration::from_secs(120),
                ..DeliveryConfig::default()
            },
        ];
        for c in bad {
            assert!(c.validate().is_err(), "{c:?}");
        }
    }
}
