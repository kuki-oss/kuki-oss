use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::FutureExt;
use kuki_store::executions::{self, Claimed, Final, UNROUTED};
use kuki_store::recommendations::{self as store, Transition, TransitionError};
use kuki_store::session::{OrgSession, with_org_tx};
use kuki_types::RecommendationStatus as Status;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::watch;

use crate::adapter::{Job, Outcome, Router};

/// The actor recorded on the outbox events of the recommendation's execution transitions.
pub const ACTOR: &str = "system:executor";

#[derive(Clone, Debug)]
pub struct Config {
    /// Executions claimed per tick, and run concurrently.
    pub batch: i32,
    /// How long a claim is held. A worker that dies mid-run is taken over after this.
    pub lease: Duration,
    /// Longest an adapter may take. Must be shorter than the lease.
    pub attempt_timeout: Duration,
    pub max_attempts: i32,
    /// Delay after the first failed attempt; doubles each time, up to `backoff_cap`.
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
    /// Pause between polls when nothing was due.
    pub idle: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            batch: 16,
            lease: Duration::from_secs(120),
            attempt_timeout: Duration::from_secs(30),
            max_attempts: 5,
            backoff_base: Duration::from_secs(5),
            backoff_cap: Duration::from_secs(3600),
            idle: Duration::from_secs(1),
        }
    }
}

impl Config {
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
                "attempt_timeout must be shorter than the lease, or a slow adapter runs twice"
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

/// What one [`Executor::tick`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Report {
    pub claimed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub retried: usize,
    /// The execution was settled by someone else first (its lease had expired).
    pub lost: usize,
    /// A database error: the lease will expire and the execution will be taken again.
    pub errored: usize,
}

enum Step {
    Succeeded,
    Failed,
    Retried,
    Lost,
}

enum Begin {
    Go,
    Refused(String),
}

pub struct Executor {
    pool: PgPool,
    router: Arc<dyn Router>,
    config: Config,
}

impl Executor {
    /// `pool` connects as the application role.
    pub fn new(pool: PgPool, router: Arc<dyn Router>, config: Config) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            pool,
            router,
            config,
        })
    }

    /// Claims a batch and runs it to completion.
    pub async fn tick(&self) -> Result<Report, sqlx::Error> {
        let claimed = {
            let mut c = self.pool.acquire().await?;
            executions::claim(&mut c, self.config.batch, self.config.lease.as_secs_f64()).await?
        };
        let mut report = Report {
            claimed: claimed.len(),
            ..Report::default()
        };
        let steps = futures::future::join_all(claimed.into_iter().map(|job| async move {
            let id = job.id;
            (id, self.process(job).await)
        }))
        .await;
        for (id, step) in steps {
            match step {
                Ok(Step::Succeeded) => report.succeeded += 1,
                Ok(Step::Failed) => report.failed += 1,
                Ok(Step::Retried) => report.retried += 1,
                Ok(Step::Lost) => report.lost += 1,
                Err(e) => {
                    report.errored += 1;
                    tracing::error!(execution = %id, error = %e, "running an execution failed; it will be taken again when its lease expires");
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
                        tracing::info!(?r, "ran executions");
                    }
                    r.claimed == 0
                }
                Err(e) => {
                    tracing::error!(error = %e, "executor tick failed");
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

    async fn process(&self, job: Claimed) -> Result<Step, sqlx::Error> {
        // The recommendation is now being executed (a repeat attempt finds it already so).
        if let Begin::Refused(why) = self.begin(&job).await? {
            let result = json!({ "error": why, "attempts": job.attempts });
            return self
                .settle(&job, &job.adapter, Final::Failed, result, None)
                .await;
        }

        let adapter = match self
            .router
            .route(job.organisation_id, &job.action_type)
            .await
        {
            Ok(Some(a)) => a,
            Ok(None) => {
                let result = json!({
                    "error": format!("no adapter is registered for action type {}", job.action_type),
                    "attempts": job.attempts,
                });
                return self
                    .settle(&job, UNROUTED, Final::Failed, result, Some(Status::Failed))
                    .await;
            }
            Err(e) => {
                return self
                    .retry_or_fail(&job, &job.adapter, format!("routing failed: {e}"))
                    .await;
            }
        };

        let label = adapter.label();
        let view = Job {
            id: job.id,
            organisation_id: job.organisation_id,
            recommendation_id: job.recommendation_id,
            action_type: job.action_type.clone(),
            input: job.input.clone(),
            attempt: job.attempts,
            created_by: job.created_by.clone(),
        };
        let run = AssertUnwindSafe(async { adapter.run(&view).await }).catch_unwind();
        let outcome = match tokio::time::timeout(self.config.attempt_timeout, run).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Outcome::Retry("the adapter panicked".into()),
            Err(_) => Outcome::Retry(format!(
                "the adapter did not answer within {} seconds",
                self.config.attempt_timeout.as_secs()
            )),
        };

        match outcome {
            Outcome::Succeeded(result) => {
                self.settle(
                    &job,
                    &label,
                    Final::Succeeded,
                    result,
                    Some(Status::Executed),
                )
                .await
            }
            Outcome::Failed(error) => {
                let result = json!({ "error": error, "attempts": job.attempts });
                self.settle(&job, &label, Final::Failed, result, Some(Status::Failed))
                    .await
            }
            Outcome::Retry(error) => self.retry_or_fail(&job, &label, error).await,
        }
    }

    async fn begin(&self, job: &Claimed) -> Result<Begin, sqlx::Error> {
        let session = OrgSession {
            org_id: job.organisation_id,
            scope_path: None,
        };
        let (org, rec) = (job.organisation_id, job.recommendation_id);
        let now = Utc::now();
        with_org_tx::<_, sqlx::Error, _>(&self.pool, &session, |c| {
            async move {
                let t = Transition {
                    id: rec,
                    to: Status::Executing,
                    expected_version: None,
                    actor: ACTOR,
                    resolution: None,
                };
                Ok(match store::transition(c, org, &t, now).await {
                    Ok(_) => Begin::Go,
                    Err(TransitionError::Db(e)) => return Err(e),
                    Err(TransitionError::NotFound) => {
                        Begin::Refused("the recommendation no longer exists".into())
                    }
                    Err(TransitionError::Illegal { from, .. }) => Begin::Refused(format!(
                        "the recommendation is {} and cannot be executed",
                        from.as_str()
                    )),
                    Err(e) => Begin::Refused(e.to_string()),
                })
            }
            .boxed()
        })
        .await
    }

    /// Ends the execution and, in the same transaction, moves its recommendation to `record`.
    async fn settle(
        &self,
        job: &Claimed,
        adapter: &str,
        outcome: Final,
        result: Value,
        record: Option<Status>,
    ) -> Result<Step, sqlx::Error> {
        let session = OrgSession {
            org_id: job.organisation_id,
            scope_path: None,
        };
        let (org, exec, rec) = (job.organisation_id, job.id, job.recommendation_id);
        let adapter = adapter.to_owned();
        let now = Utc::now();
        let finished = with_org_tx::<_, sqlx::Error, _>(&self.pool, &session, |c| {
            async move {
                if !executions::finish(c, org, exec, outcome, &adapter, &result).await? {
                    return Ok(false);
                }
                if let Some(to) = record {
                    let t = Transition { id: rec, to, expected_version: None, actor: ACTOR, resolution: None };
                    match store::transition(c, org, &t, now).await {
                        Ok(_) => {}
                        Err(TransitionError::Db(e)) => return Err(e),
                        // The execution row is the record of what happened.
                        Err(e) => tracing::warn!(execution = %exec, error = %e, "the recommendation could not follow its execution"),
                    }
                }
                Ok(true)
            }
            .boxed()
        })
        .await?;
        Ok(match (finished, outcome) {
            (false, _) => Step::Lost,
            (true, Final::Succeeded) => Step::Succeeded,
            (true, Final::Failed) => Step::Failed,
        })
    }

    async fn retry_or_fail(
        &self,
        job: &Claimed,
        adapter: &str,
        error: String,
    ) -> Result<Step, sqlx::Error> {
        if job.attempts >= self.config.max_attempts {
            let result = json!({
                "error": format!("gave up after {} attempts: {error}", job.attempts),
                "attempts": job.attempts,
            });
            return self
                .settle(job, adapter, Final::Failed, result, Some(Status::Failed))
                .await;
        }
        let session = OrgSession {
            org_id: job.organisation_id,
            scope_path: None,
        };
        let (org, id) = (job.organisation_id, job.id);
        let delay = self.config.backoff(job.attempts);
        let adapter = adapter.to_owned();
        let queued = with_org_tx::<_, sqlx::Error, _>(&self.pool, &session, |c| {
            async move { executions::retry(c, org, id, &adapter, &error, delay).await }.boxed()
        })
        .await?;
        Ok(if queued { Step::Retried } else { Step::Lost })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_is_capped() {
        let c = Config {
            backoff_base: Duration::from_secs(5),
            backoff_cap: Duration::from_secs(60),
            ..Config::default()
        };
        let got: Vec<f64> = (1..=6).map(|attempt| c.backoff(attempt)).collect();
        assert_eq!(got, [5.0, 10.0, 20.0, 40.0, 60.0, 60.0]);
    }

    #[test]
    fn configuration_rules() {
        assert!(Config::default().validate().is_ok());
        let bad = [
            Config {
                batch: 0,
                ..Config::default()
            },
            Config {
                max_attempts: 0,
                ..Config::default()
            },
            Config {
                attempt_timeout: Duration::ZERO,
                ..Config::default()
            },
            // A timeout as long as the lease would let a slow adapter run twice.
            Config {
                attempt_timeout: Duration::from_secs(120),
                ..Config::default()
            },
        ];
        for c in bad {
            assert!(c.validate().is_err(), "{c:?}");
        }
    }
}
