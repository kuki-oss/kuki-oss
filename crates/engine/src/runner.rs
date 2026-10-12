//! The evaluator service. Two loops share one `Evaluator`:
//!
//! * drain: claims entities that have been dirty for at least the debounce, coalesces them per
//!    organisation (a very large set becomes a full run) and runs the evaluator for each. If a run
//!    fails outright the entities are put back.
//!  * tick: every `tick`, runs every configured organisation in full. This is the backstop for
//!    anything the drain loses (a crash after a claim, a generator that errored) and what makes
//!    time-driven conditions, like a deadline passing, fire at all. Only one instance ticks at a
//!    time (advisory-lock leader); any number may drain.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures::FutureExt;
use kuki_extension::Trigger;
use kuki_store::{
    dirty,
    session::{OrgSession, with_org_tx},
};
use sqlx::{PgConnection, PgPool};
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::evaluator::{EngineError, Evaluator, RunReport};

pub const TOPIC_CHANGES: &str = "ontology.changes";
pub const TOPIC_SCHEDULE: &str = "schedule";

/// Distinct from the outbox relay's lock.
const LEADER_LOCK_KEY: i64 = 0x6B75_6B69_6F65_7601;

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error("{0}")]
    Config(String),
    #[error("{0} not found")]
    NotFound(String),
}

#[derive(Clone, Debug)]
pub struct RunnerConfig {
    /// How long an entity must have been dirty before it is picked up, so a bulk load settles
    /// first and becomes one run.
    pub debounce: Duration,
    /// Pause between drains when nothing was claimed.
    pub poll: Duration,
    /// Interval of the full scheduled run.
    pub tick: Duration,
    pub claim_batch: i32,
    /// More changed entities than this in one organisation becomes a full run.
    pub max_entities: usize,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(2000),
            poll: Duration::from_millis(500),
            tick: Duration::from_secs(300),
            claim_batch: 5000,
            max_entities: 500,
        }
    }
}

impl RunnerConfig {
    /// `KUKI_EVALUATE_DEBOUNCE_MS` (2000), `KUKI_EVALUATE_POLL_MS` (500, at least 10) and
    /// `KUKI_EVALUATE_TICK_SECS` (300, at least 1).
    pub fn from_env() -> Result<Self, RunnerError> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, RunnerError> {
        let num = |name: &str, default: u64, min: u64| -> Result<u64, RunnerError> {
            let Some(v) = get(name) else {
                return Ok(default);
            };
            let n: u64 = v
                .trim()
                .parse()
                .map_err(|_| RunnerError::Config(format!("{name} must be a whole number")))?;
            if n < min {
                return Err(RunnerError::Config(format!(
                    "{name} must be at least {min}"
                )));
            }
            Ok(n)
        };
        Ok(Self {
            debounce: Duration::from_millis(num("KUKI_EVALUATE_DEBOUNCE_MS", 2000, 0)?),
            poll: Duration::from_millis(num("KUKI_EVALUATE_POLL_MS", 500, 10)?),
            tick: Duration::from_secs(num("KUKI_EVALUATE_TICK_SECS", 300, 1)?),
            ..Self::default()
        })
    }
}

/// Groups claimed `(organisation, entity)` rows per organisation, ordered by organisation. An
/// empty entity list means "everything" (the trigger convention): used when the set is larger
/// than `max_entities`.
pub fn coalesce(rows: &[(Uuid, Uuid)], max_entities: usize) -> Vec<(Uuid, Vec<Uuid>)> {
    let mut per_org: BTreeMap<Uuid, BTreeSet<Uuid>> = BTreeMap::new();
    for (org, entity) in rows {
        per_org.entry(*org).or_default().insert(*entity);
    }
    per_org
        .into_iter()
        .map(|(org, set)| {
            if set.len() > max_entities {
                (org, Vec::new())
            } else {
                (org, set.into_iter().collect())
            }
        })
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DrainReport {
    /// Distinct dirty entities taken from the feed.
    pub claimed: usize,
    pub runs: usize,
    /// Runs that failed outright; their entities went back on the feed.
    pub failed: usize,
}

/// The one instance that runs the scheduled tick. Holds a Postgres advisory lock on a connection
/// of its own; if the connection dies the lock is released and another instance takes over.
pub struct Leader {
    conn: PgConnection,
}

impl Leader {
    pub async fn try_acquire(pool: &PgPool) -> Result<Option<Leader>, sqlx::Error> {
        // Detached: the lock belongs to this session, so it must never go back to the pool.
        let mut conn = pool.acquire().await?.detach();
        let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(LEADER_LOCK_KEY)
            .fetch_one(&mut conn)
            .await?;
        Ok(got.then_some(Leader { conn }))
    }

    pub async fn alive(&mut self) -> bool {
        sqlx::query("SELECT 1")
            .execute(&mut self.conn)
            .await
            .is_ok()
    }
}

pub struct Runner {
    pool: PgPool,
    evaluator: Arc<Evaluator>,
    cfg: RunnerConfig,
}

fn log_report(r: &RunReport, topic: &str) {
    for g in &r.generators {
        match &g.error {
            Some(e) => {
                tracing::warn!(org = %r.organisation_id, generator = %g.generator_id, topic, error = %e, "generator failed")
            }
            None => tracing::debug!(
                org = %r.organisation_id, generator = %g.generator_id, topic,
                created = g.created, revised = g.revised, unchanged = g.unchanged, cleared = g.cleared,
                deferred = g.deferred_by_budget, rejected = g.rejected.len(), "generator ran"
            ),
        }
    }
}

/// Sleeps for `d` or until shutdown; true when it is time to stop.
async fn stopped_after(rx: &mut watch::Receiver<bool>, d: Duration) -> bool {
    if *rx.borrow() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(d) => *rx.borrow(),
        _ = rx.changed() => true,
    }
}

impl Runner {
    pub fn new(pool: PgPool, evaluator: Arc<Evaluator>, cfg: RunnerConfig) -> Self {
        Self {
            pool,
            evaluator,
            cfg,
        }
    }

    /// One pass over the change feed.
    pub async fn drain_once_at(&self, now: DateTime<Utc>) -> Result<DrainReport, RunnerError> {
        let rows = {
            let mut c = self.pool.acquire().await?;
            dirty::claim(
                &mut c,
                self.cfg.claim_batch,
                self.cfg.debounce.as_secs_f64(),
            )
            .await?
        };
        let mut report = DrainReport {
            claimed: rows.len(),
            ..Default::default()
        };

        for (org, entities) in coalesce(&rows, self.cfg.max_entities) {
            let trigger = Trigger {
                organisation_id: org,
                topic: TOPIC_CHANGES.into(),
                entities: entities.clone(),
            };
            match self.evaluator.run(&trigger, now).await {
                Ok(r) => {
                    report.runs += 1;
                    log_report(&r, TOPIC_CHANGES);
                }
                Err(e) => {
                    report.failed += 1;
                    tracing::warn!(%org, error = %e, "evaluation failed; putting the changes back");
                    // A full run (empty list) has nothing to put back: the next tick covers it.
                    if !entities.is_empty() {
                        self.remark(org, &entities).await;
                    }
                }
            }
        }
        Ok(report)
    }

    async fn remark(&self, org: Uuid, entities: &[Uuid]) {
        let session = OrgSession {
            org_id: org,
            scope_path: None,
        };
        let ids = entities.to_vec();
        let res = with_org_tx::<_, sqlx::Error, _>(&self.pool, &session, move |c| {
            async move { dirty::mark(c, org, &ids).await }.boxed()
        })
        .await;
        if let Err(e) = res {
            tracing::warn!(%org, error = %e, "could not put changes back; the next tick will cover them");
        }
    }

    /// One full run of every configured organisation.
    pub async fn tick_once_at(&self, now: DateTime<Utc>) -> Result<Vec<RunReport>, RunnerError> {
        let orgs = {
            let mut c = self.pool.acquire().await?;
            dirty::evaluator_organisations(&mut c).await?
        };
        let mut reports = Vec::new();
        for org in orgs {
            let trigger = Trigger {
                organisation_id: org,
                topic: TOPIC_SCHEDULE.into(),
                entities: vec![],
            };
            match self.evaluator.run(&trigger, now).await {
                Ok(r) => {
                    log_report(&r, TOPIC_SCHEDULE);
                    reports.push(r);
                }
                Err(e) => tracing::warn!(%org, error = %e, "scheduled evaluation failed"),
            }
        }
        Ok(reports)
    }

    async fn drain_loop(&self, mut stop: watch::Receiver<bool>) {
        loop {
            match self.drain_once_at(Utc::now()).await {
                Ok(r) if r.claimed > 0 => {
                    if *stop.borrow() {
                        return;
                    }
                }
                Ok(_) => {
                    if stopped_after(&mut stop, self.cfg.poll).await {
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "draining the change feed failed");
                    if stopped_after(&mut stop, Duration::from_secs(5)).await {
                        return;
                    }
                }
            }
        }
    }

    async fn tick_loop(&self, mut stop: watch::Receiver<bool>) {
        let mut leader: Option<Leader> = None;
        loop {
            if let Some(l) = leader.as_mut()
                && !l.alive().await {
                    tracing::warn!("lost the scheduler lock");
                    leader = None;
                }
            if leader.is_none() {
                match Leader::try_acquire(&self.pool).await {
                    Ok(Some(l)) => {
                        tracing::info!("this instance runs the scheduled evaluation");
                        leader = Some(l);
                    }
                    Ok(None) => {}
                    Err(e) => tracing::warn!(error = %e, "could not try for the scheduler lock"),
                }
            }
            if leader.is_some() {
                match self.tick_once_at(Utc::now()).await {
                    Ok(r) => tracing::info!(organisations = r.len(), "scheduled evaluation done"),
                    Err(e) => tracing::warn!(error = %e, "scheduled evaluation failed"),
                }
            }
            if stopped_after(&mut stop, self.cfg.tick).await {
                return;
            }
        }
    }

    /// Runs both loops until `shutdown` resolves.
    pub async fn run<F: Future<Output = ()>>(self: Arc<Self>, shutdown: F) {
        let (tx, rx) = watch::channel(false);
        let stop = async move {
            shutdown.await;
            let _ = tx.send(true);
        };
        tokio::join!(stop, self.drain_loop(rx.clone()), self.tick_loop(rx));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn changes_are_grouped_per_organisation_without_duplicates() {
        let rows = [(u(1), u(11)), (u(1), u(10)), (u(2), u(10)), (u(1), u(10))];
        assert_eq!(
            coalesce(&rows, 100),
            vec![(u(1), vec![u(10), u(11)]), (u(2), vec![u(10)])]
        );
        assert!(coalesce(&[], 100).is_empty());
    }

    #[test]
    fn a_large_set_becomes_a_full_run() {
        let rows = [(u(1), u(10)), (u(1), u(11)), (u(2), u(10))];
        assert_eq!(
            coalesce(&rows, 1),
            vec![(u(1), vec![]), (u(2), vec![u(10)])]
        );
    }

    #[test]
    fn config_defaults_and_overrides() {
        let d = RunnerConfig::from_lookup(|_| None).unwrap();
        assert_eq!(
            (d.debounce, d.poll, d.tick),
            (
                Duration::from_millis(2000),
                Duration::from_millis(500),
                Duration::from_secs(300)
            )
        );
        let c = RunnerConfig::from_lookup(|k| {
            (k == "KUKI_EVALUATE_TICK_SECS").then(|| "60".to_owned())
        })
        .unwrap();
        assert_eq!(c.tick, Duration::from_secs(60));
    }

    #[test]
    fn nonsense_config_is_refused() {
        for (k, v) in [
            ("KUKI_EVALUATE_TICK_SECS", "0"),
            ("KUKI_EVALUATE_POLL_MS", "1"),
            ("KUKI_EVALUATE_DEBOUNCE_MS", "soon"),
        ] {
            assert!(
                RunnerConfig::from_lookup(|name| (name == k).then(|| v.to_owned())).is_err(),
                "{k}={v}"
            );
        }
    }
}
