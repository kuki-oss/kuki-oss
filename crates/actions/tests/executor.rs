use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use kuki_actions::{Adapter, Config, Executor, Job, Outcome, Report, StaticRouter};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

const TYPE: &str = "work.escalate.notify";

fn u(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

/// The unprivileged role, so row-level security is really in force.
async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|c, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(c).await?;
                Ok(())
            })
        })
        .connect_with((*admin.connect_options()).clone())
        .await
        .unwrap()
}

async fn seed_org(admin: &PgPool, n: u128) -> Uuid {
    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, $2, $2)")
        .bind(u(n))
        .bind(format!("org{n}"))
        .execute(admin)
        .await
        .unwrap();
    u(n)
}

async fn seed_rec(admin: &PgPool, org: Uuid, n: u128, status: &str) {
    let body = json!({
        "decision_type": "work.escalate",
        "dedup_key": format!("k{n}"),
        "org_unit_id": null,
        "action": { "proposed": true, "action_type": TYPE, "payload": { "object_id": "o-1" },
                    "payload_schema_ref": "kuki.base/work.escalate.json" },
        "rationale": { "summary": "Case overdue",
                       "factors": [{ "label": "hours overdue", "value": 5, "evidence": [] }],
                       "confidence": 0.9, "confidence_kind": "data_quality" },
        "urgency": 0.5, "impact": null, "evidence": []
    });
    let provenance = json!({
        "generator_id": "deadline.at_risk.t0", "generator_version": "1.0.0", "tier": "T0",
        "pack": "kuki.base", "ontology_version": 1, "config_hash": "abc",
        "as_of": "2026-10-01T09:59:00Z", "input_digest": "sha256:def"
    });
    sqlx::query(
        "INSERT INTO recommendations
           (id, organisation_id, decision_type, dedup_key, status, urgency, confidence,
            rationale_summary, body, provenance, resolved_by)
         VALUES ($1, $2, 'work.escalate', $3, $4, 0.5, 0.9, 'Case overdue', $5, $6, 'user:amina')",
    )
    .bind(u(n))
    .bind(org)
    .bind(format!("k{n}"))
    .bind(status)
    .bind(body)
    .bind(provenance)
    .execute(admin)
    .await
    .unwrap();
}

async fn queue(admin: &PgPool, org: Uuid, rec: u128, exec: u128) {
    sqlx::query(
        "INSERT INTO action_executions
           (id, organisation_id, recommendation_id, action_type, input, idempotency_key,
            status, adapter, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, 'queued', 'unrouted', 'user:amina')",
    )
    .bind(u(exec))
    .bind(org)
    .bind(u(rec))
    .bind(TYPE)
    .bind(json!({ "payload": { "object_id": "o-1" }, "overridden": false }))
    .bind(format!("k{exec}"))
    .execute(admin)
    .await
    .unwrap();
}

/// An accepted recommendation with one queued execution.
async fn ready(admin: &PgPool, org: Uuid, rec: u128, exec: u128) {
    seed_rec(admin, org, rec, "accepted").await;
    queue(admin, org, rec, exec).await;
}

#[derive(Debug)]
struct Exec {
    status: String,
    adapter: String,
    attempts: i32,
    result: Option<Value>,
    last_error: Option<String>,
    finished: bool,
}

async fn exec(admin: &PgPool, n: u128) -> Exec {
    let r: (String, String, i32, Option<Value>, Option<String>, bool) = sqlx::query_as(
        "SELECT status, adapter, attempts, result, last_error, finished_at IS NOT NULL
           FROM action_executions WHERE id = $1",
    )
    .bind(u(n))
    .fetch_one(admin)
    .await
    .unwrap();
    Exec {
        status: r.0,
        adapter: r.1,
        attempts: r.2,
        result: r.3,
        last_error: r.4,
        finished: r.5,
    }
}

/// Status and who resolved it.
async fn rec(admin: &PgPool, n: u128) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, resolved_by FROM recommendations WHERE id = $1")
        .bind(u(n))
        .fetch_one(admin)
        .await
        .unwrap()
}

async fn announced(admin: &PgPool, org: Uuid) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT payload->>'status' FROM outbox WHERE organisation_id = $1 ORDER BY id",
    )
    .bind(org)
    .fetch_all(admin)
    .await
    .unwrap()
}

/// Answers from a script and remembers what it was asked.
struct Script {
    outcomes: Mutex<VecDeque<Outcome>>,
    seen: Mutex<Vec<Job>>,
}

impl Script {
    fn new(outcomes: impl IntoIterator<Item = Outcome>) -> Arc<Self> {
        Arc::new(Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            seen: Mutex::default(),
        })
    }
}

impl Adapter for Script {
    fn label(&self) -> String {
        "test:notify".into()
    }

    fn run<'a>(&'a self, job: &'a Job) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(job.clone());
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Outcome::Retry("script exhausted".into()))
        })
    }
}

struct Boom;

impl Adapter for Boom {
    fn label(&self) -> String {
        "test:boom".into()
    }

    fn run<'a>(&'a self, job: &'a Job) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            // Always true; written so the compiler cannot see the panic is unconditional.
            if job.attempt > 0 {
                panic!("adapter exploded");
            }
            Outcome::Succeeded(Value::Null)
        })
    }
}

fn executor(pool: &PgPool, router: StaticRouter, config: Config) -> Executor {
    Executor::new(pool.clone(), Arc::new(router), config).unwrap()
}

fn immediate() -> Config {
    Config {
        backoff_base: Duration::ZERO,
        ..Config::default()
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_queued_execution_runs_and_completes_the_recommendation(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, 10, 100).await;
    let script = Script::new([Outcome::Succeeded(json!({ "delivered": true }))]);
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with(TYPE, script.clone()),
        immediate(),
    );

    assert_eq!(
        x.tick().await.unwrap(),
        Report {
            claimed: 1,
            succeeded: 1,
            ..Report::default()
        }
    );

    let e = exec(&admin, 100).await;
    assert_eq!(
        (e.status.as_str(), e.adapter.as_str(), e.attempts),
        ("succeeded", "test:notify", 1)
    );
    assert_eq!(e.result, Some(json!({ "delivered": true })));
    assert!(e.finished);

    // The decision stays attributed to the person who made it.
    assert_eq!(
        rec(&admin, 10).await,
        ("executed".to_owned(), Some("user:amina".to_owned()))
    );
    assert_eq!(announced(&admin, org).await, ["executing", "executed"]);

    {
        let seen = script.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            (seen[0].id, seen[0].attempt, seen[0].action_type.as_str()),
            (u(100), 1, TYPE)
        );
        assert_eq!(seen[0].created_by, "user:amina");
        assert_eq!(seen[0].input["payload"], json!({ "object_id": "o-1" }));
    }
    assert_eq!(x.tick().await.unwrap().claimed, 0, "nothing is left to do");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_permanent_failure_fails_the_execution_and_the_recommendation(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, 10, 100).await;
    let script = Script::new([Outcome::Failed("the target rejected the payload".into())]);
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with(TYPE, script),
        immediate(),
    );

    assert_eq!(
        x.tick().await.unwrap(),
        Report {
            claimed: 1,
            failed: 1,
            ..Report::default()
        }
    );

    let e = exec(&admin, 100).await;
    assert_eq!(e.status, "failed");
    assert_eq!(
        e.result,
        Some(json!({ "error": "the target rejected the payload", "attempts": 1 }))
    );
    assert_eq!(rec(&admin, 10).await.0, "failed");
    assert_eq!(announced(&admin, org).await, ["executing", "failed"]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn transient_failures_are_retried_and_then_given_up_on(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, 10, 100).await;
    let script = Script::new([
        Outcome::Retry("503".into()),
        Outcome::Retry("503".into()),
        Outcome::Retry("503".into()),
    ]);
    let config = Config {
        max_attempts: 3,
        ..immediate()
    };
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with(TYPE, script.clone()),
        config,
    );

    for attempt in 1..=2 {
        assert_eq!(
            x.tick().await.unwrap(),
            Report {
                claimed: 1,
                retried: 1,
                ..Report::default()
            }
        );
        let e = exec(&admin, 100).await;
        assert_eq!(
            (e.status.as_str(), e.attempts, e.last_error.as_deref()),
            ("queued", attempt, Some("503"))
        );
        assert_eq!(
            rec(&admin, 10).await.0,
            "executing",
            "a retry does not end the recommendation"
        );
    }

    assert_eq!(
        x.tick().await.unwrap(),
        Report {
            claimed: 1,
            failed: 1,
            ..Report::default()
        }
    );
    let e = exec(&admin, 100).await;
    assert_eq!((e.status.as_str(), e.attempts), ("failed", 3));
    assert_eq!(e.result.unwrap()["error"], "gave up after 3 attempts: 503");
    assert_eq!(rec(&admin, 10).await.0, "failed");
    assert_eq!(
        script
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|j| j.attempt)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_retry_waits_out_its_backoff(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, 10, 100).await;
    let script = Script::new([Outcome::Retry("503".into()), Outcome::Succeeded(json!({}))]);
    let config = Config {
        backoff_base: Duration::from_secs(3600),
        ..Config::default()
    };
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with(TYPE, script),
        config,
    );

    assert_eq!(x.tick().await.unwrap().retried, 1);
    assert_eq!(x.tick().await.unwrap().claimed, 0, "not due for an hour");
    assert_eq!(exec(&admin, 100).await.attempts, 1);

    sqlx::query("UPDATE action_executions SET not_before = now() - interval '1 second'")
        .execute(&admin)
        .await
        .unwrap();
    assert_eq!(
        x.tick().await.unwrap(),
        Report {
            claimed: 1,
            succeeded: 1,
            ..Report::default()
        }
    );
    let e = exec(&admin, 100).await;
    assert_eq!((e.status.as_str(), e.attempts), ("succeeded", 2));
    assert_eq!(rec(&admin, 10).await.0, "executed");
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_unrouted_action_type_fails_visibly(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, 10, 100).await;
    let x = executor(&app_pool(&admin).await, StaticRouter::new(), immediate());

    assert_eq!(
        x.tick().await.unwrap(),
        Report {
            claimed: 1,
            failed: 1,
            ..Report::default()
        }
    );
    let e = exec(&admin, 100).await;
    assert_eq!(
        (e.status.as_str(), e.adapter.as_str()),
        ("failed", "unrouted")
    );
    assert_eq!(
        e.result.unwrap()["error"],
        format!("no adapter is registered for action type {TYPE}")
    );
    assert_eq!(rec(&admin, 10).await.0, "failed");
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_execution_whose_recommendation_cannot_run_is_failed_and_nothing_else_is_touched(
    admin: PgPool,
) {
    let org = seed_org(&admin, 1).await;
    seed_rec(&admin, org, 10, "rejected").await;
    queue(&admin, org, 10, 100).await;
    let script = Script::new([Outcome::Succeeded(json!({}))]);
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with(TYPE, script.clone()),
        immediate(),
    );

    assert_eq!(
        x.tick().await.unwrap(),
        Report {
            claimed: 1,
            failed: 1,
            ..Report::default()
        }
    );
    assert!(
        script.seen.lock().unwrap().is_empty(),
        "the adapter must not run"
    );
    let e = exec(&admin, 100).await;
    assert_eq!(e.status, "failed");
    assert!(
        e.result.unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("rejected")
    );
    assert_eq!(rec(&admin, 10).await.0, "rejected");
    assert!(announced(&admin, org).await.is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_panicking_adapter_is_treated_as_a_transient_failure(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, 10, 100).await;
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with(TYPE, Arc::new(Boom)),
        immediate(),
    );

    assert_eq!(
        x.tick().await.unwrap(),
        Report {
            claimed: 1,
            retried: 1,
            ..Report::default()
        }
    );
    let e = exec(&admin, 100).await;
    assert_eq!(
        (e.status.as_str(), e.adapter.as_str()),
        ("queued", "test:boom")
    );
    assert_eq!(e.last_error.as_deref(), Some("the adapter panicked"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn every_organisations_executions_run_under_their_own_tenant(admin: PgPool) {
    let (a, b) = (seed_org(&admin, 1).await, seed_org(&admin, 2).await);
    ready(&admin, a, 10, 100).await;
    ready(&admin, b, 20, 200).await;
    let script = Script::new([Outcome::Succeeded(json!({})), Outcome::Succeeded(json!({}))]);
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with(TYPE, script),
        immediate(),
    );

    assert_eq!(
        x.tick().await.unwrap(),
        Report {
            claimed: 2,
            succeeded: 2,
            ..Report::default()
        }
    );
    assert_eq!(rec(&admin, 10).await.0, "executed");
    assert_eq!(rec(&admin, 20).await.0, "executed");
    assert_eq!(announced(&admin, a).await, ["executing", "executed"]);
    assert_eq!(announced(&admin, b).await, ["executing", "executed"]);
}
