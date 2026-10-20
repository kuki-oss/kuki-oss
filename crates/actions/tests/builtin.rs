//! The executor with the builtin router in front: `builtin:` actions run inside Kuki, and
//! nothing in that namespace can be reached by, or fall through to, a webhook.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use kuki_actions::{Adapter, BuiltinRouter, Config, Executor, Job, Outcome, Report, StaticRouter};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

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

/// An accepted recommendation whose action is `action_type`.
async fn seed_rec(admin: &PgPool, org: Uuid, n: u128, action_type: &str) {
    let body = json!({
        "decision_type": "work.escalate",
        "dedup_key": format!("k{n}"),
        "org_unit_id": null,
        "action": { "proposed": true, "action_type": action_type,
                    "payload": { "objectId": "o-1" },
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
         VALUES ($1, $2, 'work.escalate', $3, 'accepted', 0.5, 0.9, 'Case overdue', $4, $5,
                 'user:amina')",
    )
    .bind(u(n))
    .bind(org)
    .bind(format!("k{n}"))
    .bind(body)
    .bind(provenance)
    .execute(admin)
    .await
    .unwrap();
}

/// The input an accept queues: what the executor hands the adapter.
fn input(rec: u128, action_type: &str) -> Value {
    json!({
        "recommendationId": u(rec),
        "decisionType": "work.escalate",
        "actionType": action_type,
        "payload": { "objectId": "o-1" },
        "overridden": false
    })
}

async fn queue(admin: &PgPool, org: Uuid, rec: u128, exec: u128, action_type: &str, input: Value) {
    sqlx::query(
        "INSERT INTO action_executions
           (id, organisation_id, recommendation_id, action_type, input, idempotency_key,
            status, adapter, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, 'queued', 'unrouted', 'user:amina')",
    )
    .bind(u(exec))
    .bind(org)
    .bind(u(rec))
    .bind(action_type)
    .bind(input)
    .bind(format!("k{exec}"))
    .execute(admin)
    .await
    .unwrap();
}

/// An accepted recommendation with one queued execution of `action_type`.
async fn ready(admin: &PgPool, org: Uuid, action_type: &str) {
    seed_rec(admin, org, 10, action_type).await;
    queue(admin, org, 10, 100, action_type, input(10, action_type)).await;
}

#[derive(Debug)]
struct Exec {
    status: String,
    adapter: String,
    result: Option<Value>,
}

async fn exec(admin: &PgPool, n: u128) -> Exec {
    let r: (String, String, Option<Value>) =
        sqlx::query_as("SELECT status, adapter, result FROM action_executions WHERE id = $1")
            .bind(u(n))
            .fetch_one(admin)
            .await
            .unwrap();
    Exec {
        status: r.0,
        adapter: r.1,
        result: r.2,
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

/// Stands in for a webhook endpoint: succeeds, and remembers every job it was given.
struct Spy {
    seen: Mutex<Vec<Job>>,
}

impl Spy {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::default(),
        })
    }
}

impl Adapter for Spy {
    fn label(&self) -> String {
        "test:notify".into()
    }

    fn run<'a>(&'a self, job: &'a Job) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(job.clone());
            Outcome::Succeeded(json!({ "delivered": true }))
        })
    }
}

fn executor(pool: &PgPool, fallback: StaticRouter) -> Executor {
    let router = BuiltinRouter::new(Arc::new(fallback));
    let config = Config {
        backoff_base: Duration::ZERO,
        ..Config::default()
    };
    Executor::new(pool.clone(), Arc::new(router), config).unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn flag_review_runs_inside_kuki_and_completes_the_recommendation(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, "builtin:flag_review").await;
    let x = executor(&app_pool(&admin).await, StaticRouter::new());

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
        (e.status.as_str(), e.adapter.as_str()),
        ("succeeded", "builtin:flag_review")
    );
    let result = e.result.unwrap();
    assert_eq!(result["flagged"], true);
    assert_eq!(result["recommendationId"], json!(u(10)));
    assert_eq!(result["executionId"], json!(u(100)));
    assert_eq!(result["approvedBy"], "user:amina");
    assert_eq!(result["payload"], json!({ "objectId": "o-1" }));

    // The decision stays attributed to the person who made it.
    assert_eq!(
        rec(&admin, 10).await,
        ("executed".to_owned(), Some("user:amina".to_owned()))
    );
    assert_eq!(announced(&admin, org).await, ["executing", "executed"]);
    assert_eq!(x.tick().await.unwrap().claimed, 0, "nothing is left to do");
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_unknown_builtin_fails_visibly_and_never_reaches_the_fallback(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, "builtin:nope").await;
    // A fallback that would happily run it, were the namespace not reserved.
    let spy = Spy::new();
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with("builtin:nope", spy.clone()),
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
        spy.seen.lock().unwrap().is_empty(),
        "the fallback must not run"
    );
    let e = exec(&admin, 100).await;
    assert_eq!(
        (e.status.as_str(), e.adapter.as_str()),
        ("failed", "unrouted")
    );
    assert_eq!(
        e.result.unwrap()["error"],
        "no adapter is registered for action type builtin:nope"
    );
    assert_eq!(rec(&admin, 10).await.0, "failed");
}

#[sqlx::test(migrations = "../../migrations")]
async fn other_action_types_still_reach_the_registered_route(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    ready(&admin, org, "work.escalate.notify").await;
    let spy = Spy::new();
    let x = executor(
        &app_pool(&admin).await,
        StaticRouter::new().with("work.escalate.notify", spy.clone()),
    );

    assert_eq!(x.tick().await.unwrap().succeeded, 1);

    assert_eq!(spy.seen.lock().unwrap().len(), 1);
    assert_eq!(exec(&admin, 100).await.adapter, "test:notify");
    assert_eq!(rec(&admin, 10).await.0, "executed");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_flag_review_without_its_recommendation_id_fails_for_good(admin: PgPool) {
    let org = seed_org(&admin, 1).await;
    seed_rec(&admin, org, 10, "builtin:flag_review").await;
    // The input is malformed, so retrying can never help.
    queue(
        &admin,
        org,
        10,
        100,
        "builtin:flag_review",
        json!({ "payload": {}, "overridden": false }),
    )
    .await;
    let x = executor(&app_pool(&admin).await, StaticRouter::new());

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
        ("failed", "builtin:flag_review")
    );
    assert!(
        e.result.unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("recommendationId")
    );
    assert_eq!(rec(&admin, 10).await.0, "failed");
}
