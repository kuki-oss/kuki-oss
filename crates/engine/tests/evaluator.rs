use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use kuki_engine::{Evaluator, EvaluatorOptions, Registry, builtin};
use kuki_extension::{
    Capability, DecisionContext, DecisionGenerator, GenError, Proposal, ScopeSpec, Trigger,
};
use kuki_store::recommendations::{self as recs, NewRecommendation};
use kuki_types::{Candidate, ConfidenceKind, DedupKey, Factor, FactorValue, Rationale, Tier};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

fn u(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn t(s: &str) -> DateTime<Utc> {
    s.parse().unwrap()
}

async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect_with((*admin.connect_options()).clone())
        .await
        .unwrap()
}

async fn seed_org(admin: &PgPool, slug: &str, decision: Value) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO organisations (id, slug, name, config) VALUES ($1, $2, $2, $3)")
        .bind(id)
        .bind(slug)
        .bind(json!({ "decision": decision }))
        .execute(admin)
        .await
        .unwrap();
    id
}

async fn put_obj(admin: &PgPool, org: Uuid, id: u128, attrs: Value, from: &str) {
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, type, type_version, attrs, valid_from)
         VALUES ($1, $2, 'case', 1, $3, $4::text::timestamptz)",
    )
    .bind(u(id))
    .bind(org)
    .bind(attrs)
    .bind(from)
    .execute(admin)
    .await
    .unwrap();
}

/// Closes the current version and opens a new one at `at`.
async fn new_version(admin: &PgPool, org: Uuid, id: u128, attrs: Value, at: &str) {
    sqlx::query(
        "UPDATE objects SET valid_to = $3::text::timestamptz
          WHERE organisation_id = $1 AND id = $2 AND valid_to IS NULL",
    )
    .bind(org)
    .bind(u(id))
    .bind(at)
    .execute(admin)
    .await
    .unwrap();
    put_obj(admin, org, id, attrs, at).await;
}

async fn count(admin: &PgPool, sql: &str, org: Uuid) -> i64 {
    sqlx::query_scalar(sql)
        .bind(org)
        .fetch_one(admin)
        .await
        .unwrap()
}

fn now() -> DateTime<Utc> {
    t("2026-10-11T12:00:00Z")
}

fn case_attrs(status: &str, hours_from_now: i64) -> Value {
    json!({ "status": status, "due_at": (now() + Duration::hours(hours_from_now)).to_rfc3339() })
}

/// An organisation with `deadline.at_risk.t0` configured for `case` objects.
async fn deadline_org(admin: &PgPool, slug: &str, mode: &str, budget: u32, install: bool) -> Uuid {
    let org = seed_org(
        admin,
        slug,
        json!({
            "review_budget": { "per_org_unit_per_day": budget },
            "generators": { "deadline.at_risk.t0": { "mode": mode, "params": {
                "object_type": "case", "due_attr": "due_at", "status_attr": "status",
                "open_statuses": ["open"], "horizon_hours": 72 } } }
        }),
    )
    .await;
    if install {
        let mut c = admin.acquire().await.unwrap();
        builtin::install_decision_types(&mut c, org).await.unwrap();
    }
    org
}

fn trigger(org: Uuid) -> Trigger {
    Trigger {
        organisation_id: org,
        topic: "schedule".into(),
        entities: vec![],
    }
}

fn builtin_evaluator(app: &PgPool) -> Evaluator {
    let registry = Registry::builtin(&BTreeSet::new()).unwrap();
    Evaluator::new(app.clone(), Arc::new(registry), EvaluatorOptions::default())
}

// ── a generator whose output the test controls ──────────────────────────────

struct Fake {
    id: &'static str,
    scope: ScopeSpec,
    proposals: Vec<Proposal>,
    fail: bool,
}

#[async_trait]
impl DecisionGenerator for Fake {
    fn id(&self) -> &'static str {
        self.id
    }
    fn version(&self) -> semver::Version {
        semver::Version::new(1, 0, 0)
    }
    fn decision_type(&self) -> &'static str {
        "test.decision"
    }
    fn tier(&self) -> Tier {
        Tier::T0
    }
    fn requires(&self) -> &'static [Capability] {
        &[]
    }
    fn scope(&self, _t: &Trigger, _c: &DecisionContext) -> ScopeSpec {
        self.scope.clone()
    }
    async fn evaluate(
        &self,
        _c: &DecisionContext,
        _s: &ScopeSpec,
    ) -> Result<Vec<Proposal>, GenError> {
        if self.fail {
            return Err(GenError::Internal("boom".into()));
        }
        Ok(self.proposals.clone())
    }
}

fn fake(
    id: &'static str,
    scope: ScopeSpec,
    proposals: Vec<Proposal>,
    fail: bool,
) -> Arc<dyn DecisionGenerator> {
    Arc::new(Fake {
        id,
        scope,
        proposals,
        fail,
    })
}

fn proposal(org: Uuid, n: u32, urgency: f64, confidence: f64, payload: Value) -> Proposal {
    let n = n.to_string();
    Proposal {
        candidate: Candidate {
            decision_type: "test.decision".into(),
            dedup_key: DedupKey::new(org, "test.decision", &[n.as_str()]),
            org_unit_id: None,
            payload,
            rationale: Rationale {
                summary: format!("item {n}"),
                factors: vec![Factor {
                    label: "f".into(),
                    value: FactorValue::Text("v".into()),
                    unit: None,
                    weight: None,
                    direction: None,
                    evidence: vec![],
                }],
                confidence,
                confidence_kind: ConfidenceKind::DataQuality,
                counterfactual: None,
                narrative: None,
            },
            urgency,
            impact: None,
            evidence: vec![],
        },
        input_digest: "a".repeat(64),
    }
}

async fn test_org(admin: &PgPool, slug: &str, generators: &[&str]) -> Uuid {
    let gens: serde_json::Map<String, Value> = generators
        .iter()
        .map(|g| ((*g).to_owned(), json!({})))
        .collect();
    let org = seed_org(admin, slug, json!({ "generators": gens })).await;
    sqlx::query(
        "INSERT INTO decision_types (organisation_id, name, pack, payload_schema_ref, payload_schema)
         VALUES ($1, 'test.decision', 'test', 'test/test.decision.json', $2)",
    )
    .bind(org)
    .bind(json!({ "type": "object", "required": ["n"] }))
    .execute(admin)
    .await
    .unwrap();
    org
}

fn evaluator_of(app: &PgPool, gens: Vec<Arc<dyn DecisionGenerator>>) -> Evaluator {
    Evaluator::new(
        app.clone(),
        Arc::new(Registry::build(gens, &BTreeSet::new()).unwrap()),
        EvaluatorOptions::default(),
    )
}

async fn pending_from(admin: &PgPool, org: Uuid, key: &str, generator: &str) -> Uuid {
    let new = NewRecommendation {
        id: Uuid::new_v4(),
        org_unit_id: None,
        decision_type: "test.decision".into(),
        dedup_key: key.into(),
        urgency: 0.5,
        confidence: 0.9,
        rationale_summary: "x".into(),
        body: json!({ "k": key }),
        provenance: json!({ "generator_id": generator }),
        expires_at: Some(now() + Duration::days(1)),
    };
    let mut c = admin.acquire().await.unwrap();
    recs::upsert_pending(&mut c, org, &new, now())
        .await
        .unwrap();
    new.id
}

async fn status_of(admin: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM recommendations WHERE id = $1")
        .bind(id)
        .fetch_one(admin)
        .await
        .unwrap()
}

// ── tests ───────────────────────────────────────────────────────────────────

#[sqlx::test(migrations = "../../migrations")]
async fn the_first_run_creates_and_announces_recommendations(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme", "active", 20, true).await;
    put_obj(
        &admin,
        org,
        1,
        case_attrs("open", -5),
        "2026-10-01T00:00:00Z",
    )
    .await;
    put_obj(
        &admin,
        org,
        2,
        case_attrs("open", -2),
        "2026-10-01T00:00:00Z",
    )
    .await;
    put_obj(
        &admin,
        org,
        3,
        case_attrs("done", -9),
        "2026-10-01T00:00:00Z",
    )
    .await;

    let report = builtin_evaluator(&app)
        .run(&trigger(org), now())
        .await
        .unwrap();
    let g = &report.generators[0];
    assert_eq!(g.error, None);
    assert_eq!(
        (g.proposed, g.created, g.revised, g.unchanged),
        (2, 2, 0, 0)
    );

    assert_eq!(count(&admin, "SELECT count(*) FROM recommendations WHERE organisation_id = $1 AND status = 'pending'", org).await, 2);
    assert_eq!(count(&admin, "SELECT count(*) FROM outbox WHERE organisation_id = $1 AND topic = 'kuki.recommendations'", org).await, 2);

    let id: Uuid = sqlx::query_scalar(
        "SELECT id FROM recommendations WHERE organisation_id = $1 ORDER BY dedup_key LIMIT 1",
    )
    .bind(org)
    .fetch_one(&admin)
    .await
    .unwrap();
    let mut c = admin.acquire().await.unwrap();
    let row = recs::get(&mut c, org, id).await.unwrap().unwrap();
    assert_eq!(row.decision_type, "work.escalate");
    assert_eq!(row.provenance["generator_id"], "deadline.at_risk.t0");
    assert_eq!(row.provenance["pack"], "kuki.core");
    assert_eq!(row.body["action"]["proposed"], true);
    assert_eq!(
        row.body["action"]["payload_schema_ref"],
        "kuki.core/work.escalate.json"
    );
    assert_eq!(row.expires_at, Some(now() + Duration::days(7)));
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_rerun_with_nothing_new_is_silent(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme", "active", 20, true).await;
    put_obj(
        &admin,
        org,
        1,
        case_attrs("open", -5),
        "2026-10-01T00:00:00Z",
    )
    .await;
    put_obj(
        &admin,
        org,
        2,
        case_attrs("open", -2),
        "2026-10-01T00:00:00Z",
    )
    .await;
    let ev = builtin_evaluator(&app);
    ev.run(&trigger(org), now()).await.unwrap();

    let again = ev
        .run(&trigger(org), now() + Duration::minutes(1))
        .await
        .unwrap();
    let g = &again.generators[0];
    assert_eq!((g.created, g.revised, g.unchanged, g.cleared), (0, 0, 2, 0));
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM outbox WHERE organisation_id = $1",
            org
        )
        .await,
        2,
        "no new events"
    );
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM recommendations WHERE organisation_id = $1 AND version = 1",
            org
        )
        .await,
        2
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_changed_deadline_revises_the_recommendation(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme", "active", 20, true).await;
    put_obj(
        &admin,
        org,
        1,
        case_attrs("open", -5),
        "2026-10-01T00:00:00Z",
    )
    .await;
    put_obj(
        &admin,
        org,
        2,
        case_attrs("open", -2),
        "2026-10-01T00:00:00Z",
    )
    .await;
    let ev = builtin_evaluator(&app);
    ev.run(&trigger(org), now()).await.unwrap();

    new_version(
        &admin,
        org,
        1,
        case_attrs("open", -8),
        "2026-10-11T11:00:00Z",
    )
    .await;
    let again = ev
        .run(&trigger(org), now() + Duration::seconds(30))
        .await
        .unwrap();
    let g = &again.generators[0];
    assert_eq!((g.created, g.revised, g.unchanged), (0, 1, 1));
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM recommendations WHERE organisation_id = $1 AND version = 2",
            org
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM outbox WHERE organisation_id = $1",
            org
        )
        .await,
        3
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_cleared_condition_expires_the_recommendation(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme", "active", 20, true).await;
    put_obj(
        &admin,
        org,
        1,
        case_attrs("open", -5),
        "2026-10-01T00:00:00Z",
    )
    .await;
    put_obj(
        &admin,
        org,
        2,
        case_attrs("open", -2),
        "2026-10-01T00:00:00Z",
    )
    .await;
    let ev = builtin_evaluator(&app);
    ev.run(&trigger(org), now()).await.unwrap();

    new_version(
        &admin,
        org,
        1,
        case_attrs("done", -5),
        "2026-10-11T11:00:00Z",
    )
    .await;
    let again = ev
        .run(&trigger(org), now() + Duration::minutes(1))
        .await
        .unwrap();
    assert_eq!(again.generators[0].cleared, 1);

    let (status, by, reason): (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT status, resolved_by, resolution->>'reason' FROM recommendations
          WHERE organisation_id = $1 AND status <> 'pending'",
    )
    .bind(org)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(status, "expired");
    assert_eq!(by.as_deref(), Some("system:evaluator"));
    assert_eq!(reason.as_deref(), Some("condition_cleared"));
    let last: String = sqlx::query_scalar(
        "SELECT payload->>'type' FROM outbox WHERE organisation_id = $1 ORDER BY id DESC LIMIT 1",
    )
    .bind(org)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(last, "recommendation.status_changed");
}

#[sqlx::test(migrations = "../../migrations")]
async fn only_a_full_scope_run_clears_and_only_its_own_rows(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = test_org(&admin, "acme", &["test.all", "test.some"]).await;
    let a = pending_from(&admin, org, &format!("{org}/test.decision/a"), "test.all").await;
    let b = pending_from(&admin, org, &format!("{org}/test.decision/b"), "test.some").await;
    let ev = evaluator_of(
        &app,
        vec![
            fake("test.all", ScopeSpec::All, vec![], false),
            fake("test.some", ScopeSpec::Entities(vec![u(1)]), vec![], false),
        ],
    );

    let report = ev.run(&trigger(org), now()).await.unwrap();
    assert_eq!(report.generators[0].cleared, 1);
    assert_eq!(report.generators[1].cleared, 0);
    assert_eq!(status_of(&admin, a).await, "expired");
    assert_eq!(
        status_of(&admin, b).await,
        "pending",
        "a scoped run cannot conclude anything about other entities"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_review_budget_defers_new_recommendations_until_the_next_day(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme", "active", 1, true).await;
    for n in 1..=3 {
        put_obj(
            &admin,
            org,
            n,
            case_attrs("open", -5),
            "2026-10-01T00:00:00Z",
        )
        .await;
    }
    let ev = builtin_evaluator(&app);

    let first = ev.run(&trigger(org), now()).await.unwrap();
    assert_eq!(
        (
            first.generators[0].created,
            first.generators[0].deferred_by_budget
        ),
        (1, 2)
    );

    let same_day = ev
        .run(&trigger(org), now() + Duration::hours(1))
        .await
        .unwrap();
    let g = &same_day.generators[0];
    assert_eq!(
        (g.created, g.unchanged, g.deferred_by_budget),
        (0, 1, 2),
        "a confirmation costs no budget"
    );

    let next_day = ev
        .run(&trigger(org), now() + Duration::hours(24))
        .await
        .unwrap();
    let g = &next_day.generators[0];
    assert_eq!((g.created, g.unchanged, g.deferred_by_budget), (1, 1, 1));
}

#[sqlx::test(migrations = "../../migrations")]
async fn bad_proposals_are_set_aside_and_the_rest_still_land(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = test_org(&admin, "acme", &["test.bad"]).await;
    let ok = json!({ "n": 1 });

    let mut wrong_type = proposal(org, 1, 0.5, 0.9, ok.clone());
    wrong_type.candidate.decision_type = "other.type".into();
    let mut foreign = proposal(org, 2, 0.5, 0.9, ok.clone());
    foreign.candidate.dedup_key = DedupKey::new(Uuid::new_v4(), "test.decision", &["2"]);
    let bad_urgency = proposal(org, 3, 2.0, 0.9, ok.clone());
    let bad_payload = proposal(org, 4, 0.5, 0.9, json!({}));
    let low_confidence = proposal(org, 5, 0.5, 0.1, ok.clone());
    let good = proposal(org, 6, 0.5, 0.9, ok.clone());
    let duplicate = proposal(org, 6, 0.5, 0.9, ok);

    let ev = evaluator_of(
        &app,
        vec![fake(
            "test.bad",
            ScopeSpec::All,
            vec![
                wrong_type,
                foreign,
                bad_urgency,
                bad_payload,
                low_confidence,
                good,
                duplicate,
            ],
            false,
        )],
    );
    let report = ev.run(&trigger(org), now()).await.unwrap();
    let g = &report.generators[0];
    assert_eq!(g.error, None);
    assert_eq!(g.proposed, 7);
    assert_eq!(g.rejected.len(), 5, "{:?}", g.rejected);
    assert_eq!(g.below_confidence, 1);
    assert_eq!(g.created, 1);
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM recommendations WHERE organisation_id = $1",
            org
        )
        .await,
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_failing_generator_writes_nothing_clears_nothing_and_does_not_stop_the_others(
    admin: PgPool,
) {
    let app = app_pool(&admin).await;
    let org = test_org(&admin, "acme", &["test.broken", "test.fine"]).await;
    let kept = pending_from(
        &admin,
        org,
        &format!("{org}/test.decision/old"),
        "test.broken",
    )
    .await;
    let ev = evaluator_of(
        &app,
        vec![
            fake("test.broken", ScopeSpec::All, vec![], true),
            fake(
                "test.fine",
                ScopeSpec::All,
                vec![proposal(org, 1, 0.5, 0.9, json!({ "n": 1 }))],
                false,
            ),
        ],
    );

    let report = ev.run(&trigger(org), now()).await.unwrap();
    assert!(
        report.generators[0]
            .error
            .as_deref()
            .unwrap()
            .contains("boom")
    );
    assert_eq!(report.generators[1].created, 1);
    assert_eq!(status_of(&admin, kept).await, "pending");
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_unregistered_decision_type_is_reported_and_nothing_is_written(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme", "active", 20, false).await;
    put_obj(
        &admin,
        org,
        1,
        case_attrs("open", -5),
        "2026-10-01T00:00:00Z",
    )
    .await;

    let report = builtin_evaluator(&app)
        .run(&trigger(org), now())
        .await
        .unwrap();
    assert!(
        report.generators[0]
            .error
            .as_deref()
            .unwrap()
            .contains("not registered")
    );
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM recommendations WHERE organisation_id = $1",
            org
        )
        .await,
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_shadow_generator_runs_but_surfaces_nothing(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = deadline_org(&admin, "acme", "shadow", 20, true).await;
    put_obj(
        &admin,
        org,
        1,
        case_attrs("open", -5),
        "2026-10-01T00:00:00Z",
    )
    .await;
    put_obj(
        &admin,
        org,
        2,
        case_attrs("open", -2),
        "2026-10-01T00:00:00Z",
    )
    .await;

    let report = builtin_evaluator(&app)
        .run(&trigger(org), now())
        .await
        .unwrap();
    let g = &report.generators[0];
    assert!(g.shadow);
    assert_eq!((g.proposed, g.created), (2, 0));
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM recommendations WHERE organisation_id = $1",
            org
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM outbox WHERE organisation_id = $1",
            org
        )
        .await,
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn due_recommendations_expire_at_the_start_of_a_run(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = seed_org(&admin, "acme", json!({})).await;
    let new = NewRecommendation {
        id: Uuid::new_v4(),
        org_unit_id: None,
        decision_type: "work.escalate".into(),
        dedup_key: "k".into(),
        urgency: 0.5,
        confidence: 0.9,
        rationale_summary: "x".into(),
        body: json!({}),
        provenance: json!({}),
        expires_at: Some(now() - Duration::hours(1)),
    };
    {
        let mut c = admin.acquire().await.unwrap();
        recs::upsert_pending(&mut c, org, &new, now() - Duration::days(2))
            .await
            .unwrap();
    }
    let ev = evaluator_of(&app, vec![]);

    let report = ev.run(&trigger(org), now()).await.unwrap();
    assert_eq!(report.expired, 1);
    assert_eq!(status_of(&admin, new.id).await, "expired");
}
