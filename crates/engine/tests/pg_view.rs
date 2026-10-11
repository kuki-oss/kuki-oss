use std::{collections::BTreeSet, sync::Arc};

use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::FutureExt;
use kuki_engine::{PgOntologyView, Registry, load_config};
use kuki_extension::{DecisionContext, EventQuery, OntologyView, ScopeSpec};
use kuki_store::{
    engine_reads::decision_config,
    session::{OrgSession, with_org_tx},
};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

fn u(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn t(s: &str) -> DateTime<Utc> {
    s.parse().unwrap()
}

/// The unprivileged role, so row-level security is really in force.
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

async fn seed_org(admin: &PgPool, slug: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, $2, $2)")
        .bind(id)
        .bind(slug)
        .execute(admin)
        .await
        .unwrap();
    id
}

#[allow(clippy::too_many_arguments)]
async fn put_obj(
    admin: &PgPool,
    org: Uuid,
    id: u128,
    kind: &str,
    attrs: Value,
    from: &str,
    to: Option<&str>,
    unit: Option<Uuid>,
) {
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, org_unit_id, type, type_version, attrs, valid_from, valid_to)
         VALUES ($1, $2, $3, $4, 1, $5, $6::text::timestamptz, $7::text::timestamptz)",
    )
    .bind(u(id))
    .bind(org)
    .bind(unit)
    .bind(kind)
    .bind(attrs)
    .bind(from)
    .bind(to)
    .execute(admin)
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn put_link(
    admin: &PgPool,
    org: Uuid,
    id: u128,
    kind: &str,
    src: u128,
    dst: u128,
    from: &str,
    to: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO links (id, organisation_id, type, src_id, dst_id, valid_from, valid_to)
         VALUES ($1, $2, $3, $4, $5, $6::text::timestamptz, $7::text::timestamptz)",
    )
    .bind(u(id))
    .bind(org)
    .bind(kind)
    .bind(u(src))
    .bind(u(dst))
    .bind(from)
    .bind(to)
    .execute(admin)
    .await
    .unwrap();
}

async fn put_event(
    admin: &PgPool,
    org: Uuid,
    id: u128,
    kind: &str,
    subject: u128,
    occurred: &str,
    recorded: &str,
) {
    sqlx::query(
        "INSERT INTO events (id, organisation_id, type, type_version, subject_id, occurred_at, recorded_at, payload, source)
         VALUES ($1, $2, $3, 1, $4, $5::text::timestamptz, $6::text::timestamptz, '{}'::jsonb, 'test')",
    )
    .bind(u(id))
    .bind(org)
    .bind(kind)
    .bind(u(subject))
    .bind(occurred)
    .bind(recorded)
    .execute(admin)
    .await
    .unwrap();
}

fn view(app: &PgPool, org: Uuid, as_of: &str) -> PgOntologyView {
    PgOntologyView::new(app.clone(), org, t(as_of))
}

#[sqlx::test(migrations = "../../migrations")]
async fn objects_are_read_in_valid_time(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = seed_org(&admin, "acme").await;
    #[allow(clippy::too_many_arguments)]
    put_obj(
        &admin,
        org,
        1,
        "case",
        json!({"n": 1}),
        "2026-01-01T00:00:00Z",
        Some("2026-06-01T00:00:00Z"),
        None,
    )
    .await;
    #[allow(clippy::too_many_arguments)]
    put_obj(
        &admin,
        org,
        1,
        "case",
        json!({"n": 2}),
        "2026-06-01T00:00:00Z",
        None,
        None,
    )
    .await;

    let at = |s: &'static str| {
        let v = view(&app, org, s);
        async move { v.objects_of_type("case", None).await.unwrap() }
    };
    assert_eq!(at("2026-03-01T00:00:00Z").await[0].attrs["n"], 1);
    assert_eq!(
        at("2026-06-01T00:00:00Z").await[0].attrs["n"],
        2,
        "valid_to is exclusive"
    );
    assert_eq!(at("2026-07-01T00:00:00Z").await[0].attrs["n"], 2);
    assert!(at("2025-12-31T00:00:00Z").await.is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_single_object_is_read_as_of_the_clock(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = seed_org(&admin, "acme").await;
    #[allow(clippy::too_many_arguments)]
    put_obj(
        &admin,
        org,
        1,
        "case",
        json!({"n": 1}),
        "2026-01-01T00:00:00Z",
        Some("2026-06-01T00:00:00Z"),
        None,
    )
    .await;
    #[allow(clippy::too_many_arguments)]
    put_obj(
        &admin,
        org,
        1,
        "case",
        json!({"n": 2}),
        "2026-06-01T00:00:00Z",
        None,
        None,
    )
    .await;

    let old = view(&app, org, "2026-02-01T00:00:00Z")
        .object(u(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.attrs["n"], 1);
    assert_eq!(old.type_name, "case");
    let new = view(&app, org, "2026-08-01T00:00:00Z")
        .object(u(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new.attrs["n"], 2);
    assert!(
        view(&app, org, "2026-08-01T00:00:00Z")
            .object(u(99))
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn paging_returns_every_object_once_and_the_unit_filter_applies(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = seed_org(&admin, "acme").await;
    let unit = Uuid::new_v4();
    sqlx::query("INSERT INTO org_units (id, organisation_id, path, name) VALUES ($1, $2, 'hq'::ltree, 'HQ')")
        .bind(unit)
        .bind(org)
        .execute(&admin)
        .await
        .unwrap();
    // 1,100 objects (more than two pages); every tenth belongs to the unit.
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, org_unit_id, type, type_version, attrs, valid_from)
         SELECT ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid, $1,
                CASE WHEN g % 10 = 0 THEN $2 END, 'case', 1, '{}'::jsonb, '2026-01-01T00:00:00Z'
           FROM generate_series(1, 1100) g",
    )
    .bind(org)
    .bind(unit)
    .execute(&admin)
    .await
    .unwrap();

    let v = view(&app, org, "2026-02-01T00:00:00Z");
    let all = v.objects_of_type("case", None).await.unwrap();
    assert_eq!(all.len(), 1100);
    let distinct: BTreeSet<_> = all.iter().map(|o| o.id).collect();
    assert_eq!(distinct.len(), 1100);
    assert_eq!(
        v.objects_of_type("case", Some(unit)).await.unwrap().len(),
        110
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn links_are_read_in_both_directions_in_valid_time(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = seed_org(&admin, "acme").await;
    #[allow(clippy::too_many_arguments)]
    put_link(
        &admin,
        org,
        1,
        "assigned_to",
        10,
        11,
        "2026-01-01T00:00:00Z",
        None,
    )
    .await;
    #[allow(clippy::too_many_arguments)]
    put_link(
        &admin,
        org,
        2,
        "assigned_to",
        12,
        10,
        "2026-01-01T00:00:00Z",
        None,
    )
    .await;

    #[allow(clippy::too_many_arguments)]
    put_link(
        &admin,
        org,
        3,
        "member_of",
        10,
        13,
        "2026-01-01T00:00:00Z",
        Some("2026-03-01T00:00:00Z"),
    )
    .await;

    let v = view(&app, org, "2026-02-01T00:00:00Z");
    assert_eq!(v.links(u(10), None).await.unwrap().len(), 3);
    assert_eq!(v.links(u(10), Some("assigned_to")).await.unwrap().len(), 2);
    // After link 3 ended it no longer exists.
    let later = view(&app, org, "2026-04-01T00:00:00Z");
    assert_eq!(later.links(u(10), None).await.unwrap().len(), 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn events_come_back_oldest_first_and_respect_ledger_time(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = seed_org(&admin, "acme").await;
    put_event(
        &admin,
        org,
        1,
        "touched",
        10,
        "2026-02-03T00:00:00Z",
        "2026-02-03T00:00:00Z",
    )
    .await;
    put_event(
        &admin,
        org,
        2,
        "touched",
        10,
        "2026-02-01T00:00:00Z",
        "2026-02-01T00:00:00Z",
    )
    .await;
    put_event(
        &admin,
        org,
        3,
        "closed",
        10,
        "2026-02-02T00:00:00Z",
        "2026-02-02T00:00:00Z",
    )
    .await;
    // Occurred early but was only recorded later: invisible to a clock before it was recorded.
    put_event(
        &admin,
        org,
        4,
        "touched",
        10,
        "2026-02-01T12:00:00Z",
        "2026-03-15T00:00:00Z",
    )
    .await;
    put_event(
        &admin,
        org,
        5,
        "touched",
        99,
        "2026-02-01T00:00:00Z",
        "2026-02-01T00:00:00Z",
    )
    .await;

    let q = |kind: Option<&str>| EventQuery {
        subject_id: u(10),
        type_name: kind.map(str::to_owned),
        occurred_from: None,
        occurred_to: None,
    };
    let v = view(&app, org, "2026-03-01T00:00:00Z");
    let ids: Vec<_> = v
        .events(&q(None))
        .await
        .unwrap()
        .iter()
        .map(|e| e.id)
        .collect();
    assert_eq!(ids, [u(2), u(3), u(1)]);
    let touched: Vec<_> = v
        .events(&q(Some("touched")))
        .await
        .unwrap()
        .iter()
        .map(|e| e.id)
        .collect();
    assert_eq!(touched, [u(2), u(1)]);

    let after = view(&app, org, "2026-04-01T00:00:00Z");
    assert_eq!(after.events(&q(None)).await.unwrap().len(), 4);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_view_never_sees_another_organisations_data(admin: PgPool) {
    let app = app_pool(&admin).await;
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    #[allow(clippy::too_many_arguments)]
    put_obj(
        &admin,
        a,
        1,
        "case",
        json!({}),
        "2026-01-01T00:00:00Z",
        None,
        None,
    )
    .await;

    let v = view(&app, b, "2026-02-01T00:00:00Z");
    assert!(v.objects_of_type("case", None).await.unwrap().is_empty());
    assert!(v.object(u(1)).await.unwrap().is_none());
}

#[sqlx::test(migrations = "../../migrations")]
async fn config_is_only_readable_for_the_callers_own_organisation(admin: PgPool) {
    let app = app_pool(&admin).await;
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    sqlx::query("UPDATE organisations SET config = $2 WHERE id = $1")
        .bind(a)
        .bind(json!({ "decision": { "min_confidence_for_display": 0.5 } }))
        .execute(&admin)
        .await
        .unwrap();

    let session = OrgSession {
        org_id: a,
        scope_path: None,
    };
    let own = with_org_tx::<_, sqlx::Error, _>(&app, &session, |c| {
        async move { decision_config(c, a).await }.boxed()
    })
    .await
    .unwrap();
    assert_eq!(own.unwrap()["min_confidence_for_display"], 0.5);

    let other = with_org_tx::<_, sqlx::Error, _>(&app, &session, |c| {
        async move { decision_config(c, b).await }.boxed()
    })
    .await
    .unwrap();
    assert!(
        other.is_none(),
        "a session must not read another organisation's config"
    );

    // Nothing configured is an empty object, which parses to the default.
    let session_b = OrgSession {
        org_id: b,
        scope_path: None,
    };
    let cfg = with_org_tx::<_, kuki_engine::ConfigError, _>(&app, &session_b, |c| {
        async move { load_config(c, b).await }.boxed()
    })
    .await
    .unwrap();
    assert_eq!(cfg, Default::default());
}

#[sqlx::test(migrations = "../../migrations")]
async fn configured_generators_run_against_the_database(admin: PgPool) {
    let app = app_pool(&admin).await;
    let org = seed_org(&admin, "acme").await;
    sqlx::query("UPDATE organisations SET config = $2 WHERE id = $1")
        .bind(org)
        .bind(
            json!({ "decision": { "generators": { "deadline.at_risk.t0": { "params": {
            "object_type": "case", "due_attr": "due_at", "status_attr": "status",
            "open_statuses": ["open"], "horizon_hours": 72 } } } } }),
        )
        .execute(&admin)
        .await
        .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap();
    let due = |h: i64| (now + Duration::hours(h)).to_rfc3339();
    #[allow(clippy::too_many_arguments)]
    put_obj(
        &admin,
        org,
        1,
        "case",
        json!({"status": "open", "due_at": due(-5)}),
        "2026-10-01T00:00:00Z",
        None,
        None,
    )
    .await;
    #[allow(clippy::too_many_arguments)]
    put_obj(
        &admin,
        org,
        2,
        "case",
        json!({"status": "done", "due_at": due(-5)}),
        "2026-10-01T00:00:00Z",
        None,
        None,
    )
    .await;
    #[allow(clippy::too_many_arguments)]
    put_obj(
        &admin,
        org,
        3,
        "case",
        json!({"status": "open", "due_at": due(500)}),
        "2026-10-01T00:00:00Z",
        None,
        None,
    )
    .await;

    let session = OrgSession {
        org_id: org,
        scope_path: None,
    };
    let cfg = with_org_tx::<_, kuki_engine::ConfigError, _>(&app, &session, |c| {
        async move { load_config(c, org).await }.boxed()
    })
    .await
    .unwrap();
    let cfg = Arc::new(cfg);

    let registry = Registry::builtin(&BTreeSet::new()).unwrap();
    let enabled: Vec<_> = registry.enabled(&cfg).collect();
    assert_eq!(enabled.len(), 1);

    let g = enabled[0];
    let ctx = DecisionContext::new(
        org,
        now,
        g.id(),
        cfg.clone(),
        Arc::new(PgOntologyView::new(app.clone(), org, now)),
    );
    let out = g.evaluate(&ctx, &ScopeSpec::All).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].candidate.payload["objectId"],
        json!(u(1).to_string())
    );
    assert_eq!(out[0].candidate.urgency, 1.0);

    // An organisation with nothing configured runs nothing.
    let none = kuki_extension::config::OrgDecisionConfig::default();
    assert_eq!(registry.enabled(&none).count(), 0);
}
