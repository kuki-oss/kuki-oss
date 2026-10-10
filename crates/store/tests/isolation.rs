use futures::FutureExt;
use kuki_store::session::{OrgSession, with_org_tx};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

struct Seed {
    org_a: Uuid,
    org_b: Uuid,
}

/// FORCE RLS filters a non-superuser owner, which would silently turn the seed inserts into no-ops.
async fn require_superuser(owner: &PgPool) {
    let su: bool = sqlx::query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
        .fetch_one(owner)
        .await
        .unwrap();
    assert!(
        su,
        "DATABASE_URL must be a superuser: FORCE RLS filters a non-superuser owner"
    );
}

async fn insert_object(owner: &PgPool, org: Uuid, unit: Option<Uuid>) {
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, org_unit_id, type, type_version, attrs, valid_from)
         VALUES ($1, $2, $3, 'item', 1, '{}', now())",
    )
    .bind(Uuid::now_v7()).bind(org).bind(unit)
    .execute(owner).await.unwrap();
}

async fn insert_rec(owner: &PgPool, org: Uuid, unit: Option<Uuid>, key: &str) {
    sqlx::query(
        "INSERT INTO recommendations (id, organisation_id, org_unit_id, decision_type, dedup_key, status,
                                      urgency, confidence, rationale_summary, body, provenance)
         VALUES ($1, $2, $3, 'inventory.replenish', $4, 'pending', 0.5, 0.5, 'x', '{}', '{}')",
    )
    .bind(Uuid::now_v7()).bind(org).bind(unit).bind(key)
    .execute(owner).await.unwrap();
}

async fn seed(owner: &PgPool) -> Seed {
    require_superuser(owner).await;
    let (a, b) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, 'acme', 'Acme'), ($2, 'globex', 'Globex')")
        .bind(a).bind(b).execute(owner).await.unwrap();

    let (hq_a, east_a, hq_b) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    for (id, org, path) in [(hq_a, a, "hq"), (east_a, a, "hq.east"), (hq_b, b, "hq")] {
        sqlx::query("INSERT INTO org_units (id, organisation_id, path, name) VALUES ($1, $2, $3::text::ltree, $3)")
            .bind(id).bind(org).bind(path).execute(owner).await.unwrap();
    }

    // org A: one org-wide object, one in hq, one in hq.east. org B: one org-wide object.
    insert_object(owner, a, None).await;
    insert_object(owner, a, Some(hq_a)).await;
    insert_object(owner, a, Some(east_a)).await;
    insert_object(owner, b, None).await;

    insert_rec(owner, a, Some(hq_a), "a-hq").await;
    insert_rec(owner, a, Some(east_a), "a-east").await;
    insert_rec(owner, b, Some(hq_b), "b-hq").await;

    for org in [a, b] {
        sqlx::query(
            "INSERT INTO events (id, organisation_id, type, type_version, subject_id, occurred_at, payload, source)
             VALUES ($1, $2, 'stock.delta', 1, $3, now(), '{}', 'test')",
        )
        .bind(Uuid::now_v7()).bind(org).bind(Uuid::now_v7())
        .execute(owner).await.unwrap();
    }

    sqlx::query(
        "INSERT INTO api_keys (id, key_id, organisation_id, name, secret_hash, scopes)
                 VALUES ($1, 'abcdefghij12', $2, 'test', $3, '{decisions:read}')",
    )
    .bind(Uuid::now_v7())
    .bind(a)
    .bind(vec![0u8; 32])
    .execute(owner)
    .await
    .unwrap();

    Seed { org_a: a, org_b: b }
}

/// A pool whose every connection runs as the unprivileged application role.
async fn app_pool(owner: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(conn).await?;
                Ok::<_, sqlx::Error>(())
            })
        })
        .connect_with(owner.connect_options().as_ref().clone())
        .await
        .unwrap()
}

fn org(org_id: Uuid) -> OrgSession {
    OrgSession {
        org_id,
        scope_path: None,
    }
}

async fn count(app: &PgPool, s: &OrgSession, table: &'static str) -> i64 {
    with_org_tx::<_, sqlx::Error, _>(app, s, move |c| {
        async move {
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&mut *c)
                .await
        }
        .boxed()
    })
    .await
    .unwrap()
}

fn err_text<T>(r: Result<T, sqlx::Error>) -> String {
    r.err().expect("expected an error").to_string()
}

#[sqlx::test(migrations = "../../migrations")]
async fn app_role_is_unprivileged(owner: PgPool) {
    let bypass: bool = sqlx::query_scalar(
        "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = 'kuki_app'",
    )
    .fetch_one(&owner)
    .await
    .unwrap();
    assert!(!bypass, "kuki_app must be neither superuser nor BYPASSRLS");
}

#[sqlx::test(migrations = "../../migrations")]
async fn each_org_sees_only_its_own_rows(owner: PgPool) {
    let s = seed(&owner).await;
    let app = app_pool(&owner).await;

    let a = org(s.org_a);
    assert_eq!(count(&app, &a, "organisations").await, 1);
    assert_eq!(count(&app, &a, "org_units").await, 2);
    assert_eq!(count(&app, &a, "objects").await, 3);
    assert_eq!(count(&app, &a, "recommendations").await, 2);
    assert_eq!(count(&app, &a, "events").await, 1);

    let b = org(s.org_b);
    assert_eq!(count(&app, &b, "organisations").await, 1);
    assert_eq!(count(&app, &b, "org_units").await, 1);
    assert_eq!(count(&app, &b, "objects").await, 1);
    assert_eq!(count(&app, &b, "recommendations").await, 1);
    assert_eq!(count(&app, &b, "events").await, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn no_tenant_context_means_no_rows(owner: PgPool) {
    seed(&owner).await;
    let app = app_pool(&owner).await;
    for t in ["organisations", "objects", "recommendations", "events"] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {t}"))
            .fetch_one(&app)
            .await
            .unwrap();
        assert_eq!(n, 0, "{t} must be empty without a tenant");
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn cannot_write_or_modify_across_tenants(owner: PgPool) {
    let s = seed(&owner).await;
    let app = app_pool(&owner).await;
    let (org_b, a) = (s.org_b, org(s.org_a));

    let insert =
        with_org_tx::<_, sqlx::Error, _>(&app, &a, move |c| {
            async move {
                sqlx::query(
                "INSERT INTO objects (id, organisation_id, type, type_version, attrs, valid_from)
                 VALUES ($1, $2, 'item', 1, '{}', now())",
            )
            .bind(Uuid::now_v7()).bind(org_b)
            .execute(&mut *c).await.map(|_| ())
            }
            .boxed()
        })
        .await;
    assert!(err_text(insert).contains("row-level security"));

    let affected = with_org_tx::<_, sqlx::Error, _>(&app, &a, move |c| {
        async move {
            sqlx::query("UPDATE recommendations SET status = 'expired' WHERE organisation_id = $1")
                .bind(org_b)
                .execute(&mut *c)
                .await
                .map(|r| r.rows_affected())
        }
        .boxed()
    })
    .await
    .unwrap();
    assert_eq!(affected, 0, "org A must not be able to touch org B rows");
}

#[sqlx::test(migrations = "../../migrations")]
async fn org_unit_scope_narrows_visibility(owner: PgPool) {
    let s = seed(&owner).await;
    let app = app_pool(&owner).await;
    let scoped = OrgSession {
        org_id: s.org_a,
        scope_path: Some("hq.east".into()),
    };

    assert_eq!(count(&app, &scoped, "org_units").await, 1);
    assert_eq!(
        count(&app, &scoped, "objects").await,
        2,
        "org-wide object plus the hq.east one"
    );
    assert_eq!(count(&app, &scoped, "recommendations").await, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn ledger_is_append_only(owner: PgPool) {
    let s = seed(&owner).await;
    let app = app_pool(&owner).await;

    // Even the owner cannot rewrite history: the trigger fires.
    let r = sqlx::query("UPDATE events SET payload = '{\"x\":1}'")
        .execute(&owner)
        .await
        .map(|_| ());
    assert!(err_text(r).contains("append-only"));
    let r = sqlx::query("DELETE FROM events")
        .execute(&owner)
        .await
        .map(|_| ());
    assert!(err_text(r).contains("append-only"));

    // The application role has no UPDATE or DELETE privilege at all.
    let r = with_org_tx::<_, sqlx::Error, _>(&app, &org(s.org_a), |c| {
        async move {
            sqlx::query("UPDATE events SET payload = '{}'")
                .execute(&mut *c)
                .await
                .map(|_| ())
        }
        .boxed()
    })
    .await;
    assert!(err_text(r).contains("permission denied"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn api_keys_are_reachable_only_through_the_lookup_function(owner: PgPool) {
    seed(&owner).await;
    let app = app_pool(&owner).await;

    let r = sqlx::query("SELECT 1 FROM api_keys")
        .fetch_all(&app)
        .await
        .map(|_| ());
    assert!(err_text(r).contains("permission denied"));

    let hit: i64 = sqlx::query_scalar("SELECT count(*) FROM auth_lookup_api_key('abcdefghij12')")
        .fetch_one(&app)
        .await
        .unwrap();
    let miss: i64 = sqlx::query_scalar("SELECT count(*) FROM auth_lookup_api_key('zzzzzzzzzzzz')")
        .fetch_one(&app)
        .await
        .unwrap();
    assert_eq!((hit, miss), (1, 0));
}
