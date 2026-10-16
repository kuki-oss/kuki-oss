use futures::FutureExt;
use kuki_store::session::{with_org_tx, OrgSession};
use kuki_store::webhook_endpoints as we;
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

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
    sqlx::query("INSERT INTO organisations (id, slug, name, config) VALUES ($1, $2, $2, '{}'::jsonb)")
        .bind(id)
        .bind(slug)
        .execute(admin)
        .await
        .unwrap();
    id
}

fn session(org: Uuid) -> OrgSession {
    OrgSession { org_id: org, scope_path: None }
}

const URL: &str = "https://example.org/hooks/po";
const REF: &str = "kuki/webhooks/acme/draft_po";

#[sqlx::test(migrations = "../../migrations")]
async fn upsert_replaces_in_place_and_keeps_the_id(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let (first, second) = with_org_tx::<_, sqlx::Error, _>(&app, &session(org), move |c| {
        async move {
            let a = we::upsert(c, org, "draft_po", URL, REF, true).await?;
            let b = we::upsert(c, org, "draft_po", "http://localhost:9/x", "kuki/other", false).await?;
            Ok((a, b))
        }
        .boxed()
    })
    .await
    .unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(second.url, "http://localhost:9/x");
    assert_eq!(second.secret_ref, "kuki/other");
    assert!(!second.enabled);
}

#[sqlx::test(migrations = "../../migrations")]
async fn tenants_cannot_see_or_remove_each_others_endpoints(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let app = app_pool(&admin).await;

    with_org_tx::<_, sqlx::Error, _>(&app, &session(a), move |c| {
        async move { we::upsert(c, a, "draft_po", URL, REF, true).await.map(|_| ()) }.boxed()
    })
    .await
    .unwrap();

    let (seen, removed) = with_org_tx::<_, sqlx::Error, _>(&app, &session(b), move |c| {
        async move {
            // Even naming org a explicitly, org b's session sees nothing.
            let seen = we::find_enabled(c, a, "draft_po").await?;
            let removed = we::remove(c, a, "draft_po").await?;
            Ok((seen, removed))
        }
        .boxed()
    })
    .await
    .unwrap();
    assert!(seen.is_none());
    assert!(!removed);

    let still = with_org_tx::<_, sqlx::Error, _>(&app, &session(a), move |c| {
        async move { we::list(c, a).await }.boxed()
    })
    .await
    .unwrap();
    assert_eq!(still.len(), 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn disabled_endpoints_are_not_found(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let (on, off) = with_org_tx::<_, sqlx::Error, _>(&app, &session(org), move |c| {
        async move {
            we::upsert(c, org, "on", URL, REF, true).await?;
            we::upsert(c, org, "off", URL, REF, false).await?;
            Ok((
                we::find_enabled(c, org, "on").await?,
                we::find_enabled(c, org, "off").await?,
            ))
        }
        .boxed()
    })
    .await
    .unwrap();
    assert!(on.is_some());
    assert!(off.is_none());
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_database_rejects_bad_urls_and_secret_refs(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    for (url, secret_ref) in [
        ("ftp://example.org/x", REF),
        ("not a url", REF),
        (URL, ""),
        (URL, "../etc/passwd"),
        (URL, "/leading/slash"),
        (URL, "kuki/has space"),
    ] {
        let r = with_org_tx::<_, sqlx::Error, _>(&app, &session(org), move |c| {
            async move { we::upsert(c, org, "t", url, secret_ref, true).await.map(|_| ()) }.boxed()
        })
        .await;
        assert!(r.is_err(), "accepted url={url:?} ref={secret_ref:?}");
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn remove_reports_whether_anything_was_deleted(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let (first, second) = with_org_tx::<_, sqlx::Error, _>(&app, &session(org), move |c| {
        async move {
            we::upsert(c, org, "t", URL, REF, true).await?;
            Ok((we::remove(c, org, "t").await?, we::remove(c, org, "t").await?))
        }
        .boxed()
    })
    .await
    .unwrap();
    assert!(first);
    assert!(!second);
}
