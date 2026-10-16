use std::sync::Arc;

use kuki_actions::{RegistryRouter, Router, StaticSecrets, WebhookConfig};
use sqlx::{PgPool, postgres::PgPoolOptions};
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

async fn endpoint(admin: &PgPool, org: Uuid, action_type: &str, url: &str, enabled: bool) {
    sqlx::query(
        "INSERT INTO webhook_endpoints (id, organisation_id, action_type, url, secret_ref, enabled)
         VALUES ($1, $2, $3, $4, 'kuki/webhooks/acme/draft_po', $5)",
    )
    .bind(Uuid::new_v4())
    .bind(org)
    .bind(action_type)
    .bind(url)
    .bind(enabled)
    .execute(admin)
    .await
    .unwrap();
}

async fn router(admin: &PgPool) -> RegistryRouter {
    RegistryRouter::new(app_pool(admin).await, Arc::new(StaticSecrets::new()), &WebhookConfig::default())
        .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_registered_action_routes_to_its_endpoint_without_leaking_credentials(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    endpoint(&admin, org, "draft_po", "https://user:pw@example.org/hooks/po?token=abc", true).await;

    let adapter = router(&admin).await.route(org, "draft_po").await.unwrap().expect("routed");
    assert_eq!(adapter.label(), "webhook:https://example.org/hooks/po");
}

#[sqlx::test(migrations = "../../migrations")]
async fn unregistered_disabled_and_foreign_endpoints_do_not_route(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    endpoint(&admin, a, "draft_po", "https://example.org/po", true).await;
    endpoint(&admin, a, "paused", "https://example.org/paused", false).await;

    let r = router(&admin).await;
    assert!(r.route(a, "draft_po").await.unwrap().is_some());
    assert!(r.route(a, "unknown").await.unwrap().is_none());
    assert!(r.route(a, "paused").await.unwrap().is_none());
    assert!(r.route(b, "draft_po").await.unwrap().is_none());
}
