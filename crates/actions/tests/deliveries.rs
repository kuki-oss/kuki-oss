use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use futures::{FutureExt, future::BoxFuture};
use kuki_actions::{
    DeliveryConfig, DeliveryWorker, SecretBytes, SecretError, SecretSource, StaticSecrets,
    WebhookConfig,
};
use kuki_store::session::{OrgSession, with_org_tx};
use kuki_store::{webhook_deliveries as wd, webhook_subscriptions as ws};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

const REF: &str = "kuki/webhooks/acme/events";
const SECRET: &[u8] = b"whsec_events_test";
const CHANGED: &str = "recommendation.status_changed";

struct Hit {
    headers: HeaderMap,
    body: Vec<u8>,
}

#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<Hit>>>);

impl Seen {
    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    fn nth(&self, n: usize) -> (HeaderMap, Vec<u8>) {
        let all = self.0.lock().unwrap();
        (all[n].headers.clone(), all[n].body.clone())
    }
    fn header_values(&self, name: &str) -> Vec<String> {
        let all = self.0.lock().unwrap();
        let mut v: Vec<String> =
            all.iter().map(|h| h.headers[name].to_str().unwrap().to_owned()).collect();
        v.sort();
        v
    }
}

async fn handle(State(seen): State<Seen>, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    seen.0.lock().unwrap().push(Hit { headers, body: body.to_vec() });
    match uri.path().split('/').filter(|s| !s.is_empty()).collect::<Vec<_>>().as_slice() {
        ["json"] => (StatusCode::OK, "{\"ok\":true}").into_response(),
        ["slow"] => {
            tokio::time::sleep(Duration::from_secs(2)).await;
            StatusCode::OK.into_response()
        }
        ["s", code] => (StatusCode::from_u16(code.parse().unwrap()).unwrap(), "nope").into_response(),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn spawn() -> (String, Seen) {
    let seen = Seen::default();
    let app = Router::new().fallback(handle).with_state(seen.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (format!("http://{addr}"), seen)
}

async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(8)
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

fn payload(version: i32) -> Value {
    json!({ "type": CHANGED, "id": "r1", "version": version,
            "status": "accepted", "previousStatus": "pending" })
}

async fn subscribe(app: &PgPool, org: Uuid, name: &str, url: &str) {
    let (name, url) = (name.to_owned(), url.to_owned());
    with_org_tx::<_, sqlx::Error, _>(app, &session(org), move |c| {
        async move {
            ws::upsert(c, org, &name, &url, REF, &[CHANGED.to_string()], true).await.map(|_| ())
        }
        .boxed()
    })
    .await
    .unwrap();
}

async fn fan_out(app: &PgPool, org: Uuid, event_id: &str, p: Value) {
    let e = event_id.to_owned();
    with_org_tx::<_, sqlx::Error, _>(app, &session(org), move |c| {
        async move { wd::enqueue_for_event(c, org, &e, CHANGED, &p).await.map(|_| ()) }.boxed()
    })
    .await
    .unwrap();
}

fn secrets() -> Arc<dyn SecretSource> {
    Arc::new(StaticSecrets::new().with(REF, SECRET))
}

fn worker(app: &PgPool, secrets: Arc<dyn SecretSource>, max_attempts: i32) -> DeliveryWorker {
    let webhook = WebhookConfig {
        timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_millis(500),
        allow_insecure_http: true,
    };
    let config = DeliveryConfig { max_attempts, ..DeliveryConfig::default() };
    DeliveryWorker::new(app.clone(), secrets, &webhook, config).unwrap()
}

async fn make_due(admin: &PgPool) {
    sqlx::query("UPDATE webhook_deliveries SET not_before = now() WHERE status = 'queued'")
        .execute(admin)
        .await
        .unwrap();
}

async fn states(admin: &PgPool) -> Vec<(String, String, i32, Option<String>)> {
    sqlx::query_as(
        "SELECT s.name, d.status, d.attempts, d.last_error
           FROM webhook_deliveries d JOIN webhook_subscriptions s ON s.id = d.subscription_id
          ORDER BY s.name, d.event_id",
    )
    .fetch_all(admin)
    .await
    .unwrap()
}

struct Failing(SecretError);

impl SecretSource for Failing {
    fn read<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<SecretBytes, SecretError>> {
        let e = self.0.clone();
        async move { Err(e) }.boxed()
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_queued_event_is_delivered_signed_with_its_event_headers(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let (base, seen) = spawn().await;
    subscribe(&app, org, "ops", &format!("{base}/json")).await;
    fan_out(&app, org, "e1", payload(2)).await;

    let r = worker(&app, secrets(), 5).tick().await.unwrap();
    assert_eq!((r.claimed, r.delivered, r.dead, r.retried), (1, 1, 0, 0));

    let (headers, body) = seen.nth(0);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), payload(2));
    let sig = headers["kuki-signature"].to_str().unwrap();
    let now = chrono::Utc::now().timestamp();
    assert_eq!(kuki_outbox::signing::verify(SECRET, sig, &body, now), Ok(()));
    assert_eq!(headers["kuki-event-id"].to_str().unwrap(), "e1");
    assert_eq!(headers["kuki-event-type"].to_str().unwrap(), CHANGED);
    assert_eq!(headers["kuki-organisation-id"].to_str().unwrap(), org.to_string());
    assert_eq!(headers["kuki-attempt"].to_str().unwrap(), "1");

    assert_eq!(states(&admin).await, [("ops".into(), "delivered".into(), 1, None)]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn transient_failures_back_off_and_keep_the_event_id(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let (base, seen) = spawn().await;
    subscribe(&app, org, "ops", &format!("{base}/s/503")).await;
    fan_out(&app, org, "e1", payload(2)).await;
    let w = worker(&app, secrets(), 5);

    let r = w.tick().await.unwrap();
    assert_eq!((r.claimed, r.retried), (1, 1));
    let rows = states(&admin).await;
    assert_eq!((rows[0].1.as_str(), rows[0].2), ("queued", 1));
    assert!(rows[0].3.as_deref().unwrap().contains("503"));
    let delayed: bool = sqlx::query_scalar("SELECT not_before > now() FROM webhook_deliveries")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert!(delayed);

    // Backing off: nothing is due yet.
    assert_eq!(w.tick().await.unwrap().claimed, 0);

    make_due(&admin).await;
    assert_eq!(w.tick().await.unwrap().retried, 1);
    let (first, _) = seen.nth(0);
    let (second, _) = seen.nth(1);
    assert_eq!(first["kuki-event-id"], second["kuki-event-id"]);
    assert_eq!(second["kuki-attempt"].to_str().unwrap(), "2");
}

#[sqlx::test(migrations = "../../migrations")]
async fn permanent_failures_and_exhausted_retries_park_the_event_as_dead(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let (base, _) = spawn().await;
    subscribe(&app, org, "gone", &format!("{base}/s/410")).await;
    subscribe(&app, org, "flaky", &format!("{base}/s/503")).await;
    fan_out(&app, org, "e1", payload(2)).await;
    let w = worker(&app, secrets(), 2);

    let r = w.tick().await.unwrap();
    assert_eq!((r.delivered, r.dead, r.retried), (0, 1, 1));
    make_due(&admin).await;
    let r = w.tick().await.unwrap();
    assert_eq!((r.claimed, r.dead), (1, 1));

    let rows = states(&admin).await;
    assert_eq!((rows[0].0.as_str(), rows[0].1.as_str()), ("flaky", "dead"));
    assert!(rows[0].3.as_deref().unwrap().contains("gave up after 2 attempts: HTTP 503"));
    assert_eq!((rows[1].0.as_str(), rows[1].1.as_str()), ("gone", "dead"));
    assert!(rows[1].3.as_deref().unwrap().contains("HTTP 410"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_unusable_secret_is_dead_and_a_backend_outage_retries_without_sending(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let (base, seen) = spawn().await;
    subscribe(&app, org, "ops", &format!("{base}/json")).await;

    fan_out(&app, org, "e1", payload(1)).await;
    let missing: Arc<dyn SecretSource> = Arc::new(Failing(SecretError::Missing("no secret".into())));
    assert_eq!(worker(&app, missing, 5).tick().await.unwrap().dead, 1);

    fan_out(&app, org, "e2", payload(2)).await;
    let down: Arc<dyn SecretSource> = Arc::new(Failing(SecretError::Unavailable));
    assert_eq!(worker(&app, down, 5).tick().await.unwrap().retried, 1);

    assert_eq!(seen.count(), 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_stuck_receiver_does_not_hold_up_another_in_the_same_batch(admin: PgPool) {
    let org = seed_org(&admin, "a").await;
    let app = app_pool(&admin).await;
    let (base, _) = spawn().await;
    subscribe(&app, org, "fast", &format!("{base}/json")).await;
    subscribe(&app, org, "stuck", &format!("{base}/slow")).await;
    fan_out(&app, org, "e1", payload(2)).await;

    let r = worker(&app, secrets(), 5).tick().await.unwrap();
    assert_eq!((r.claimed, r.delivered, r.retried), (2, 1, 1));
    let rows = states(&admin).await;
    assert_eq!((rows[0].0.as_str(), rows[0].1.as_str()), ("fast", "delivered"));
    assert_eq!((rows[1].0.as_str(), rows[1].1.as_str()), ("stuck", "queued"));
    assert!(rows[1].3.as_deref().unwrap().contains("request failed"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn every_organisations_events_are_served_with_their_own_organisation_header(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let app = app_pool(&admin).await;
    let (base, seen) = spawn().await;
    subscribe(&app, a, "ops", &format!("{base}/json")).await;
    subscribe(&app, b, "ops", &format!("{base}/json")).await;
    fan_out(&app, a, "e1", payload(1)).await;
    fan_out(&app, b, "e2", payload(1)).await;

    let r = worker(&app, secrets(), 5).tick().await.unwrap();
    assert_eq!((r.claimed, r.delivered), (2, 2));
    let mut want = vec![a.to_string(), b.to_string()];
    want.sort();
    assert_eq!(seen.header_values("kuki-organisation-id"), want);
    assert_eq!(seen.header_values("kuki-event-id"), ["e1", "e2"]);
}
