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
    Delivery, Outcome, SecretBytes, SecretError, SecretSource, StaticSecrets, WebhookAdapter,
    WebhookConfig, webhook_client,
};
use uuid::Uuid;

const REF: &str = "kuki/webhooks/acme/draft_po";
const SECRET: &[u8] = b"whsec_live_test";

struct Hit {
    path: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<Hit>>>);

impl Seen {
    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    fn paths(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|h| h.path.clone())
            .collect()
    }
    fn nth(&self, n: usize) -> (HeaderMap, Vec<u8>) {
        let all = self.0.lock().unwrap();
        (all[n].headers.clone(), all[n].body.clone())
    }
}

async fn handle(State(seen): State<Seen>, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    let path = uri.path().to_owned();
    seen.0.lock().unwrap().push(Hit {
        path: path.clone(),
        headers,
        body: body.to_vec(),
    });
    match path
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["json"] => (
            StatusCode::OK,
            [("content-type", "application/json")],
            r#"{"ok":true}"#,
        )
            .into_response(),
        ["text"] => (StatusCode::OK, "accepted").into_response(),
        ["slow"] => {
            tokio::time::sleep(Duration::from_secs(2)).await;
            StatusCode::OK.into_response()
        }
        ["s", code] => {
            let code: u16 = code.parse().unwrap();
            let status = StatusCode::from_u16(code).unwrap();
            match code {
                302 => (status, [("location", "/s/200")], "").into_response(),
                204 => status.into_response(),
                _ => (status, "nope").into_response(),
            }
        }
        ["big", code] => (
            StatusCode::from_u16(code.parse().unwrap()).unwrap(),
            "x".repeat(1 << 20),
        )
            .into_response(),
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

fn cfg() -> WebhookConfig {
    WebhookConfig {
        timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_millis(500),
        allow_insecure_http: true,
    }
}

fn adapter_with(
    url: String,
    secrets: Arc<dyn SecretSource>,
    cfg: &WebhookConfig,
) -> WebhookAdapter {
    WebhookAdapter::new(
        webhook_client(cfg).unwrap(),
        url,
        REF.into(),
        secrets,
        cfg.allow_insecure_http,
    )
}

fn adapter(url: String) -> WebhookAdapter {
    adapter_with(
        url,
        Arc::new(StaticSecrets::new().with(REF, SECRET)),
        &cfg(),
    )
}

fn delivery() -> Delivery {
    Delivery {
        id: Uuid::new_v4(),
        attempt: 2,
        body: br#"{"x":1}"#.to_vec(),
    }
}

fn kind(o: &Outcome) -> &'static str {
    match o {
        Outcome::Succeeded(_) => "succeeded",
        Outcome::Retry(_) => "retry",
        Outcome::Failed(_) => "failed",
    }
}

struct Failing(SecretError);

impl SecretSource for Failing {
    fn read<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<SecretBytes, SecretError>> {
        let e = self.0.clone();
        async move { Err(e) }.boxed()
    }
}

#[tokio::test]
async fn a_delivery_is_signed_and_keeps_one_idempotency_key_across_retries() {
    let (base, seen) = spawn().await;
    let a = adapter(format!("{base}/json"));
    let d = delivery();
    assert_eq!(kind(&a.deliver(&d).await), "succeeded");
    assert_eq!(
        kind(
            &a.deliver(&Delivery {
                attempt: 3,
                ..d.clone()
            })
            .await
        ),
        "succeeded"
    );

    let (headers, body) = seen.nth(0);
    assert_eq!(body, d.body);
    let sig = headers["kuki-signature"].to_str().unwrap();
    let now = chrono::Utc::now().timestamp();
    assert_eq!(
        kuki_outbox::signing::verify(SECRET, sig, &body, now),
        Ok(())
    );
    assert!(kuki_outbox::signing::verify(b"another", sig, &body, now).is_err());
    assert_eq!(headers["kuki-attempt"].to_str().unwrap(), "2");
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );

    let (second, _) = seen.nth(1);
    assert_eq!(headers["idempotency-key"], second["idempotency-key"]);
    assert_eq!(
        headers["idempotency-key"].to_str().unwrap(),
        d.id.to_string()
    );
    assert_eq!(second["kuki-attempt"].to_str().unwrap(), "3");
}

#[tokio::test]
async fn a_success_records_the_status_and_the_response() {
    let (base, _) = spawn().await;
    match adapter(format!("{base}/json")).deliver(&delivery()).await {
        Outcome::Succeeded(v) => {
            assert_eq!(v["status"], 200);
            assert_eq!(v["response"], serde_json::json!({ "ok": true }));
        }
        _ => panic!("expected success"),
    }
    match adapter(format!("{base}/text")).deliver(&delivery()).await {
        Outcome::Succeeded(v) => assert_eq!(v["response"], "accepted"),
        _ => panic!("expected success"),
    }
}

#[tokio::test]
async fn statuses_decide_between_success_retry_and_failure() {
    let (base, _) = spawn().await;
    for (codes, want) in [
        (&[200u16, 201, 204][..], "succeeded"),
        (&[408, 425, 429, 500, 502, 503, 504][..], "retry"),
        (&[400, 401, 403, 404, 410, 422][..], "failed"),
    ] {
        for code in codes {
            let out = adapter(format!("{base}/s/{code}"))
                .deliver(&delivery())
                .await;
            assert_eq!(kind(&out), want, "HTTP {code}");
        }
    }
}

#[tokio::test]
async fn redirects_are_never_followed() {
    let (base, seen) = spawn().await;
    let out = adapter(format!("{base}/s/302")).deliver(&delivery()).await;
    assert_eq!(kind(&out), "failed");
    assert_eq!(seen.paths(), vec!["/s/302".to_string()]);
}

#[tokio::test]
async fn network_trouble_is_transient_and_never_leaks_the_url() {
    // Accepts, then hangs up.
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((s, _)) = l.accept().await {
                drop(s);
            }
        }
    });
    match adapter(format!("http://{addr}/hook?token=SEKRET"))
        .deliver(&delivery())
        .await
    {
        Outcome::Retry(msg) => assert!(!msg.contains("SEKRET"), "{msg}"),
        _ => panic!("expected a retry"),
    }

    let (base, _) = spawn().await;
    assert_eq!(
        kind(&adapter(format!("{base}/slow")).deliver(&delivery()).await),
        "retry"
    );
}

#[tokio::test]
async fn an_unusable_secret_fails_and_backend_trouble_retries_without_sending() {
    let (base, seen) = spawn().await;
    let url = format!("{base}/json");

    let missing = adapter_with(
        url.clone(),
        Arc::new(Failing(SecretError::Missing("no secret".into()))),
        &cfg(),
    );
    assert_eq!(kind(&missing.deliver(&delivery()).await), "failed");

    let down = adapter_with(url, Arc::new(Failing(SecretError::Unavailable)), &cfg());
    assert_eq!(kind(&down.deliver(&delivery()).await), "retry");

    assert_eq!(seen.count(), 0);
}

#[tokio::test]
async fn plain_http_needs_explicit_permission() {
    let (base, seen) = spawn().await;
    let strict = WebhookConfig {
        allow_insecure_http: false,
        ..cfg()
    };
    let a = adapter_with(
        format!("{base}/json"),
        Arc::new(StaticSecrets::new().with(REF, SECRET)),
        &strict,
    );
    match a.deliver(&delivery()).await {
        Outcome::Failed(msg) => assert!(msg.contains("https"), "{msg}"),
        _ => panic!("expected a failure"),
    }
    assert_eq!(seen.count(), 0);
}

#[tokio::test]
async fn response_bodies_are_capped() {
    let (base, _) = spawn().await;
    match adapter(format!("{base}/big/200"))
        .deliver(&delivery())
        .await
    {
        Outcome::Succeeded(v) => assert!(v["response"].as_str().unwrap().len() <= 2048),
        _ => panic!("expected success"),
    }
    match adapter(format!("{base}/big/400"))
        .deliver(&delivery())
        .await
    {
        Outcome::Failed(msg) => assert!(msg.len() <= 300, "{}", msg.len()),
        _ => panic!("expected a failure"),
    }
}
