//! The `webhook` adapter: POSTs the approved action to the organisation's
//! registered endpoint, signed with a secret read from OpenBao at delivery
//! time.
//!
//! Request:
//!   POST `<endpoint>`   Content-Type: application/json
//!   Kuki-Signature: t=`<unix>`,v1=`<hex>`   (see kuki_outbox::signing; 300 s replay window)
//!   Idempotency-Key: `<execution id>`     (identical on every retry: receivers must dedupe on it)
//!   Kuki-Attempt: `<n>`
//!   body: { type: "action.requested", executionId, organisationId, recommendationId,
//!           actionType, attempt, createdBy, input }
//!
//! Outcomes: 2xx succeeds; 408/425/429/5xx and network errors are retried;
//! every other status (including 3xx: redirects are never followed) fails.

use std::{sync::Arc, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use kuki_outbox::signing;
use reqwest::{Url, header::CONTENT_TYPE};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    adapter::{Adapter, Job, Outcome},
    secrets::{SecretError, SecretSource},
};

/// Most of a response body we keep: for the result JSON and for error text.
const MAX_BODY: usize = 2048;
const MAX_DETAIL_CHARS: usize = 200;

#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// Whole-request timeout. Keep it below the executor's `attempt_timeout`.
    pub timeout: Duration,
    pub connect_timeout: Duration,
    /// Allow `http://` endpoints (development only).
    pub allow_insecure_http: bool,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(20),
            connect_timeout: Duration::from_secs(5),
            allow_insecure_http: false,
        }
    }
}

fn ensure_crypto_provider() {
    // Err means a provider is already installed; that is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// The shared HTTP client. Redirects are never followed: a registered endpoint
/// must not be able to bounce a signed request somewhere else.
pub fn client(cfg: &WebhookConfig) -> Result<reqwest::Client, String> {
    ensure_crypto_provider();
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(cfg.connect_timeout)
        .timeout(cfg.timeout)
        .user_agent(concat!("kuki-actions/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| e.without_url().to_string())
}

#[derive(Debug, Clone)]
pub struct Delivery {
    /// The execution id; sent as the idempotency key.
    pub id: Uuid,
    pub attempt: i64,
    /// Exact bytes sent and signed.
    pub body: Vec<u8>,
}

pub struct WebhookAdapter {
    http: reqwest::Client,
    url: String,
    secret_ref: String,
    secrets: Arc<dyn SecretSource>,
    allow_insecure: bool,
}

impl WebhookAdapter {
    pub fn new(
        http: reqwest::Client,
        url: String,
        secret_ref: String,
        secrets: Arc<dyn SecretSource>,
        allow_insecure: bool,
    ) -> Self {
        Self {
            http,
            url,
            secret_ref,
            secrets,
            allow_insecure,
        }
    }

    pub async fn deliver(&self, d: &Delivery) -> Outcome {
        let target = Target {
            http: &self.http,
            url: &self.url,
            secret_ref: &self.secret_ref,
            secrets: self.secrets.as_ref(),
            allow_insecure: self.allow_insecure,
        };
        post_signed(
            &target,
            &d.body,
            &[
                ("Idempotency-Key", d.id.to_string()),
                ("Kuki-Attempt", d.attempt.to_string()),
            ],
        )
        .await
    }
}

impl Adapter for WebhookAdapter {
    fn label(&self) -> String {
        format!("webhook:{}", public_url(&self.url))
    }

    fn run<'a>(&'a self, job: &'a Job) -> BoxFuture<'a, Outcome> {
        async move {
            let attempt = job.attempt as i64;
            let body = envelope(
                job.id,
                job.organisation_id,
                job.recommendation_id,
                &job.action_type,
                attempt,
                &job.input,
                json!(job.created_by),
            );
            match serde_json::to_vec(&body) {
                Ok(body) => {
                    self.deliver(&Delivery {
                        id: job.id,
                        attempt,
                        body,
                    })
                    .await
                }
                Err(e) => Outcome::Failed(format!("cannot encode request: {e}")),
            }
        }
        .boxed()
    }
}

/// Where and how to deliver one signed request.
pub(crate) struct Target<'a> {
    pub http: &'a reqwest::Client,
    pub url: &'a str,
    pub secret_ref: &'a str,
    pub secrets: &'a dyn SecretSource,
    pub allow_insecure: bool,
}

/// Signs `body` with the secret behind `secret_ref`, POSTs it, and maps the response to
/// an [`Outcome`]. `headers` are sent in addition to the content type and the signature.
pub(crate) async fn post_signed(
    t: &Target<'_>,
    body: &[u8],
    headers: &[(&'static str, String)],
) -> Outcome {
    let Ok(url) = Url::parse(t.url) else {
        return Outcome::Failed("invalid endpoint url".into());
    };
    let scheme_ok = url.scheme() == "https" || (t.allow_insecure && url.scheme() == "http");
    if !scheme_ok {
        return Outcome::Failed("endpoint must use https".into());
    }

    let secret = match t.secrets.read(t.secret_ref).await {
        Ok(s) => s,
        Err(SecretError::Missing(why)) => return Outcome::Failed(why),
        Err(SecretError::Unavailable) => {
            return Outcome::Retry("secrets backend unavailable".into());
        }
    };
    let signature = signing::sign(secret.expose(), chrono::Utc::now().timestamp(), body);

    let mut req = t
        .http
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .header(signing::HEADER, signature);
    for (name, value) in headers {
        req = req.header(*name, value.as_str());
    }
    let res = match req.body(body.to_vec()).send().await {
        Ok(r) => r,
        Err(e) => {
            let builder = e.is_builder();
            // Never include the URL: it may carry a query token.
            let msg = e.without_url().to_string();
            return if builder {
                Outcome::Failed(format!("cannot build request: {msg}"))
            } else {
                Outcome::Retry(format!("request failed: {msg}"))
            };
        }
    };

    let status = res.status().as_u16();
    let body = snippet(res, MAX_BODY).await;
    match classify(status) {
        Class::Success => Outcome::Succeeded(json!({ "status": status, "response": parse(&body) })),
        Class::Transient => Outcome::Retry(format!("HTTP {status}{}", detail(&body))),
        Class::Permanent => Outcome::Failed(format!("HTTP {status}{}", detail(&body))),
    }
}

pub(crate) fn envelope(
    id: Uuid,
    organisation_id: Uuid,
    recommendation_id: Uuid,
    action_type: &str,
    attempt: i64,
    input: &Value,
    created_by: Value,
) -> Value {
    json!({
        "type": "action.requested",
        "executionId": id,
        "organisationId": organisation_id,
        "recommendationId": recommendation_id,
        "actionType": action_type,
        "attempt": attempt,
        "createdBy": created_by,
        "input": input,
    })
}

#[derive(Debug, PartialEq, Eq)]
enum Class {
    Success,
    Transient,
    Permanent,
}

fn classify(status: u16) -> Class {
    match status {
        200..=299 => Class::Success,
        408 | 425 | 429 | 500..=599 => Class::Transient,
        _ => Class::Permanent,
    }
}

/// The endpoint as shown in `action_executions.adapter`: no credentials, query or fragment.
pub(crate) fn public_url(raw: &str) -> String {
    match Url::parse(raw) {
        Ok(mut u) => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.set_query(None);
            u.set_fragment(None);
            u.to_string()
        }
        Err(_) => "invalid-url".into(),
    }
}

/// At most `max` bytes of the body, never buffering the rest.
async fn snippet(mut res: reqwest::Response, max: usize) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < max {
        match res.chunk().await {
            Ok(Some(chunk)) => {
                let take = chunk.len().min(max - out.len());
                out.extend_from_slice(&chunk[..take]);
            }
            _ => break,
        }
    }
    out
}

fn parse(body: &[u8]) -> Value {
    if body.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()))
}

fn detail(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text: String = text.trim().chars().take(MAX_DETAIL_CHARS).collect();
    if text.is_empty() {
        String::new()
    } else {
        format!(": {text}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_are_classified() {
        for s in [200, 201, 202, 204, 299] {
            assert_eq!(classify(s), Class::Success, "{s}");
        }
        for s in [408, 425, 429, 500, 502, 503, 504, 599] {
            assert_eq!(classify(s), Class::Transient, "{s}");
        }
        for s in [301, 302, 400, 401, 403, 404, 409, 410, 422] {
            assert_eq!(classify(s), Class::Permanent, "{s}");
        }
    }

    #[test]
    fn the_public_form_hides_credentials_query_and_fragment() {
        assert_eq!(
            public_url("https://user:pw@example.org/hooks/po?token=abc#frag"),
            "https://example.org/hooks/po"
        );
        assert_eq!(public_url("not a url"), "invalid-url");
    }

    #[test]
    fn the_envelope_has_the_documented_fields() {
        let (id, org, rec) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3));
        let v = envelope(
            id,
            org,
            rec,
            "draft_po",
            2,
            &json!({ "a": 1 }),
            json!("alice"),
        );
        assert_eq!(v["type"], "action.requested");
        assert_eq!(v["executionId"], id.to_string());
        assert_eq!(v["organisationId"], org.to_string());
        assert_eq!(v["recommendationId"], rec.to_string());
        assert_eq!(v["actionType"], "draft_po");
        assert_eq!(v["attempt"], 2);
        assert_eq!(v["createdBy"], "alice");
        assert_eq!(v["input"], json!({ "a": 1 }));
    }
}
