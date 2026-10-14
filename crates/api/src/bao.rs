use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use reqwest::header::HeaderValue;
use serde_json::{Value, json};

use crate::auth::KeyHasher;
use crate::config::Secret;
use crate::error::ApiError;
use crate::state::Readiness;

const UNAVAILABLE: &str = "secrets backend unavailable";

pub struct BaoClient {
    http: reqwest::Client,
    addr: String,
    token: Secret,
}

fn ensure_crypto_provider() {
    // Err means a provider is already installed (another client, another
    // test thread). That is fine, so the result is deliberately ignored.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

impl BaoClient {
    pub fn new(addr: String, token: Secret) -> Result<Self, reqwest::Error> {
        ensure_crypto_provider();
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            http,
            addr: addr.trim_end_matches('/').to_string(),
            token,
        })
    }

    fn token_header(&self) -> Result<HeaderValue, ApiError> {
        let mut v = HeaderValue::from_str(self.token.expose_str())
            .map_err(|_| ApiError::internal("openbao token is not a valid header value"))?;
        v.set_sensitive(true); // never printed by Debug
        Ok(v)
    }

    async fn finish(&self, res: reqwest::RequestBuilder, path: &str) -> Result<Value, ApiError> {
        let res = res.send().await.map_err(|e| {
            tracing::warn!(error = %e, path, "openbao request failed");
            ApiError::unavailable(UNAVAILABLE)
        })?;
        if !res.status().is_success() {
            tracing::warn!(status = %res.status(), path, "openbao returned an error");
            return Err(ApiError::unavailable(UNAVAILABLE));
        }
        res.json().await.map_err(ApiError::internal)
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        let req = self
            .http
            .post(format!("{}/v1/{path}", self.addr))
            .header("X-Vault-Token", self.token_header()?)
            .json(&body);
        self.finish(req, path).await
    }

    async fn get(&self, path: &str) -> Result<Value, ApiError> {
        let req = self
            .http
            .get(format!("{}/v1/{path}", self.addr))
            .header("X-Vault-Token", self.token_header()?);
        self.finish(req, path).await
    }
}

pub struct BaoHasher {
    pub client: Arc<BaoClient>,
    pub key_name: String,
}

#[async_trait]
impl KeyHasher for BaoHasher {
    async fn hash(&self, secret: &str) -> Result<[u8; 32], ApiError> {
        let v = self
            .client
            .post(
                &format!("transit/hmac/{}/sha2-256", self.key_name),
                json!({ "input": B64.encode(secret.as_bytes()) }),
            )
            .await?;

        // data.hmac looks like "vault:v1:<base64>"
        let tagged = v["data"]["hmac"]
            .as_str()
            .ok_or_else(|| ApiError::internal("bad transit response"))?;
        let b64 = tagged.rsplit(':').next().unwrap_or_default();
        let bytes = B64.decode(b64).map_err(ApiError::internal)?;
        bytes
            .try_into()
            .map_err(|_| ApiError::internal("unexpected hmac length"))
    }
}

pub struct BaoReadiness(pub Arc<BaoClient>);

#[async_trait]
impl Readiness for BaoReadiness {
    async fn check(&self) -> Result<(), ApiError> {
        // 200 = active, 429 = unsealed standby; 503 sealed, 501 uninitialised.
        let health = self
            .0
            .http
            .get(format!("{}/v1/sys/health", self.0.addr))
            .send()
            .await;
        match health {
            Ok(r) if matches!(r.status().as_u16(), 200 | 429) => {}
            _ => return Err(ApiError::unavailable(UNAVAILABLE)),
        }
        // The token must still be valid, otherwise every authentication would 503.
        self.0.get("auth/token/lookup-self").await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::Path;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    use super::*;

    const TOKEN: &str = "good-token";
    const MOCK_KEY: &[u8] = b"mock-transit-key";

    fn token_ok(h: &HeaderMap) -> bool {
        h.get("x-vault-token").map(|v| v.as_bytes()) == Some(TOKEN.as_bytes())
    }

    fn reference(secret: &str) -> [u8; 32] {
        let mut m = Hmac::<Sha256>::new_from_slice(MOCK_KEY).unwrap();
        m.update(secret.as_bytes());
        m.finalize().into_bytes().into()
    }

    async fn transit(
        Path(name): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        if !token_ok(&headers) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({ "errors": ["permission denied"] })),
            );
        }
        if name != "kuki-api-keys" {
            return (StatusCode::NOT_FOUND, Json(json!({})));
        }
        let raw = B64.decode(body["input"].as_str().unwrap()).unwrap();
        let mut m = Hmac::<Sha256>::new_from_slice(MOCK_KEY).unwrap();
        m.update(&raw);
        let tag = B64.encode(m.finalize().into_bytes());
        (
            StatusCode::OK,
            Json(json!({ "data": { "hmac": format!("vault:v1:{tag}") } })),
        )
    }

    async fn lookup_self(headers: HeaderMap) -> (StatusCode, Json<Value>) {
        if token_ok(&headers) {
            (StatusCode::OK, Json(json!({ "data": {} })))
        } else {
            (StatusCode::FORBIDDEN, Json(json!({})))
        }
    }

    fn mock(health: StatusCode) -> Router {
        Router::new()
            .route("/v1/transit/hmac/{name}/sha2-256", post(transit))
            .route(
                "/v1/sys/health",
                get(move || async move { (health, Json(json!({}))) }),
            )
            .route("/v1/auth/token/lookup-self", get(lookup_self))
    }

    async fn spawn(app: Router) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn client(addr: String, token: &str) -> Arc<BaoClient> {
        Arc::new(BaoClient::new(addr, Secret::new(token.as_bytes().to_vec())).unwrap())
    }

    fn status(e: ApiError) -> StatusCode {
        e.into_response().status()
    }

    #[tokio::test]
    async fn hasher_returns_the_backend_hmac() {
        let addr = spawn(mock(StatusCode::OK)).await;
        let h = BaoHasher {
            client: client(addr, TOKEN),
            key_name: "kuki-api-keys".into(),
        };
        let secret = "a".repeat(64);
        assert_eq!(h.hash(&secret).await.unwrap(), reference(&secret));
    }

    #[tokio::test]
    async fn hasher_uses_the_configured_key_name() {
        let addr = spawn(mock(StatusCode::OK)).await;
        let h = BaoHasher {
            client: client(addr, TOKEN),
            key_name: "other".into(),
        };
        assert_eq!(
            status(h.hash("x").await.unwrap_err()),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn rejected_token_is_503_not_401() {
        let addr = spawn(mock(StatusCode::OK)).await;
        let h = BaoHasher {
            client: client(addr, "wrong"),
            key_name: "kuki-api-keys".into(),
        };
        assert_eq!(
            status(h.hash("x").await.unwrap_err()),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn unreachable_backend_is_503() {
        // A server that accepts and immediately hangs up: always a transport error,
        // with no freed-port race against sibling tests.
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                if let Ok((s, _)) = l.accept().await {
                    drop(s);
                }
            }
        });
        let h = BaoHasher {
            client: client(addr, TOKEN),
            key_name: "kuki-api-keys".into(),
        };
        assert_eq!(
            status(h.hash("x").await.unwrap_err()),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn readiness_accepts_active_and_standby() {
        for code in [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS] {
            let addr = spawn(mock(code)).await;
            assert!(
                BaoReadiness(client(addr, TOKEN)).check().await.is_ok(),
                "{code}"
            );
        }
    }

    #[tokio::test]
    async fn readiness_rejects_sealed_and_uninitialised() {
        for code in [StatusCode::SERVICE_UNAVAILABLE, StatusCode::NOT_IMPLEMENTED] {
            let addr = spawn(mock(code)).await;
            assert!(
                BaoReadiness(client(addr, TOKEN)).check().await.is_err(),
                "{code}"
            );
        }
    }

    #[tokio::test]
    async fn readiness_rejects_an_invalid_token() {
        let addr = spawn(mock(StatusCode::OK)).await;
        assert!(BaoReadiness(client(addr, "wrong")).check().await.is_err());
    }
}
