//! GraphQL schema root — wires together the query, mutation, and type modules.
//!
//! Exposes the shared `TcsSchema` type alias, auth helpers used by resolvers,
//! and the `build` function that constructs the executable schema.

pub mod mutation;
pub mod query;
pub mod types;
pub mod jwks;

#[derive(Clone)]
pub struct UploadBucket(pub String);

/// The concrete GraphQL schema type for the entire application.
pub type TcsSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// Extract a Bearer token OR a sandbox_session_id cookie from the request
/// headers, resolve to a merchant, and inject both `Claims` (existing shape,
/// JWT path only) and `MerchantAuth` (new, both paths) into the context.
/// Missing/invalid credentials on both paths are silently ignored so
/// unauthenticated resolvers still work, exactly as before.
pub async fn graphql_request_with_auth(
  headers: &HeaderMap,
  request: async_graphql::Request,
  jwks: &JwksCache,
  db: &PgPool,
) -> async_graphql::Request {
  let token = headers
    .get(axum::http::header::AUTHORIZATION)
    .and_then(|v| v.to_str().ok())
    .and_then(|v| v.strip_prefix("Bearer "));

  if let Some(t) = token {
    match jwks.verify(t).await {
      Ok(claims) => {
        let merchant_id = match Uuid::parse_str(&claims.sub) {
          Ok(id) => id,
          Err(_) => return request,
        };
        return match crate::merchant::queries::merchant_active_and_kind(db, merchant_id).await {
          Ok(Some((true, kind_str))) => {
            let request = request.data(claims);
            request.data(MerchantAuth {
              id: merchant_id,
              kind: MerchantKind::from_db_str(&kind_str),
            })
          }
          Ok(Some((false, _))) => {
            tracing::warn!("token valid but merchant {merchant_id} is deactivated/deleted");
            request
          }
          Ok(None) => {
            tracing::warn!("token valid but merchant {merchant_id} not found");
            request
          }
          Err(e) => {
            tracing::error!("merchant active-check failed: {e}");
            request
          }
        };
      }
      Err(e) => {
        tracing::warn!("JWT verification failed: {e}");
        return request;
      }
    }
  }

  // ── No Bearer token: try the sandbox session cookie path ──────────────
  if let Some(session_token) = extract_cookie(headers, "sandbox_session_id") {
    return match crate::merchant::queries::resolve_sandbox_session(db, &session_token).await {
      Ok(Some(merchant_id)) => request.data(MerchantAuth {
        id: merchant_id,
        kind: MerchantKind::Sandbox,
      }),
      Ok(None) => {
        tracing::warn!("sandbox_session_id cookie present but no live session found");
        request
      }
      Err(e) => {
        tracing::error!("sandbox session lookup failed: {e}");
        request
      }
    };
  }

  request
}

/// Minimal manual cookie-header parser, no cookie crate currently in the
/// dependency tree. Looks for `name=value` in the `Cookie` header.
fn extract_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let pair = pair.trim();
        let (k, v) = pair.split_once('=')?;
        if k == name {
            Some(v.to_string())
        } else {
            None
        }
    })
}

/// Build the executable GraphQL schema, injecting the database connection pool
/// as shared context data.
pub fn build(
    pool: PgPool,
    s3_client: aws_sdk_s3::Client,
    upload_bucket: String,
) -> TcsSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(pool)
        .data(s3_client)
        .data(UploadBucket(upload_bucket))
        .finish()
}
