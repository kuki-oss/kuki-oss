//! GraphQL data plane (contract: api/schema/kuki.graphql).
//! Organisation is never an argument: it comes from the `Principal`.

mod catalog;
mod connection;
mod guard;
mod ontology;
mod problems;
mod query;
mod registry;

use std::sync::OnceLock;

use async_graphql::{EmptyMutation, EmptySubscription, Request, Response, Schema};
use axum::Json;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;

use crate::auth::Principal;
use crate::error::ApiError;
use crate::state::AppState;

pub use query::QueryRoot;

pub type AppSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Query limits. Placeholders until connection fields exist; page-size aware
/// complexity comes with `orgUnits`.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub depth: usize,
    pub complexity: usize,
}

impl Limits {
    pub const DEFAULT: Limits = Limits {
        depth: 12,
        complexity: 5000,
    };
}

pub fn build(limits: Limits) -> AppSchema {
    Schema::build(QueryRoot::default(), EmptyMutation, EmptySubscription)
        .limit_depth(limits.depth)
        .limit_complexity(limits.complexity)
        // Not referenced by any field; registered so SDKs see them (contract).
        .register_output_type::<registry::ErrorCodeRegistry>()
        .register_output_type::<registry::ErrorReasonRegistry>()
        .finish()
}

pub fn schema() -> &'static AppSchema {
    static SCHEMA: OnceLock<AppSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| build(Limits::DEFAULT))
}

/// `POST /v1/graphql`. The `Principal` extractor has already authenticated,
/// rate-limited and touched `last_used_at`.
pub async fn post_graphql(
    State(st): State<AppState>,
    principal: Principal,
    body: Result<Json<Request>, JsonRejection>,
) -> Result<Json<Response>, ApiError> {
    let Json(req) =
        body.map_err(|_| ApiError::validation("body is not a valid GraphQL request"))?;
    let req = req.data(principal).data(st.pool.clone());
    let resp = schema().execute(req).await;
    Ok(Json(problems::normalise(resp)))
}
