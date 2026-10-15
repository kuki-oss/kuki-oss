use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::Value;
use uuid::Uuid;

/// One attempt to run an accepted action.
#[derive(Clone, Debug)]
pub struct Job {
    /// Stable across attempts: send it to the target so a repeated delivery can be recognised.
    pub id: Uuid,
    pub organisation_id: Uuid,
    pub recommendation_id: Uuid,
    pub action_type: String,
    /// `{ recommendationId, decisionType, actionType, payload, overridden }`, where `payload` is
    /// what the operator approved (their override, when they made one).
    pub input: Value,
    /// 1 on the first attempt.
    pub attempt: i32,
    /// Who accepted the recommendation.
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// Stored as the execution's `result`.
    Succeeded(Value),
    /// Worth trying again later: a timeout, a 5xx, a refused connection.
    Retry(String),
    /// Will never work: the target rejected the payload, or the action is unsupported.
    Failed(String),
}

pub trait Adapter: Send + Sync {
    /// Recorded in `action_executions.adapter`, for example `webhook:https://api.kuki.co.ke/hook`.
    fn label(&self) -> String;
    /// Delivery is at least once: this must be safe to repeat for the same `job.id`.
    fn run<'a>(&'a self, job: &'a Job) -> BoxFuture<'a, Outcome>;
}

/// Chooses the adapter for an action. An `Err` is a transient lookup failure and is retried;
/// `Ok(None)` fails the execution with "no adapter".
pub trait Router: Send + Sync {
    fn route<'a>(
        &'a self,
        organisation_id: Uuid,
        action_type: &'a str,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn Adapter>>, String>>;
}

/// Maps exact action types to adapters, the same for every organisation.
#[derive(Default)]
pub struct StaticRouter {
    by_type: BTreeMap<String, Arc<dyn Adapter>>,
}

impl StaticRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with<A: Adapter + 'static>(
        mut self,
        action_type: impl Into<String>,
        adapter: Arc<A>,
    ) -> Self {
        self.by_type.insert(action_type.into(), adapter);
        self
    }
}

impl Router for StaticRouter {
    fn route<'a>(
        &'a self,
        _organisation_id: Uuid,
        action_type: &'a str,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn Adapter>>, String>> {
        Box::pin(async move { Ok(self.by_type.get(action_type).cloned()) })
    }
}
