//! Actions that ship with kuki-oss. They are domain-neutral: nothing here
//! knows what an organisation does, only that a person asked for something
//! to be flagged.
//!
//! They live in the `builtin:` namespace, which the router reserves. A
//! webhook endpoint registered for `builtin:flag_review` is neverr consulted,
//! so an organisation cannot shadow a builtin by accident or on purpose.

use std::sync::Arc;

use futures::{FutureExt, future::BoxFuture};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::adapter::{Adapter, Job, Outcome, Router, StaticRouter};

/// Every builtin action type starts with this.
pub const PREFIX: &str = "builtin:";
pub const FLAG_REVIEW: &str = "builtin:flag_review";

/// Marks a recommendation as needing a person's review, and records
/// what was approved.
///
/// It has no side effect outside kuki: the execution row is the durable
/// record, and it is readable through the API like any other execution.
/// Repeating it for the same job gives the same result, so at-least-once
/// delivery is harmless.
pub struct FlagReview;

impl Adapter for FlagReview {
    fn label(&self) -> String {
        FLAG_REVIEW.to_owned()
    }

    fn run<'a>(&'a self, job: &'a Job) -> BoxFuture<'a, Outcome> {
        async move {
            let Some(id) = job.input.get("recommendationId").and_then(Value::as_str) else {
                return Outcome::Failed("the action input has no recommendationId".into());
            };
            if id.parse::<Uuid>().ok() != Some(job.recommendation_id) {
                return Outcome::Failed(
                    "the action input names a different recommendation than the job".into(),
                );
            }
            Outcome::Succeeded(json!({
                "flagged": true,
                "recommendationId": job.recommendation_id,
                "executionId": job.id,
                "approvedBy": job.created_by,
                "payload": job.input.get("payload").cloned().unwrap_or(Value::Null),
            }))
        }
        .boxed()
    }
}

/// Serves the `builtin:` actions itself and hands every other action type to `rest`.
pub struct BuiltinRouter {
    builtin: StaticRouter,
    rest: Arc<dyn Router>,
}

impl BuiltinRouter {
    pub fn new(rest: Arc<dyn Router>) -> Self {
        Self {
            builtin: StaticRouter::new().with(FLAG_REVIEW, Arc::new(FlagReview)),
            rest,
        }
    }
}

impl Router for BuiltinRouter {
    fn route<'a>(
        &'a self,
        organisation_id: Uuid,
        action_type: &'a str,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn Adapter>>, String>> {
        // An unknown `builtin:` type resolves to nothing, so the execution fails visibly
        // rather than falling through to a webhook.
        if action_type.starts_with(PREFIX) {
            self.builtin.route(organisation_id, action_type)
        } else {
            self.rest.route(organisation_id, action_type)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Named(&'static str);

    impl Adapter for Named {
        fn label(&self) -> String {
            self.0.to_owned()
        }
        fn run<'a>(&'a self, _job: &'a Job) -> BoxFuture<'a, Outcome> {
            async { Outcome::Succeeded(Value::Null) }.boxed()
        }
    }

    fn job(recommendation_id: Uuid, input: Value) -> Job {
        Job {
            id: Uuid::new_v4(),
            organisation_id: Uuid::new_v4(),
            recommendation_id,
            action_type: FLAG_REVIEW.into(),
            input,
            attempt: 1,
            created_by: "key:ops".into(),
        }
    }

    fn fallback() -> Arc<dyn Router> {
        Arc::new(
            StaticRouter::new()
                .with("custom.notify", Arc::new(Named("custom")))
                // Present on purpose: the builtin namespace must not reach it.
                .with("builtin:nope", Arc::new(Named("shadow"))),
        )
    }

    #[tokio::test]
    async fn flag_review_records_what_was_approved() {
        let rec = Uuid::new_v4();
        let j = job(
            rec,
            json!({ "recommendationId": rec, "payload": { "objectId": "o-1" } }),
        );
        let Outcome::Succeeded(v) = FlagReview.run(&j).await else {
            panic!("expected success");
        };
        assert_eq!(v["flagged"], true);
        assert_eq!(v["recommendationId"], json!(rec));
        assert_eq!(v["executionId"], json!(j.id));
        assert_eq!(v["approvedBy"], "key:ops");
        assert_eq!(v["payload"]["objectId"], "o-1");
    }

    #[tokio::test]
    async fn flag_review_refuses_input_without_a_recommendation_id() {
        let j = job(Uuid::new_v4(), json!({ "payload": {} }));
        assert!(matches!(FlagReview.run(&j).await, Outcome::Failed(_)));
    }

    #[tokio::test]
    async fn flag_review_refuses_input_for_another_recommendation() {
        let j = job(
            Uuid::new_v4(),
            json!({ "recommendationId": Uuid::new_v4() }),
        );
        assert!(matches!(FlagReview.run(&j).await, Outcome::Failed(_)));
    }

    #[tokio::test]
    async fn the_router_serves_builtins_itself() {
        let r = BuiltinRouter::new(fallback());
        let a = r.route(Uuid::new_v4(), FLAG_REVIEW).await.unwrap().unwrap();
        assert_eq!(a.label(), FLAG_REVIEW);
    }

    #[tokio::test]
    async fn the_builtin_namespace_is_reserved() {
        let r = BuiltinRouter::new(fallback());
        let found = r.route(Uuid::new_v4(), "builtin:nope").await.unwrap();
        assert!(
            found.is_none(),
            "an unknown builtin must not reach the fallback"
        );
    }

    #[tokio::test]
    async fn other_action_types_go_to_the_fallback() {
        let r = BuiltinRouter::new(fallback());
        let a = r
            .route(Uuid::new_v4(), "custom.notify")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.label(), "custom");
        assert!(
            r.route(Uuid::new_v4(), "unknown.type")
                .await
                .unwrap()
                .is_none()
        );
    }
}
