//! Every GraphQL error carries `extensions.code` (spec 3.3). Errors from resolvers
//! already do (via `From<ApiError>`); this fills in the ones async-graphql raises itself.

use async_graphql::{Response, ServerError};

pub fn normalise(mut resp: Response) -> Response {
    for e in &mut resp.errors {
        fill(e);
    }
    resp
}

fn fill(e: &mut ServerError) {
    if e.extensions
        .as_ref()
        .is_some_and(|x| x.get("code").is_some())
    {
        return;
    }
    let msg = e.message.clone();
    let from_engine = e.path.is_empty(); // parse and validation errors have no path
    let ext = e.extensions.get_or_insert_with(Default::default);
    if msg.starts_with("Query is nested too deep") {
        ext.set("code", "VALIDATION_FAILED");
        ext.set("reason", "DEPTH_LIMIT_EXCEEDED");
    } else if msg.starts_with("Query is too complex") {
        // `cost` extension needs the computed value, which async-graphql does not expose.
        ext.set("code", "VALIDATION_FAILED");
        ext.set("reason", "COST_LIMIT_EXCEEDED");
    } else if from_engine {
        ext.set("code", "VALIDATION_FAILED");
    } else {
        // Unknown resolver failure: never leak its text.
        ext.set("code", "INTERNAL");
        e.message = "internal error".to_owned();
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::Value;

    use super::*;
    use crate::graphql::{Limits, build};

    fn ext<'a>(r: &'a Response, k: &str) -> Option<&'a Value> {
        r.errors[0].extensions.as_ref().and_then(|x| x.get(k))
    }

    #[tokio::test]
    async fn depth_limit_maps_to_validation_failed_with_reason() {
        let s = build(Limits {
            depth: 1,
            complexity: 500,
        });
        let r = normalise(s.execute("{ viewer { organisation { id } } }").await);
        assert_eq!(ext(&r, "code"), Some(&Value::from("VALIDATION_FAILED")));
        assert_eq!(
            ext(&r, "reason"),
            Some(&Value::from("DEPTH_LIMIT_EXCEEDED"))
        );
    }

    #[tokio::test]
    async fn syntax_error_maps_to_validation_failed() {
        let r = normalise(build(Limits::DEFAULT).execute("{ viewer ").await);
        assert_eq!(ext(&r, "code"), Some(&Value::from("VALIDATION_FAILED")));
    }

    #[tokio::test]
    async fn page_sized_complexity_maps_to_cost_limit() {
        let s = build(Limits {
            depth: 12,
            complexity: 5,
        });
        let r = normalise(
            s.execute("{ orgUnits(first: 100) { edges { cursor } } }")
                .await,
        );
        assert_eq!(ext(&r, "code"), Some(&Value::from("VALIDATION_FAILED")));
        assert_eq!(ext(&r, "reason"), Some(&Value::from("COST_LIMIT_EXCEEDED")));
    }
}
