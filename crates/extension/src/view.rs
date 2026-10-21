//! Read access to the ontology for generators. Every implementation MUST return only rows with
//! `recorded_at <= as_of` of the context it was built for; that is what makes evaluation
//! reproducible (invariant I1). Metrics currently are unavailable here, they will be added with
//! the first generator that needs them.
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::GenError;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub id: Uuid,
    pub type_name: String,
    pub type_version: i32,
    pub org_unit_id: Option<Uuid>,
    pub external_ids: Value,
    pub attrs: Value,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkRecord {
    pub id: Uuid,
    pub type_name: String,
    pub src_id: Uuid,
    pub dst_id: Uuid,
    pub attrs: Value,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: Uuid,
    pub type_name: String,
    pub type_version: i32,
    pub subject_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
    pub payload: Value,
    pub supersedes: Option<Uuid>,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventQuery {
    pub subject_id: Uuid,
    pub type_name: Option<String>,
    pub occurred_from: Option<DateTime<Utc>>,
    pub occurred_to: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait OntologyView: Send + Sync {
    /// The current version of an object as of the context clock.
    async fn object(&self, id: Uuid) -> Result<Option<ObjectRecord>, GenError>;
    async fn objects_of_type(
        &self,
        type_name: &str,
        org_unit: Option<Uuid>,
    ) -> Result<Vec<ObjectRecord>, GenError>;
    async fn links(
        &self,
        object: Uuid,
        type_name: Option<&str>,
    ) -> Result<Vec<LinkRecord>, GenError>;
    /// Ordered by `occurred_at` ascending, then `id`, so iteration order is deterministic.
    async fn events(&self, query: &EventQuery) -> Result<Vec<EventRecord>, GenError>;
}

/// A view with nothing in it. Used in generator and orchestrator tests.
pub struct EmptyView;

#[async_trait]
impl OntologyView for EmptyView {
    async fn object(&self, _id: Uuid) -> Result<Option<ObjectRecord>, GenError> {
        Ok(None)
    }
    async fn objects_of_type(
        &self,
        _t: &str,
        _u: Option<Uuid>,
    ) -> Result<Vec<ObjectRecord>, GenError> {
        Ok(vec![])
    }
    async fn links(&self, _o: Uuid, _t: Option<&str>) -> Result<Vec<LinkRecord>, GenError> {
        Ok(vec![])
    }
    async fn events(&self, _q: &EventQuery) -> Result<Vec<EventRecord>, GenError> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_empty_view_is_empty() {
        let v = EmptyView;
        assert!(v.object(Uuid::nil()).await.unwrap().is_none());
        assert!(v.objects_of_type("item", None).await.unwrap().is_empty());
        let q = EventQuery {
            subject_id: Uuid::nil(),
            type_name: None,
            occurred_from: None,
            occurred_to: None,
        };
        assert!(v.events(&q).await.unwrap().is_empty());
    }
}
