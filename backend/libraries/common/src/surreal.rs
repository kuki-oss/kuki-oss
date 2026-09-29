//! SurrealDB supply graph client, using the official Rust SDK.
//! Shared between kuki-engine (reads supply edges) and api-service
//! (writes supplier/product nodes + edges on confirmed merchant
//! uploads).
//!
//! NOTE: The surrealdb crate API has changed across major versions
//! (record-id binding types, SurrealValue vs Serialize). This is
//! written against the currently-documented pattern as of writing.
//! If cargo reports a type mismatch on RecordId/Thing or the bind()
//! signature, check docs.rs for the correct version ;), these are
//! just mechanical fixes not design changes.

use surrealdb::{
    Surreal,
    types::SurrealValue,
    engine::any::{connect, Any},
    opt::auth::Root,
};
use uuid::Uuid;

use crate::errors::SurrealError;

#[derive(Debug, Clone, SurrealValue)]
pub struct SupplyEdge {
    pub supplier_id: String,
    pub avg_lead_time_days: f64,
    pub lead_time_variance: f64,
    pub lead_time_samples: i32,
    pub reliability_score: f64,
    pub moq: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct SurrealClient {
    db: Surreal<Any>,
}

impl SurrealClient {
    /// Connects, authenticates, and selects namespace/database once
    /// Call this once at service startup and share the resulting client
    /// (it's cheap to clone, Surreal<Any> wraps an internal connection
    /// handle) rather than reconnecting per request
    pub async fn connect(
        url: &str,
        username: String,
        password: String,
        namespace: &str,
        database: &str,
    ) -> Result<Self, SurrealError> {
        let db = connect(url)
            .await
            .map_err(|e| SurrealError::Connect(e.to_string()))?;

        db.signin(Root { username, password })
        .await
        .map_err(|e| SurrealError::Auth(e.to_string()))?;

        db.use_ns(namespace)
            .use_db(database)
            .await
            .map_err(|e| SurrealError::UseNsDb(e.to_string()))?;

        Ok(Self { db })
    }

    /// Read path. Returns None on any failure or missing edge, callers
    /// fallback to Postgres supplier defaults (unchanged behaviour)
    pub async fn get_supply_edges(&self, product_slug: &str) -> Option<SupplyEdge> {
        match self.get_supply_edges_inner(product_slug).await {
            Ok(edge) => edge,
            Err(e) => {
                tracing::warn!(error = %e, product_slug = %product_slug, "supply edge lookup failed, falling back to Postgres defaults");
                None
            }
        }
    }

    async fn get_supply_edges_inner(&self, product_slug: &str) -> Result<Option<SupplyEdge>, SurrealError> {
        let mut result = self
            .db
            .query(
                r#"SELECT in.id AS supplier_id, avg_lead_time_days, lead_time_variance,
                          lead_time_samples, reliability_score, moq
                   FROM supplies
                   WHERE out = type::thing('product', $slug)
                   ORDER BY reliability_score DESC LIMIT 1;"#,
            )
            .bind(("slug", product_slug.to_string()))
            .await
            .map_err(|e| SurrealError::Query(e.to_string()))?;

        let edges: Vec<SupplyEdge> = result
            .take(0)
            .map_err(|e| SurrealError::Decode(e.to_string()))?;

        Ok(edges.into_iter().next())
    }

    pub async fn get_supply_edge_by_product_id(&self, _product_id: Uuid, product_slug: &str) -> Option<SupplyEdge> {
        self.get_supply_edges(product_slug).await
    }

    // ── Write paths ──────────────────────────────────────────────────

    pub async fn upsert_supplier_node(&self, id: Uuid, name: &str, merchant_id: Uuid) -> Result<(), SurrealError> {
        self.db
            .query(
                r#"UPSERT type::thing('supplier', $id)
                   SET id = $id_str, name = $name, merchant_id = $merchant_id;"#,
            )
            .bind(("id", id.to_string()))
            .bind(("id_str", id.to_string()))
            .bind(("name", name.to_string()))
            .bind(("merchant_id", merchant_id.to_string()))
            .await
            .map_err(|e| SurrealError::Query(e.to_string()))?;
        Ok(())
    }

    pub async fn upsert_product_node(
        &self,
        product_id: Uuid,
        slug: &str,
        sku: &str,
        category_path: &[String],
        merchant_id: Uuid,
    ) -> Result<(), SurrealError> {
        self.db
            .query(
                r#"UPSERT type::thing('product', $slug)
                   SET id = $product_id, sku = $sku,
                       category_path = $category_path, merchant_id = $merchant_id;"#,
            )
            .bind(("slug", slug.to_string()))
            .bind(("product_id", product_id.to_string()))
            .bind(("sku", sku.to_string()))
            .bind(("category_path", category_path.to_vec()))
            .bind(("merchant_id", merchant_id.to_string()))
            .await
            .map_err(|e| SurrealError::Query(e.to_string()))?;
        Ok(())
    }

    /// Both nodes must already exist (SCHEMAFULL, no implicit
    /// vivification), call the two upserts above first
    pub async fn upsert_supply_edge(
        &self,
        supplier_id: Uuid,
        product_slug: &str,
        avg_lead_time_days: f64,
        lead_time_variance: f64,
        reliability_score: f64,
        lead_time_samples: i32,
        moq: Option<f64>,
    ) -> Result<(), SurrealError> {
        self.db
            .query(
                r#"RELATE type::thing('supplier', $supplier_id)->supplies->type::thing('product', $slug)
                   SET avg_lead_time_days = $avg_lead_time_days,
                       lead_time_variance = $lead_time_variance,
                       reliability_score = $reliability_score,
                       lead_time_samples = $lead_time_samples,
                       moq = $moq;"#,
            )
            .bind(("supplier_id", supplier_id.to_string()))
            .bind(("slug", product_slug.to_string()))
            .bind(("avg_lead_time_days", avg_lead_time_days))
            .bind(("lead_time_variance", lead_time_variance))
            .bind(("reliability_score", reliability_score))
            .bind(("lead_time_samples", lead_time_samples))
            .bind(("moq", moq))
            .await
            .map_err(|e| SurrealError::Query(e.to_string()))?;
        Ok(())
    }
}
