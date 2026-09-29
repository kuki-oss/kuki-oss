//! Core data types for Kuki intelligence recommendations.
//!
//! Mirrors the PostgreSQL `kuki_recommendations` table structure and maps
//! directly to the GraphQL types exposed by `api-service` to the frontend dashboard.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationType {
    Restock,
    Reschedule,
    PricingFlag,
    CrossSell,
    DemandAlert,
}

impl RecommendationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Restock => "restock",
            Self::Reschedule => "reschedule",
            Self::PricingFlag => "pricing_flag",
            Self::CrossSell => "cross_sell",
            Self::DemandAlert => "demand_alert",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationStatus {
    Pending,
    Accepted,
    Rejected,
    AutoExecuted,
}

impl RecommendationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::AutoExecuted => "auto_executed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KukiFactor {
    pub label: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KukiRationale {
    pub summary: String,
    pub factors: Vec<KukiFactor>,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestockAction {
    pub product_id: Uuid,
    pub suggested_supplier_id: Uuid,
    pub suggested_quantity: f64,
    pub days_until_stockout: f64,
    pub target_coverage_days: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KukiRecommendation {
    pub id: Uuid,
    pub merchant_id: Uuid,
    pub r#type: RecommendationType,
    pub source: String,
    pub proposed: bool,
    pub payload: serde_json::Value,
    pub rationale: KukiRationale,
    pub confidence: f64,
    pub status: RecommendationStatus,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}
