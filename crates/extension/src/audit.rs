use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::error::ExtensionError;

#[derive(Clone, Debug, PartialEq)]
pub struct AuditEntry {
    pub id: Uuid,
    pub organisation_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    /// `key:<uuid>`, `user:<sub>` or `policy:<id>`
    pub actor: String,
    /// Example `recommendation.accepted`
    pub action: String,
    /// What it acted on, for example `recommendation:<uuid>`.
    pub subject: String,
    pub details: Value,
}

#[async_trait]
pub trait AuditSink: Send + Sync {
    async fn record(&self, entry: &AuditEntry) -> Result<(), ExtensionError>;
}

/// The Kuki OSS default: kernel's own append-only table is the audit log
pub struct NoopAuditSink;

#[async_trait]
impl AuditSink for NoopAuditSink {
    async fn record(&self, _entry: &AuditEntry) -> Result<(), ExtensionError> {
        Ok(())
    }
}
