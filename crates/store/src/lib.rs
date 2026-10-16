pub mod admin;
pub mod audit;
pub mod backfill;
pub mod catalog;
pub mod dirty;
pub mod engine_reads;
pub mod executions;
pub mod ingest;
pub mod links;
pub mod metrics;
pub mod objects;
pub mod ontology;
pub mod outbox;
pub mod recommendation_reads;
pub mod recommendations;
pub mod session;
pub mod tenant;
pub mod webhook_endpoints;

/// Migrations are embedded at compile time, used by `kuki serve` on boot and by tests
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");
