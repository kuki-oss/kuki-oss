pub mod admin;
pub mod backfill;
pub mod catalog;
pub mod ingest;
pub mod links;
pub mod objects;
pub mod ontology;
pub mod outbox;
pub mod session;
pub mod tenant;

/// Migrations are embedded at compile time, used by `kuki serve` on boot and by tests
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");
