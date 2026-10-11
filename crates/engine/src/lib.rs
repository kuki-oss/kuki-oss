//! The decision evaluator: a Postgres-backed ontology view, per-organisation
//! configuration, the generator registry and the orchestrator.

pub mod config;
pub mod generators;
pub mod registry;
pub mod view;

pub use config::{ConfigError, load_config, parse_config};
pub use generators::deadline::DeadlineAtRisk;
pub use registry::{Registry, RegistryError, Skipped};
pub use view::PgOntologyView;
