//! The decision evaluator: a Postgres-backed ontology view, per-organisation configuration, the
//! generator registry and the orchestrator that turns generator output into recommendations.

pub mod builtin;
pub mod config;
pub mod evaluator;
pub mod generators;
pub mod registry;
pub mod view;

pub use config::{ConfigError, load_config, parse_config};
pub use evaluator::{
    EngineError, Evaluator, EvaluatorOptions, GeneratorReport, Rejection, RunReport,
};
pub use generators::deadline::DeadlineAtRisk;
pub use registry::{Registry, RegistryError, Skipped};
pub use view::PgOntologyView;
