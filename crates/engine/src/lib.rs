//! The decision evaluator: a Postgres-backed ontology view, per-organisation configuration, the
//! generator registry, the orchestrator and the service that drives it.

pub mod builtin;
pub mod config;
pub mod evaluator;
pub mod generators;
pub mod pack;
pub mod registry;
pub mod run;
pub mod runner;
pub mod view;

pub use config::{ConfigError, load_config, parse_config};
pub use evaluator::{
    EngineError, Evaluator, EvaluatorOptions, GeneratorReport, Rejection, RunReport,
};
pub use generators::deadline::DeadlineAtRisk;
pub use pack::{DecisionTypeDef, Pack, PackError};
pub use registry::{Registry, RegistryError, Skipped};
pub use runner::{DrainReport, Leader, Runner, RunnerConfig, RunnerError};
pub use view::PgOntologyView;
