//! kuki actions: runs accepted actions.
//!
//! Accepting a recommendation queues an `action_executiions` row. The [`Executor`] claims queued
//! rows, hands each to an [`Adapter`] chosen by a [`Router`], and settles the execution and its
//! recommendation together. Delivery is at least once, so adapters must be idempotent per
//! [`Job::id`]. Several executors may run side by side: claims never overlap.

mod adapter;
mod builtin;
mod deliveries;
mod executor;
mod registry;
pub mod secrets;
mod webhook;

pub use adapter::{Adapter, Job, Outcome, Router, StaticRouter};
pub use builtin::{BuiltinRouter, FLAG_REVIEW, FlagReview, PREFIX as BUILTIN_PREFIX};
pub use deliveries::{DeliveryConfig, DeliveryReport, DeliveryWorker};
pub use executor::{ACTOR, Config, Executor, Report};
pub use registry::RegistryRouter;
pub use secrets::{
    CachedSecrets, SecretBytes, SecretError, SecretSource, StaticSecrets, valid_reference,
};
pub use webhook::{Delivery, WebhookAdapter, WebhookConfig, client as webhook_client};
