//! kuki actions: runs accepted actions.
//!
//! Accepting a recommendation queues an `action_executiions` row. The [`Executor`] claims queued
//! rows, hands each to an [`Adapter`] chosen by a [`Router`], and settles the execution and its
//! recommendation together. Delivery is at least once, so adapters must be idempotent per
//! [`Job::id`]. Several executors may run side by side: claims never overlap.

mod adapter;
mod executor;

pub use adapter::{Adapter, Job, Outcome, Router, StaticRouter};
pub use executor::{ACTOR, Config, Executor, Report};
