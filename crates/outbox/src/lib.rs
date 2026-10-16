//! Publishes the transactional outbox to Kafka.

mod error;
mod kafka;
mod publisher;
mod relay;
mod run;
mod consume;
pub mod signing;

pub use error::RelayError;
pub use kafka::{KafkaConfig, KafkaPublisher, ensure_topics};
pub use publisher::{Message, PublishFailure, Published, Publisher};
pub use relay::{LEADER_LOCK_KEY, Relay, RelayConfig, Step};
pub use consume::{ConsumerConfig, Fanout, FanoutConsumer, Handled, Incoming, Parsed, Skip, parse};
pub use run::{run_fanout_from_env, run_from_env};
