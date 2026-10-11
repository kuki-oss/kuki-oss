use async_trait::async_trait;
use uuid::Uuid;

use kuki_store::outbox::OutboxRow;

/// One outbox row, ready to publish.
#[derive(Clone, Debug)]
pub struct Message {
    pub id: i64,
    pub organisation_id: Uuid,
    pub topic: String,
    pub key: String,
    pub payload: serde_json::Value,
    pub attempts: i32,
}

impl From<OutboxRow> for Message {
    fn from(r: OutboxRow) -> Self {
        Message {
            id: r.id,
            organisation_id: r.organisation_id,
            topic: r.topic,
            key: r.key,
            payload: r.payload,
            attempts: r.attempts,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PublishFailure {
    pub reason: String,
    /// False for a message the broker will never accept (for example, too large): it is parked
    /// as dead instead of retried forever. Broker outages are always retriable.
    pub retriable: bool,
}

/// The result of publishing a batch in order.
#[derive(Debug)]
pub struct Published {
    /// How many leading messages the broker acknowledged.
    pub acked: usize,
    /// Why the message at index `acked` was not acknowledged. `None` means the whole batch was.
    pub failure: Option<PublishFailure>,
}

#[async_trait]
pub trait Publisher: Send + Sync {
    /// Publish in order and report the acknowledged prefix.
    async fn publish(&self, batch: &[Message]) -> Published;
}
