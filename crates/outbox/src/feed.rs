//! Live feed of recommendation events for GraphQL subscriptions.
//!
//! Every API process runs one consumer in a group of its own, starting at the
//! latest offset and never committing, so every process sees every event and a
//! restart starts from "now". Events go into a broadcast channel; each live
//! subscription holds a receiver and filters. A receiver that falls behind skips
//! events (`RecvError::Lagged`): delivery to a connected client is at most once,
//! and clients catch up with the `recommendationChanges` query.

use std::time::Duration;

use rdkafka::{
    Message as _,
    consumer::{Consumer, StreamConsumer},
};
use serde_json::Value;
use tokio::{
    sync::{broadcast, watch},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    consume::{Incoming, header, parse},
    error::RelayError,
    kafka::{KafkaConfig, base},
};

/// One recommendation event, as the relay published it.
#[derive(Clone, Debug, PartialEq)]
pub struct LiveEvent {
    pub organisation_id: Uuid,
    /// The outbox row id (`kuki-outbox-id`).
    pub event_id: String,
    /// `recommendation.created`, `.updated` or `.status_changed`.
    pub event_type: String,
    pub payload: Value,
}

#[derive(Clone)]
pub struct EventFeed {
    tx: broadcast::Sender<LiveEvent>,
}

impl EventFeed {
    /// A feed with no Kafka behind it. Events arrive through [`EventFeed::publish`]; this is
    /// what tests of the subscription layer use.
    pub fn local(capacity: usize) -> Self {
        Self { tx: broadcast::channel(capacity.max(1)).0 }
    }

    /// Delivers an event to every current receiver and returns how many there were.
    pub fn publish(&self, event: LiveEvent) -> usize {
        self.tx.send(event).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LiveEvent> {
        self.tx.subscribe()
    }

    /// Starts the consumer. It stops when `stop` becomes `true` (the same convention as the
    /// executor and the delivery worker); the handle completes once it has.
    pub fn start(
        kafka: &KafkaConfig,
        topic: &str,
        capacity: usize,
        stop: watch::Receiver<bool>,
    ) -> Result<(Self, JoinHandle<()>), RelayError> {
        let consumer: StreamConsumer = base(kafka)
            .set("group.id", format!("kuki-live-{}", Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .create()
            .map_err(|e| RelayError::Kafka(e.to_string()))?;
        consumer.subscribe(&[topic]).map_err(|e| RelayError::Kafka(e.to_string()))?;

        let feed = Self::local(capacity);
        let task = tokio::spawn(pump(consumer, feed.tx.clone(), stop));
        Ok((feed, task))
    }
}

async fn pump(consumer: StreamConsumer, tx: broadcast::Sender<LiveEvent>, mut stop: watch::Receiver<bool>) {
    loop {
        let received = tokio::select! {
            _ = stop.wait_for(|s| *s) => return,
            m = consumer.recv() => m,
        };
        let msg = match received {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "live feed: kafka receive failed");
                tokio::select! {
                    _ = stop.wait_for(|s| *s) => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                continue;
            }
        };
        let incoming = Incoming {
            outbox_id: header(&msg, "kuki-outbox-id"),
            organisation_id: header(&msg, "kuki-organisation-id"),
            payload: msg.payload(),
        };
        match parse(&incoming) {
            Ok(p) => {
                // No receivers (nobody is subscribed) is not an error.
                let _ = tx.send(LiveEvent {
                    organisation_id: p.organisation_id,
                    event_id: p.event_id,
                    event_type: p.event_type,
                    payload: p.payload,
                });
            }
            Err(why) => tracing::debug!(reason = %why, "live feed: skipped a message"),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::sync::broadcast::error::RecvError;

    use super::*;

    fn event(id: &str) -> LiveEvent {
        LiveEvent {
            organisation_id: Uuid::nil(),
            event_id: id.into(),
            event_type: "recommendation.created".into(),
            payload: json!({ "type": "recommendation.created", "id": "r1", "version": 1 }),
        }
    }

    #[tokio::test]
    async fn every_receiver_sees_every_event() {
        let feed = EventFeed::local(8);
        let (mut a, mut b) = (feed.subscribe(), feed.subscribe());
        assert_eq!(feed.publish(event("1")), 2);
        assert_eq!(a.recv().await.unwrap(), event("1"));
        assert_eq!(b.recv().await.unwrap(), event("1"));
        // Nobody listening is fine.
        drop((a, b));
        assert_eq!(feed.publish(event("2")), 0);
    }

    #[tokio::test]
    async fn a_slow_receiver_skips_ahead_instead_of_blocking_the_feed() {
        let feed = EventFeed::local(2);
        let mut slow = feed.subscribe();
        for i in 1..=5 {
            feed.publish(event(&i.to_string()));
        }
        assert!(matches!(slow.recv().await, Err(RecvError::Lagged(3))));
        assert_eq!(slow.recv().await.unwrap().event_id, "4");
        assert_eq!(slow.recv().await.unwrap().event_id, "5");
    }
}
