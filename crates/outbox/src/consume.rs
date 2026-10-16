//! Kafka → delivery queue. Consumes `kuki.recommendations` and queues each
//! event for every subscription that wants it (`webhook_deliveries`); the
//! delivery worker in `kuki-actions` sends them.
//!
//! At least once: an offset is stored only after its message was handled, and
//! librdkafka commits stored offsets in the background (plus once on shutdown).
//! A replay is harmless: deliveries are unique on `(subscription_id, event_id)`,
//! and `event_id` is the outbox row id from the `kuki-outbox-id` header.

use std::{future::Future, time::Duration};

use futures::FutureExt;
use kuki_store::{
    outbox::TOPIC_RECOMMENDATIONS,
    session::{OrgSession, with_org_tx},
    webhook_deliveries,
    webhook_subscriptions::EVENT_TYPES,
};
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{BorrowedMessage, Headers},
};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::RelayError,
    kafka::{KafkaConfig, base},
};

#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    pub group_id: String,
    pub topic: String,
    /// Where a group that has never committed starts: the beginning, or only new events.
    pub from_start: bool,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: "kuki-webhooks".into(),
            topic: TOPIC_RECOMMENDATIONS.into(),
            from_start: false,
        }
    }
}

impl ConsumerConfig {
    /// `KUKI_WEBHOOKS_GROUP` (default `kuki-webhooks`) and `KUKI_KAFKA_START`
    /// (`latest`, the default, or `earliest`; only matters for a group's first start).
    pub fn from_env() -> Result<Self, RelayError> {
        let mut c = Self::default();
        if let Ok(group) = std::env::var("KUKI_WEBHOOKS_GROUP") {
            if group.trim().is_empty() {
                return Err(RelayError::Config("KUKI_WEBHOOKS_GROUP must not be empty".into()));
            }
            c.group_id = group;
        }
        match std::env::var("KUKI_KAFKA_START").as_deref() {
            Err(_) | Ok("latest") => {}
            Ok("earliest") => c.from_start = true,
            Ok(other) => {
                return Err(RelayError::Config(format!(
                    "KUKI_KAFKA_START must be latest or earliest, got {other:?}"
                )));
            }
        }
        Ok(c)
    }
}

/// One Kafka message, reduced to what fan-out needs.
#[derive(Clone, Copy, Debug, Default)]
pub struct Incoming<'a> {
    /// `kuki-outbox-id` header.
    pub outbox_id: Option<&'a str>,
    /// `kuki-organisation-id` header.
    pub organisation_id: Option<&'a str>,
    pub payload: Option<&'a [u8]>,
}

#[derive(Debug, PartialEq)]
pub struct Parsed {
    pub organisation_id: Uuid,
    pub event_id: String,
    pub event_type: String,
    pub payload: Value,
}

/// Why a message is not delivered. Never retried: retrying cannot make it valid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Skip {
    MissingHeader(&'static str),
    BadOrganisationId,
    BadPayload,
    UnknownEventType(String),
    /// The database refused the row for a reason retrying will not fix.
    Rejected(String),
}

impl std::fmt::Display for Skip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeader(h) => write!(f, "missing header {h}"),
            Self::BadOrganisationId => f.write_str("kuki-organisation-id is not a UUID"),
            Self::BadPayload => f.write_str("payload is not a JSON object with a string `type`"),
            Self::UnknownEventType(t) => write!(f, "unknown event type {t:?}"),
            Self::Rejected(why) => write!(f, "rejected by the database: {why}"),
        }
    }
}

pub fn parse(m: &Incoming<'_>) -> Result<Parsed, Skip> {
    let event_id = m
        .outbox_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or(Skip::MissingHeader("kuki-outbox-id"))?;
    let org = m
        .organisation_id
        .ok_or(Skip::MissingHeader("kuki-organisation-id"))?;
    let organisation_id = Uuid::parse_str(org.trim()).map_err(|_| Skip::BadOrganisationId)?;
    let bytes = m.payload.ok_or(Skip::BadPayload)?;
    let payload: Value = serde_json::from_slice(bytes).map_err(|_| Skip::BadPayload)?;
    let event_type = payload
        .get("type")
        .and_then(Value::as_str)
        .ok_or(Skip::BadPayload)?
        .to_owned();
    if !EVENT_TYPES.contains(&event_type.as_str()) {
        return Err(Skip::UnknownEventType(event_type));
    }
    Ok(Parsed { organisation_id, event_id: event_id.to_owned(), event_type, payload })
}

#[derive(Debug, PartialEq, Eq)]
pub enum Handled {
    /// How many deliveries were newly queued (0 when nobody subscribes, or on a replay).
    Queued(usize),
    Skipped(Skip),
}

fn is_poison(e: &sqlx::Error) -> bool {
    match e {
        // Integrity (23) and data (22) errors: the same row will fail the same way every time.
        sqlx::Error::Database(d) => d.code().is_some_and(|c| c.starts_with("22") || c.starts_with("23")),
        _ => false,
    }
}

/// Queues an event for its organisation's subscriptions, under that organisation's tenant
/// context. `pool` connects as the application role.
#[derive(Clone)]
pub struct Fanout {
    pool: PgPool,
}

impl Fanout {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// `Err` is a transient database failure: the caller must retry the same message.
    pub async fn handle(&self, m: &Incoming<'_>) -> Result<Handled, sqlx::Error> {
        let p = match parse(m) {
            Ok(p) => p,
            Err(skip) => return Ok(Handled::Skipped(skip)),
        };
        let org = p.organisation_id;
        let session = OrgSession { org_id: org, scope_path: None };
        let queued = with_org_tx::<_, sqlx::Error, _>(&self.pool, &session, move |c| {
            async move {
                webhook_deliveries::enqueue_for_event(c, org, &p.event_id, &p.event_type, &p.payload)
                    .await
            }
            .boxed()
        })
        .await;
        match queued {
            Ok(n) => Ok(Handled::Queued(n)),
            Err(e) if is_poison(&e) => Ok(Handled::Skipped(Skip::Rejected(e.to_string()))),
            Err(e) => Err(e),
        }
    }
}

pub struct FanoutConsumer {
    consumer: StreamConsumer,
    fanout: Fanout,
}

impl FanoutConsumer {
    pub fn connect(kafka: &KafkaConfig, cfg: &ConsumerConfig, pool: PgPool) -> Result<Self, RelayError> {
        let consumer: StreamConsumer = base(kafka)
            .set("group.id", &cfg.group_id)
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "1000")
            // Offsets are stored only after a message is handled (see `run`).
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", if cfg.from_start { "earliest" } else { "latest" })
            .create()
            .map_err(|e| RelayError::Kafka(e.to_string()))?;
        consumer
            .subscribe(&[cfg.topic.as_str()])
            .map_err(|e| RelayError::Kafka(e.to_string()))?;
        Ok(Self { consumer, fanout: Fanout::new(pool) })
    }

    /// Consumes until `shutdown` completes.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) -> Result<(), RelayError> {
        tokio::pin!(shutdown);
        'messages: loop {
            let received = tokio::select! {
                _ = &mut shutdown => break 'messages,
                m = self.consumer.recv() => m,
            };
            let msg = match received {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "kafka receive failed");
                    tokio::select! {
                        _ = &mut shutdown => break 'messages,
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
            let mut delay = Duration::from_millis(500);
            loop {
                match self.fanout.handle(&incoming).await {
                    Ok(Handled::Queued(n)) => {
                        if n > 0 {
                            tracing::info!(event = incoming.outbox_id.unwrap_or(""), deliveries = n, "queued an event");
                        }
                        break;
                    }
                    Ok(Handled::Skipped(why)) => {
                        tracing::warn!(
                            topic = msg.topic(),
                            partition = msg.partition(),
                            offset = msg.offset(),
                            reason = %why,
                            "skipped an event that cannot be delivered"
                        );
                        break;
                    }
                    Err(e) => {
                        // Do not move on: the offset stays unstored, so nothing is lost.
                        tracing::error!(error = %e, retry_in_ms = delay.as_millis() as u64, "queueing an event failed; retrying");
                        tokio::select! {
                            _ = &mut shutdown => break 'messages,
                            _ = tokio::time::sleep(delay) => {}
                        }
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                }
            }
            if let Err(e) = self.consumer.store_offset_from_message(&msg) {
                tracing::warn!(error = %e, "could not store the offset");
            }
        }
        // Commit what was handled before stopping. Nothing stored is not an error.
        if let Err(e) = self.consumer.commit_consumer_state(CommitMode::Sync) {
            tracing::debug!(error = %e, "final offset commit");
        }
        Ok(())
    }
}

fn header<'a>(m: &'a BorrowedMessage<'_>, name: &str) -> Option<&'a str> {
    let value = m.headers()?.iter().find(|h| h.key == name)?.value?;
    std::str::from_utf8(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORG: &str = "2f6c1c1e-7d1a-4c55-9a3e-0b6d3c3a9a11";
    const OK: &[u8] = br#"{"type":"recommendation.status_changed","id":"r1","version":2}"#;

    fn msg<'a>(id: Option<&'a str>, org: Option<&'a str>, payload: Option<&'a [u8]>) -> Incoming<'a> {
        Incoming { outbox_id: id, organisation_id: org, payload }
    }

    #[test]
    fn a_wellformed_event_parses() {
        let p = parse(&msg(Some("42"), Some(ORG), Some(OK))).unwrap();
        assert_eq!(p.event_id, "42");
        assert_eq!(p.organisation_id, Uuid::parse_str(ORG).unwrap());
        assert_eq!(p.event_type, "recommendation.status_changed");
        assert_eq!(p.payload["version"], 2);
    }

    #[test]
    fn anything_unusable_is_skipped_with_a_reason() {
        let cases = [
            (msg(None, Some(ORG), Some(OK)), Skip::MissingHeader("kuki-outbox-id")),
            (msg(Some("  "), Some(ORG), Some(OK)), Skip::MissingHeader("kuki-outbox-id")),
            (msg(Some("1"), None, Some(OK)), Skip::MissingHeader("kuki-organisation-id")),
            (msg(Some("1"), Some("not-a-uuid"), Some(OK)), Skip::BadOrganisationId),
            (msg(Some("1"), Some(ORG), None), Skip::BadPayload),
            (msg(Some("1"), Some(ORG), Some(b"garbage")), Skip::BadPayload),
            (msg(Some("1"), Some(ORG), Some(br#""just a string""#)), Skip::BadPayload),
            (msg(Some("1"), Some(ORG), Some(b"{}")), Skip::BadPayload),
            (msg(Some("1"), Some(ORG), Some(br#"{"type":5}"#)), Skip::BadPayload),
            (
                msg(Some("1"), Some(ORG), Some(br#"{"type":"recommendation.deleted"}"#)),
                Skip::UnknownEventType("recommendation.deleted".into()),
            ),
        ];
        for (m, want) in cases {
            assert_eq!(parse(&m), Err(want));
        }
    }
}
