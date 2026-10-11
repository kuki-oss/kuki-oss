use std::time::Duration;

use async_trait::async_trait;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::error::RelayError;
use crate::publisher::{Message, PublishFailure, Published, Publisher};

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub brokers: String,
    pub client_id: String,
    pub partitions: i32,
    pub replication: i32,
}

impl KafkaConfig {
    /// `KUKI_KAFKA_BROKERS` (required), `KUKI_KAFKA_CLIENT_ID` (default `kuki`),
    /// `KUKI_KAFKA_PARTITIONS` (default 6) and `KUKI_KAFKA_REPLICATION` (default 1).
    pub fn from_env() -> Result<Self, RelayError> {
        let brokers = std::env::var("KUKI_KAFKA_BROKERS")
            .map_err(|_| RelayError::Config("KUKI_KAFKA_BROKERS is required".into()))?;
        let int = |name: &str, default: i32| -> Result<i32, RelayError> {
            match std::env::var(name) {
                Ok(v) => v
                    .parse()
                    .map_err(|_| RelayError::Config(format!("{name} must be an integer"))),
                Err(_) => Ok(default),
            }
        };
        Ok(Self {
            brokers,
            client_id: std::env::var("KUKI_KAFKA_CLIENT_ID").unwrap_or_else(|_| "kuki".into()),
            partitions: int("KUKI_KAFKA_PARTITIONS", 6)?,
            replication: int("KUKI_KAFKA_REPLICATION", 1)?,
        })
    }
}

fn base(cfg: &KafkaConfig) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.brokers)
        .set("client.id", &cfg.client_id);
    c
}

/// Creates any missing topics. Safe to run on every start
pub async fn ensure_topics(cfg: &KafkaConfig, topics: &[&str]) -> Result<(), RelayError> {
    let admin: AdminClient<DefaultClientContext> = base(cfg)
        .create()
        .map_err(|e| RelayError::Kafka(e.to_string()))?;
    let new: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|t| NewTopic::new(t, cfg.partitions, TopicReplication::Fixed(cfg.replication)))
        .collect();
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(15)));
    let results = admin
        .create_topics(&new, &opts)
        .await
        .map_err(|e| RelayError::Kafka(e.to_string()))?;
    for r in results {
        match r {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic, code)) => {
                return Err(RelayError::Kafka(format!("creating topic {topic}: {code}")));
            }
        }
    }
    Ok(())
}

/// An idempotent, acks=all producer: a broker retry never duplicates or reorders a partition.
pub struct KafkaPublisher {
    producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn connect(cfg: &KafkaConfig) -> Result<Self, RelayError> {
        let producer = base(cfg)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("max.in.flight.requests.per.connection", "5")
            .set("linger.ms", "5")
            .set("message.timeout.ms", "30000")
            .create()
            .map_err(|e| RelayError::Kafka(e.to_string()))?;
        Ok(Self { producer })
    }
}

fn classify(e: &KafkaError) -> PublishFailure {
    let poison = matches!(
        e.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::MessageSizeTooLarge
                | RDKafkaErrorCode::InvalidMessageSize
                | RDKafkaErrorCode::InvalidMessage
        )
    );
    PublishFailure {
        reason: e.to_string(),
        retriable: !poison,
    }
}

#[async_trait]
impl Publisher for KafkaPublisher {
    async fn publish(&self, batch: &[Message]) -> Published {
        // Queue everything first so the producer can batch, then await in order.
        let mut pending = Vec::with_capacity(batch.len());
        let mut queue_failure = None;
        for m in batch {
            let bytes = match serde_json::to_vec(&m.payload) {
                Ok(b) => b,
                Err(e) => {
                    queue_failure = Some(PublishFailure {
                        reason: format!("payload: {e}"),
                        retriable: false,
                    });
                    break;
                }
            };
            let (id, org) = (m.id.to_string(), m.organisation_id.to_string());
            let headers = OwnedHeaders::new()
                .insert(Header {
                    key: "kuki-outbox-id",
                    value: Some(&id),
                })
                .insert(Header {
                    key: "kuki-organisation-id",
                    value: Some(&org),
                })
                .insert(Header {
                    key: "content-type",
                    value: Some("application/json"),
                });
            let record = FutureRecord::to(&m.topic)
                .key(&m.key)
                .payload(&bytes)
                .headers(headers);
            match self.producer.send_result(record) {
                Ok(f) => pending.push(f),
                Err((e, _)) => {
                    queue_failure = Some(classify(&e));
                    break;
                }
            }
        }

        let mut acked = 0;
        let mut delivery_failure = None;
        for f in pending {
            match f.await {
                Ok(Ok(_)) => acked += 1,
                Ok(Err((e, _))) => {
                    delivery_failure = Some(classify(&e));
                    break;
                }
                Err(_) => {
                    delivery_failure = Some(PublishFailure {
                        reason: "delivery was cancelled".into(),
                        retriable: true,
                    });
                    break;
                }
            }
        }
        Published {
            acked,
            failure: delivery_failure.or(queue_failure),
        }
    }
}
