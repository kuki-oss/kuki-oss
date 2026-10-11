#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("kafka: {0}")]
    Kafka(String),
    #[error("configuration: {0}")]
    Config(String),
}
