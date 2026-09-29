use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

/// Connect to Postgres with retry + exponential backoff before giving up.
///
/// Unlike other startup-time .expect() calls, a DB connection failure at
/// boot can be transient; a brief network blip, a cloud failover, a
/// security group not yet applied, momentary connection-budget exhaustion.
/// Those resolve themselves in seconds; panicking immediately on the first
/// failure turns a non-issue into a failed deploy / crash-loop
///
/// Retries 5 times with backoff (2s, 4s, 8s, 16s), enough to ride out a
/// transient blip without meaningfully delaying a genuine failure (bad
/// host, bad credentials, unreachable network), which still panics loud
/// within ~30s, same as before. Do not revert this to a bare .expect(),
/// this retry logic is the direct fix for a real incident where a
/// transient DB connection failure took down a service at boot.
pub async fn connect(database_url: &str) -> PgPool {
  let mut attempt = 0;
  loop {
    attempt += 1;
    match PgPoolOptions::new()
      .max_connections(10)
      .min_connections(1)
      .idle_timeout(Duration::from_secs(600))
      .acquire_timeout(Duration::from_secs(3))
      .connect(&database_url)
      .await
    {
      Ok(pool) => return pool,
      Err(e) if attempt < 5 => {
        let backoff = Duration::from_secs(2u64.pow(attempt.min(4)));
        tracing::warn!(
          error = %e,
          attempt,
          "Failed to connect to PostgreSQL, retrying in {:?}",
          backoff
        );
        tokio::time::sleep(backoff).await;
      }
      Err(e) => {
        panic!("Failed to connect to PostgreSQL after {attempt} attempts: {e}");
      }
    }
  }
}
