use std::{error::Error, process::ExitCode};

use clap::{Parser, Subcommand};
use kuki_api::{ApiConfig, serve};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

mod admin;

type Res = Result<(), Box<dyn Error>>;

#[derive(Parser)]
#[command(name = "kuki", version, about = "Kuki OSS server and administration")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the API server (the default).
    Serve,
    /// Apply database migrations (KUKI_MIGRATE_DATABASE_URL).
    Migrate,
    /// Manage organisations.
    Orgs {
        #[command(subcommand)]
        cmd: admin::OrgsCmd,
    },
    /// Manage API keys.
    Keys {
        #[command(subcommand)]
        cmd: admin::KeysCmd,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    // Logs go to stderr: `kuki keys create` prints the key on stdout, and only that.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let result = match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => run_serve().await,
        Command::Migrate => run_migrate().await,
        Command::Orgs { cmd } => admin::run_orgs(cmd).await,
        Command::Keys { cmd } => admin::run_keys(cmd).await,
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "fatal");
            ExitCode::FAILURE
        }
    }
}

async fn run_serve() -> Res {
    let config = ApiConfig::from_env()?;
    serve(config).await?;
    Ok(())
}

async fn run_migrate() -> Res {
    let url = ApiConfig::read_setting("KUKI_MIGRATE_DATABASE_URL")?
        .ok_or("missing required setting KUKI_MIGRATE_DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    kuki_store::MIGRATOR.run(&pool).await?;
    tracing::info!("migrations applied");
    Ok(())
}
