use std::{error::Error, process::ExitCode};

use clap::{Parser, Subcommand};
use kuki_api::{ApiConfig, serve};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

mod admin;
mod webhooks;

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
    /// Publish the transactional outbox to Kafka
    Outbox,
    /// Run the decision evaluator
    Evaluate {
        #[command(subcommand)]
        cmd: EvaluateCmd,
    },
    /// Run the action evaluator, which delivers accepted actions to registered webhooks.
    Actions {
        #[command(subcommand)]
        cmd: ActionsCmd,
    },
    /// Manage the webhook endpoint for each action type.
    Webhooks {
        #[command(subcommand)]
        cmd: webhooks::WebhooksCmd,
    },
    /// Inspect what generators in shadow mode would have proposed.
    Shadow {
        #[command(subcommand)]
        cmd: ShadowCmd,
    },
}

#[derive(Subcommand)]
enum EvaluateCmd {
    /// Run the evaluator service: reacts to changes and re-evaluates on a schedule.
    Run,
    /// Evaluate one organisation once and print the report (KUKI_MIGRATE_DATABASE_URL resolves the organisation).
    Once {
        /// Organisation slug or id.
        #[arg(long)]
        org: String,
    },
    /// Install the built-in decision types for an organisation (KUKI_MIGRATE_DATABASE_URL).
    Install {
        /// Organisation slug or id.
        #[arg(long)]
        org: String,
    },
}

#[derive(Subcommand)]
enum ActionsCmd {
    /// Run the executor service
    Run,
}

#[derive(Subcommand)]
enum ShadowCmd {
    /// Print a generator's shadow proposals, most urgent first (KUKI_MIGRATE_DATABASE_URL
    /// resolves the organisation).
    Report {
        /// Generator id, e.g. deadline.at_risk.t0.
        generator: String,
        /// Organisation slug or id.
        #[arg(long)]
        org: String,
        /// At most this many proposals.
        #[arg(long, default_value_t = 50)]
        limit: i64,
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
        Command::Outbox => run_outbox().await,
        Command::Evaluate { cmd } => run_evaluate(cmd).await,
        Command::Actions { cmd } => run_actions(cmd).await,
        Command::Webhooks { cmd } => webhooks::run(cmd).await,
        Command::Shadow { cmd } => run_shadow(cmd).await,
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

async fn run_outbox() -> Res {
    kuki_outbox::run_from_env().await?;
    Ok(())
}

async fn run_actions(cmd: ActionsCmd) -> Res {
    match cmd {
        ActionsCmd::Run => kuki_api::run_actions(ApiConfig::from_env()?).await?,
    }
    Ok(())
}

async fn run_shadow(cmd: ShadowCmd) -> Res {
    match cmd {
        ShadowCmd::Report {
            generator,
            org,
            limit,
        } => println!(
            "{}",
            kuki_engine::run::shadow_report_from_env(&org, &generator, limit.clamp(1, 1000))
                .await?
        ),
    }
    Ok(())
}

async fn run_evaluate(cmd: EvaluateCmd) -> Res {
    match cmd {
        EvaluateCmd::Run => kuki_engine::run::run_from_env().await?,
        EvaluateCmd::Once { org } => println!("{}", kuki_engine::run::once_from_env(&org).await?),
        EvaluateCmd::Install { org } => {
            for name in kuki_engine::run::install_from_env(&org).await? {
                println!("installed {name}");
            }
        }
    }
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
