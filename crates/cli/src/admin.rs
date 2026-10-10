//! `kuki orgs ...` and `kuki keys ...`: thin argument parsing and printing over `kuki_api::admin`.

use std::{error::Error, sync::Arc};

use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use kuki_api::{
    ApiConfig,
    admin::{self as core, ApiKeyRow, CreateKey, OrgRef, Revoked},
    auth::Scope,
    bao::{BaoClient, BaoHasher},
    config::Secret,
};
use sqlx::{PgPool, postgres::PgPoolOptions};

type Res = Result<(), Box<dyn Error>>;

fn setting(name: &'static str) -> Result<String, Box<dyn Error>> {
    ApiConfig::read_setting(name)?.ok_or_else(|| format!("missing required setting {name}").into())
}

/// The owner role (KUKI_MIGRATE_DATABASE_URL); `kuki_app` cannot touch `api_keys`.
async fn admin_pool() -> Result<PgPool, Box<dyn Error>> {
    let url = setting("KUKI_MIGRATE_DATABASE_URL")?;
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?)
}

/// The same Transit key the API verifies with, so the stored HMAC matches at request time.
fn hasher() -> Result<(BaoHasher, String), Box<dyn Error>> {
    let client = BaoClient::new(
        setting("KUKI_BAO_ADDR")?,
        Secret::new(setting("KUKI_BAO_TOKEN")?.into_bytes()),
    )?;
    let key_name =
        ApiConfig::read_setting("KUKI_BAO_TRANSIT_KEY")?.unwrap_or_else(|| "kuki-api-keys".into());
    Ok((
        BaoHasher {
            client: Arc::new(client),
            key_name,
        },
        setting("KUKI_KEY_ENV")?,
    ))
}

#[derive(Subcommand)]
pub enum OrgsCmd {
    /// Create an organisation, or print the existing one with that slug. Prints the id on stdout.
    Create(OrgCreateArgs),
}

#[derive(Args)]
pub struct OrgCreateArgs {
    #[arg(long)]
    slug: String,
    /// Display name (defaults to the slug).
    #[arg(long)]
    name: Option<String>,
}

pub async fn run_orgs(cmd: OrgsCmd) -> Res {
    match cmd {
        OrgsCmd::Create(a) => {
            let pool = admin_pool().await?;
            let name = a.name.unwrap_or_else(|| a.slug.clone());
            let (id, created) = core::create_org(&pool, &a.slug, &name).await?;
            eprintln!(
                "{} organisation `{}`",
                if created { "Created" } else { "Found existing" },
                a.slug
            );
            println!("{id}");
            Ok(())
        }
    }
}

#[derive(Subcommand)]
pub enum KeysCmd {
    /// Mint a key. The key is printed once on stdout and cannot be recovered.
    Create(CreateArgs),
    /// List keys (never their secrets).
    List(ListArgs),
    /// Revoke a key by its key id. Takes effect on the next request.
    Revoke(RevokeArgs),
}

#[derive(Args)]
pub struct CreateArgs {
    /// Organisation id (UUID) or slug. A slug needs a role that bypasses row-level security.
    #[arg(long)]
    org: String,
    #[arg(long)]
    name: String,
    /// Comma-separated: ingest:write, ingest:read, decisions:read, decisions:resolve.
    #[arg(long, value_delimiter = ',', required = true)]
    scopes: Vec<String>,
    /// Limit the key to an org-unit subtree, e.g. emea.uk.
    #[arg(long)]
    unit: Option<String>,
    /// Expiry, RFC 3339 (e.g. 2027-01-01T00:00:00Z).
    #[arg(long)]
    expires: Option<String>,
}

#[derive(Args)]
pub struct ListArgs {
    /// Only this organisation (id or slug).
    #[arg(long)]
    org: Option<String>,
}

#[derive(Args)]
pub struct RevokeArgs {
    /// The 12-character key id from `kuki keys list`, not the secret.
    key_id: String,
}

pub async fn run_keys(cmd: KeysCmd) -> Res {
    match cmd {
        KeysCmd::Create(a) => create(a).await,
        KeysCmd::List(a) => list(a).await,
        KeysCmd::Revoke(a) => revoke(a).await,
    }
}

async fn create(a: CreateArgs) -> Res {
    let valid = Scope::ALL
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let scopes = a
        .scopes
        .iter()
        .map(|s| {
            Scope::parse(s.trim())
                .ok_or_else(|| format!("unknown scope `{s}`; valid scopes: {valid}"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let expires_at = a
        .expires
        .as_deref()
        .map(|s| DateTime::parse_from_rfc3339(s).map(|d| d.with_timezone(&Utc)))
        .transpose()?;

    let pool = admin_pool().await?;
    let (hasher, key_env) = hasher()?;
    let key = core::create_key(
        &pool,
        &hasher,
        &key_env,
        CreateKey {
            org: OrgRef::parse(&a.org),
            name: a.name,
            scopes,
            unit: a.unit,
            expires_at,
        },
    )
    .await?;

    eprintln!(
        "Created key {} for organisation {}. The key below is shown once and cannot be recovered.",
        key.key_id, key.organisation_id
    );
    println!("{}", key.raw);
    Ok(())
}

fn status(k: &ApiKeyRow, now: DateTime<Utc>) -> &'static str {
    if k.revoked_at.is_some() {
        "revoked"
    } else if k.expires_at.is_some_and(|t| t <= now) {
        "expired"
    } else {
        "active"
    }
}

async fn list(a: ListArgs) -> Res {
    let pool = admin_pool().await?;
    let keys = core::list_keys(&pool, a.org.as_deref().map(OrgRef::parse)).await?;
    let now = Utc::now();
    let when = |t: Option<DateTime<Utc>>| {
        t.map_or("-".to_owned(), |t| t.format("%Y-%m-%d %H:%M").to_string())
    };
    println!(
        "{:<14} {:<8} {:<16} {:<34} {:<10} {:<17} {:<17} NAME",
        "KEY ID", "STATUS", "ORG", "SCOPES", "UNIT", "EXPIRES", "LAST USED"
    );
    for k in &keys {
        let org = k
            .org_slug
            .clone()
            .unwrap_or_else(|| k.organisation_id.to_string());
        println!(
            "{:<14} {:<8} {:<16} {:<34} {:<10} {:<17} {:<17} {}",
            k.key_id,
            status(k, now),
            org,
            k.scopes.join(","),
            k.unit.as_deref().unwrap_or("-"),
            when(k.expires_at),
            when(k.last_used_at),
            k.name
        );
    }
    Ok(())
}

async fn revoke(a: RevokeArgs) -> Res {
    let pool = admin_pool().await?;
    match core::revoke_key(&pool, &a.key_id).await? {
        Revoked::Now => eprintln!("Revoked {}.", a.key_id),
        Revoked::Already => eprintln!("{} was already revoked.", a.key_id),
        Revoked::NotFound => return Err(format!("no key with id {}", a.key_id).into()),
    }
    Ok(())
}
