//! `kuki webhooks`: manage the endpoint each action type is delivered to.
//!
//! Only a *reference* to the signing secret is stored. Put the value in OpenBao yourself, e.g.
//!     bao kv put -mount=secret kuki/webhooks/acme/draft_po secret=@secret.txt
//! so it never passes through this command, a shell history or a database.

use std::error::Error;

use clap::Subcommand;
use futures::FutureExt;
use kuki_actions::valid_reference;
use kuki_api::{ApiConfig, connect_app_pool};
use kuki_store::{
    session::{OrgSession, with_org_tx},
    webhook_endpoints as we,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

type Res = Result<(), Box<dyn Error>>;

#[derive(Subcommand)]
pub enum WebhooksCmd {
    /// Create or replace the endpoint for an action type.
    Set {
        /// Organisation slug or id.
        #[arg(long)]
        org: String,
        #[arg(long)]
        action_type: String,
        #[arg(long)]
        url: String,
        /// OpenBao KV path of the signing secret (field `secret`), e.g. kuki/webhooks/acme/draft_po.
        #[arg(long)]
        secret_ref: String,
        /// Register it switched off.
        #[arg(long)]
        disabled: bool,
        /// Permit an http:// url (development only).
        #[arg(long)]
        allow_http: bool,
    },
    /// List an organisation's endpoints.
    List {
        #[arg(long)]
        org: String,
    },
    /// Remove the endpoint for an action type.
    Remove {
        #[arg(long)]
        org: String,
        #[arg(long)]
        action_type: String,
    },
}

pub async fn run(cmd: WebhooksCmd) -> Res {
    match cmd {
        WebhooksCmd::Set { org, action_type, url, secret_ref, disabled, allow_http } => {
            validate(&action_type, &url, &secret_ref, allow_http)?;
            let (org_id, pool) = connect(&org).await?;
            let (at, u, sr) = (action_type.clone(), url.clone(), secret_ref.clone());
            let ep = with_org_tx::<_, sqlx::Error, _>(&pool, &session(org_id), move |c| {
                async move { we::upsert(c, org_id, &at, &u, &sr, !disabled).await }.boxed()
            })
            .await?;
            println!(
                "{}\t{}\t{}\t{}",
                ep.action_type,
                if ep.enabled { "enabled" } else { "disabled" },
                ep.url,
                ep.secret_ref
            );
            eprintln!("store the signing secret in OpenBao KV at `{secret_ref}` (field `secret`)");
        }
        WebhooksCmd::List { org } => {
            let (org_id, pool) = connect(&org).await?;
            let rows = with_org_tx::<_, sqlx::Error, _>(&pool, &session(org_id), move |c| {
                async move { we::list(c, org_id).await }.boxed()
            })
            .await?;
            for ep in rows {
                println!(
                    "{}\t{}\t{}\t{}",
                    ep.action_type,
                    if ep.enabled { "enabled" } else { "disabled" },
                    ep.url,
                    ep.secret_ref
                );
            }
        }
        WebhooksCmd::Remove { org, action_type } => {
            let (org_id, pool) = connect(&org).await?;
            let at = action_type.clone();
            let removed = with_org_tx::<_, sqlx::Error, _>(&pool, &session(org_id), move |c| {
                async move { we::remove(c, org_id, &at).await }.boxed()
            })
            .await?;
            println!("{}", if removed { "removed" } else { "no such endpoint" });
        }
    }
    Ok(())
}

fn session(org_id: Uuid) -> OrgSession {
    OrgSession { org_id, scope_path: None }
}

/// Resolve the organisation with the owner connection (as `evaluate once` does), then
/// return a pool that works under row-level security as the application role.
async fn connect(org: &str) -> Result<(Uuid, PgPool), Box<dyn Error>> {
    let owner = ApiConfig::read_setting("KUKI_MIGRATE_DATABASE_URL")?
        .ok_or("missing required setting KUKI_MIGRATE_DATABASE_URL")?;
    let owner_pool = PgPoolOptions::new().max_connections(1).connect(&owner).await?;
    let id: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM organisations WHERE slug = $1 OR id::text = $1")
            .bind(org)
            .fetch_optional(&owner_pool)
            .await?;
    let id = id.ok_or_else(|| format!("no organisation {org:?}"))?;

    let app = ApiConfig::read_setting("KUKI_DATABASE_URL")?
        .ok_or("missing required setting KUKI_DATABASE_URL")?;
    Ok((id, connect_app_pool(&app, 2).await?))
}

fn validate(action_type: &str, url: &str, secret_ref: &str, allow_http: bool) -> Result<(), String> {
    if action_type.is_empty() || action_type.len() > 128 || action_type.chars().any(char::is_whitespace) {
        return Err("action type must be 1-128 characters with no whitespace".into());
    }
    let scheme_ok = url.starts_with("https://") || (allow_http && url.starts_with("http://"));
    if !scheme_ok || url.chars().any(char::is_whitespace) {
        return Err("url must be an https:// address (pass --allow-http for development)".into());
    }
    if !valid_reference(secret_ref) {
        return Err(
            "secret reference must be an OpenBao KV path such as kuki/webhooks/acme/draft_po".into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate;

    #[test]
    fn registrations_are_validated_before_touching_the_database() {
        let ok = |a, u, s, h| validate(a, u, s, h).is_ok();
        assert!(ok("draft_po", "https://example.org/h", "kuki/webhooks/a/b", false));
        assert!(ok("draft_po", "http://localhost:9/h", "kuki/webhooks/a/b", true));
        assert!(!ok("draft_po", "http://localhost:9/h", "kuki/webhooks/a/b", false));
        assert!(!ok("draft_po", "ftp://example.org/h", "kuki/webhooks/a/b", true));
        assert!(!ok("draft_po", "https://exa mple.org", "kuki/webhooks/a/b", false));
        assert!(!ok("", "https://example.org/h", "kuki/webhooks/a/b", false));
        assert!(!ok("has space", "https://example.org/h", "kuki/webhooks/a/b", false));
        assert!(!ok("draft_po", "https://example.org/h", "../x", false));
        assert!(!ok("draft_po", "https://example.org/h", "", false));
    }
}
