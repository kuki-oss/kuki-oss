//! Routes an action the webhook endpoint registered for its organisation and
//! action type. No registration means no route, so the execution fails visibly.

use std::sync::Arc;

use futures::{FutureExt, future::BoxFuture};
use kuki_store::{
    session::{OrgSession, with_org_tx},
    webhook_endpoints,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    adapter::{Adapter, Router},
    secrets::SecretSource,
    webhook::{WebhookAdapter, WebhookConfig, client},
};

pub struct RegistryRouter {
    pool: PgPool,
    secrets: Arc<dyn SecretSource>,
    http: reqwest::Client,
    allow_insecure: bool,
}

impl RegistryRouter {
    /// `pool` must connect as the application role: lookups run under the
    /// organisation's tenant context, so one tenant never sees another's endpoints.
    pub fn new(
        pool: PgPool,
        secrets: Arc<dyn SecretSource>,
        cfg: &WebhookConfig,
    ) -> Result<Self, String> {
        Ok(Self {
            pool,
            secrets,
            http: client(cfg)?,
            allow_insecure: cfg.allow_insecure_http,
        })
    }
}

impl Router for RegistryRouter {
    fn route<'a>(
        &'a self,
        organisation_id: Uuid,
        action_type: &'a str,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn Adapter>>, String>> {
        async move {
            let wanted = action_type.to_owned();
            let session = OrgSession {
                org_id: organisation_id,
                scope_path: None,
            };
            let found = with_org_tx::<_, sqlx::Error, _>(&self.pool, &session, move |c| {
                async move { webhook_endpoints::find_enabled(c, organisation_id, &wanted).await }
                    .boxed()
            })
            .await
            .map_err(|e| format!("endpoint lookup failed: {e}"))?;

            Ok(found.map(|ep| {
                Arc::new(WebhookAdapter::new(
                    self.http.clone(),
                    ep.url,
                    ep.secret_ref,
                    self.secrets.clone(),
                    self.allow_insecure,
                )) as Arc<dyn Adapter>
            }))
        }
        .boxed()
    }
}
