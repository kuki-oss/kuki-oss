use std::collections::BTreeSet;

use async_graphql::{Context, ID, Json, MergedObject, Object, Result, SimpleObject};
use futures::FutureExt;
use sqlx::PgPool;

use super::connection::{
    Cursor, PageInfo, check_ltree, decode_path_cursor, encode_path_cursor, page_size,
};
use super::guard::RequiresScope;
use crate::auth::Principal;
use crate::error::ApiError;

#[derive(Default)]
pub struct TenancyQuery;

/// The contract's root `Query`, merged from one object per domain.
#[derive(MergedObject, Default)]
#[graphql(name = "Query")]
pub struct QueryRoot(
    TenancyQuery,
    super::ontology::OntologyQuery,
    super::catalog::CatalogQuery,
    super::recommendations::RecommendationQuery,
    super::metric::MetricQuery,
);

#[derive(SimpleObject)]
pub struct Organisation {
    id: ID,
    slug: String,
    name: String,
    /// Data residency region code.
    region: Option<String>,
}

#[derive(SimpleObject)]
pub struct OrgUnit {
    id: ID,
    /// ltree path, e.g. hq.region_east.depot_nairobi.
    path: String,
    name: String,
    attrs: Json<serde_json::Value>,
}

#[derive(SimpleObject)]
pub struct OrgUnitEdge {
    node: OrgUnit,
    cursor: Cursor,
}

#[derive(SimpleObject)]
pub struct OrgUnitConnection {
    edges: Vec<OrgUnitEdge>,
    page_info: PageInfo,
}

pub struct Viewer {
    principal_id: String,
    scopes: Vec<String>,
    org_unit_scope_path: Option<String>,
}

#[Object]
impl Viewer {
    async fn principal_id(&self) -> &str {
        &self.principal_id
    }
    async fn scopes(&self) -> &[String] {
        &self.scopes
    }
    /// ltree path, null = whole organisation.
    async fn org_unit_scope_path(&self) -> Option<&str> {
        self.org_unit_scope_path.as_deref()
    }
    /// Not scope-gated: any authenticated key may read its own organisation here.
    async fn organisation(&self, ctx: &Context<'_>) -> Result<Organisation> {
        load_organisation(ctx).await
    }
}

#[Object]
impl TenancyQuery {
    async fn viewer(&self, ctx: &Context<'_>) -> Result<Viewer> {
        let p = ctx.data::<Principal>()?;
        let scopes: BTreeSet<&str> = p.scopes.iter().map(|s| s.as_str()).collect();
        Ok(Viewer {
            principal_id: p.actor.clone(),
            scopes: scopes.into_iter().map(str::to_owned).collect(),
            org_unit_scope_path: p.session.scope_path.clone(),
        })
    }

    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn organisation(&self, ctx: &Context<'_>) -> Result<Organisation> {
        load_organisation(ctx).await
    }

    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    async fn org_units(
        &self,
        ctx: &Context<'_>,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
        under_path: Option<String>,
    ) -> Result<OrgUnitConnection> {
        let limit = page_size(first)?;
        let after_path = after.as_ref().map(decode_path_cursor).transpose()?;
        if let Some(u) = &under_path {
            check_ltree(u, "underPath")?;
        }

        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let mut rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let (under, after) = (under_path.clone(), after_path.clone());
            async move {
                kuki_store::tenant::org_units_page(
                    c,
                    org,
                    under.as_deref(),
                    after.as_deref(),
                    limit + 1,
                )
                .await
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let has_next_page = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        let edges: Vec<OrgUnitEdge> = rows
            .into_iter()
            .map(|r| OrgUnitEdge {
                cursor: encode_path_cursor(&r.path),
                node: OrgUnit {
                    id: ID(r.id.to_string()),
                    path: r.path,
                    name: r.name,
                    attrs: Json(r.attrs),
                },
            })
            .collect();
        Ok(OrgUnitConnection {
            page_info: PageInfo {
                has_next_page,
                has_previous_page: false, // forward-only pagination (contract)
                start_cursor: edges.first().map(|e| e.cursor.clone()),
                end_cursor: edges.last().map(|e| e.cursor.clone()),
            },
            edges,
        })
    }
}

async fn load_organisation(ctx: &Context<'_>) -> Result<Organisation> {
    let p = ctx.data::<Principal>()?;
    let pool = ctx.data::<PgPool>()?;
    let id = p.session.org_id;
    let row = kuki_store::session::with_org_tx(pool, &p.session, |c| {
        async move { kuki_store::tenant::organisation_get(c, id).await }.boxed()
    })
    .await
    .map_err(ApiError::internal)?;
    // Row missing means the credential's organisation is gone: treat as unauthenticated.
    let row = row.ok_or_else(ApiError::unauthenticated)?;
    Ok(Organisation {
        id: ID(row.id.to_string()),
        slug: row.slug,
        name: row.name,
        region: row.region,
    })
}
