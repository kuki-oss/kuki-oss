//! GraphQL query root — contains all read-only resolver functions.
//!
//! Each resolver is mapped to a corresponding GraphQL query field.

use super::types::{
    onboarding::Merchant,
    kuki::{
        KukiRecommendationGql,
        KukiInsightsSummaryGql,
    },
    merchant_upload::MerchantUploadGql,
};
use crate::{
    schema::{require_merchant, require_merchant_with_kind},
    merchant::queries as merchant_queries,
    kuki::queries as kuki_queries,
    merchant_upload::queries as merchant_upload_queries,
};

use async_graphql::{Context, Object, Result};
use sqlx::PgPool;
use uuid::Uuid;

/// Root type for the GraphQL query.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get a single merchant upload by ID (must belong to the authenticated
    /// merchant or sandbox session)
    async fn merchant_upload(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<MerchantUploadGql>> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        Ok(merchant_upload_queries::get_upload(db, merchant_id, id).await?)
    }

    /// Return pending Kuki recommendations for the authenticated merchant.
    /// Optionally filter by `status` (default: `"pending"`) and `rec_type`.
    async fn kuki_recommendations(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = "pending")] status: String,
        rec_type: Option<String>,
        #[graphql(default = 20)] limit: i64,
    ) -> Result<Vec<KukiRecommendationGql>> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        let recs = kuki_queries::list_recommendations(
            db,
            merchant_id,
            Some(status.as_str()),
            rec_type.as_deref(),
            limit,
        )
        .await?;

        Ok(recs)
    }

    async fn kuki_recommendation(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<KukiRecommendationGql>> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        Ok(kuki_queries::get_recommendation(db, merchant_id, id).await?)
    }

    async fn kuki_insights_summary(
        &self,
        ctx: &Context<'_>,
    ) -> Result<KukiInsightsSummaryGql> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        Ok(kuki_queries::insights_summary(db, merchant_id).await?)
    }

    /// Get the current merchant's details, including onboarding status.
    async fn merchant(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<Merchant>> {
        let merchant_id = require_merchant(ctx)?;
        let db = ctx.data::<PgPool>()?;
        let row = merchant_queries::get_merchant(db, merchant_id).await?;
        Ok(row.map(|r| Merchant {
            id: r.id,
            name: r.name,
            slug: r.slug,
            onboarding_status: r.onboarding_status,
            onboarding_step: r.onboarding_step,
            contact_email: r.contact_email,
            phone: r.phone,
            description: r.description,
            instagram: r.instagram,
            twitter: r.twitter,
            tiktok: r.tiktok,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }))
    }
}
