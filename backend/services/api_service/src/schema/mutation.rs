//! GraphQL mutation root — contains all write resolver functions.
//!
//! Each resolver is mapped to a corresponding GraphQL mutation field.

use super::types::{
    discounts::{CreateDiscountInput, UpdateDiscountInput, Discount},
    enquiries::{CreateEnquiryInput, EnquirySubmitted},
    customers::{UpdateCustomerInput},
    customer_segments::{CreateSegmentInput, UpdateSegmentInput},
    marketing::{CreateCampaignInput, UpdateCampaignInput, UpsertMarketingCostInput},
    orders::{
        CreateOrderInput,
        CreatedOrder,
        UpdateOrderInput,
        UpdateOrderStatusInput,
    },
    installations::InstallationInput,
    payments::{CreatePaymentInput, PaymentConfirmed},
    categories::{CreateCategoryInput, UpdateCategoryInput},
    services::{
        CreateServiceCategoryInput,
        UpdateServiceCategoryInput,
        CreateServiceInput,
        UpdateServiceInput,
    },
    catalogue::{
        CreateProductInput,
        UpdateProductInput,
        CreateProductImageInput,
    },
    website::UpdateStoreSettingsInput,
    onboarding::{UpdateOnboardingInput, UpdateMerchantProfileInput, ChangeMerchantPasswordInput},
    kuki::{
        AcceptKukiRecommendationInput,
        RejectKukiRecommendationInput,
        KukiResolveResultGql,
    },
    merchant_upload::{
        ConfirmMerchantUploadResultGql,
        InitiateMerchantUploadInput,
        InitiateMerchantUploadResultGql,
        MarkMerchantUploadUploadedInput,
        MerchantUploadGql,
        RemapMerchantUploadInput,
        ConfirmMerchantUploadInput,
    },
    kuki_config::{UpsertKukiConfigInput, TriggerKukiEvaluationInput},
};
use crate::{
    schema::{require_merchant, require_merchant_with_kind, UploadBucket},
    merchant::queries as merchant_queries,
    enquiries::{
        queries as enquiry_queries,
        models::SubmitEnquiryItem,
    },
    customer_segments::mutations as segment_mutations,
    customers::queries as customer_queries,
    discounts::mutations as discount_mutations,
    marketing::mutations as marketing_mutations,
    orders::{
        models::{CreateOrderItem, UpdateOrderItem},
        queries as order_queries
    },
    installations::queries as installation_queries,
    payments::queries as payment_queries,
    catalogue::mutations as catalogue_mutations,
};
use crate::categories::mutations as category_mutations;
use crate::website::mutations as website_mutations;
use crate::services::mutations as service_mutations;
use crate::kuki::mutations as kuki_mutations;
use crate::kuki_config::mutations as kuki_config_mutations;
use crate::merchant_upload::mutations as merchant_upload_mutations;

use common::surreal::SurrealClient;

use async_graphql::{Context, Object, Result};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::password_hash::SaltString;
use sqlx::{types::BigDecimal, PgPool};
use uuid::Uuid;

/// Root type for the GraphQL mutation.
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn initiate_merchant_upload(
        &self,
        ctx: &Context<'_>,
        input: InitiateMerchantUploadInput,
    ) -> Result<InitiateMerchantUploadResultGql> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        let s3_client = ctx.data::<aws_sdk_s3::Client>()?;
        let bucket = ctx.data::<UploadBucket>()?;
        Ok(merchant_upload_mutations::initiate_upload(
            db,
            s3_client,
            &bucket.0,
            merchant_id,
            &input.kind,
        )
        .await?)
    }

    async fn mark_merchant_upload_uploaded(
        &self,
        ctx: &Context<'_>,
        input: MarkMerchantUploadUploadedInput,
    ) -> Result<MerchantUploadGql> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        Ok(merchant_upload_mutations::mark_uploaded(db, merchant_id, input.upload_id).await?)
    }

    async fn remap_merchant_upload(
        &self,
        ctx: &Context<'_>,
        input: RemapMerchantUploadInput,
    ) -> Result<MerchantUploadGql> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        Ok(merchant_upload_mutations::remap_upload(
            db,
            merchant_id,
            input.upload_id,
            input.mapping_overrides,
        )
        .await?)
    }

    async fn confirm_merchant_upload(
        &self,
        ctx: &Context<'_>,
        input: ConfirmMerchantUploadInput,
    ) -> Result<ConfirmMerchantUploadResultGql> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        let surreal = ctx.data::<SurrealClient>()?;
        Ok(merchant_upload_mutations::confirm_upload(db, surreal, merchant_id, input.upload_id).await?)
    }

    async fn upsert_kuki_config(
        &self,
        ctx: &Context<'_>,
        input: UpsertKukiConfigInput,
    ) -> Result<bool> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        kuki_config_mutations::upsert_kuki_config(
            db,
            merchant_id,
            input.velocity_window_days,
            input.safety_buffer_multiplier,
            input.merchant_buffer_days,
        )
        .await?;

        Ok(true)
    }

    async fn trigger_kuki_evaluation(
        &self,
        ctx: &Context<'_>,
        input: TriggerKukiEvaluationInput,
    ) -> Result<bool> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        kuki_config_mutations::trigger_kuki_evaluation(db, merchant_id, &input.engine).await?;
        Ok(true)
    }

    async fn accept_kuki_recommendation(
        &self,
        ctx: &Context<'_>,
        input: AcceptKukiRecommendationInput,
    ) -> Result<KukiResolveResultGql> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        Ok(kuki_mutations::accept_recommendation(
            db,
            merchant_id,
            input.recommendation_id,
            input.note.as_deref(),
        )
        .await?)
    }

    async fn reject_kuki_recommendation(
        &self,
        ctx: &Context<'_>,
        input: RejectKukiRecommendationInput,
    ) -> Result<KukiResolveResultGql> {
        let merchant_id = require_merchant_with_kind(ctx)?.id;
        let db = ctx.data::<PgPool>()?;
        Ok(kuki_mutations::reject_recommendation(
            db,
            merchant_id,
            input.recommendation_id,
            input.reason.as_deref(),
        )
        .await?)
    }

    /// Update the onboarding progress for the current merchant.
    async fn update_onboarding(
        &self,
        ctx: &Context<'_>,
        input: UpdateOnboardingInput,
    ) -> Result<bool> {
        let merchant_id = require_merchant(ctx)?;
        let db = ctx.data::<PgPool>()?;

        merchant_queries::update_onboarding(
            db,
            merchant_id,
            input.step,
            &input.status,
            input.contact_email.as_deref(),
            input.phone.as_deref(),
            input.description.as_deref(),
            input.instagram.as_deref(),
            input.twitter.as_deref(),
            input.tiktok.as_deref(),
        ).await?;

        Ok(true)
    }

    /// Update the merchant's store profile (name, contact email, phone, description).
    async fn update_merchant_profile(
        &self,
        ctx: &Context<'_>,
        input: UpdateMerchantProfileInput,
    ) -> Result<bool> {
        let merchant_id = require_merchant(ctx)?;
        let db = ctx.data::<PgPool>()?;

        merchant_queries::update_merchant_profile(
            db,
            merchant_id,
            input.name.as_deref(),
            input.contact_email.as_deref(),
            input.phone.as_deref(),
            input.description.as_deref(),
            input.instagram.as_deref(),
            input.twitter.as_deref(),
            input.tiktok.as_deref(),
        ).await?;

        Ok(true)
    }

    /// Change the merchant's password (requires current password for verification).
    async fn change_merchant_password(
        &self,
        ctx: &Context<'_>,
        input: ChangeMerchantPasswordInput,
    ) -> Result<bool> {
        let merchant_id = require_merchant(ctx)?;
        let db = ctx.data::<PgPool>()?;

        let merchant = merchant_queries::get_merchant_by_email_for_id(db, merchant_id)
            .await?
            .ok_or_else(|| async_graphql::Error::new("Merchant account not found"))?;

        let parsed_hash = PasswordHash::new(&merchant.password_hash)
            .map_err(|_| async_graphql::Error::new("Internal server error"))?;

        Argon2::default()
            .verify_password(input.current_password.as_bytes(), &parsed_hash)
            .map_err(|_| async_graphql::Error::new("Current password is incorrect"))?;

        let mut salt_bytes = [0u8; 16];
        rand::fill(&mut salt_bytes[..]);
        let salt = SaltString::encode_b64(&salt_bytes)
            .map_err(|_| async_graphql::Error::new("Failed to generate salt"))?;
        let hash = Argon2::default()
            .hash_password(input.new_password.as_bytes(), &salt)
            .map_err(|_| async_graphql::Error::new("Failed to hash password"))?
            .to_string();

        merchant_queries::update_merchant_password(db, merchant_id, &hash).await?;

        Ok(true)
    }

    /// Soft-delete the merchant's store. Data is kept for 30 days.
    async fn delete_merchant(
        &self,
        ctx: &Context<'_>,
    ) -> Result<bool> {
        let merchant_id = require_merchant(ctx)?;
        let db = ctx.data::<PgPool>()?;
        merchant_queries::soft_delete_merchant(db, merchant_id).await?;
        Ok(true)
    }

    /// Deactivate the merchant's store (store front, checkout, etc. become
    /// inaccessible). The merchant can reactivate within 15 days.
    async fn deactivate_merchant(
        &self,
        ctx: &Context<'_>,
    ) -> Result<bool> {
        let merchant_id = require_merchant(ctx)?;
        let db = ctx.data::<PgPool>()?;
        merchant_queries::deactivate_merchant(db, merchant_id).await?;
        Ok(true)
    }

    /// Reactivate a deactivated merchant store.
    async fn reactivate_merchant(
        &self,
        ctx: &Context<'_>,
    ) -> Result<bool> {
        let merchant_id = require_merchant(ctx)?;
        let db = ctx.data::<PgPool>()?;
        merchant_queries::reactivate_merchant(db, merchant_id).await?;
        Ok(true)
    }
}
