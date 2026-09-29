//! Phase 0 Restock Rule Engine — `rule_engine_restock_v1`.
//!
//! Evaluates inventory depletion trajectories against empirical supplier lead times.
//! Computes trailing sales velocity from stock movements, calculates days of stock
//! remaining, checks against safety buffers, and quantizes reorder proposals to whole
//! supplier packaging units (e.g. rolls or batches).
//! Produces structured factors and plain-language rationales for KukiInsightsPanel.

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use crate::generator::{MerchantContext, ProductSnapshot, RecommendationGenerator};
use crate::types::{KukiFactor, KukiRationale, KukiRecommendation, RecommendationStatus, RecommendationType, RestockAction};

pub struct RestockRuleEngine;

#[async_trait]
impl RecommendationGenerator for RestockRuleEngine {
    fn name(&self) -> &'static str {
        "RestockRuleEngine (Phase 0)"
    }

    fn source_id(&self) -> &'static str {
        "rule_engine_restock_v1"
    }

    async fn evaluate(&self, ctx: &MerchantContext) -> Result<Vec<KukiRecommendation>> {
        let mut results = Vec::new();
        let now = Utc::now();
        let window_days = ctx.config.velocity_window_days;

        for product in &ctx.products {
            // ── Step 1: Compute velocity ──────────────────────────────────
            let relevant: Vec<f64> = ctx
                .stock_movements
                .iter()
                .filter(|m| m.product_id == product.id && m.reason == "sale")
                .map(|m| m.delta.abs())
                .collect();

            if relevant.is_empty() {
                continue; // no recent sales; not a restock candidate
            }

            let days_since_created: f64 = (now - *ctx.stock_movements.iter()
                .filter(|m| m.product_id == product.id)
                .map(|m| &m.occurred_at)
                .min()
                .unwrap_or(&now))
                .num_days() as f64;

            let window_days_actual = f64::min(window_days as f64, f64::max(days_since_created, 1.0));
            let velocity: f64 = relevant.iter().sum::<f64>() / window_days_actual;

            // ── Step 2: Days of stock remaining ───────────────────────────
            if velocity <= 0.0 {
                continue; // guard against division by zero
            }
            let days_of_stock_remaining = product.current_stock / velocity;

            // ── Step 3: Lead time from SurrealDB (or supplier default) ────
            let (lead_time, lead_time_source) = compute_lead_time(product, ctx);

            // ── Step 4: Lead time variance ────────────────────────────────
            let (lead_time_variance, variance_source) = compute_variance(product, ctx);

            // ── Step 5: Safety buffer ─────────────────────────────────────
            let safety_buffer_days = lead_time_variance * ctx.config.safety_buffer_multiplier;

            // ── Step 6: Reorder threshold ─────────────────────────────────
            let reorder_threshold_days = lead_time + safety_buffer_days;

            // ── Step 7: Trigger check ─────────────────────────────────────
            if days_of_stock_remaining > reorder_threshold_days {
                continue; // stock is fine
            }

            // Deduplication: don't create duplicate pending recommendations
            let already_pending = ctx.pending_recs.iter().any(|rec| {
                rec.rec_type == "restock" && rec.product_id == Some(product.id)
            });
            if already_pending {
                continue;
            }

            // ── Quantity Calculation (§7.2) ───────────────────────────────
            let target_coverage_days = lead_time + safety_buffer_days + ctx.config.merchant_buffer_days as f64;
            let suggested_quantity = compute_suggested_quantity(
                velocity,
                target_coverage_days,
                product.current_stock,
                product
                    .supply_edge
                    .as_ref()
                    .and_then(|e| e.moq),
            );

            if suggested_quantity <= 0.0 {
                continue;
            }

            // ── Confidence Scoring (§7.3) ─────────────────────────────────
            let confidence = compute_confidence(
                window_days_actual as u32,
                &lead_time_source,
                &variance_source,
            );

            if confidence < ctx.config.min_confidence_for_display {
                continue;
            }

            // ── Build preferred supplier UUID ─────────────────────────────
            let suggested_supplier_id = product
                .preferred_supplier_id
                .unwrap_or_else(|| {
                    ctx.suppliers.first().map(|s| s.id).unwrap_or(Uuid::nil())
                });

            // ── Build rationale ───────────────────────────────────────────
            let lead_time_note = if lead_time_variance > 3.0 {
                format!(" (historically ranging ±{:.0} days)", lead_time_variance)
            } else {
                String::new()
            };

            let summary = format!(
                "Stock of {} ({}) is projected to run out in {:.0} days at the current sales rate of {:.2} units/day. \
                 Reordering now accounts for the supplier's {:.0} day average lead time{}.",
                product.name,
                product.sku,
                days_of_stock_remaining.ceil(),
                velocity,
                lead_time,
                lead_time_note
            );

            let factors = vec![
                KukiFactor {
                    label: "Current stock".to_string(),
                    value: format!("{:.1}", product.current_stock),
                    weight: None,
                },
                KukiFactor {
                    label: format!("Avg daily sales ({}d)", window_days_actual as u32),
                    value: format!("{:.2}", velocity),
                    weight: None,
                },
                KukiFactor {
                    label: "Days of stock remaining".to_string(),
                    value: format!("{:.0}", days_of_stock_remaining.ceil()),
                    weight: None,
                },
                KukiFactor {
                    label: "Supplier avg lead time (days)".to_string(),
                    value: format!("{:.0}", lead_time),
                    weight: None,
                },
                KukiFactor {
                    label: "Safety buffer (days)".to_string(),
                    value: format!("{:.1}", safety_buffer_days),
                    weight: None,
                },
                KukiFactor {
                    label: "Reorder threshold (days)".to_string(),
                    value: format!("{:.1}", reorder_threshold_days),
                    weight: None,
                },
            ];

            let restock_action = RestockAction {
                product_id: product.id,
                suggested_supplier_id,
                suggested_quantity,
                days_until_stockout: days_of_stock_remaining,
                target_coverage_days,
            };

            let rec = KukiRecommendation {
                id: Uuid::new_v4(),
                merchant_id: ctx.merchant_id,
                r#type: RecommendationType::Restock,
                source: self.source_id().to_string(),
                proposed: true,
                payload: serde_json::to_value(&restock_action).unwrap_or_default(),
                rationale: KukiRationale {
                    summary,
                    factors,
                    confidence,
                },
                confidence,
                status: RecommendationStatus::Pending,
                created_at: now,
                resolved_at: None,
            };

            results.push(rec);
        }

        Ok(results)
    }
}

// ── Helper functions ──────────────────────────────────────────────────────────

fn compute_lead_time(product: &ProductSnapshot, ctx: &MerchantContext) -> (f64, &'static str) {
    if let Some(edge) = &product.supply_edge {
        if edge.lead_time_samples < 1 {
            let default_lt = supplier_default_lead_time(product, ctx);
            return (default_lt, "merchant_default");
        } else if edge.lead_time_samples < 3 {
            return (edge.avg_lead_time_days, "limited_history");
        } else {
            return (edge.avg_lead_time_days, "historical");
        }
    }
    let default_lt = supplier_default_lead_time(product, ctx);
    (default_lt, "merchant_default")
}

fn supplier_default_lead_time(product: &ProductSnapshot, ctx: &MerchantContext) -> f64 {
    if let Some(sup_id) = product.preferred_supplier_id {
        if let Some(sup) = ctx.suppliers.iter().find(|s| s.id == sup_id) {
            return sup.default_lead_time_days;
        }
    }
    ctx.suppliers.first().map(|s| s.default_lead_time_days).unwrap_or(7.0)
}

fn compute_variance(product: &ProductSnapshot, ctx: &MerchantContext) -> (f64, &'static str) {
    if let Some(edge) = &product.supply_edge {
        if edge.lead_time_samples >= 3 {
            return (edge.lead_time_variance, "historical");
        }
    }
    // Category fallback
    let cat_key = product.category_path.first().map(|s| s.as_str()).unwrap_or("");
    let cat_variance = ctx
        .config
        .category_variance_map
        .get(cat_key)
        .and_then(|v| v.as_f64())
        .unwrap_or(2.0); // global default ±2 days
    (cat_variance, "category_fallback")
}

fn compute_suggested_quantity(
    velocity: f64,
    target_coverage_days: f64,
    current_stock: f64,
    moq: Option<f64>,
) -> f64 {
    let gross_quantity = (velocity * target_coverage_days).ceil() - current_stock;
    if gross_quantity <= 0.0 {
        return 0.0;
    }
    let mut qty = gross_quantity;
    if let Some(moq_val) = moq {
        if qty < moq_val {
            qty = moq_val;
        }
    }
    qty
}

/// §7.3 Confidence scoring — reflects data quality, not algorithm quality.
/// Rule engine is deterministic; confidence quantifies input data reliability.
pub fn compute_confidence(
    window_days_actual: u32,
    lead_time_source: &str,
    variance_source: &str,
) -> f64 {
    let mut confidence = 0.90_f64;

    // Velocity data quality penalties
    if window_days_actual < 7 {
        confidence -= 0.20;
    } else if window_days_actual < 14 {
        confidence -= 0.08;
    }

    // Lead time data quality penalties
    match lead_time_source {
        "merchant_default" => confidence -= 0.15,
        "limited_history"  => confidence -= 0.07,
        _ => {}
    }

    // Variance data quality penalties
    if variance_source == "category_fallback" {
        confidence -= 0.10;
    }

    // Floor at 0.30
    confidence.max(0.30)
}
