use std::{collections::BTreeSet, sync::Arc};

use crate::{
    audit::{AuditSink, NoopAuditSink},
    generator::{Capability, DecisionGenerator},
    inference::InferenceClient,
    llm::{DisabledLlm, LlmProvider},
    policy::{PolicyEvaluator, SingleApproverPolicy},
    priors::{NoPriors, PriorProvider},
};

#[derive(Clone, Debug, Default)]
pub struct Capabilities(BTreeSet<Capability>);

impl Capabilities {
    pub fn has(&self, c: Capability) -> bool {
        self.0.contains(&c)
    }
    pub fn insert(&mut self, c: Capability) {
        self.0.insert(c);
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn iter(&self) -> impl Iterator<Item = &Capability> {
        self.0.iter()
    }
}

/// Everything an edition plugs into the kernel. `Default` is Kuki OSS.
/// T0 to T2 generators are not here: they live in `kuki-packs`. `generators`
/// hold extras (Kuki Core's T3 and T5).
#[derive(Clone)]
pub struct Extensions {
    pub policy: Arc<dyn PolicyEvaluator>,
    pub audit: Arc<dyn AuditSink>,
    pub priors: Arc<dyn PriorProvider>,
    pub llm: Arc<dyn LlmProvider>,
    pub inference: Option<Arc<dyn InferenceClient>>,
    pub generators: Vec<Arc<dyn DecisionGenerator>>,
}

impl Default for Extensions {
    fn default() -> Self {
        Self {
            policy: Arc::new(SingleApproverPolicy),
            audit: Arc::new(NoopAuditSink),
            priors: Arc::new(NoPriors),
            llm: Arc::new(DisabledLlm),
            inference: None,
            generators: vec![],
        }
    }
}

impl Extensions {
    pub fn with_policy(mut self, p: Arc<dyn PolicyEvaluator>) -> Self {
        self.policy = p;
        self
    }
    pub fn with_audit(mut self, a: Arc<dyn AuditSink>) -> Self {
        self.audit = a;
        self
    }
    pub fn with_priors(mut self, p: Arc<dyn PriorProvider>) -> Self {
        self.priors = p;
        self
    }
    pub fn with_llm(mut self, l: Arc<dyn LlmProvider>) -> Self {
        self.llm = l;
        self
    }
    pub fn with_inference(mut self, i: Arc<dyn InferenceClient>) -> Self {
        self.inference = Some(i);
        self
    }
    pub fn with_generators(mut self, g: Vec<Arc<dyn DecisionGenerator>>) -> Self {
        self.generators.extend(g);
        self
    }

    /// What generators may rely on, derived from what is registered.
    pub fn capabilities(&self) -> Capabilities {
        let mut c = Capabilities::default();
        if self.inference.is_some() {
            c.insert(Capability::Inference);
        }
        if self.priors.is_networked() {
            c.insert(Capability::NetworkPriors);
        }
        if self.llm.enabled() {
            c.insert(Capability::Llm);
        }
        c
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::{
        audit::AuditEntry,
        error::ExtensionError,
        inference::{InferenceRequest, InferenceResponse},
        llm::{DataClass, LlmRequest, LlmResponse, LlmTask},
        priors::{Prior, PriorRequest},
    };

    #[tokio::test]
    async fn the_defaults_are_inert() {
        let ext = Extensions::default();
        assert!(ext.capabilities().is_empty());

        let llm = LlmRequest {
            task: LlmTask::Explain,
            data_class: DataClass::Internal,
            system: String::new(),
            input: String::new(),
            json_schema: None,
        };
        assert!(matches!(
            ext.llm.complete(&llm).await,
            Err(ExtensionError::Disabled("llm"))
        ));

        let prior = PriorRequest {
            organisation_id: Uuid::nil(),
            signal: "lead_time_days".into(),
            segment: json!({}),
        };
        assert!(ext.priors.prior(&prior).await.unwrap().is_none());

        let entry = AuditEntry {
            id: Uuid::nil(),
            organisation_id: Uuid::nil(),
            occurred_at: Utc::now(),
            actor: "key:1".into(),
            action: "recommendation.accepted".into(),
            subject: "recommendation:x".into(),
            details: json!({}),
        };
        assert!(ext.audit.record(&entry).await.is_ok());
    }

    struct EnabledLlm;
    #[async_trait]
    impl LlmProvider for EnabledLlm {
        fn enabled(&self) -> bool {
            true
        }
        async fn complete(&self, _r: &LlmRequest) -> Result<LlmResponse, ExtensionError> {
            Ok(LlmResponse {
                text: String::new(),
                model: "m".into(),
            })
        }
    }

    struct NetworkedPriors;
    #[async_trait]
    impl PriorProvider for NetworkedPriors {
        fn is_networked(&self) -> bool {
            true
        }
        async fn prior(&self, _r: &PriorRequest) -> Result<Option<Prior>, ExtensionError> {
            Ok(None)
        }
    }

    struct Model;
    #[async_trait]
    impl InferenceClient for Model {
        async fn predict(
            &self,
            _r: &InferenceRequest,
        ) -> Result<InferenceResponse, ExtensionError> {
            Ok(InferenceResponse {
                output: json!(1),
                model_artefact: "sha256:x".into(),
                explanation: None,
            })
        }
    }

    #[test]
    fn registered_extensions_become_capabilities() {
        let ext = Extensions::default()
            .with_llm(Arc::new(EnabledLlm))
            .with_priors(Arc::new(NetworkedPriors))
            .with_inference(Arc::new(Model));
        let caps = ext.capabilities();
        assert!(
            caps.has(Capability::Llm)
                && caps.has(Capability::NetworkPriors)
                && caps.has(Capability::Inference)
        );
        assert_eq!(caps.iter().count(), 3);
    }
}
