//! `input_digest`: the hash of exactly what a generator read (spec 9.1, invariant I1).
//! Generators add their inputs in a fixed order; the same inputs always give the same digest.
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::canonical::canonical_json;

const DOMAIN: &[u8] = b"kuki.input_digest.v1\n";

pub struct InputDigestBuilder {
    hasher: Sha256,
}

impl Default for InputDigestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InputDigestBuilder {
    pub fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(DOMAIN);
        Self { hasher }
    }

    /// Adds one labelled input. Order matters, callers must add in a deterministic order.
    pub fn add(mut self, label: &str, value: &Value) -> Self {
        let canon = canonical_json(value);
        self.hasher.update((label.len() as u32).to_be_bytes());
        self.hasher.update(label.as_bytes());
        self.hasher.update((canon.len() as u64).to_be_bytes());
        self.hasher.update(&canon);
        self
    }

    /// Lowercase hex, 64 characters.
    pub fn finish(self) -> String {
        hex::encode(self.hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Expected values computed with an independent implementation (Python hashlib) of the same framing.
    #[test]
    fn known_vectors() {
        assert_eq!(
            InputDigestBuilder::new().finish(),
            "1638ca563ce1babbbd3dbe5213e3be040b96aba24789202d2597389d262cfee7"
        );
        assert_eq!(
            InputDigestBuilder::new().add("x", &json!(1)).finish(),
            "5cdc1bc3bcda4d318be0bbaec3fef60d0e7a25bcd749d0953b3f3dd3418480a1"
        );
        assert_eq!(
            InputDigestBuilder::new()
                .add("events", &json!([{"b": 1, "a": 2}]))
                .add("config", &json!({"z": null, "y": "é"}))
                .finish(),
            "1a47fa227aee6086111d99e6b0d7e2c9a144ffa1c4583adffd529e38582b110e"
        );
    }

    #[test]
    fn object_key_order_does_not_change_the_digest() {
        let a: Value = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        assert_eq!(
            InputDigestBuilder::new().add("k", &a).finish(),
            InputDigestBuilder::new().add("k", &b).finish()
        );
    }

    #[test]
    fn item_order_and_labels_change_the_digest() {
        let one = InputDigestBuilder::new()
            .add("a", &json!(1))
            .add("b", &json!(2))
            .finish();
        let swapped = InputDigestBuilder::new()
            .add("b", &json!(2))
            .add("a", &json!(1))
            .finish();
        assert_ne!(one, swapped);
        let ab_c = InputDigestBuilder::new().add("ab", &json!("c")).finish();
        let a_bc = InputDigestBuilder::new().add("a", &json!("bc")).finish();
        assert_ne!(
            ab_c, a_bc,
            "length framing must prevent label/value boundary ambiguity"
        );
    }
}
