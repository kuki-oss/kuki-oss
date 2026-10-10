//! Deterministic ids for ingest. These are a wire-compatibility promise: a retry after a
//! crash must derive the same ids, forever. Never change NAMESPACE or the name layout.
use uuid::Uuid;

const NAMESPACE: Uuid = Uuid::from_u128(0x6b756b69_696e_4765_8573_740000000001);

fn derive(kind: &str, org: Uuid, key: &str, index: Option<u32>) -> Uuid {
    // NUL cannot occur in an HTTP header value, so the fields cannot be confused
    let name = match index {
        Some(i) => format!("{kind}\0{org}\0{key}\0{i}"),
        None => format!("{kind}\0{org}\0{key}"),
    };
    Uuid::new_v5(&NAMESPACE, name.as_bytes())
}

pub fn batch_id(org: Uuid, idempotency_key: &str) -> Uuid {
    derive("batch", org, idempotency_key, None)
}
pub fn event_id(org: Uuid, idempotency_key: &str, index: u32) -> Uuid {
    derive("event", org, idempotency_key, Some(index))
}
pub fn object_id(org: Uuid, idempotency_key: &str, index: u32) -> Uuid {
    derive("object", org, idempotency_key, Some(index))
}
pub fn quarantine_id(org: Uuid, idempotency_key: &str, index: u32) -> Uuid {
    derive("quarantine", org, idempotency_key, Some(index))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORG: Uuid = Uuid::from_u128(1);

    #[test]
    fn ids_are_deterministic() {
        assert_eq!(event_id(ORG, "k", 0), event_id(ORG, "k", 0));
    }

    #[test]
    fn every_input_matters() {
        let base = event_id(ORG, "k", 0);
        assert_ne!(base, event_id(ORG, "k", 1));
        assert_ne!(base, event_id(ORG, "k2", 0));
        assert_ne!(base, event_id(Uuid::from_u128(2), "k", 0));
    }

    #[test]
    fn kinds_never_collide() {
        let all = [
            event_id(ORG, "k", 0),
            object_id(ORG, "k", 0),
            quarantine_id(ORG, "k", 0),
        ];
        assert!(all[0] != all[1] && all[1] != all[2] && all[0] != all[2]);
        assert_ne!(batch_id(ORG, "k"), event_id(ORG, "k", 0));
    }
}
