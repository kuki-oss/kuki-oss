//! The idempotency key of a file import.
//!
//! The key is derived from what is imported: the file's bytes, the mapping's bytes and the sheet.
//! Importing the same file with the same mapping again resumes the same batch, so every row is
//! `unchanged` and nothing is duplicated. A changed cell, mapping or sheet is a new batch.

use sha2::{Digest, Sha256};

/// `import:<sha-256 hex>`. Each part is length-prefixed, so moving a byte from one part to the
/// next gives a different key.
pub fn idempotency_key(file: &[u8], mapping: &[u8], sheet: Option<&str>) -> String {
    let mut hash = Sha256::new();
    for part in [file, mapping, sheet.unwrap_or("").as_bytes()] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    format!("import:{}", hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_input_gives_the_same_key_and_any_change_gives_another() {
        let key = idempotency_key(b"a,b\n1,2\n", b"{}", None);
        assert_eq!(key, idempotency_key(b"a,b\n1,2\n", b"{}", None));
        assert!(key.starts_with("import:") && key.len() == "import:".len() + 64);
        assert_ne!(key, idempotency_key(b"a,b\n1,3\n", b"{}", None));
        assert_ne!(key, idempotency_key(b"a,b\n1,2\n", b"{ }", None));
        assert_ne!(key, idempotency_key(b"a,b\n1,2\n", b"{}", Some("Open")));
    }

    #[test]
    fn moving_bytes_between_the_file_and_the_mapping_changes_the_key() {
        assert_ne!(
            idempotency_key(b"ab", b"c", None),
            idempotency_key(b"a", b"bc", None)
        );
    }
}
