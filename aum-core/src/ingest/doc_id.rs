//! Stable document-ID generation for the ingest pipeline.

use std::path::Path;

/// Generate a stable document ID from a canonical file path and part index.
///
/// Uses blake3 for speed and consistency with the rest of the crate.
/// Returns a 16-character lowercase hex string.
#[must_use]
pub fn file_doc_id(canonical_path: &Path, index: u64) -> String {
    let key = format!("{}:{index}", canonical_path.display());
    let hash = blake3::hash(key.as_bytes());
    hash.to_hex().as_str()[..16].to_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn deterministic_output() {
        let path = PathBuf::from("/data/docs/report.pdf");
        let id1 = file_doc_id(&path, 0);
        let id2 = file_doc_id(&path, 0);
        assert_eq!(id1, id2);
    }

    #[test]
    fn length_is_16_hex_chars() {
        let id = file_doc_id(Path::new("/foo/bar.txt"), 0);
        assert_eq!(id.len(), 16);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn different_paths_produce_different_ids() {
        let a = file_doc_id(Path::new("/a.txt"), 0);
        let b = file_doc_id(Path::new("/b.txt"), 0);
        assert_ne!(a, b);
    }

    #[test]
    fn different_indices_produce_different_ids() {
        let path = Path::new("/same.pdf");
        let a = file_doc_id(path, 0);
        let b = file_doc_id(path, 1);
        assert_ne!(a, b);
    }

    #[test]
    fn known_value() {
        // Pin a known input to catch accidental algorithm changes.
        let id = file_doc_id(Path::new("/data/test.pdf"), 0);
        let expected = blake3::hash(b"/data/test.pdf:0").to_hex();
        assert_eq!(id, &expected.as_str()[..16]);
    }
}
