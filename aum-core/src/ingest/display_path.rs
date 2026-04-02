//! Display-path relativisation for ingested documents.

use std::path::Path;

use crate::extraction::{AUM_DISPLAY_PATH_KEY, AUM_EXTRACTED_FROM_KEY};
use crate::models::{Document, MetadataValue};

/// Set `_aum_display_path` to a path relative to `source_dir`.
///
/// For top-level documents the display path is derived from `source_path`.
/// For attachments the extractor has already set `_aum_display_path` to an
/// absolute logical path — this function resolves both cases to a relative
/// path.  Also relativises `_aum_extracted_from` (the parent container path)
/// so it matches the parent document's stored display path.
pub fn set_display_path(doc: &mut Document, source_dir: &Path) {
    let logical = match doc.metadata.get(AUM_DISPLAY_PATH_KEY) {
        Some(MetadataValue::Single(s)) if !s.is_empty() => Path::new(s).to_owned(),
        _ => doc.source_path.clone(),
    };

    let display = relativise_or_filename(&logical, source_dir);
    doc.metadata.insert(
        AUM_DISPLAY_PATH_KEY.to_owned(),
        MetadataValue::Single(display),
    );

    relativise_extracted_from(doc, source_dir);
}

/// Relativise `_aum_extracted_from`, removing it if the path cannot be made
/// relative (to avoid leaking absolute paths through the API).
fn relativise_extracted_from(doc: &mut Document, source_dir: &Path) {
    let Some(MetadataValue::Single(ef)) = doc.metadata.get(AUM_EXTRACTED_FROM_KEY) else {
        return;
    };
    let ef_path = Path::new(ef).to_owned();
    match ef_path.strip_prefix(source_dir) {
        Ok(rel) => {
            let val = MetadataValue::Single(rel.to_string_lossy().into_owned());
            doc.metadata.insert(AUM_EXTRACTED_FROM_KEY.to_owned(), val);
        }
        Err(_) => {
            doc.metadata.remove(AUM_EXTRACTED_FROM_KEY);
        }
    }
}

/// Return `path` relative to `base`, falling back to just the filename.
fn relativise_or_filename(path: &Path, base: &Path) -> String {
    path.strip_prefix(base).map_or_else(
        |_| {
            path.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        },
        |rel| rel.to_string_lossy().into_owned(),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::models::{Document, MetadataValue};

    fn make_doc(source: &str, meta: Vec<(&str, &str)>) -> Document {
        let metadata = meta
            .into_iter()
            .map(|(k, v)| (k.to_owned(), MetadataValue::Single(v.to_owned())))
            .collect::<HashMap<_, _>>();
        Document {
            source_path: PathBuf::from(source),
            content: String::new(),
            metadata,
        }
    }

    #[test]
    fn top_level_document_gets_relative_path() {
        let mut doc = make_doc("/data/inbox/report.pdf", vec![]);
        set_display_path(&mut doc, Path::new("/data/inbox"));
        assert_eq!(
            doc.metadata.get(AUM_DISPLAY_PATH_KEY),
            Some(&MetadataValue::Single("report.pdf".to_owned()))
        );
    }

    #[test]
    fn nested_document_gets_relative_path() {
        let mut doc = make_doc("/data/inbox/sub/deep/file.txt", vec![]);
        set_display_path(&mut doc, Path::new("/data/inbox"));
        assert_eq!(
            doc.metadata.get(AUM_DISPLAY_PATH_KEY),
            Some(&MetadataValue::Single("sub/deep/file.txt".to_owned()))
        );
    }

    #[test]
    fn attachment_display_path_is_relativised() {
        let mut doc = make_doc(
            "/data/inbox/extracted/abc/attach.pdf",
            vec![(AUM_DISPLAY_PATH_KEY, "/data/inbox/email.eml/attach.pdf")],
        );
        set_display_path(&mut doc, Path::new("/data/inbox"));
        assert_eq!(
            doc.metadata.get(AUM_DISPLAY_PATH_KEY),
            Some(&MetadataValue::Single("email.eml/attach.pdf".to_owned()))
        );
    }

    #[test]
    fn outside_source_dir_falls_back_to_filename() {
        let mut doc = make_doc("/other/place/file.pdf", vec![]);
        set_display_path(&mut doc, Path::new("/data/inbox"));
        assert_eq!(
            doc.metadata.get(AUM_DISPLAY_PATH_KEY),
            Some(&MetadataValue::Single("file.pdf".to_owned()))
        );
    }

    #[test]
    fn extracted_from_is_relativised() {
        let mut doc = make_doc(
            "/data/inbox/extracted/abc/attach.pdf",
            vec![(AUM_EXTRACTED_FROM_KEY, "/data/inbox/archive.zip")],
        );
        set_display_path(&mut doc, Path::new("/data/inbox"));
        assert_eq!(
            doc.metadata.get(AUM_EXTRACTED_FROM_KEY),
            Some(&MetadataValue::Single("archive.zip".to_owned()))
        );
    }

    #[test]
    fn extracted_from_removed_when_not_relative() {
        let mut doc = make_doc(
            "/data/inbox/file.pdf",
            vec![(AUM_EXTRACTED_FROM_KEY, "/somewhere/else/archive.zip")],
        );
        set_display_path(&mut doc, Path::new("/data/inbox"));
        assert!(!doc.metadata.contains_key(AUM_EXTRACTED_FROM_KEY));
    }

    #[test]
    fn missing_extracted_from_is_noop() {
        let mut doc = make_doc("/data/inbox/file.pdf", vec![]);
        set_display_path(&mut doc, Path::new("/data/inbox"));
        assert!(!doc.metadata.contains_key(AUM_EXTRACTED_FROM_KEY));
    }
}
