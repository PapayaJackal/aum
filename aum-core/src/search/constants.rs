//! Shared metadata constants: MIME type aliases, metadata key display names,
//! hidden keys, facet field definitions, and date facet labels.

use std::collections::HashMap;
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// MIME type display aliases
// ---------------------------------------------------------------------------

/// Maps raw MIME type strings to short human-readable labels.
pub static MIMETYPE_ALIASES: phf::Map<&'static str, &'static str> = phf::phf_map! {
    "application/pdf" => "PDF",
    "application/msword" => "Word",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "Word",
    "application/vnd.ms-excel" => "Excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "Excel",
    "application/vnd.ms-powerpoint" => "PowerPoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation" => "PowerPoint",
    "text/plain" => "Text",
    "text/html" => "HTML",
    "text/csv" => "CSV",
    "text/markdown" => "Markdown",
    "text/rtf" => "RTF",
    "application/rtf" => "RTF",
    "application/zip" => "ZIP",
    "application/x-tar" => "TAR",
    "application/gzip" => "GZip",
    "application/json" => "JSON",
    "application/xml" => "XML",
    "application/epub+zip" => "EPUB",
    "application/x-mobipocket-ebook" => "Mobi",
    "image/png" => "PNG",
    "image/jpeg" => "JPEG",
    "image/gif" => "GIF",
    "image/svg+xml" => "SVG",
    "message/rfc822" => "Email",
};

/// Maps short human-readable labels back to raw MIME type strings.
///
/// Derived from [`MIMETYPE_ALIASES`] at first use.
pub static REVERSE_MIMETYPE_ALIASES: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| MIMETYPE_ALIASES.entries().map(|(k, v)| (*v, *k)).collect());

// ---------------------------------------------------------------------------
// Metadata key display aliases
// ---------------------------------------------------------------------------

/// Maps Tika metadata key names to human-readable display labels.
pub static METADATA_KEY_ALIASES: phf::Map<&'static str, &'static str> = phf::phf_map! {
    "Content-Type" => "File Type",
    "dc:creator" => "Creator",
    "Author" => "Creator",
    "creator" => "Creator",
    "xmp:dc:creator" => "Creator",
    "meta:author" => "Creator",
    "dcterms:created" => "Created",
    "Creation-Date" => "Created",
    "meta:creation-date" => "Created",
    "created" => "Created",
    "date" => "Created",
    "dcterms:modified" => "Modified",
    "Last-Modified" => "Modified",
    "meta:save-date" => "Modified",
    "modified" => "Modified",
    "Content-Length" => "File Size",
    "dc:title" => "Title",
    "title" => "Title",
    "dc:subject" => "Subject",
    "subject" => "Subject",
    "dc:description" => "Description",
    "dc:language" => "Language",
    "language" => "Language",
    "Message-From" => "From",
    "Message-To" => "To",
    "Message-CC" => "CC",
    "Message-Subject" => "Subject",
    "Message:Raw-Header:Message-ID" => "Message ID",
    "Message:Raw-Header:In-Reply-To" => "In Reply To",
    "Message:Raw-Header:References" => "References",
};

// ---------------------------------------------------------------------------
// Hidden metadata keys
// ---------------------------------------------------------------------------

/// Tika metadata keys that are too noisy or internal to show in the UI.
pub static HIDDEN_METADATA_KEYS: phf::Set<&'static str> = phf::phf_set! {
    "X-TIKA:EXCEPTION:warn",
    "X-TIKA:EXCEPTION:runtime",
    "X-TIKA:content",
    "X-TIKA:embedded_resource_path",
    "X-TIKA:content_handler",
    "X-TIKA:content_handler_type",
    "X-TIKA:parse_time_millis",
    "X-TIKA:Parsed-By",
    "X-TIKA:Parsed-By-Full-Set",
    "tiff:BitsPerSample",
    "tiff:ImageLength",
    "tiff:ImageWidth",
    "tiff:ResolutionUnit",
    "tiff:XResolution",
    "tiff:YResolution",
    "tiff:Orientation",
    "exif:ISOSpeedRatings",
    "exif:ExposureTime",
    "exif:FNumber",
    "exif:Flash",
    "exif:FocalLength",
    "exif:PixelXDimension",
    "exif:PixelYDimension",
    "exif:WhiteBalance",
    "exif:ColorSpace",
    "exif:ComponentsConfiguration",
    "exif:ExifVersion",
    "exif:FlashPixVersion",
    "exif:InteroperabilityIndex",
    "exif:SceneCaptureType",
    "exif:CustomRendered",
    "exif:ExposureMode",
    "exif:DigitalZoomRatio",
    "exif:SubjectDistanceRange",
    "exif:LightSource",
    "exif:MeteringMode",
    "exif:SensingMethod",
    "exif:Sharpness",
    "exif:Saturation",
    "exif:Contrast",
    "exif:GainControl",
    "resourceName",
};

// ---------------------------------------------------------------------------
// Facet field definitions
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Facet label string constants
// ---------------------------------------------------------------------------

/// Facet label for document file type (maps to `meta_content_type`).
pub const FACET_FILE_TYPE: &str = "File Type";
/// Facet label for document creator/author (maps to `meta_creator`).
pub const FACET_CREATOR: &str = "Creator";
/// Facet label for email address participants (maps to `meta_email_addresses`).
pub const FACET_EMAIL_ADDRESSES: &str = "Email Addresses";
/// Facet label for document creation year (maps to `meta_created_year`).
pub const FACET_CREATED: &str = "Created";

/// Maps facet display labels to their indexed Meilisearch field names.
pub static FACET_FIELDS: phf::Map<&'static str, &'static str> = phf::phf_map! {
    "Created" => "meta_created_year",
    "File Type" => "meta_content_type",
    "Creator" => "meta_creator",
    "Email Addresses" => "meta_email_addresses",
};

/// Canonical display order for facet panels: Created, File Type, Creator, Email Addresses.
/// Unknown facets fall after this list.
pub const FACET_ORDER: &[&str] = &[
    FACET_CREATED,
    FACET_FILE_TYPE,
    FACET_CREATOR,
    FACET_EMAIL_ADDRESSES,
];

/// Maps indexed Meilisearch field names back to their facet display labels.
///
/// Derived from [`FACET_FIELDS`] at first use.
pub static REVERSE_FACET_FIELDS: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        FACET_FIELDS
            .entries()
            .map(|(label, field)| (*field, *label))
            .collect()
    });

/// Facet labels that use date-range (year) aggregation rather than term counts.
pub static DATE_FACETS: phf::Set<&'static str> = phf::phf_set! {
    "Created",
};

// ---------------------------------------------------------------------------
// Highlight tag constants
// ---------------------------------------------------------------------------

/// Opening tag injected around highlighted terms by both search backends.
pub const HIGHLIGHT_PRE_TAG: &str = "<mark>";
/// Closing tag injected around highlighted terms by both search backends.
pub const HIGHLIGHT_POST_TAG: &str = "</mark>";
