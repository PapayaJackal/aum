/** Maps raw MIME type strings to short human-readable labels. */
export const MIME_ALIASES: Record<string, string> = {
  "application/pdf": "PDF",
  "application/msword": "Word",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "Word",
  "application/vnd.ms-excel": "Excel",
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "Excel",
  "application/vnd.ms-powerpoint": "PowerPoint",
  "application/vnd.openxmlformats-officedocument.presentationml.presentation": "PowerPoint",
  "text/plain": "Text",
  "text/html": "HTML",
  "text/csv": "CSV",
  "text/markdown": "Markdown",
  "text/rtf": "RTF",
  "application/rtf": "RTF",
  "application/zip": "ZIP",
  "application/x-tar": "TAR",
  "application/gzip": "GZip",
  "application/json": "JSON",
  "application/xml": "XML",
  "application/epub+zip": "EPUB",
  "application/x-mobipocket-ebook": "Mobi",
  "image/png": "PNG",
  "image/jpeg": "JPEG",
  "image/gif": "GIF",
  "image/svg+xml": "SVG",
  "message/rfc822": "Email",
};

/** Return a human-readable alias for a MIME type, or the original string if unknown. */
export function mimeAlias(mime: string): string {
  if (!mime) return "";
  return MIME_ALIASES[mime.toLowerCase()] ?? mime;
}
