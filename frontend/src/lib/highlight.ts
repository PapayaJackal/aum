import DOMPurify from "dompurify";

const MARK_ONLY = { ALLOWED_TAGS: ["mark"] };

/**
 * Highlight search terms in plain text, returning sanitised HTML.
 *
 * Runs the regex on raw text then sanitises the result so only
 * `<mark>` tags survive — any HTML in the source content is escaped.
 */
export function highlightTerms(text: string, query: string): string {
  const terms = query.split(/\s+/).filter(Boolean);
  if (terms.length === 0) return DOMPurify.sanitize(text, MARK_ONLY);

  const escaped = terms.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const pattern = new RegExp(`(${escaped.join("|")})`, "gi");

  const html = text.replace(pattern, "<mark>$1</mark>");
  return DOMPurify.sanitize(html, MARK_ONLY);
}

/**
 * Sanitise HTML that already contains `<mark>` highlight tags
 * (e.g. snippets and display paths returned by Elasticsearch).
 * Strips everything except `<mark>`.
 */
export function sanitizeHighlight(html: string): string {
  return DOMPurify.sanitize(html, MARK_ONLY);
}

/**
 * Escape a plain string so it is safe to concatenate into an
 * {@html ...} expression.  Uses DOMPurify with no allowed tags
 * so every HTML character is escaped.
 */
export function escapeHtml(text: string): string {
  return DOMPurify.sanitize(text, { ALLOWED_TAGS: [] });
}
