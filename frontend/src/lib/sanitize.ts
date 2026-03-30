/**
 * HTML sanitization for document preview rendering.
 *
 * Uses an isolated DOMPurify instance so hooks do not interfere with
 * the highlight module's sanitization calls.
 */
import DOMPurify, { type Config } from "dompurify";

// Isolated instance — hooks registered here do not affect other DOMPurify usage.
const purifier = DOMPurify();

const HTML_PREVIEW_CONFIG: Config = {
  ALLOWED_TAGS: [
    "a",
    "abbr",
    "address",
    "article",
    "aside",
    "b",
    "bdi",
    "bdo",
    "big",
    "blockquote",
    "br",
    "caption",
    "center",
    "cite",
    "code",
    "col",
    "colgroup",
    "dd",
    "del",
    "details",
    "dfn",
    "div",
    "dl",
    "dt",
    "em",
    "figcaption",
    "figure",
    "font",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "html",
    "head",
    "body",
    "i",
    "img",
    "ins",
    "kbd",
    "li",
    "main",
    "mark",
    "nav",
    "ol",
    "p",
    "pre",
    "q",
    "rp",
    "rt",
    "ruby",
    "s",
    "samp",
    "section",
    "small",
    "span",
    "strong",
    "sub",
    "summary",
    "sup",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "time",
    "tr",
    "u",
    "ul",
    "var",
    "wbr",
  ],
  ALLOWED_ATTR: [
    "align",
    "alt",
    "bgcolor",
    "border",
    "cellpadding",
    "cellspacing",
    "class",
    "color",
    "colspan",
    "dir",
    "face",
    "height",
    "href",
    "id",
    "lang",
    "rowspan",
    "size",
    "src",
    "start",
    "style",
    "title",
    "type",
    "valign",
    "width",
  ],
  ALLOW_DATA_ATTR: false,
  WHOLE_DOCUMENT: true,
  // Forbid <style> tags — CSS blocks can exfiltrate data via url(),
  // @import, and @font-face which cannot be reliably stripped with regex.
  // Inline style attributes are kept but sanitized in the hook below.
  FORBID_TAGS: ["style"],
};

/**
 * Strip external resource references from an inline CSS style string.
 *
 * Removes any CSS function that could trigger a network request:
 * ``url()``, ``image-set()``, ``@import``, etc.  Only ``data:image/*``
 * URIs are preserved in ``url()`` calls.
 */
function sanitizeCssValue(css: string): string {
  // Remove @import statements that may appear in inline styles via CSS injection.
  let cleaned = css.replace(/@import\b[^;]*/gi, "");
  // Remove url() calls that don't use safe data:image/ URIs.
  cleaned = cleaned.replace(/url\s*\(([^)]*)\)/gi, (_match: string, inner: string) => {
    const trimmed = inner.trim().replace(/^['"]|['"]$/g, "");
    if (/^data:image\//i.test(trimmed)) {
      return `url(${inner})`;
    }
    return "none";
  });
  // Remove image-set() which can also load external resources.
  cleaned = cleaned.replace(/image-set\s*\([^)]*\)/gi, "none");
  return cleaned;
}

// Register URL-filtering hook on the isolated instance.
purifier.addHook("afterSanitizeAttributes", (node: Element) => {
  // img src: only allow data:image/ URIs (inline/embedded images).
  if (node.tagName === "IMG") {
    const src = node.getAttribute("src") || "";
    if (!/^data:image\//i.test(src)) {
      node.removeAttribute("src");
    }
  }

  if (node.tagName === "A") {
    node.setAttribute("rel", "noopener noreferrer");
  }

  // Sanitize inline style attributes to block external resource loading.
  const style = node.getAttribute("style");
  if (style) {
    const cleaned = sanitizeCssValue(style);
    if (cleaned !== style) {
      node.setAttribute("style", cleaned);
    }
  }
});

/**
 * Sanitize an HTML string for safe rendering inside a sandboxed iframe.
 *
 * Strips scripts, event handlers, ``<style>`` tags, and external resource
 * references.  Allows inline styles (with sanitized ``url()`` calls) and
 * ``data:image/*`` URIs.
 */
export function sanitizeHtmlForPreview(dirty: string): string {
  return purifier.sanitize(dirty, HTML_PREVIEW_CONFIG);
}
