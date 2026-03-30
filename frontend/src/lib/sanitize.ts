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
    "style",
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
};

// Register URL-filtering hook on the isolated instance.
purifier.addHook("afterSanitizeAttributes", (node: Element) => {
  // img src: only allow data: URIs (inline/embedded images).
  if (node.tagName === "IMG") {
    const src = node.getAttribute("src") || "";
    if (!src.startsWith("data:")) {
      node.removeAttribute("src");
    }
  }

  if (node.tagName === "A") {
    node.setAttribute("rel", "noopener noreferrer");
  }

  // Strip any background-image CSS that loads external URLs.
  // DOMPurify allows style attributes but we need to block url() calls
  // that aren't data: URIs.
  const style = node.getAttribute("style");
  if (style && /url\s*\(/i.test(style)) {
    // Remove url() calls that don't use data: URIs.
    const cleaned = style.replace(/url\s*\(\s*(?:['"]?)(?!data:)[^)]*(?:['"]?)\s*\)/gi, "none");
    node.setAttribute("style", cleaned);
  }
});

/**
 * Sanitize an HTML string for safe rendering inside a sandboxed iframe.
 *
 * Strips scripts, event handlers, and external resource references.
 * Allows inline styles and ``data:`` image URIs.
 */
export function sanitizeHtmlForPreview(dirty: string): string {
  return purifier.sanitize(dirty, HTML_PREVIEW_CONFIG);
}
