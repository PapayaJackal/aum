<script lang="ts">
  import { getDocument, downloadDocument, type DocumentDetail } from "../lib/api";
  import { searchState } from "../lib/searchState.svelte";
  import { highlightTerms } from "../lib/highlight";

  let {
    docId,
    index = "",
    onClose,
    onNavigateDoc,
  }: {
    docId: string;
    index: string;
    onClose: () => void;
    onNavigateDoc: (docId: string, index: string) => void;
  } = $props();

  let doc = $state<DocumentDetail | null>(null);
  let loading = $state(true);
  let error = $state("");
  let showAllMeta = $state(false);

  // Human-readable aliases for common Tika metadata keys.
  const KEY_ALIASES: Record<string, string> = {
    "Content-Type": "Content Type",
    "dc:creator": "Author",
    "meta:author": "Author",
    "Author": "Author",
    "creator": "Creator",
    "dcterms:created": "Created",
    "Creation-Date": "Created",
    "meta:creation-date": "Created",
    "dcterms:modified": "Modified",
    "Last-Modified": "Modified",
    "meta:save-date": "Modified",
    "Content-Length": "File Size",
    "dc:title": "Title",
    "dc:subject": "Subject",
    "dc:description": "Description",
    "Message-From": "From",
    "Message-To": "To",
    "Message-CC": "CC",
    "Message-BCC": "BCC",
    "subject": "Subject",
    "pdf:PDFVersion": "PDF Version",
    "xmpTPg:NPages": "Page Count",
    "meta:page-count": "Page Count",
    "meta:word-count": "Word Count",
    "meta:character-count": "Character Count",
    "Application-Name": "Application",
    "producer": "Producer",
    "pdf:docinfo:producer": "Producer",
  };

  // Keys whose display name should appear in the priority section (default).
  const DEFAULT_PRIORITY = new Set([
    "Title", "Creator", "From", "To", "CC",
    "Created", "Modified", "Content Type", "File Type",
    "File Size", "Page Count", "Word Count", "Subject",
  ]);

  // For email documents, show only these fields in priority, in this order.
  const EMAIL_PRIORITY_ORDER = ["From", "To", "CC", "BCC", "Subject", "Created", "File Size"];
  const EMAIL_PRIORITY = new Set(EMAIL_PRIORITY_ORDER);

  // Noisy internal keys to hide by default.
  const HIDDEN_PREFIXES = ["X-TIKA:", "X-Parsed-By", "access_permission:", "pdf:has", "pdf:encrypted", "pdf:unmapped", "pdf:charsPerPage", "pdf:containsDamagedFont", "pdf:totalUnmapped", "resourceName", "pdf:docinfo:custom:"];
  // Keys injected by the backend for faceting that shouldn't appear as metadata rows.
  const HIDDEN_EXACT = new Set(["Email Addresses"]);

  // Keys whose values are email addresses and should link to the Email Addresses facet.
  const EMAIL_KEYS = new Set(["Message-From", "Message-To", "Message-CC", "Message-BCC"]);

  // Facet labels (injected by backend) that can be clicked to filter search results.
  const FACET_LABELS = new Set(["File Type", "Creator", "Created"]);

  function isHidden(key: string): boolean {
    if (key.startsWith("_aum_")) return true;
    if (HIDDEN_EXACT.has(key)) return true;
    return HIDDEN_PREFIXES.some((p) => key.startsWith(p));
  }

  function displayKey(key: string): string {
    return KEY_ALIASES[key] ?? key;
  }

  function displayValue(value: string | string[]): string {
    return Array.isArray(value) ? value.join(", ") : value;
  }

  /** Extract the email address from an RFC 2822 string like "Name <email>" and lowercase it. */
  function extractEmail(raw: string): string {
    const match = raw.match(/<([^>]+)>/);
    const addr = match ? match[1] : raw;
    return addr.trim().toLowerCase();
  }

  const DATE_DISPLAY_KEYS = new Set(["Created", "Modified"]);

  function formatLocalDate(value: string): string {
    const d = new Date(value);
    if (isNaN(d.getTime())) return value;
    return d.toLocaleString();
  }

  function humanFileSize(bytes: string | number): string {
    const n = typeof bytes === "string" ? parseInt(bytes, 10) : bytes;
    if (isNaN(n)) return String(bytes);
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  }

  // Build search URL with facet active (closes sidebar and navigates to filtered search).
  function facetSearchHref(label: string, value: string): string {
    const facets: Record<string, string[]> = {};
    facets[label] = [value];
    const params = new URLSearchParams();
    params.set("q", searchState.query || "*");
    params.set("facets", JSON.stringify(facets));
    return `#/?${params.toString()}`;
  }

  function handleFacetClick() {
    // Close sidebar — hash navigation will trigger search re-parse.
    onClose();
  }

  type MetaEntry = {
    key: string;
    display: string;
    value: string | string[];
    facetLabel?: string;
    isEmail?: boolean;
    isFileSize?: boolean;
    isDate?: boolean;
  };

  let isEmailDoc = $derived(
    doc?.metadata["File Type"] === "Email"
      || doc?.metadata["Content-Type"]?.toString().startsWith("message/rfc822")
  );

  let metaEntries = $derived.by(() => {
    if (!doc) return { priority: [] as MetaEntry[], extra: [] as MetaEntry[] };
    const priorityNames = isEmailDoc ? EMAIL_PRIORITY : DEFAULT_PRIORITY;
    const priority: MetaEntry[] = [];
    const extra: MetaEntry[] = [];
    const seen = new Set<string>();

    // Collect Created value to compare against Modified.
    let createdValue: string | undefined;
    for (const [key, value] of Object.entries(doc.metadata)) {
      const d = displayKey(key);
      if (d === "Created" && typeof value === "string") { createdValue = value; break; }
    }

    for (const [key, value] of Object.entries(doc.metadata)) {
      if (isHidden(key)) continue;
      const display = displayKey(key);
      if (seen.has(display)) continue;
      // Hide Modified if it matches Created (non-email docs only).
      if (!isEmailDoc && display === "Modified" && typeof value === "string" && value === createdValue) continue;
      seen.add(display);
      const facetLabel = FACET_LABELS.has(key) ? key : undefined;
      const isEmail = EMAIL_KEYS.has(key);
      const isFileSize = display === "File Size";
      const isDate = DATE_DISPLAY_KEYS.has(display);
      const entry: MetaEntry = { key, display, value, facetLabel, isEmail, isFileSize, isDate };
      if (priorityNames.has(display)) {
        priority.push(entry);
      } else {
        extra.push(entry);
      }
    }

    // For emails, sort priority entries to match the defined order.
    if (isEmailDoc) {
      priority.sort((a, b) => EMAIL_PRIORITY_ORDER.indexOf(a.display) - EMAIL_PRIORITY_ORDER.indexOf(b.display));
    }

    return { priority, extra };
  });

  $effect(() => {
    loading = true;
    error = "";
    showAllMeta = false;
    getDocument(docId, index)
      .then((d) => (doc = d))
      .catch((err) => (error = err.message))
      .finally(() => (loading = false));
  });

  let contentHtml = $derived(
    doc ? highlightTerms(doc.content, searchState.query) : "",
  );
</script>

<div class="sidebar-header">
  <h2>
    {#if doc}
      {doc.display_path.split("/").pop()}
    {:else}
      Document
    {/if}
  </h2>
  <button class="close-btn" onclick={onClose} title="Close">✕</button>
</div>

<div class="sidebar-body">
  {#if loading}
    <p class="loading">Loading document...</p>
  {:else if error}
    <div class="error">{error}</div>
  {:else if doc}
    <div class="doc-info">
      {#if doc.extracted_from}
        <p class="doc-path">Extracted from <button class="link-btn" onclick={() => onNavigateDoc(doc!.extracted_from!.doc_id, index)}>{doc.extracted_from.display_path}</button></p>
      {:else}
        <p class="doc-path">{index}/{doc.display_path}</p>
      {/if}
      <button class="download-btn" onclick={() => downloadDocument(docId, index)}>Download original</button>
    </div>

    {#snippet metaValue(entry: MetaEntry)}
      {#if entry.isDate}
        {formatLocalDate(entry.value as string)}
      {:else if entry.isFileSize}
        {humanFileSize(entry.value as string)}
      {:else if entry.isEmail}
        {#if Array.isArray(entry.value)}
          {#each entry.value as v, i}
            {#if i > 0}, {/if}
            <a class="facet-link" href={facetSearchHref("Email Addresses", extractEmail(v))} onclick={handleFacetClick}>{v}</a>
          {/each}
        {:else}
          <a class="facet-link" href={facetSearchHref("Email Addresses", extractEmail(entry.value))} onclick={handleFacetClick}>{entry.value}</a>
        {/if}
      {:else if entry.facetLabel && !Array.isArray(entry.value)}
        <a class="facet-link" href={facetSearchHref(entry.facetLabel, entry.value)} onclick={handleFacetClick}>{entry.value}</a>
      {:else if entry.facetLabel && Array.isArray(entry.value)}
        {#each entry.value as v, i}
          {#if i > 0}, {/if}
          <a class="facet-link" href={facetSearchHref(entry.facetLabel, v)} onclick={handleFacetClick}>{v}</a>
        {/each}
      {:else}
        {displayValue(entry.value)}
      {/if}
    {/snippet}

    <div class="meta-table">
      <h3>Metadata</h3>
      <div class="meta-scroll">
        <table>
          <tbody>
            {#each metaEntries.priority as entry}
              <tr>
                <td class="meta-key">{entry.display}</td>
                <td>{@render metaValue(entry)}</td>
              </tr>
            {/each}
            {#if metaEntries.extra.length > 0}
              <tr>
                <td colspan="2">
                  <button class="toggle-extra" onclick={() => (showAllMeta = !showAllMeta)}>
                    {showAllMeta ? "Hide" : "Show"} {metaEntries.extra.length} more fields
                  </button>
                </td>
              </tr>
              {#if showAllMeta}
                {#each metaEntries.extra as entry}
                  <tr>
                    <td class="meta-key">{entry.display}</td>
                    <td>{@render metaValue(entry)}</td>
                  </tr>
                {/each}
              {/if}
            {/if}
          </tbody>
        </table>
      </div>
    </div>

    <div class="content-section">
      <h3>Content</h3>
      <pre>{@html contentHtml}</pre>
    </div>

    {#if doc.attachments.length > 0}
      <div class="attachments-section">
        <h3>Attachments</h3>
        <ul>
          {#each doc.attachments as att}
            <li><button class="link-btn" onclick={() => onNavigateDoc(att.doc_id, index)}>{att.display_path.split("/").pop()}</button></li>
          {/each}
        </ul>
      </div>
    {/if}
  {/if}
</div>

<style>
  .sidebar-header {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.75rem 1rem;
    border-bottom: 1px solid #ddd;
    background: #fafafa;
    position: sticky;
    top: 0;
    z-index: 1;
  }

  .sidebar-header h2 {
    margin: 0;
    font-size: 1rem;
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .close-btn {
    flex-shrink: 0;
    background: none;
    border: none;
    font-size: 1.1rem;
    color: #888;
    cursor: pointer;
    padding: 0.2rem 0.4rem;
    border-radius: 4px;
    line-height: 1;
  }

  .close-btn:hover {
    background: #eee;
    color: #333;
  }

  .sidebar-body {
    padding: 0.75rem 1rem;
  }

  .doc-info {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
  }

  .doc-path {
    margin: 0;
    font-size: 0.82rem;
    color: #888;
    word-break: break-all;
    flex: 1;
    min-width: 0;
  }

  .link-btn {
    background: none;
    border: none;
    color: #4a7cf7;
    cursor: pointer;
    font: inherit;
    padding: 0;
    text-decoration: none;
  }

  .link-btn:hover {
    text-decoration: underline;
  }

  .download-btn {
    flex-shrink: 0;
    font-size: 0.8rem;
    font-family: inherit;
    color: #4a7cf7;
    background: transparent;
    cursor: pointer;
    border: 1px solid #4a7cf7;
    padding: 0.2rem 0.55rem;
    border-radius: 4px;
  }

  .download-btn:hover {
    background: #4a7cf7;
    color: white;
  }

  .attachments-section {
    background: white;
    border-radius: 6px;
    padding: 0.75rem;
    margin: 0.75rem 0;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .attachments-section h3 {
    margin: 0 0 0.4rem;
    font-size: 0.9rem;
    color: #666;
  }

  .attachments-section ul {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .attachments-section li {
    padding: 0.25rem 0;
    border-bottom: 1px solid #eee;
    font-size: 0.85rem;
  }

  .attachments-section li:last-child {
    border-bottom: none;
  }

  .loading {
    color: #888;
    padding: 1rem;
  }

  .error {
    background: #fee;
    color: #c33;
    padding: 0.75rem;
    border-radius: 4px;
    margin: 0.75rem 0;
  }

  .meta-table {
    background: white;
    border-radius: 6px;
    padding: 0.75rem;
    margin: 0.75rem 0;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .meta-table h3 {
    margin: 0 0 0.5rem;
    font-size: 0.9rem;
    color: #666;
  }

  .meta-scroll {
    max-height: 300px;
    overflow-y: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  td {
    padding: 0.3rem 0.4rem;
    border-bottom: 1px solid #eee;
    font-size: 0.85rem;
    vertical-align: top;
  }

  .meta-key {
    font-weight: 600;
    white-space: nowrap;
    width: 130px;
    color: #555;
  }

  .facet-link {
    color: #4a7cf7;
    text-decoration: none;
  }

  .facet-link:hover {
    text-decoration: underline;
  }

  .toggle-extra {
    background: none;
    border: none;
    color: #66a;
    font-size: 0.82rem;
    cursor: pointer;
    padding: 0.3rem 0;
  }

  .toggle-extra:hover {
    text-decoration: underline;
  }

  .content-section {
    background: white;
    border-radius: 6px;
    padding: 0.75rem;
    margin: 0.75rem 0;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .content-section h3 {
    margin: 0 0 0.5rem;
    font-size: 0.9rem;
    color: #666;
  }

  pre {
    white-space: pre-wrap;
    word-wrap: break-word;
    font-size: 0.85rem;
    line-height: 1.5;
    margin: 0;
  }

  pre :global(mark) {
    background: #fff3b0;
    padding: 0.1em;
    border-radius: 2px;
  }
</style>
