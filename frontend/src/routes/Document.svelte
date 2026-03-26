<script lang="ts">
  import { getDocument, downloadDocument, type DocumentDetail } from "../lib/api";

  let { docId, index = "", qs = "" }: { docId: string; index: string; qs: string } = $props();

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
  const EMAIL_PRIORITY_ORDER = ["From", "To", "CC", "BCC", "Subject", "File Size"];
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

  function humanFileSize(bytes: string | number): string {
    const n = typeof bytes === "string" ? parseInt(bytes, 10) : bytes;
    if (isNaN(n)) return String(bytes);
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  }

  // Build search URL with facet active.
  function facetSearchHref(label: string, value: string): string {
    const params = new URLSearchParams(qs.startsWith("?") ? qs.slice(1) : qs);
    const facets: Record<string, string[]> = {};
    const existing = params.get("facets");
    if (existing) {
      try { Object.assign(facets, JSON.parse(existing)); } catch {}
    }
    facets[label] = [value];
    params.set("facets", JSON.stringify(facets));
    if (!params.get("q")) params.set("q", "*");
    return `#/?${params.toString()}`;
  }

  type MetaEntry = {
    key: string;
    display: string;
    value: string | string[];
    facetLabel?: string;
    isEmail?: boolean;
    isFileSize?: boolean;
  };

  let isEmailDoc = $derived(
    doc?.metadata["File Type"] === "Email (EML)"
      || doc?.metadata["Content-Type"]?.toString().startsWith("message/rfc822")
  );

  let metaEntries = $derived.by(() => {
    if (!doc) return { priority: [] as MetaEntry[], extra: [] as MetaEntry[] };
    const priorityNames = isEmailDoc ? EMAIL_PRIORITY : DEFAULT_PRIORITY;
    const priority: MetaEntry[] = [];
    const extra: MetaEntry[] = [];
    const seen = new Set<string>();

    for (const [key, value] of Object.entries(doc.metadata)) {
      if (isHidden(key)) continue;
      const display = displayKey(key);
      if (seen.has(display)) continue;
      seen.add(display);
      const facetLabel = FACET_LABELS.has(key) ? key : undefined;
      const isEmail = EMAIL_KEYS.has(key);
      const isFileSize = display === "File Size";
      const entry: MetaEntry = { key, display, value, facetLabel, isEmail, isFileSize };
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

  function docHref(id: string): string {
    return `#/document/${encodeURIComponent(index)}/${id}${qs}`;
  }

  $effect(() => {
    loading = true;
    error = "";
    getDocument(docId, index)
      .then((d) => (doc = d))
      .catch((err) => (error = err.message))
      .finally(() => (loading = false));
  });
</script>

<svelte:head>
  <title>{doc ? `aum - ${doc.display_path.split("/").pop()}` : "aum"}</title>
</svelte:head>

{#if loading}
  <p class="loading">Loading document...</p>
{:else if error}
  <div class="error">{error}</div>
{:else if doc}
  <div class="doc-header">
    <div class="doc-title">
      <h2>{doc.display_path.split("/").pop()}</h2>
      {#if doc.extracted_from}
        <p class="doc-path">Extracted from <a href={docHref(doc.extracted_from.doc_id)}>{doc.extracted_from.display_path}</a></p>
      {:else}
        <p class="doc-path">{index}/{doc.display_path}</p>
      {/if}
    </div>
    <button class="download-btn" onclick={() => downloadDocument(docId, index)}>Download original</button>
  </div>

  {#snippet metaValue(entry: MetaEntry)}
    {#if entry.isFileSize}
      {humanFileSize(entry.value as string)}
    {:else if entry.isEmail}
      {#if Array.isArray(entry.value)}
        {#each entry.value as v, i}
          {#if i > 0}, {/if}
          <a class="facet-link" href={facetSearchHref("Email Addresses", extractEmail(v))}>{v}</a>
        {/each}
      {:else}
        <a class="facet-link" href={facetSearchHref("Email Addresses", extractEmail(entry.value))}>{entry.value}</a>
      {/if}
    {:else if entry.facetLabel && !Array.isArray(entry.value)}
      <a class="facet-link" href={facetSearchHref(entry.facetLabel, entry.value)}>{entry.value}</a>
    {:else if entry.facetLabel && Array.isArray(entry.value)}
      {#each entry.value as v, i}
        {#if i > 0}, {/if}
        <a class="facet-link" href={facetSearchHref(entry.facetLabel, v)}>{v}</a>
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
    <pre>{doc.content}</pre>
  </div>

  {#if doc.attachments.length > 0}
    <div class="attachments-section">
      <h3>Attachments</h3>
      <ul>
        {#each doc.attachments as att}
          <li><a href={docHref(att.doc_id)}>{att.display_path.split("/").pop()}</a></li>
        {/each}
      </ul>
    </div>
  {/if}
{/if}

<style>
  .doc-header {
    display: flex;
    align-items: flex-start;
    gap: 1rem;
    margin: 1rem 0 0.5rem;
  }

  .doc-title {
    flex: 1;
    min-width: 0;
  }

  h2 {
    margin: 0;
    word-break: break-all;
  }

  .doc-path {
    margin: 0.2rem 0 0;
    font-size: 0.85rem;
    color: #888;
    word-break: break-all;
  }

  .download-btn {
    flex-shrink: 0;
    font-size: 0.85rem;
    font-family: inherit;
    color: #4a7cf7;
    background: transparent;
    cursor: pointer;
    border: 1px solid #4a7cf7;
    padding: 0.25rem 0.65rem;
    border-radius: 4px;
  }

  .download-btn:hover {
    background: #4a7cf7;
    color: white;
  }

  .doc-path a {
    color: #4a7cf7;
    text-decoration: none;
  }

  .doc-path a:hover {
    text-decoration: underline;
  }

  .attachments-section {
    background: white;
    border-radius: 6px;
    padding: 1rem;
    margin: 1rem 0;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .attachments-section h3 {
    margin: 0 0 0.5rem;
    font-size: 0.95rem;
    color: #666;
  }

  .attachments-section ul {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .attachments-section li {
    padding: 0.3rem 0;
    border-bottom: 1px solid #eee;
    font-size: 0.9rem;
  }

  .attachments-section li:last-child {
    border-bottom: none;
  }

  .attachments-section a {
    color: #4a7cf7;
    text-decoration: none;
  }

  .attachments-section a:hover {
    text-decoration: underline;
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
    margin: 1rem 0;
  }

  .meta-table {
    background: white;
    border-radius: 6px;
    padding: 1rem;
    margin: 1rem 0;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .meta-table h3 {
    margin: 0 0 0.75rem;
    font-size: 0.95rem;
    color: #666;
  }

  .meta-scroll {
    max-height: 400px;
    overflow-y: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  td {
    padding: 0.35rem 0.5rem;
    border-bottom: 1px solid #eee;
    font-size: 0.9rem;
    vertical-align: top;
  }

  .meta-key {
    font-weight: 600;
    white-space: nowrap;
    width: 200px;
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
    font-size: 0.85rem;
    cursor: pointer;
    padding: 0.35rem 0;
  }

  .toggle-extra:hover {
    text-decoration: underline;
  }

  .content-section {
    background: white;
    border-radius: 6px;
    padding: 1rem;
    margin: 1rem 0;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .content-section h3 {
    margin: 0 0 0.75rem;
    font-size: 0.95rem;
    color: #666;
  }

  pre {
    white-space: pre-wrap;
    word-wrap: break-word;
    font-size: 0.9rem;
    line-height: 1.6;
    margin: 0;
  }
</style>
