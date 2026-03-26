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
  let downloadError = $state("");
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

<div class="flex items-center gap-3 px-4 py-3 border-b border-gray-300 bg-gray-50 sticky top-0 z-[1]">
  <h2 class="m-0 text-base flex-1 min-w-0 overflow-hidden text-ellipsis whitespace-nowrap">
    {#if doc}
      {doc.display_path.split("/").pop()}
    {:else}
      Document
    {/if}
  </h2>
  <button class="shrink-0 bg-transparent border-none text-lg text-gray-400 cursor-pointer p-1 rounded leading-none hover:bg-gray-200 hover:text-gray-800" onclick={onClose} title="Close">&#x2715;</button>
</div>

<div class="p-3 px-4">
  {#if loading}
    <p class="text-gray-400 p-4">Loading document...</p>
  {:else if error}
    <div class="bg-red-50 text-red-600 p-3 rounded my-3">{error}</div>
  {:else if doc}
    <div class="flex items-center gap-3 mb-3">
      {#if doc.extracted_from}
        <p class="m-0 text-xs text-gray-400 break-all flex-1 min-w-0">Extracted from <button class="bg-transparent border-none text-(--color-accent) cursor-pointer font-[inherit] p-0 no-underline hover:underline" onclick={() => onNavigateDoc(doc!.extracted_from!.doc_id, index)}>{doc.extracted_from.display_path}</button></p>
      {:else}
        <p class="m-0 text-xs text-gray-400 break-all flex-1 min-w-0">{index}/{doc.display_path}</p>
      {/if}
      <button class="shrink-0 text-xs font-[inherit] text-(--color-accent) bg-transparent cursor-pointer border border-(--color-accent) px-2 py-1 rounded hover:bg-(--color-accent) hover:text-white" onclick={() => { downloadError = ""; downloadDocument(docId, index).catch((err) => { downloadError = err.message || "Download failed"; }); }}>Download original</button>
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
            <a class="text-(--color-accent) no-underline hover:underline" href={facetSearchHref("Email Addresses", extractEmail(v))} onclick={handleFacetClick}>{v}</a>
          {/each}
        {:else}
          <a class="text-(--color-accent) no-underline hover:underline" href={facetSearchHref("Email Addresses", extractEmail(entry.value))} onclick={handleFacetClick}>{entry.value}</a>
        {/if}
      {:else if entry.facetLabel && !Array.isArray(entry.value)}
        <a class="text-(--color-accent) no-underline hover:underline" href={facetSearchHref(entry.facetLabel, entry.value)} onclick={handleFacetClick}>{entry.value}</a>
      {:else if entry.facetLabel && Array.isArray(entry.value)}
        {#each entry.value as v, i}
          {#if i > 0}, {/if}
          <a class="text-(--color-accent) no-underline hover:underline" href={facetSearchHref(entry.facetLabel, v)} onclick={handleFacetClick}>{v}</a>
        {/each}
      {:else}
        {displayValue(entry.value)}
      {/if}
    {/snippet}

    {#if downloadError}
      <div class="bg-red-50 text-red-600 p-3 rounded my-3 text-sm">{downloadError}</div>
    {/if}

    <div class="bg-white rounded-md p-3 my-3 shadow-sm">
      <h3 class="m-0 mb-2 text-sm text-gray-500">Metadata</h3>
      <div class="max-h-[300px] overflow-y-auto">
        <table class="w-full border-collapse">
          <tbody>
            {#each metaEntries.priority as entry}
              <tr>
                <td class="p-1.5 border-b border-gray-100 text-sm align-top font-semibold whitespace-nowrap w-[130px] text-gray-500">{entry.display}</td>
                <td class="p-1.5 border-b border-gray-100 text-sm align-top">{@render metaValue(entry)}</td>
              </tr>
            {/each}
            {#if metaEntries.extra.length > 0}
              <tr>
                <td colspan="2" class="p-1.5">
                  <button class="bg-transparent border-none text-indigo-500 text-xs cursor-pointer py-1 px-0 hover:underline" onclick={() => (showAllMeta = !showAllMeta)}>
                    {showAllMeta ? "Hide" : "Show"} {metaEntries.extra.length} more fields
                  </button>
                </td>
              </tr>
              {#if showAllMeta}
                {#each metaEntries.extra as entry}
                  <tr>
                    <td class="p-1.5 border-b border-gray-100 text-sm align-top font-semibold whitespace-nowrap w-[130px] text-gray-500">{entry.display}</td>
                    <td class="p-1.5 border-b border-gray-100 text-sm align-top">{@render metaValue(entry)}</td>
                  </tr>
                {/each}
              {/if}
            {/if}
          </tbody>
        </table>
      </div>
    </div>

    {#if doc.attachments.length > 0}
      <div class="bg-white rounded-md p-3 my-3 shadow-sm">
        <h3 class="m-0 mb-1.5 text-sm text-gray-500">Attachments</h3>
        <ul class="list-none m-0 p-0">
          {#each doc.attachments as att}
            <li class="py-1 border-b border-gray-100 text-sm last:border-b-0"><button class="bg-transparent border-none text-(--color-accent) cursor-pointer font-[inherit] p-0 no-underline hover:underline" onclick={() => onNavigateDoc(att.doc_id, index)}>{att.display_path.split("/").pop()}</button></li>
          {/each}
        </ul>
      </div>
    {/if}

    <div class="bg-white rounded-md p-3 my-3 shadow-sm">
      <h3 class="m-0 mb-2 text-sm text-gray-500">Content</h3>
      <pre class="whitespace-pre-wrap break-words text-sm leading-relaxed m-0">{@html contentHtml}</pre>
    </div>
  {/if}
</div>
