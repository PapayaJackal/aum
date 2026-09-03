<script lang="ts">
  import {
    getDocument,
    downloadDocument,
    isPreviewable,
    getContentType,
    type DocumentDetail,
    type ThreadMessage,
  } from "../lib/api";
  import { mimeAlias } from "../lib/mime";
  import { highlightTerms } from "../lib/highlight";
  import HtmlPreview from "../components/HtmlPreview.svelte";
  import ImagePreview from "../components/ImagePreview.svelte";
  import PdfPreview from "../components/PdfPreview.svelte";
  import { Button } from "$lib/components/ui/button/index.js";
  import { Skeleton } from "$lib/components/ui/skeleton/index.js";
  import * as Tooltip from "$lib/components/ui/tooltip/index.js";
  import type { Snippet } from "svelte";
  import DownloadIcon from "@lucide/svelte/icons/download";
  import Maximize2Icon from "@lucide/svelte/icons/maximize-2";
  import Minimize2Icon from "@lucide/svelte/icons/minimize-2";
  import XIcon from "@lucide/svelte/icons/x";

  let {
    docId,
    index = "",
    highlightQuery = "",
    onClose,
    onNavigateDoc,
    onToggleFullscreen,
    previewFullscreen = false,
  }: {
    docId: string;
    index: string;
    highlightQuery?: string;
    onClose: () => void;
    onNavigateDoc: (docId: string, index: string) => void;
    onToggleFullscreen?: () => void;
    previewFullscreen?: boolean;
  } = $props();

  let doc = $state<DocumentDetail | null>(null);
  let loading = $state(true);
  let error = $state("");
  let downloadError = $state("");
  let showAllMeta = $state(false);
  let showRichPreview = $state(true);

  // Human-readable aliases for metadata keys returned by the Rust backend (snake_case).
  const KEY_ALIASES: Record<string, string> = {
    content_type: "Content Type",
    creator: "Creator",
    created: "Created",
    modified: "Modified",
    file_size: "File Size",
    email_subject: "Subject",
    email_from: "From",
    email_to: "To",
    email_cc: "CC",
    email_bcc: "BCC",
    message_id: "Message ID",
    document_type: "Document Type",
  };

  // Keys whose display name should appear in the priority section (default).
  const DEFAULT_PRIORITY = new Set([
    "Creator",
    "From",
    "To",
    "CC",
    "Created",
    "Modified",
    "Content Type",
    "File Size",
    "Subject",
  ]);

  // For email documents, show only these fields in priority, in this order.
  const EMAIL_PRIORITY_ORDER = ["From", "To", "CC", "BCC", "Subject", "Created", "File Size"];
  const EMAIL_PRIORITY = new Set(EMAIL_PRIORITY_ORDER);

  // Internal keys to hide from the metadata table.
  const HIDDEN_PREFIXES: string[] = [];
  // Exact keys to hide (internal faceting fields, email threading, etc.).
  const HIDDEN_EXACT = new Set(["email_addresses", "created_year", "in_reply_to", "references"]);

  // Keys whose values are email addresses and should link to the Email Addresses facet.
  const EMAIL_KEYS = new Set(["email_from", "email_to", "email_cc", "email_bcc"]);

  // Maps metadata keys to their facet labels for clickable filter links.
  const FACET_LABEL_MAP: Record<string, string> = {
    content_type: "File Type",
    creator: "Creator",
  };

  function isHidden(key: string): boolean {
    if (HIDDEN_EXACT.has(key)) return true;
    if (HIDDEN_PREFIXES.length > 0) return HIDDEN_PREFIXES.some((p) => key.startsWith(p));
    return false;
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
    params.set("q", highlightQuery || "*");
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
    displayFn?: (v: string) => string;
  };

  let isEmailDoc = $derived(doc?.metadata["content_type"]?.toString().startsWith("message/rfc822") === true);

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
      if (d === "Created" && typeof value === "string") {
        createdValue = value;
        break;
      }
    }

    for (const [key, value] of Object.entries(doc.metadata)) {
      if (isHidden(key)) continue;
      const display = displayKey(key);
      if (seen.has(display)) continue;
      // Hide Modified if it matches Created (non-email docs only).
      if (!isEmailDoc && display === "Modified" && typeof value === "string" && value === createdValue) continue;
      seen.add(display);
      const facetLabel = FACET_LABEL_MAP[key];
      const isEmail = EMAIL_KEYS.has(key);
      const isFileSize = display === "File Size";
      const isDate = DATE_DISPLAY_KEYS.has(display);
      const displayFn = key === "content_type" ? mimeAlias : undefined;
      const entry: MetaEntry = { key, display, value, facetLabel, isEmail, isFileSize, isDate, displayFn };
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

  // The previous document stays on screen while the next one is fetched, so a
  // fast swap never blanks the panel. Only the very first load shows skeletons.
  $effect(() => {
    const id = docId;
    const idx = index;
    let cancelled = false;
    loading = true;
    error = "";
    getDocument(id, idx)
      .then((d) => {
        if (cancelled) return;
        doc = d;
        showAllMeta = false;
        showRichPreview = true;
      })
      .catch((err) => {
        if (!cancelled) error = err.message;
      })
      .finally(() => {
        if (!cancelled) loading = false;
      });

    return () => {
      cancelled = true;
    };
  });

  // A swap over existing content: dim it and run the header bar instead of
  // tearing the panel down to skeletons.
  let refreshing = $derived(loading && !!doc && !error);

  let previewable = $derived(doc ? isPreviewable(doc.metadata) : false);
  let contentType = $derived(doc ? getContentType(doc.metadata) : "");
  let isImage = $derived(contentType.startsWith("image/"));
  let isPdf = $derived(contentType === "application/pdf");
  let isHtml = $derived(contentType === "text/html" || contentType === "message/rfc822");

  let contentHtml = $derived(doc ? highlightTerms(doc.content, highlightQuery) : "");

  /** Unified thread: all thread messages plus the current document, sorted by date. */
  let unifiedThread = $derived.by<Array<ThreadMessage & { isCurrent?: boolean }>>(() => {
    if (!doc || !doc.thread.length) return [];
    const docDate = typeof doc.metadata["created"] === "string" ? (doc.metadata["created"] as string) : "";
    const currentEntry: ThreadMessage & { isCurrent: boolean } = {
      doc_id: doc.doc_id,
      display_path: doc.display_path,
      subject: (doc.metadata["email_subject"] ?? "") as string,
      sender: (Array.isArray(doc.metadata["email_from"])
        ? doc.metadata["email_from"][0]
        : (doc.metadata["email_from"] ?? "")) as string,
      date: docDate,
      snippet: doc.content?.slice(0, 200) ?? "",
      isCurrent: true,
    };
    const all = [...doc.thread.map((m) => ({ ...m, isCurrent: false as const })), currentEntry];
    all.sort((a, b) => a.date.localeCompare(b.date));
    return all;
  });

  let threadContainerEl = $state<HTMLDivElement | null>(null);

  // Scroll the thread container to the current message whenever the thread loads.
  $effect(() => {
    if (unifiedThread.length > 0 && threadContainerEl) {
      // Use tick to wait for DOM update.
      const el = threadContainerEl;
      requestAnimationFrame(() => {
        const current = el.querySelector<HTMLElement>("[data-current-thread]");
        // Move only the thread box: scrollIntoView would drag every scrollable
        // ancestor, including the preview pane and the window, along with it.
        if (current) {
          el.scrollTop = current.offsetTop - (el.clientHeight - current.offsetHeight) / 2;
        }
      });
    }
  });
</script>

{#snippet iconAction(label: string, onclick: () => void, icon: Snippet)}
  <Tooltip.Root>
    <Tooltip.Trigger>
      {#snippet child({ props })}
        <Button {...props} variant="ghost" size="icon" class="size-7 shrink-0 text-muted-foreground" {onclick}>
          {@render icon()}
          <span class="sr-only">{label}</span>
        </Button>
      {/snippet}
    </Tooltip.Trigger>
    <Tooltip.Content>{label}</Tooltip.Content>
  </Tooltip.Root>
{/snippet}

<!--
  Sections are ruled off from one another rather than boxed: nothing in the
  panel is an island, so there are no card edges to pad away from.
-->
{#snippet panel(title: string, body: Snippet, action?: Snippet)}
  <section class="border-t border-border first:border-t-0">
    <header class="flex items-center justify-between gap-2 px-4 pt-3 pb-1.5">
      <h3 class="m-0 font-mono text-[0.7rem] tracking-[0.18em] uppercase text-muted-foreground">{title}</h3>
      {#if action}{@render action()}{/if}
    </header>
    <div class="px-4 pt-1 pb-3">{@render body()}</div>
  </section>
{/snippet}

<Tooltip.Provider delayDuration={300}>
  <div
    class="sticky top-0 z-1 flex items-center gap-1.5 border-b border-border/70 bg-card/95 px-4 py-2.5 backdrop-blur-sm relative"
  >
    {#if refreshing}
      <span aria-hidden="true" class="loading-bar absolute inset-x-0 -bottom-px h-0.5"></span>
    {/if}
    <h2
      class="m-0 min-w-0 flex-1 overflow-hidden font-display text-[0.975rem] font-semibold text-ellipsis whitespace-nowrap"
      title={doc?.display_path}
    >
      {#if doc}{doc.display_path.split("/").pop()}{:else}Document{/if}
    </h2>
    {#if onToggleFullscreen}
      {#snippet fullscreenIcon()}
        {#if previewFullscreen}
          <Minimize2Icon class="size-4" />
        {:else}
          <Maximize2Icon class="size-4" />
        {/if}
      {/snippet}
      {@render iconAction(previewFullscreen ? "Exit full screen" : "Full screen", onToggleFullscreen, fullscreenIcon)}
    {/if}
    {#snippet closeIcon()}
      <XIcon class="size-4" />
    {/snippet}
    {@render iconAction("Close", onClose, closeIcon)}
  </div>

  <div
    class="flex flex-col transition-opacity duration-200 {refreshing
      ? 'pointer-events-none opacity-50'
      : ''}"
    aria-busy={loading}
  >
    {#if loading && !doc}
      <div class="skeleton-delayed flex flex-col gap-4 px-4 py-3" aria-busy="true" aria-label="Loading document">
        <Skeleton class="h-3.5 w-3/5" />
        <Skeleton class="h-28 w-full rounded-md" />
        <Skeleton class="h-56 w-full rounded-md" />
      </div>
    {:else if error}
      <div class="border-b border-destructive/25 bg-destructive/10 px-4 py-2 text-sm text-destructive" role="alert">
        {error}
      </div>
    {:else if doc}
      <div class="flex items-start gap-3 px-4 py-3">
        {#if doc.extracted_from}
          <p class="m-0 min-w-0 flex-1 font-mono text-[0.7rem] leading-relaxed break-all text-muted-foreground">
            Extracted from
            <button
              class="cursor-pointer border-none bg-transparent p-0 font-[inherit] text-primary underline-offset-2 hover:underline"
              onclick={() => onNavigateDoc(doc!.extracted_from!.doc_id, index)}
              >{doc.extracted_from.display_path}</button
            >
          </p>
        {:else}
          <p class="m-0 min-w-0 flex-1 font-mono text-[0.7rem] leading-relaxed break-all text-muted-foreground">
            {index}/{doc.display_path}
          </p>
        {/if}
        <Button
          variant="outline"
          size="sm"
          class="h-7 shrink-0 gap-1.5 px-2 text-xs"
          onclick={() => {
            downloadError = "";
            downloadDocument(docId, index).catch((err) => {
              downloadError = err.message || "Download failed";
            });
          }}
        >
          <DownloadIcon class="size-3.5" />Original
        </Button>
      </div>

      {#if downloadError}
        <div
          class="rounded-md border border-destructive/25 bg-destructive/10 px-3 py-2 text-sm text-destructive"
          role="alert"
        >
          {downloadError}
        </div>
      {/if}

      {#snippet metaValue(entry: MetaEntry)}
        {#if entry.isDate}
          {formatLocalDate(entry.value as string)}
        {:else if entry.isFileSize}
          {humanFileSize(entry.value as string)}
        {:else if entry.isEmail}
          {#if Array.isArray(entry.value)}
            {#each entry.value as v, i}
              {#if i > 0},
              {/if}
              <a
                class="text-primary underline-offset-2 hover:underline"
                href={facetSearchHref("Email Addresses", extractEmail(v))}
                onclick={handleFacetClick}>{v}</a
              >
            {/each}
          {:else}
            <a
              class="text-primary underline-offset-2 hover:underline"
              href={facetSearchHref("Email Addresses", extractEmail(entry.value))}
              onclick={handleFacetClick}>{entry.value}</a
            >
          {/if}
        {:else if entry.facetLabel && !Array.isArray(entry.value)}
          <a
            class="text-primary underline-offset-2 hover:underline"
            href={facetSearchHref(entry.facetLabel, entry.value)}
            onclick={handleFacetClick}>{entry.displayFn ? entry.displayFn(entry.value) : entry.value}</a
          >
        {:else if entry.facetLabel && Array.isArray(entry.value)}
          {#each entry.value as v, i}
            {#if i > 0},
            {/if}
            <a
              class="text-primary underline-offset-2 hover:underline"
              href={facetSearchHref(entry.facetLabel, v)}
              onclick={handleFacetClick}>{entry.displayFn ? entry.displayFn(v) : v}</a
            >
          {/each}
        {:else}
          {entry.displayFn ? entry.displayFn(displayValue(entry.value)) : displayValue(entry.value)}
        {/if}
      {/snippet}

      {#snippet metaRow(entry: MetaEntry)}
        <tr class="border-b border-border/40 last:border-b-0">
          <th
            scope="row"
            class="w-[7.5rem] py-1.5 pr-3 text-left align-top font-mono text-[0.68rem] font-medium tracking-[0.1em] uppercase text-muted-foreground"
            >{entry.display}</th
          >
          <td class="py-1.5 align-top text-sm break-words">{@render metaValue(entry)}</td>
        </tr>
      {/snippet}

      {#snippet metadataBody()}
        <div class="max-h-[300px] overflow-y-auto">
          <table class="w-full border-collapse">
            <tbody>
              {#each metaEntries.priority as entry}
                {@render metaRow(entry)}
              {/each}
              {#if showAllMeta}
                {#each metaEntries.extra as entry}
                  {@render metaRow(entry)}
                {/each}
              {/if}
            </tbody>
          </table>
        </div>
      {/snippet}

      {#snippet metadataAction()}
        {#if metaEntries.extra.length > 0}
          <Button variant="link" size="sm" class="h-auto p-0 text-xs" onclick={() => (showAllMeta = !showAllMeta)}>
            {showAllMeta ? "Fewer" : `${metaEntries.extra.length} more`} fields
          </Button>
        {/if}
      {/snippet}

      {@render panel("Metadata", metadataBody, metadataAction)}

      {#if unifiedThread.length > 0}
        {#snippet threadBody()}
          <div bind:this={threadContainerEl} class="flex max-h-[270px] flex-col gap-1.5 overflow-y-auto">
            {#each unifiedThread as msg}
              {@const current = msg.isCurrent}
              <svelte:element
                this={current ? "div" : "button"}
                {...current
                  ? { "data-current-thread": true }
                  : { type: "button", onclick: () => onNavigateDoc(msg.doc_id, index) }}
                class="w-full rounded-r-sm border-l-2 py-1.5 pr-2 pl-2.5 text-left transition-colors {current
                  ? 'border-l-primary bg-accent/50'
                  : 'cursor-pointer border-l-border bg-muted/50 hover:border-l-primary/50 hover:bg-muted'}"
              >
                <div class="mb-0.5 flex items-baseline gap-2">
                  <span class="truncate text-xs font-semibold {current ? 'text-foreground' : 'text-foreground/80'}"
                    >{msg.sender || "Unknown"}</span
                  >
                  <span class="shrink-0 font-mono text-[0.65rem] tabular-nums text-muted-foreground"
                    >{msg.date ? formatLocalDate(msg.date) : ""}</span
                  >
                </div>
                {#if msg.subject}
                  <div
                    class="mb-0.5 truncate text-xs {current ? 'font-medium text-foreground/90' : 'text-foreground/70'}"
                  >
                    {msg.subject}
                  </div>
                {/if}
                <div class="line-clamp-2 text-xs text-muted-foreground">{msg.snippet}</div>
              </svelte:element>
            {/each}
          </div>
        {/snippet}
        {@render panel(`Thread (${unifiedThread.length})`, threadBody)}
      {/if}

      {#if doc.attachments.length > 0}
        {#snippet attachmentsBody()}
          <ul class="m-0 list-none p-0">
            {#each doc!.attachments as att}
              <li class="border-b border-border/40 py-1 last:border-b-0">
                <button
                  class="cursor-pointer border-none bg-transparent p-0 text-left font-[inherit] text-sm text-primary underline-offset-2 hover:underline"
                  onclick={() => onNavigateDoc(att.doc_id, index)}>{att.display_path.split("/").pop()}</button
                >
              </li>
            {/each}
          </ul>
        {/snippet}
        {@render panel("Attachments", attachmentsBody)}
      {/if}

      {#snippet contentBody()}
        {#if previewable && showRichPreview}
          {#if isImage}
            <ImagePreview {docId} {index} />
          {:else if isPdf}
            <PdfPreview {docId} {index} />
          {:else if isHtml}
            <HtmlPreview {docId} {index} />
          {/if}
        {:else}
          <pre class="m-0 font-sans text-sm leading-7 break-words whitespace-pre-wrap">{@html contentHtml}</pre>
        {/if}
      {/snippet}

      {#snippet contentAction()}
        {#if previewable && doc?.content}
          <Button
            variant="link"
            size="sm"
            class="h-auto p-0 text-xs"
            onclick={() => (showRichPreview = !showRichPreview)}
          >
            {showRichPreview ? "Extracted text" : "Preview"}
          </Button>
        {/if}
      {/snippet}

      {@render panel("Content", contentBody, contentAction)}
    {/if}
  </div>
</Tooltip.Provider>
