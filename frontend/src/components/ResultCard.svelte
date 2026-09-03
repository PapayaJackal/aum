<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import { searchState } from "../lib/searchState.svelte";
  import { sanitizeHighlight, escapeHtml } from "../lib/highlight";
  import { mimeAlias } from "../lib/mime";
  import { Badge } from "$lib/components/ui/badge/index.js";

  let { result, multiIndex = false }: { result: SearchResult; multiIndex: boolean } = $props();

  let index = $derived(result.index);
  let safeIndex = $derived(escapeHtml(index));

  let parts = $derived(result.display_path.split("/"));
  let filename = $derived(parts[parts.length - 1] || result.display_path);
  let dirPart = $derived(parts.length > 1 ? parts.slice(0, -1).join("/") + "/" : "");

  let snippet = $derived(sanitizeHighlight(result.snippet));

  let hasPathHighlight = $derived(!!result.display_path_highlighted);
  // Protect </mark> closing tags from being split on their "/"
  let hlParts = $derived(
    result.display_path_highlighted
      ? sanitizeHighlight(result.display_path_highlighted)
          .replaceAll("</mark>", "\x00mark\x01")
          .split("/")
          .map((s: string) => s.replaceAll("\x00mark\x01", "</mark>"))
      : [],
  );
  let hlFilename = $derived(hlParts.length > 0 ? hlParts[hlParts.length - 1] : "");
  let hlDirPart = $derived(hlParts.length > 1 ? hlParts.slice(0, -1).join("/") + "/" : "");

  let fileType = $derived(mimeAlias((result.metadata["content_type"] as string) || ""));
  let isSelected = $derived(searchState.selectedDocId === result.doc_id);

  function humanFileSize(bytes: string | number | undefined): string {
    if (bytes == null) return "";
    const n = typeof bytes === "string" ? parseInt(bytes, 10) : bytes;
    if (isNaN(n) || n < 0) return "";
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  }

  let fileSize = $derived(humanFileSize(result.metadata["file_size"] as string | undefined));

  function formatYear(dateStr: string | undefined): string {
    if (!dateStr) return "";
    const year = String(dateStr).slice(0, 4);
    return /^\d{4}$/.test(year) ? year : "";
  }

  let dateLabel = $derived(formatYear(result.metadata["created"] as string | undefined));

  let buttonEl = $state<HTMLButtonElement | null>(null);

  $effect(() => {
    if (isSelected && buttonEl) {
      buttonEl.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  });

  function handleClick() {
    searchState.selectedDocId = result.doc_id;
    searchState.selectedDocIndex = index;
  }
</script>

<button
  type="button"
  bind:this={buttonEl}
  class="group relative block w-full cursor-pointer rounded-md border border-border/70 bg-card p-4 pl-5 text-left font-[inherit] text-inherit shadow-[0_1px_2px_oklch(0_0_0/0.04)] transition-[box-shadow,border-color,transform] duration-150 hover:-translate-y-px hover:shadow-[0_6px_18px_-10px_oklch(0_0_0/0.35)] {isSelected
    ? 'border-primary/45 bg-accent/35'
    : ''}"
  onclick={handleClick}
>
  <!-- Ruled margin, like a card in a catalogue drawer -->
  <span
    aria-hidden="true"
    class="absolute inset-y-2 left-2 w-px rounded-full transition-colors {isSelected
      ? 'bg-primary'
      : 'bg-border group-hover:bg-primary/45'}"
  ></span>

  <div class="mb-1.5 flex items-baseline justify-between gap-3">
    <span class="font-display text-[0.975rem] leading-snug font-semibold text-foreground">
      {#if hasPathHighlight}{@html hlFilename}{:else}{filename}{/if}
    </span>
    {#if dateLabel}
      <span
        class="shrink-0 font-mono text-[0.7rem] tabular-nums text-muted-foreground/80"
        title="Score: {result.score.toFixed(3)}">{dateLabel}</span
      >
    {/if}
  </div>

  <p class="m-0 mb-2.5 text-sm leading-relaxed text-muted-foreground">{@html snippet}</p>

  <div class="flex items-center justify-between gap-2">
    <span
      class="min-w-0 overflow-hidden font-mono text-[0.7rem] text-ellipsis whitespace-nowrap text-muted-foreground/75"
      title={index + "/" + result.display_path}
    >
      {#if hasPathHighlight}{@html safeIndex + "/" + hlDirPart + hlFilename}{:else}{index}/{dirPart}{filename}{/if}
    </span>
    <div class="flex shrink-0 items-center gap-1">
      {#if multiIndex && index}
        <Badge variant="outline" class="border-primary/30 font-mono text-[0.65rem] text-primary">{index}</Badge>
      {/if}
      {#if fileSize}
        <Badge variant="secondary" class="font-mono text-[0.65rem] tabular-nums">{fileSize}</Badge>
      {/if}
      {#if fileType}
        <Badge variant="secondary" class="font-mono text-[0.65rem] tracking-wide uppercase">{fileType}</Badge>
      {/if}
    </div>
  </div>
</button>
