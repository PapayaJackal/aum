<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import { searchState } from "../lib/searchState.svelte";
  import { sanitizeHighlight } from "../lib/highlight";
  import { mimeAlias } from "../lib/mime";

  let { result, multiIndex = false }: { result: SearchResult; multiIndex: boolean } = $props();

  let index = $derived(result.index);

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

<!--
  A line in a catalogue, not a card: two tight rows — path and snippet — with a
  rule on the left standing in for the selection border.
-->
<button
  type="button"
  bind:this={buttonEl}
  class="group relative block w-full cursor-pointer border-none py-1.5 pr-3 pl-3.5 text-left font-[inherit] text-inherit transition-colors {isSelected
    ? 'bg-accent/40'
    : 'bg-transparent hover:bg-muted/60'}"
  onclick={handleClick}
  title={index + "/" + result.display_path}
>
  <span
    aria-hidden="true"
    class="absolute inset-y-0 left-0 w-0.5 transition-colors {isSelected
      ? 'bg-primary'
      : 'bg-transparent group-hover:bg-primary/40'}"
  ></span>

  <div class="flex items-baseline gap-2">
    <span class="min-w-0 shrink truncate text-sm leading-6 font-medium text-foreground">
      {#if hasPathHighlight}{@html hlFilename}{:else}{filename}{/if}
    </span>
    <span class="min-w-0 flex-1 truncate font-mono text-[0.7rem] text-muted-foreground/70">
      {#if hasPathHighlight}{@html hlDirPart}{:else}{dirPart}{/if}
    </span>
    <span
      class="flex shrink-0 items-baseline gap-1.5 font-mono text-[0.7rem] tabular-nums text-muted-foreground/80"
      title="Score: {result.score.toFixed(3)}"
    >
      {#if multiIndex && index}
        <span class="text-primary/80">{index}</span>
      {/if}
      {#if fileType}
        <span class="tracking-wide uppercase">{fileType}</span>
      {/if}
      {#if fileSize}
        <span>{fileSize}</span>
      {/if}
      {#if dateLabel}
        <span>{dateLabel}</span>
      {/if}
    </span>
  </div>

  <p class="m-0 line-clamp-1 text-[0.8rem] leading-5 text-muted-foreground">{@html snippet}</p>
</button>
