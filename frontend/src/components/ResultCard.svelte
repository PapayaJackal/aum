<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import { searchState } from "../lib/searchState.svelte";
  import { sanitizeHighlight, escapeHtml } from "../lib/highlight";
  import { mimeAlias } from "../lib/mime";

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
  class="block w-full text-left font-[inherit] bg-white p-4 rounded-md shadow-sm no-underline text-inherit border-2 cursor-pointer transition-[box-shadow,border-color] duration-150 hover:shadow-md {isSelected
    ? 'border-(--color-accent) bg-blue-50'
    : 'border-transparent'}"
  onclick={handleClick}
>
  <div class="flex justify-between items-center mb-2">
    {#if hasPathHighlight}
      <span class="font-semibold text-(--color-brand)">{@html hlFilename}</span>
    {:else}
      <span class="font-semibold text-(--color-brand)">{filename}</span>
    {/if}
    {#if dateLabel}
      <span class="text-xs text-gray-400 shrink-0" title="Score: {result.score.toFixed(3)}">{dateLabel}</span>
    {/if}
  </div>

  <p class="text-sm leading-relaxed text-gray-500 m-0 mb-2">{@html snippet}</p>

  <div class="flex justify-between items-center text-xs text-gray-400 gap-2">
    {#if hasPathHighlight}
      <span
        class="overflow-hidden text-ellipsis whitespace-nowrap min-w-0 font-mono text-gray-500"
        title={index + "/" + result.display_path}>{@html safeIndex + "/" + hlDirPart + hlFilename}</span
      >
    {:else}
      <span
        class="overflow-hidden text-ellipsis whitespace-nowrap min-w-0 font-mono text-gray-500"
        title={index + "/" + result.display_path}>{index}/{dirPart}{filename}</span
      >
    {/if}
    <div class="flex gap-1 shrink-0">
      {#if multiIndex && index}
        <span class="bg-blue-50 text-blue-600 px-2 py-0.5 rounded text-xs shrink-0">{index}</span>
      {/if}
      {#if fileSize}
        <span class="bg-gray-100 text-gray-500 px-2 py-0.5 rounded text-xs shrink-0">{fileSize}</span>
      {/if}
      {#if fileType}
        <span class="bg-indigo-50 text-indigo-600 px-2 py-0.5 rounded text-xs shrink-0">{fileType}</span>
      {/if}
    </div>
  </div>
</button>
