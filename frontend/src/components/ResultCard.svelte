<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import { searchState } from "../lib/searchState.svelte";
  import { sanitizeHighlight, escapeHtml } from "../lib/highlight";

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

  let fileType = $derived(result.metadata["File Type"] || "");
  let isSelected = $derived(searchState.selectedDocId === result.doc_id);

  function handleClick() {
    searchState.selectedDocId = result.doc_id;
    searchState.selectedDocIndex = index;
  }
</script>

<button
  type="button"
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
    <span class="text-xs text-gray-400 font-mono">{result.score.toFixed(3)}</span>
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
      {#if fileType}
        <span class="bg-indigo-50 text-indigo-600 px-2 py-0.5 rounded text-xs shrink-0">{fileType}</span>
      {/if}
    </div>
  </div>
</button>
