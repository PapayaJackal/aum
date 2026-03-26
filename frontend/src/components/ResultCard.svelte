<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import { searchState } from "../lib/searchState.svelte";
  import { sanitizeHighlight } from "../lib/highlight";

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

  let fileType = $derived(result.metadata["File Type"] || "");
  let isSelected = $derived(searchState.selectedDocId === result.doc_id);

  function handleClick() {
    searchState.selectedDocId = result.doc_id;
    searchState.selectedDocIndex = index;
  }
</script>

<button type="button" class="card" class:selected={isSelected} onclick={handleClick}>
  <div class="card-header">
    {#if hasPathHighlight}
      <span class="filename">{@html hlFilename}</span>
    {:else}
      <span class="filename">{filename}</span>
    {/if}
    <span class="score">{result.score.toFixed(3)}</span>
  </div>

  <p class="snippet">{@html snippet}</p>

  <div class="card-footer">
    {#if hasPathHighlight}
      <span class="path" title={index + "/" + result.display_path}>{@html index + "/" + hlDirPart + hlFilename}</span>
    {:else}
      <span class="path" title={index + "/" + result.display_path}>{index}/{dirPart}{filename}</span>
    {/if}
    <div class="badges">
      {#if multiIndex && index}
        <span class="badge index-badge">{index}</span>
      {/if}
      {#if fileType}
        <span class="badge">{fileType}</span>
      {/if}
    </div>
  </div>
</button>

<style>
  .card {
    display: block;
    width: 100%;
    text-align: left;
    font: inherit;
    background: white;
    padding: 1rem;
    border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
    text-decoration: none;
    color: inherit;
    transition: box-shadow 0.15s, border-color 0.15s;
    border: 2px solid transparent;
    cursor: pointer;
  }

  .card:hover {
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
  }

  .card.selected {
    border-color: #4a7cf7;
    background: #f0f4ff;
  }

  .card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.5rem;
  }

  .filename {
    font-weight: 600;
    color: #1a1a2e;
  }

  .score {
    font-size: 0.8rem;
    color: #999;
    font-family: monospace;
  }

  .snippet {
    font-size: 0.9rem;
    line-height: 1.5;
    color: #555;
    margin: 0 0 0.5rem;
  }

  .snippet :global(mark),
  .filename :global(mark),
  .path :global(mark) {
    background: #fff3b0;
    padding: 0.1em;
    border-radius: 2px;
  }

  .card-footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.8rem;
    color: #999;
    gap: 0.5rem;
  }

  .path {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
    font-family: monospace;
    font-size: 0.78rem;
    color: #777;
  }

  .badges {
    display: flex;
    gap: 0.35rem;
    flex-shrink: 0;
  }

  .badge {
    background: #eef;
    color: #55a;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    font-size: 0.75rem;
    flex-shrink: 0;
  }

  .index-badge {
    background: #e8f0fe;
    color: #1a73e8;
  }
</style>
