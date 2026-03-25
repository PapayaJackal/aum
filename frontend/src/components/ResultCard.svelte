<script lang="ts">
  import type { SearchResult } from "../lib/api";

  let { result, index = "" }: { result: SearchResult; index: string } = $props();

  let filename = $derived(result.source_path.split("/").pop() || result.source_path);
  let contentType = $derived(result.metadata["Content-Type"] || "");
</script>

<a href="#/document/{encodeURIComponent(index)}/{result.doc_id}" class="card">
  <div class="card-header">
    <span class="filename">{filename}</span>
    <span class="score">{result.score.toFixed(3)}</span>
  </div>

  <p class="snippet">{@html result.snippet}</p>

  {#if contentType}
    <div class="card-footer">
      <span class="badge">{contentType}</span>
    </div>
  {/if}
</a>

<style>
  .card {
    display: block;
    background: white;
    padding: 1rem;
    border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
    text-decoration: none;
    color: inherit;
    transition: box-shadow 0.15s;
  }

  .card:hover {
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
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

  .snippet :global(mark) {
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
  }

  .path {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 70%;
  }

  .badge {
    background: #eef;
    color: #55a;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    font-size: 0.75rem;
  }
</style>
