<script lang="ts">
  import { getDocument, type DocumentDetail } from "../lib/api";
  import { searchState } from "../lib/searchState.svelte";
  import { highlightTerms } from "../lib/highlight";

  let { docId }: { docId: string } = $props();

  let doc = $state<DocumentDetail | null>(null);
  let loading = $state(true);
  let error = $state("");

  $effect(() => {
    loading = true;
    getDocument(docId)
      .then((d) => (doc = d))
      .catch((err) => (error = err.message))
      .finally(() => (loading = false));
  });

  let previewHtml = $derived(
    doc
      ? highlightTerms(
          doc.content.slice(0, 2000) + (doc.content.length > 2000 ? "..." : ""),
          searchState.query,
        )
      : "",
  );
</script>

<div class="preview">
  {#if loading}
    <p class="status">Loading...</p>
  {:else if error}
    <p class="status error">{error}</p>
  {:else if doc}
    <div class="preview-header">
      <strong>{doc.display_path}</strong>
      <a href="#/document/{doc.doc_id}" class="expand-link">Full view &rarr;</a>
    </div>
    <pre class="preview-content">{@html previewHtml}</pre>
  {/if}
</div>

<style>
  .preview {
    background: white;
    border-radius: 6px;
    padding: 1rem;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
  }

  .status {
    color: #888;
    font-size: 0.9rem;
  }

  .error {
    color: #c33;
  }

  .preview-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.75rem;
    font-size: 0.9rem;
  }

  .expand-link {
    color: #66a;
    text-decoration: none;
    font-size: 0.85rem;
  }

  .expand-link:hover {
    text-decoration: underline;
  }

  .preview-content {
    white-space: pre-wrap;
    word-wrap: break-word;
    font-size: 0.85rem;
    line-height: 1.5;
    max-height: 400px;
    overflow-y: auto;
    margin: 0;
    color: #555;
  }

  .preview-content :global(mark) {
    background: #fff3b0;
    padding: 0.1em;
    border-radius: 2px;
  }
</style>
