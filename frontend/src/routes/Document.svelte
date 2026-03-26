<script lang="ts">
  import { getDocument, downloadUrl, type DocumentDetail } from "../lib/api";

  let { docId, index = "", qs = "" }: { docId: string; index: string; qs: string } = $props();

  let doc = $state<DocumentDetail | null>(null);
  let loading = $state(true);
  let error = $state("");

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
    <a class="download-btn" href={downloadUrl(docId, index)} download>Download original</a>
  </div>

  <div class="meta-table">
    <h3>Metadata</h3>
    <table>
      <tbody>
        {#each Object.entries(doc.metadata) as [key, value]}
          <tr>
            <td class="meta-key">{key}</td>
            <td>{value}</td>
          </tr>
        {/each}
      </tbody>
    </table>
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
    color: #4a7cf7;
    text-decoration: none;
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
