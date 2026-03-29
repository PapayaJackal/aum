<script lang="ts">
  import { fetchPreviewBlob } from "../lib/api";
  import { sanitizeHtmlForPreview } from "../lib/sanitize";

  let {
    docId,
    index = "",
  }: {
    docId: string;
    index: string;
  } = $props();

  let sanitizedHtml = $state<string | null>(null);
  let loading = $state(true);
  let error = $state("");

  $effect(() => {
    loading = true;
    error = "";
    sanitizedHtml = null;
    let cancelled = false;

    fetchPreviewBlob(docId, index)
      .then(async (blob) => {
        if (cancelled) return;
        const text = await blob.text();
        if (cancelled) return;
        sanitizedHtml = sanitizeHtmlForPreview(text);
      })
      .catch((err) => {
        if (!cancelled) error = err instanceof Error ? err.message : "Failed to load preview";
      })
      .finally(() => {
        if (!cancelled) loading = false;
      });

    return () => {
      cancelled = true;
    };
  });
</script>

{#if loading}
  <div class="flex items-center justify-center py-12 text-gray-400 text-sm">Loading preview...</div>
{:else if error}
  <div class="bg-red-50 text-red-600 p-3 rounded text-sm">{error}</div>
{:else if sanitizedHtml}
  <iframe
    srcdoc={sanitizedHtml}
    sandbox="allow-scripts"
    class="w-full border-none rounded bg-white"
    style="min-height: 400px; height: 70vh;"
    title="Document preview"
  ></iframe>
{/if}
