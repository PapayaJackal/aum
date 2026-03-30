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
  let iframeEl = $state<HTMLIFrameElement | null>(null);
  let resizeObserver: ResizeObserver | null = null;

  function syncIframeHeight() {
    if (!iframeEl?.contentDocument) return;
    const h = iframeEl.contentDocument.documentElement.scrollHeight;
    if (h > 0) iframeEl.style.height = h + "px";
  }

  function handleLoad() {
    if (!iframeEl?.contentDocument) return;

    // Block link navigation while keeping right-click "Copy link address" functional.
    iframeEl.contentDocument.addEventListener("click", (e: MouseEvent) => {
      if ((e.target as Element)?.closest("a")) e.preventDefault();
    });

    // Observe content size changes so the iframe always fits its content.
    resizeObserver?.disconnect();
    resizeObserver = new ResizeObserver(syncIframeHeight);
    resizeObserver.observe(iframeEl.contentDocument.documentElement);

    syncIframeHeight();
  }

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
      resizeObserver?.disconnect();
      resizeObserver = null;
    };
  });
</script>

{#if loading}
  <div class="flex items-center justify-center py-12 text-gray-400 text-sm">Loading preview...</div>
{:else if error}
  <div class="bg-red-50 text-red-600 p-3 rounded text-sm">{error}</div>
{:else if sanitizedHtml}
  <iframe
    bind:this={iframeEl}
    srcdoc={sanitizedHtml}
    sandbox="allow-same-origin"
    class="w-full border-none rounded bg-white"
    style="min-height: 200px; overflow: hidden;"
    title="Document preview"
    onload={handleLoad}
  ></iframe>
{/if}
