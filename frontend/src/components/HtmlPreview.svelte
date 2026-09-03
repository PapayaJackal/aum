<script lang="ts">
  import { fetchPreviewBlob } from "../lib/api";
  import { sanitizeHtmlForPreview } from "../lib/sanitize";
  import PreviewStatus from "./PreviewStatus.svelte";

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
    const id = docId;
    const idx = index;
    loading = true;
    error = "";
    let cancelled = false;

    fetchPreviewBlob(id, idx)
      .then(async (blob) => {
        if (cancelled) return;
        const text = await blob.text();
        if (cancelled) return;
        sanitizedHtml = sanitizeHtmlForPreview(text);
      })
      .catch((err) => {
        if (!cancelled) {
          error = err instanceof Error ? err.message : "Failed to load preview";
          sanitizedHtml = null;
        }
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

<PreviewStatus {loading} {error} label="preview" hasContent={!!sanitizedHtml} />

{#if sanitizedHtml}
  <!-- The document brings its own styling, so the frame keeps a white page
       ground in both themes and supplies only the surrounding border. -->
  <div class="overflow-hidden rounded-md border border-border/70 bg-white">
    <iframe
      bind:this={iframeEl}
      srcdoc={sanitizedHtml}
      sandbox="allow-same-origin"
      class="block w-full border-none"
      style="min-height: 200px; overflow: hidden;"
      title="Document preview"
      onload={handleLoad}
    ></iframe>
  </div>
{/if}
