<script lang="ts">
  import { fetchPreviewBlob } from "../lib/api";
  import {
    getDocument as getPdfDocument,
    GlobalWorkerOptions,
    type PDFDocumentProxy,
    type PDFPageProxy,
  } from "pdfjs-dist";
  import PdfWorkerUrl from "pdfjs-dist/build/pdf.worker.min.mjs?url";

  GlobalWorkerOptions.workerSrc = PdfWorkerUrl;

  let {
    docId,
    index = "",
  }: {
    docId: string;
    index: string;
  } = $props();

  let loading = $state(true);
  let error = $state("");
  let totalPages = $state(0);
  let containerEl = $state<HTMLDivElement | null>(null);

  // Track the PDF document and rendered pages for cleanup.
  let pdfDoc: PDFDocumentProxy | null = null;
  let renderedPages = new Map<number, HTMLCanvasElement>();
  let observer: IntersectionObserver | null = null;

  const SCALE = 1.5;

  async function renderPage(page: PDFPageProxy, canvas: HTMLCanvasElement): Promise<void> {
    const viewport = page.getViewport({ scale: SCALE });
    canvas.width = viewport.width;
    canvas.height = viewport.height;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    await page.render({ canvasContext: ctx, viewport, canvas }).promise;
  }

  $effect(() => {
    loading = true;
    error = "";
    totalPages = 0;
    let cancelled = false;

    // Clean up previous state.
    cleanup();

    (async () => {
      try {
        const blob = await fetchPreviewBlob(docId, index);
        if (cancelled) return;

        const arrayBuffer = await blob.arrayBuffer();
        if (cancelled) return;

        const pdf = await getPdfDocument({
          data: arrayBuffer,
          disableAutoFetch: true,
          disableStream: true,
          isEvalSupported: false,
        }).promise;

        if (cancelled) {
          pdf.destroy();
          return;
        }

        pdfDoc = pdf;
        totalPages = pdf.numPages;
        loading = false;

        // Wait for container to be in the DOM.
        await new Promise((r) => requestAnimationFrame(r));
        if (cancelled || !containerEl) return;

        // Create placeholder divs for each page and observe them.
        const pageEls: HTMLDivElement[] = [];
        for (let i = 1; i <= pdf.numPages; i++) {
          const pageDiv = document.createElement("div");
          pageDiv.className = "pdf-page mb-2 flex justify-center";
          pageDiv.dataset.page = String(i);

          // Create canvas for this page.
          const canvas = document.createElement("canvas");
          canvas.className = "max-w-full h-auto";
          pageDiv.appendChild(canvas);
          containerEl.appendChild(pageDiv);
          pageEls.push(pageDiv);
          renderedPages.set(i, canvas);
        }

        // Use IntersectionObserver for lazy rendering.
        const rendered = new Set<number>();
        observer = new IntersectionObserver(
          (entries) => {
            for (const entry of entries) {
              if (!entry.isIntersecting) continue;
              const pageNum = parseInt((entry.target as HTMLDivElement).dataset.page || "0");
              if (pageNum < 1 || rendered.has(pageNum)) continue;
              rendered.add(pageNum);

              const canvas = renderedPages.get(pageNum);
              if (!canvas || !pdfDoc) continue;

              pdfDoc.getPage(pageNum).then((page) => {
                if (cancelled) return;
                renderPage(page, canvas);
              });
            }
          },
          { root: null, rootMargin: "200px 0px" },
        );

        for (const el of pageEls) {
          observer.observe(el);
        }
      } catch (err: unknown) {
        if (!cancelled) {
          error = err instanceof Error ? err.message : "Failed to load PDF";
          loading = false;
        }
      }
    })();

    return () => {
      cancelled = true;
      cleanup();
    };
  });

  function cleanup(): void {
    if (observer) {
      observer.disconnect();
      observer = null;
    }
    if (pdfDoc) {
      pdfDoc.destroy();
      pdfDoc = null;
    }
    renderedPages.clear();
    // Clear page containers from the DOM.
    if (containerEl) {
      containerEl.innerHTML = "";
    }
  }
</script>

{#if loading}
  <div class="flex items-center justify-center py-12 text-gray-400 text-sm">Loading PDF...</div>
{:else if error}
  <div class="bg-red-50 text-red-600 p-3 rounded text-sm">{error}</div>
{:else}
  <div class="text-xs text-gray-400 mb-2 text-center">{totalPages} page{totalPages === 1 ? "" : "s"}</div>
{/if}
<div bind:this={containerEl} class="bg-gray-100 rounded"></div>
