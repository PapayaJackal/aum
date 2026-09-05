<script lang="ts">
  import { fetchPreviewBlob } from "../lib/api";
  import PreviewStatus from "./PreviewStatus.svelte";
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
  // Tracks whether painted pages are on screen (including the outgoing document's).
  let renderedPageCount = $state(0);
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
    const id = docId;
    const idx = index;
    loading = true;
    error = "";
    let cancelled = false;

    // Release the previous document, but leave its already-painted canvases in
    // the DOM: they stay on screen until the new pages are ready to replace
    // them, so switching documents never blanks the preview.
    releaseDocument();

    (async () => {
      try {
        const blob = await fetchPreviewBlob(id, idx);
        if (cancelled) return;

        const arrayBuffer = await blob.arrayBuffer();
        if (cancelled) return;

        const pdf = await getPdfDocument({
          data: arrayBuffer,
          disableAutoFetch: true,
          disableStream: true,
        }).promise;

        if (cancelled) {
          void pdf.loadingTask.destroy();
          return;
        }

        pdfDoc = pdf;
        totalPages = pdf.numPages;
        loading = false;

        // Wait for container to be in the DOM.
        await new Promise((r) => requestAnimationFrame(r));
        if (cancelled || !containerEl) return;

        // Now that the replacement is ready, drop the outgoing pages.
        containerEl.innerHTML = "";
        renderedPages.clear();
        renderedPageCount = 0;

        // Create placeholder divs for each page and observe them.
        const pageEls: HTMLDivElement[] = [];
        for (let i = 1; i <= pdf.numPages; i++) {
          const pageDiv = document.createElement("div");
          pageDiv.className = "pdf-page flex justify-center";
          pageDiv.dataset.page = String(i);

          // Create canvas for this page.
          const canvas = document.createElement("canvas");
          canvas.className = "h-auto max-w-full rounded-sm shadow-[0_2px_10px_-6px_oklch(0_0_0/0.5)]";
          pageDiv.appendChild(canvas);
          containerEl.appendChild(pageDiv);
          pageEls.push(pageDiv);
          renderedPages.set(i, canvas);
        }
        renderedPageCount = pageEls.length;

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
          totalPages = 0;
          containerEl?.replaceChildren();
          renderedPages.clear();
          renderedPageCount = 0;
        }
      }
    })();

    return () => {
      cancelled = true;
      releaseDocument();
    };
  });

  // Canvases are only torn out of the DOM when the preview itself unmounts;
  // a document switch replaces them in place once the new pages exist.
  $effect(() => () => cleanup());

  /** Tear down the pdf.js document and observer, leaving rendered canvases alone. */
  function releaseDocument(): void {
    if (observer) {
      observer.disconnect();
      observer = null;
    }
    if (pdfDoc) {
      void pdfDoc.loadingTask.destroy();
      pdfDoc = null;
    }
  }

  function cleanup(): void {
    releaseDocument();
    renderedPages.clear();
    renderedPageCount = 0;
    // Clear page containers from the DOM.
    if (containerEl) {
      containerEl.innerHTML = "";
    }
  }
</script>

<PreviewStatus {loading} {error} label="PDF" shape="page" hasContent={renderedPageCount > 0} />

{#if !error && renderedPageCount > 0}
  <p class="m-0 mb-2 text-center font-mono text-[0.7rem] tracking-[0.16em] uppercase text-muted-foreground">
    {totalPages} page{totalPages === 1 ? "" : "s"}
  </p>
{/if}
<div bind:this={containerEl} class="flex flex-col gap-2 rounded-md bg-muted/50 [&:not(:empty)]:p-2"></div>
