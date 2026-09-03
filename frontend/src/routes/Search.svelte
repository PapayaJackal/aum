<script lang="ts">
  import type { Snippet } from "svelte";
  import { onMount, untrack } from "svelte";
  import { search, listIndices, type IndexInfo } from "../lib/api";
  import { mimeAlias } from "../lib/mime";
  import {
    searchState,
    getSearchQs,
    savePrefs,
    saveIndexSearchType,
    getIndexSearchType,
    saveBaselineFacets,
    restoreBaselineFacets,
    clearBaselineFacets,
  } from "../lib/searchState.svelte";
  import ResultList from "../components/ResultList.svelte";
  import FacetPanel from "../components/FacetPanel.svelte";
  import IndexSelector from "../components/IndexSelector.svelte";
  import Document from "./Document.svelte";
  import KeyboardHelp from "../components/KeyboardHelp.svelte";
  import { Button } from "$lib/components/ui/button/index.js";
  import ChevronLeftIcon from "@lucide/svelte/icons/chevron-left";
  import ChevronRightIcon from "@lucide/svelte/icons/chevron-right";
  import SlidersHorizontalIcon from "@lucide/svelte/icons/sliders-horizontal";

  let { header }: { header: Snippet<[() => ReturnType<Snippet>, () => void]> } = $props();

  // Load available indices once on mount, set default if not yet chosen
  onMount(() => {
    listIndices()
      .then((res) => {
        indices = res.indices;
        const names = indices.map((i) => i.name);
        // Remove any selected indices that no longer exist
        const valid = searchState.selectedIndices.filter((n) => names.includes(n));
        if (valid.length === 0 && indices.length > 0) {
          searchState.selectedIndices = [indices[0].name];
        } else if (valid.length !== searchState.selectedIndices.length) {
          searchState.selectedIndices = valid;
        }
        _syncSearchType();
      })
      .catch(() => {
        indices = [];
        error = "Failed to load indices";
      });
  });

  let indices = $state<IndexInfo[]>([]);

  function _syncSearchType() {
    if (!hybridEnabled) {
      searchState.searchType = "text";
      return;
    }
    const saved = getIndexSearchType(searchState.selectedIndices);
    searchState.searchType = saved ?? "hybrid";
  }

  // Hybrid is enabled only if ALL selected indices have embeddings
  let hybridEnabled = $derived(
    searchState.selectedIndices.length > 0 &&
      searchState.selectedIndices.every((name) => indices.find((i) => i.name === name)?.has_embeddings),
  );

  let loading = $state(false);
  let error = $state("");

  let sliderVisible = $state(false);
  let sliderHideTimer: ReturnType<typeof setTimeout> | undefined;
  function showSlider() {
    clearTimeout(sliderHideTimer);
    sliderVisible = true;
  }
  function hideSlider() {
    clearTimeout(sliderHideTimer);
    sliderHideTimer = setTimeout(() => {
      sliderVisible = false;
    }, 400);
  }

  function updateSearchUrl() {
    const qs = getSearchQs();
    history.replaceState(null, "", qs ? `#/?${qs}` : "#/");
  }

  // Re-search when active facets change (server-side filtering)
  let prevFacetsJson = $state("");
  $effect(() => {
    const json = JSON.stringify(searchState.activeFacets);
    if (json !== prevFacetsJson) {
      const isInitial = prevFacetsJson === "";
      prevFacetsJson = json;
      untrack(() => {
        if (searchState.searched && !isInitial) {
          doSearch(1, false);
        }
      });
    }
  });

  // Sync URL when selected document changes
  $effect(() => {
    const _ = searchState.selectedDocId; // track
    untrack(() => {
      if (searchState.searched) updateSearchUrl();
    });
  });

  let joinedIndex = $derived(searchState.selectedIndices.join(","));

  async function doSearch(page: number = 1, resetFacets = true) {
    if (!searchState.query.trim()) return;
    loading = true;
    error = "";
    searchState.searched = true;
    searchState.submittedQuery = searchState.query;
    searchState.currentPage = page;
    const offset = (page - 1) * searchState.pageSize;
    try {
      const activeFilters = resetFacets ? {} : searchState.activeFacets;
      if (resetFacets) {
        searchState.activeFacets = {};
        prevFacetsJson = "{}";
      }
      const res = await search(
        searchState.query,
        searchState.searchType,
        searchState.pageSize,
        joinedIndex,
        offset,
        activeFilters,
        searchState.searchType === "hybrid" ? searchState.semanticRatio : undefined,
        searchState.sortBy !== "relevance" ? searchState.sortBy : undefined,
      );
      searchState.results = res.results;
      searchState.total = res.total;
      if (res.facets !== null) {
        searchState.facets = res.facets;
        if (resetFacets || Object.keys(searchState.baselineFacets).length === 0) {
          searchState.baselineFacets = res.facets;
          saveBaselineFacets(searchState.query, joinedIndex);
        }
      }
    } catch (err: any) {
      error = err.message || "Search failed";
      searchState.results = [];
      searchState.total = 0;
    } finally {
      loading = false;
      updateSearchUrl();
      window.scrollTo({ top: 0 });
    }
  }

  function clearSearch() {
    searchState.query = "";
    searchState.submittedQuery = "";
    searchState.results = [];
    searchState.total = 0;
    searchState.searched = false;
    searchState.activeFacets = {};
    searchState.facets = {};
    searchState.baselineFacets = {};
    clearBaselineFacets();
    searchState.currentPage = 1;
    searchState.selectedDocId = "";
    searchState.selectedDocIndex = "";
    window.location.hash = "#/";
  }

  /** Parse URL hash parameters and restore search/sidebar state. */
  function parseUrlState() {
    const hash = window.location.hash;
    const qIdx = hash.indexOf("?");
    if (qIdx < 0) return;
    const params = new URLSearchParams(hash.slice(qIdx + 1));
    const q = params.get("q");
    if (!q) return;
    searchState.query = q;
    const typeParam = params.get("type");
    if (typeParam === "text" || typeParam === "hybrid") searchState.searchType = typeParam;
    const srParam = params.get("semanticRatio");
    if (srParam != null) searchState.semanticRatio = Math.max(0, Math.min(1, parseFloat(srParam) || 0.5));
    const indexParam = params.get("index");
    if (indexParam) {
      searchState.selectedIndices = indexParam.split(",").filter(Boolean);
    }
    searchState.pageSize = Math.min(
      100,
      Math.max(1, parseInt(params.get("pageSize") || String(searchState.pageSize)) || 20),
    );
    const facetsStr = params.get("facets");
    if (facetsStr) {
      try {
        searchState.activeFacets = JSON.parse(facetsStr);
      } catch {}
    } else {
      searchState.activeFacets = {};
    }
    const docParam = params.get("doc");
    const docIndexParam = params.get("docIndex");
    if (docParam) {
      searchState.selectedDocId = docParam;
      searchState.selectedDocIndex = docIndexParam || searchState.selectedIndices[0] || "";
    } else {
      searchState.selectedDocId = "";
      searchState.selectedDocIndex = "";
    }
    const sortParam = params.get("sort");
    const validSorts = ["date:desc", "date:asc", "size:desc", "size:asc"];
    searchState.sortBy = sortParam && validSorts.includes(sortParam) ? sortParam : "relevance";
    restoreBaselineFacets(q, searchState.selectedIndices.join(","));
    doSearch(parseInt(params.get("page") || "1"), false);
  }

  onMount(() => {
    parseUrlState();
  });

  // Re-parse URL on hash changes (e.g. facet links from sidebar navigating to a new search).
  function onHashChange() {
    const hash = window.location.hash;
    if (!hash.startsWith("#/") || hash.startsWith("#/login")) return;
    // If the URL changed externally (e.g. facet link in sidebar), re-parse and re-search.
    const qIdx = hash.indexOf("?");
    if (qIdx < 0) return;
    const params = new URLSearchParams(hash.slice(qIdx + 1));
    const q = params.get("q");
    if (!q) return;
    searchState.query = q;
    const typeParam = params.get("type");
    if (typeParam === "text" || typeParam === "hybrid") searchState.searchType = typeParam;
    const srParam = params.get("semanticRatio");
    if (srParam != null) searchState.semanticRatio = Math.max(0, Math.min(1, parseFloat(srParam) || 0.5));
    const indexParam = params.get("index");
    if (indexParam) {
      searchState.selectedIndices = indexParam.split(",").filter(Boolean);
    }
    searchState.pageSize = Math.min(
      100,
      Math.max(1, parseInt(params.get("pageSize") || String(searchState.pageSize)) || 20),
    );
    const facetsStr = params.get("facets");
    if (facetsStr) {
      try {
        searchState.activeFacets = JSON.parse(facetsStr);
      } catch {}
    } else {
      searchState.activeFacets = {};
    }
    searchState.selectedDocId = params.get("doc") || "";
    searchState.selectedDocIndex = params.get("docIndex") || "";
    const sortParam2 = params.get("sort");
    const validSorts2 = ["date:desc", "date:asc", "size:desc", "size:asc"];
    searchState.sortBy = sortParam2 && validSorts2.includes(sortParam2) ? sortParam2 : "relevance";
    restoreBaselineFacets(q, searchState.selectedIndices.join(","));
    doSearch(parseInt(params.get("page") || "1"), false);
  }

  function handleSubmit(e: Event) {
    e.preventDefault();
    searchState.selectedDocId = "";
    searchState.selectedDocIndex = "";
    (document.activeElement as HTMLElement)?.blur();
    doSearch(1);
  }

  function handlePageSizeChange() {
    savePrefs();
    if (searchState.searched) doSearch(1, false);
  }

  function handleSortChange() {
    savePrefs();
    if (searchState.searched) doSearch(1, false);
  }

  function handleIndicesChange(selected: string[]) {
    searchState.selectedIndices = selected;
    _syncSearchType();
    savePrefs();
    if (searchState.searched) doSearch(1);
  }

  function handleSearchTypeChange() {
    if (searchState.selectedIndices.length > 0)
      saveIndexSearchType(searchState.selectedIndices, searchState.searchType);
    savePrefs();
    if (searchState.searched) doSearch(1);
  }

  function closeSidebar() {
    searchState.selectedDocId = "";
    searchState.selectedDocIndex = "";
    previewFullscreen = false;
  }

  function navigateDoc(docId: string, index: string) {
    searchState.selectedDocId = docId;
    searchState.selectedDocIndex = index;
  }

  let sidebarOpen = $derived(!!searchState.selectedDocId);

  let totalPages = $derived(Math.max(1, Math.ceil(searchState.total / searchState.pageSize)));

  let facets = $derived(
    Object.keys(searchState.baselineFacets).length > 0 ? searchState.baselineFacets : searchState.facets,
  );

  let multiIndex = $derived(searchState.selectedIndices.length > 1);

  function pageNumbers(current: number, total: number): (number | "...")[] {
    if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);
    const pages: (number | "...")[] = [1];
    if (current > 3) pages.push("...");
    for (let p = Math.max(2, current - 1); p <= Math.min(total - 1, current + 1); p++) {
      pages.push(p);
    }
    if (current < total - 2) pages.push("...");
    pages.push(total);
    return pages;
  }

  // Layout preferences
  let resultsSplit = $state(35);
  let facetVisible = $state(true);
  let previewFullscreen = $state(false);
  let searchInputEl = $state<HTMLInputElement | null>(null);
  let previewAsideEl = $state<HTMLElement | null>(null);
  let showKeyboardHelp = $state(false);
  let currentMarkIndex = $state(-1);
  let mainContainer = $state<HTMLElement | null>(null);
  let dragging = $state(false);
  let dragStartX = 0;
  let dragStartValue = 0;

  onMount(() => {
    const saved = localStorage.getItem("aum_layout");
    if (saved) {
      try {
        const p = JSON.parse(saved);
        resultsSplit = p.resultsSplit ?? 35;
        facetVisible = p.facetVisible ?? true;
      } catch {}
    }
  });

  $effect(() => {
    localStorage.setItem("aum_layout", JSON.stringify({ resultsSplit, facetVisible }));
  });

  function startResultsDrag(e: MouseEvent) {
    dragging = true;
    dragStartX = e.clientX;
    dragStartValue = resultsSplit;
    document.addEventListener("mousemove", onDragMove);
    document.addEventListener("mouseup", stopDrag);
    e.preventDefault();
  }

  function onDragMove(e: MouseEvent) {
    if (dragging && mainContainer) {
      const containerWidth = mainContainer.getBoundingClientRect().width;
      const deltaPercent = ((e.clientX - dragStartX) / containerWidth) * 100;
      resultsSplit = Math.max(20, Math.min(60, dragStartValue + deltaPercent));
    }
  }

  function stopDrag() {
    dragging = false;
    document.removeEventListener("mousemove", onDragMove);
    document.removeEventListener("mouseup", stopDrag);
  }

  // --- Vim-style keyboard navigation ---

  // Reset mark index when selected document changes.
  $effect(() => {
    const _ = searchState.selectedDocId;
    currentMarkIndex = -1;
  });

  function isEditableActive(): boolean {
    const el = document.activeElement;
    if (!el) return false;
    const tag = el.tagName;
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
    if ((el as HTMLElement).isContentEditable) return true;
    return false;
  }

  function scrollPreview(delta: number) {
    if (!previewAsideEl) return;
    previewAsideEl.scrollBy({ top: delta, behavior: "instant" });
  }

  function jumpToMark(direction: 1 | -1) {
    if (!previewAsideEl) return;
    const marks = previewAsideEl.querySelectorAll("mark");
    if (marks.length === 0) return;

    const prev = previewAsideEl.querySelector("mark.active-mark");
    if (prev) prev.classList.remove("active-mark");

    // Clamp in case mark count changed since last navigation.
    if (currentMarkIndex >= marks.length) currentMarkIndex = -1;

    if (currentMarkIndex < 0) {
      currentMarkIndex = direction === 1 ? 0 : marks.length - 1;
    } else {
      currentMarkIndex += direction;
      if (currentMarkIndex >= marks.length) currentMarkIndex = 0;
      if (currentMarkIndex < 0) currentMarkIndex = marks.length - 1;
    }

    const target = marks[currentMarkIndex];
    target.classList.add("active-mark");
    target.scrollIntoView({ behavior: "smooth", block: "center" });
  }

  async function navigateResult(direction: 1 | -1) {
    const results = searchState.results;
    if (results.length === 0) return;

    const currentIdx = results.findIndex((r) => r.doc_id === searchState.selectedDocId);

    if (currentIdx < 0) {
      const target = direction === 1 ? results[0] : results[results.length - 1];
      navigateDoc(target.doc_id, target.index);
      return;
    }

    const nextIdx = currentIdx + direction;

    if (nextIdx >= 0 && nextIdx < results.length) {
      navigateDoc(results[nextIdx].doc_id, results[nextIdx].index);
      return;
    }

    // Auto-pagination
    if (direction === 1 && searchState.currentPage < totalPages) {
      await doSearch(searchState.currentPage + 1, false);
      if (searchState.results.length > 0) {
        const first = searchState.results[0];
        navigateDoc(first.doc_id, first.index);
      }
    } else if (direction === -1 && searchState.currentPage > 1) {
      await doSearch(searchState.currentPage - 1, false);
      if (searchState.results.length > 0) {
        const last = searchState.results[searchState.results.length - 1];
        navigateDoc(last.doc_id, last.index);
      }
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    // ? toggles help (Shift+/ on US keyboards)
    if (e.key === "?" && !e.ctrlKey && !e.metaKey && !isEditableActive()) {
      e.preventDefault();
      showKeyboardHelp = !showKeyboardHelp;
      return;
    }

    if (e.key === "Escape") {
      if (showKeyboardHelp) {
        showKeyboardHelp = false;
        return;
      }
      if (isEditableActive()) {
        (document.activeElement as HTMLElement)?.blur();
        return;
      }
      if (sidebarOpen) {
        closeSidebar();
        return;
      }
    }

    // All other vim keys: skip if in editable element or help overlay is open
    if (isEditableActive() || showKeyboardHelp) return;

    switch (e.key) {
      case "/":
        e.preventDefault();
        searchInputEl?.focus();
        break;
      case "j":
        scrollPreview(120);
        break;
      case "k":
        scrollPreview(-120);
        break;
      case "n":
        jumpToMark(1);
        break;
      case "b":
        jumpToMark(-1);
        break;
      case "l":
        navigateResult(1);
        break;
      case "h":
        navigateResult(-1);
        break;
    }
  }
</script>

<svelte:window onhashchange={onHashChange} onkeydown={handleKeydown} />

<svelte:head>
  <title>{searchState.submittedQuery ? `aum - ${searchState.submittedQuery}` : "aum"}</title>
</svelte:head>

{#snippet searchForm()}
  <form class="flex-1 flex gap-2 items-center min-w-0" onsubmit={handleSubmit}>
    <input
      type="search"
      placeholder="Search documents..."
      bind:value={searchState.query}
      bind:this={searchInputEl}
      class="min-w-0 flex-1 rounded-md border border-primary-foreground/15 bg-background/95 px-3 py-1.5 text-base text-foreground shadow-inner transition-[box-shadow,border-color] placeholder:text-muted-foreground/70 focus:border-primary-foreground/40 focus:outline-none focus:ring-2 focus:ring-primary-foreground/25"
    />
    {#if indices.length > 0}
      <IndexSelector {indices} selectedIndices={searchState.selectedIndices} onchange={handleIndicesChange} />
    {/if}
    {#if hybridEnabled}
      <!-- svelte-ignore a11y_no_static_element_interactions -->
      <div class="relative shrink-0" onmouseenter={showSlider} onmouseleave={hideSlider}>
        <label
          class="flex cursor-pointer items-center gap-1.5 px-2 py-1.5 font-mono text-[0.7rem] tracking-wider text-primary-foreground/80 uppercase select-none hover:text-primary-foreground"
          title="Combine keyword and semantic search"
        >
          <input
            type="checkbox"
            class="accent-primary-foreground"
            checked={searchState.searchType === "hybrid"}
            onchange={(e) => {
              searchState.searchType = e.currentTarget.checked ? "hybrid" : "text";
              handleSearchTypeChange();
            }}
          />
          Hybrid
        </label>
        {#if searchState.searchType === "hybrid" && sliderVisible}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <div
            class="absolute top-full right-0 z-10 mt-1.5 flex items-center gap-2 rounded-md border border-border bg-popover px-3 py-2 font-mono text-[10px] tracking-wider text-muted-foreground uppercase whitespace-nowrap shadow-lg"
            onmouseenter={showSlider}
            onmouseleave={hideSlider}
          >
            <span>Keyword</span>
            <input
              type="range"
              min="0"
              max="1"
              step="0.05"
              class="w-24 accent-primary"
              bind:value={searchState.semanticRatio}
              oninput={() => savePrefs()}
              onchange={() => {
                if (searchState.searched) doSearch(1);
              }}
            />
            <span>Semantic</span>
          </div>
        {/if}
      </div>
    {/if}
    <button
      type="submit"
      disabled={loading || !searchState.query.trim()}
      class="shrink-0 rounded-md bg-background px-4 py-1.5 text-sm font-medium text-primary transition-colors hover:enabled:bg-background/85 disabled:cursor-not-allowed disabled:opacity-50"
    >
      {loading ? "…" : "Search"}
    </button>
  </form>
{/snippet}

{@render header(searchForm, clearSearch)}

<main class="px-4">
  {#if error}
    <div class="my-3 rounded-md border border-destructive/25 bg-destructive/10 px-3 py-2.5 text-sm text-destructive" role="alert">{error}</div>
  {/if}

  {#if searchState.searched}
    <div
      class="flex mt-3"
      bind:this={mainContainer}
      style={dragging ? "cursor: col-resize; user-select: none;" : undefined}
    >
      {#if Object.keys(facets).length > 0 && facetVisible && !previewFullscreen}
        <aside
          class="shrink-0 basis-[220px] max-w-[220px] min-w-0 mr-4 sticky top-12 self-start max-h-[calc(100vh-3.5rem)] overflow-y-auto"
        >
          <FacetPanel
            {facets}
            bind:activeFacets={searchState.activeFacets}
            dateFacets={["Created"]}
            valueLabelFn={(key, value) => (key === "File Type" ? mimeAlias(value) : value)}
          />
        </aside>
      {/if}

      <div
        class="flex-1 min-w-0 {previewFullscreen ? 'hidden' : ''}"
        style={sidebarOpen && !previewFullscreen ? `flex: 0 0 ${resultsSplit}%; max-width: ${resultsSplit}%;` : ""}
      >
        <div class="sticky top-10 z-10 mb-3 flex flex-wrap items-center justify-between gap-3 border-b border-border/60 bg-background/95 py-2 backdrop-blur-sm">
          <div class="flex items-center gap-2">
            {#if Object.keys(facets).length > 0}
              <Button
                variant="outline"
                size="sm"
                class="h-7 gap-1.5 px-2 font-mono text-[0.7rem] tracking-wider uppercase"
                onclick={() => (facetVisible = !facetVisible)}
                title={facetVisible ? "Hide filters" : "Show filters"}
              >
                <SlidersHorizontalIcon class="size-3.5" />
                {facetVisible ? "Hide" : "Filters"}
              </Button>
            {/if}
            <p class="m-0 font-mono text-xs tracking-[0.14em] text-muted-foreground uppercase tabular-nums">
              {searchState.total} result{searchState.total !== 1 ? "s" : ""}
            </p>
          </div>
          <div class="flex items-center gap-1 flex-wrap">
            <select
              class="cursor-pointer rounded-md border border-border bg-card px-2 py-1 text-sm text-foreground transition-colors hover:border-primary/40"
              bind:value={searchState.sortBy}
              onchange={handleSortChange}
            >
              <option value="relevance">Best match</option>
              <option value="date:desc">Newest first</option>
              <option value="date:asc">Oldest first</option>
              <option value="size:desc">Largest first</option>
              <option value="size:asc">Smallest first</option>
            </select>
            <Button
              variant="outline"
              size="sm"
              class="h-7 gap-1 px-2 text-xs"
              disabled={searchState.currentPage <= 1 || loading}
              onclick={() => doSearch(searchState.currentPage - 1, false)}
            >
              <ChevronLeftIcon class="size-3.5" />Prev
            </Button>

            {#each pageNumbers(searchState.currentPage, totalPages) as p}
              {#if p === "..."}
                <span class="px-1 font-mono text-xs text-muted-foreground">&hellip;</span>
              {:else}
                <Button
                  variant={p === searchState.currentPage ? "default" : "outline"}
                  size="sm"
                  class="h-7 min-w-7 px-2 font-mono text-xs tabular-nums"
                  disabled={loading}
                  onclick={() => doSearch(p, false)}>{p}</Button
                >
              {/if}
            {/each}

            <Button
              variant="outline"
              size="sm"
              class="h-7 gap-1 px-2 text-xs"
              disabled={searchState.currentPage >= totalPages || loading}
              onclick={() => doSearch(searchState.currentPage + 1, false)}
            >
              Next<ChevronRightIcon class="size-3.5" />
            </Button>

            <select
              class="ml-2 cursor-pointer rounded-md border border-border bg-card px-2 py-1 text-sm text-foreground transition-colors hover:border-primary/40"
              bind:value={searchState.pageSize}
              onchange={handlePageSizeChange}
            >
              <option value={20}>20 / page</option>
              <option value={50}>50 / page</option>
              <option value={100}>100 / page</option>
            </select>
          </div>
        </div>
        <ResultList results={searchState.results} {multiIndex} {loading} />
      </div>

      {#if sidebarOpen && !previewFullscreen}
        <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
        <div
          class="drag-handle {dragging ? 'active' : ''}"
          onmousedown={startResultsDrag}
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize document preview"
        ></div>
      {/if}

      {#if sidebarOpen}
        <aside
          bind:this={previewAsideEl}
          class="sticky top-12 max-h-[calc(100vh-3.5rem)] min-w-0 flex-1 self-start overflow-y-auto rounded-md border border-border/70 bg-card shadow-[0_10px_30px_-24px_oklch(0_0_0/0.5)]"
        >
          {#key searchState.selectedDocId}
            <Document
              docId={searchState.selectedDocId}
              index={searchState.selectedDocIndex}
              highlightQuery={searchState.submittedQuery}
              onClose={closeSidebar}
              onNavigateDoc={navigateDoc}
              onToggleFullscreen={() => (previewFullscreen = !previewFullscreen)}
              {previewFullscreen}
            />
          {/key}
        </aside>
      {/if}
    </div>
  {/if}
</main>

{#if showKeyboardHelp}
  <KeyboardHelp onClose={() => (showKeyboardHelp = false)} />
{/if}

<style>
  .drag-handle {
    flex: 0 0 8px;
    width: 8px;
    cursor: col-resize;
    position: sticky;
    top: 3rem;
    align-self: flex-start;
    height: calc(100vh - 3.5rem);
    z-index: 11;
  }
  .drag-handle::after {
    content: "";
    position: absolute;
    top: 0;
    bottom: 0;
    left: 50%;
    width: 2px;
    transform: translateX(-50%);
    background: transparent;
    border-radius: 1px;
    transition: background 0.15s;
  }
  .drag-handle:hover::after,
  .drag-handle.active::after {
    background: var(--primary);
  }
  :global(aside mark.active-mark) {
    outline: 2px solid var(--highlight-active);
    outline-offset: 1px;
    border-radius: 2px;
  }
</style>
