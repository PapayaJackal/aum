<script lang="ts">
  import type { Snippet } from "svelte";
  import { onMount, untrack } from "svelte";
  import { search, listIndices, type IndexInfo } from "../lib/api";
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
    searchState.pageSize = parseInt(params.get("pageSize") || String(searchState.pageSize));
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
    searchState.pageSize = parseInt(params.get("pageSize") || String(searchState.pageSize));
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
</script>

<svelte:window onhashchange={onHashChange} />

<svelte:head>
  <title>{searchState.submittedQuery ? `aum - ${searchState.submittedQuery}` : "aum"}</title>
</svelte:head>

{#snippet searchForm()}
  <form class="flex-1 flex gap-2 items-center min-w-0" onsubmit={handleSubmit}>
    <input
      type="search"
      placeholder="Search documents..."
      bind:value={searchState.query}
      class="flex-1 px-3 py-[0.45rem] border-none rounded bg-white/95 text-gray-800 text-base min-w-0 focus:outline-2 focus:outline-(--color-accent)"
    />
    {#if indices.length > 0}
      <IndexSelector {indices} selectedIndices={searchState.selectedIndices} onchange={handleIndicesChange} />
    {/if}
    {#if hybridEnabled}
      <!-- svelte-ignore a11y_no_static_element_interactions -->
      <div class="relative shrink-0" onmouseenter={showSlider} onmouseleave={hideSlider}>
        <label
          class="flex items-center gap-1.5 text-xs text-white/90 select-none cursor-pointer px-2 py-[0.45rem]"
          title="Combine keyword and semantic search"
        >
          <input
            type="checkbox"
            class="accent-(--color-accent)"
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
            class="absolute top-full right-0 mt-1 flex items-center gap-1.5 bg-(--color-brand) border border-white/20 rounded px-3 py-2 shadow-lg z-10 whitespace-nowrap text-[10px] text-white/70"
            onmouseenter={showSlider}
            onmouseleave={hideSlider}
          >
            <span>Keyword</span>
            <input
              type="range"
              min="0"
              max="1"
              step="0.05"
              class="w-24 accent-(--color-accent)"
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
      class="px-4 py-[0.45rem] bg-(--color-accent) text-white border-none rounded text-sm cursor-pointer shrink-0 hover:enabled:bg-(--color-accent-hover) disabled:opacity-50 disabled:cursor-not-allowed"
    >
      {loading ? "..." : "Search"}
    </button>
  </form>
{/snippet}

{@render header(searchForm, clearSearch)}

<main class="px-4">
  {#if error}
    <div class="bg-red-50 text-red-600 p-3 rounded my-3">{error}</div>
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
          <FacetPanel {facets} bind:activeFacets={searchState.activeFacets} dateFacets={["Created"]} />
        </aside>
      {/if}

      <div
        class="flex-1 min-w-0 {previewFullscreen ? 'hidden' : ''}"
        style={sidebarOpen && !previewFullscreen ? `flex: 0 0 ${resultsSplit}%; max-width: ${resultsSplit}%;` : ""}
      >
        <div class="flex items-center justify-between gap-3 mb-3 py-1.5 flex-wrap sticky top-10 bg-gray-100 z-10">
          <div class="flex items-center gap-2">
            {#if Object.keys(facets).length > 0}
              <button
                class="px-2 py-1 text-sm bg-gray-100 text-gray-800 border border-gray-300 rounded cursor-pointer shrink-0 hover:bg-blue-50 hover:border-(--color-accent) hover:text-(--color-accent) leading-none"
                onclick={() => (facetVisible = !facetVisible)}
                title={facetVisible ? "Hide filters" : "Show filters"}
              >
                {#if facetVisible}
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    width="12"
                    height="12"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2.5"
                    stroke-linecap="round"
                    stroke-linejoin="round"><polyline points="15 18 9 12 15 6"></polyline></svg
                  >
                {:else}
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    width="12"
                    height="12"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2.5"
                    stroke-linecap="round"
                    stroke-linejoin="round"><polyline points="9 18 15 12 9 6"></polyline></svg
                  >
                {/if}
              </button>
            {/if}
            <p class="text-gray-400 text-sm m-0">
              {searchState.total} result{searchState.total !== 1 ? "s" : ""}
            </p>
          </div>
          <div class="flex items-center gap-1 flex-wrap">
            <select
              class="py-1 px-1.5 border border-gray-300 rounded bg-gray-100 text-sm cursor-pointer"
              bind:value={searchState.sortBy}
              onchange={handleSortChange}
            >
              <option value="relevance">Best match</option>
              <option value="date:desc">Newest first</option>
              <option value="date:asc">Oldest first</option>
              <option value="size:desc">Largest first</option>
              <option value="size:asc">Smallest first</option>
            </select>
            <button
              class="px-2.5 py-1 text-sm bg-gray-100 text-gray-800 border border-gray-300 rounded cursor-pointer shrink-0 hover:enabled:bg-blue-50 hover:enabled:border-(--color-accent) hover:enabled:text-(--color-accent) disabled:opacity-40 disabled:cursor-not-allowed"
              disabled={searchState.currentPage <= 1 || loading}
              onclick={() => doSearch(searchState.currentPage - 1, false)}>&lsaquo; Prev</button
            >

            {#each pageNumbers(searchState.currentPage, totalPages) as p}
              {#if p === "..."}
                <span class="px-1 py-1 text-gray-400 text-sm">&hellip;</span>
              {:else}
                <button
                  class="px-2.5 py-1 text-sm border rounded cursor-pointer shrink-0 disabled:opacity-40 disabled:cursor-not-allowed {p ===
                  searchState.currentPage
                    ? 'bg-(--color-accent) text-white border-(--color-accent)'
                    : 'bg-gray-100 text-gray-800 border-gray-300 hover:enabled:bg-blue-50 hover:enabled:border-(--color-accent) hover:enabled:text-(--color-accent)'}"
                  disabled={loading}
                  onclick={() => doSearch(p, false)}>{p}</button
                >
              {/if}
            {/each}

            <button
              class="px-2.5 py-1 text-sm bg-gray-100 text-gray-800 border border-gray-300 rounded cursor-pointer shrink-0 hover:enabled:bg-blue-50 hover:enabled:border-(--color-accent) hover:enabled:text-(--color-accent) disabled:opacity-40 disabled:cursor-not-allowed"
              disabled={searchState.currentPage >= totalPages || loading}
              onclick={() => doSearch(searchState.currentPage + 1, false)}>Next &rsaquo;</button
            >

            <select
              class="py-1 px-1.5 border border-gray-300 rounded bg-gray-100 text-sm ml-2 cursor-pointer"
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
          class="flex-1 min-w-0 bg-gray-50 border-l border-gray-300 rounded-md shadow-[-2px_0_8px_rgba(0,0,0,0.05)] sticky top-12 self-start max-h-[calc(100vh-3.5rem)] overflow-y-auto"
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
    background: #4a7cf7;
  }
</style>
