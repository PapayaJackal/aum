<script lang="ts">
  import type { Snippet } from "svelte";
  import { onMount, untrack } from "svelte";
  import { search, listIndices } from "../lib/api";
  import { searchState, getSearchQs } from "../lib/searchState.svelte";
  import ResultList from "../components/ResultList.svelte";
  import FacetPanel from "../components/FacetPanel.svelte";
  import Document from "./Document.svelte";

  let { header }: { header: Snippet<[() => ReturnType<Snippet>]> } = $props();

  // Load available indices on mount, set default if not yet chosen
  $effect(() => {
    listIndices()
      .then((res) => {
        if (res.indices.length > 0 && !searchState.selectedIndex) {
          searchState.selectedIndex = res.indices[0];
        }
        indices = res.indices;
      })
      .catch(() => { indices = []; });
  });

  let indices = $state<string[]>([]);
  let loading = $state(false);
  let error = $state("");

  function updateSearchUrl() {
    const qs = getSearchQs();
    history.replaceState(null, "", qs ? `#/?${qs}` : "#/");
  }

  // Sync URL when active facets change (client-side filter, no re-search needed)
  $effect(() => {
    const _ = JSON.stringify(searchState.activeFacets); // track
    untrack(() => {
      if (searchState.searched) updateSearchUrl();
    });
  });

  // Sync URL when selected document changes
  $effect(() => {
    const _ = searchState.selectedDocId; // track
    untrack(() => {
      if (searchState.searched) updateSearchUrl();
    });
  });

  async function doSearch(page: number = 1, resetFacets = true) {
    if (!searchState.query.trim()) return;
    loading = true;
    error = "";
    searchState.searched = true;
    searchState.currentPage = page;
    const offset = (page - 1) * searchState.pageSize;
    try {
      const res = await search(searchState.query, searchState.searchType, searchState.pageSize, searchState.selectedIndex, offset);
      searchState.results = res.results;
      searchState.total = res.total;
      if (res.facets !== null) {
        searchState.facets = res.facets;
        if (resetFacets) searchState.activeFacets = {};
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

  /** Parse URL hash parameters and restore search/sidebar state. */
  function parseUrlState() {
    const hash = window.location.hash;
    const qIdx = hash.indexOf("?");
    if (qIdx < 0) return;
    const params = new URLSearchParams(hash.slice(qIdx + 1));
    const q = params.get("q");
    if (!q) return;
    searchState.query = q;
    searchState.searchType = (params.get("type") as "text" | "vector" | "hybrid") || "text";
    const indexParam = params.get("index");
    if (indexParam) searchState.selectedIndex = indexParam;
    searchState.pageSize = parseInt(params.get("pageSize") || "20");
    const facetsStr = params.get("facets");
    if (facetsStr) {
      try { searchState.activeFacets = JSON.parse(facetsStr); } catch {}
    } else {
      searchState.activeFacets = {};
    }
    const docParam = params.get("doc");
    const docIndexParam = params.get("docIndex");
    if (docParam) {
      searchState.selectedDocId = docParam;
      searchState.selectedDocIndex = docIndexParam || searchState.selectedIndex;
    } else {
      searchState.selectedDocId = "";
      searchState.selectedDocIndex = "";
    }
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
    searchState.searchType = (params.get("type") as "text" | "vector" | "hybrid") || "text";
    const indexParam = params.get("index");
    if (indexParam) searchState.selectedIndex = indexParam;
    searchState.pageSize = parseInt(params.get("pageSize") || "20");
    const facetsStr = params.get("facets");
    if (facetsStr) {
      try { searchState.activeFacets = JSON.parse(facetsStr); } catch {}
    } else {
      searchState.activeFacets = {};
    }
    searchState.selectedDocId = params.get("doc") || "";
    searchState.selectedDocIndex = params.get("docIndex") || "";
    doSearch(parseInt(params.get("page") || "1"), false);
  }

  function handleSubmit(e: Event) {
    e.preventDefault();
    searchState.selectedDocId = "";
    searchState.selectedDocIndex = "";
    doSearch(1);
  }

  function handlePageSizeChange() {
    if (searchState.searched) doSearch(1);
  }

  function closeSidebar() {
    searchState.selectedDocId = "";
    searchState.selectedDocIndex = "";
  }

  function navigateDoc(docId: string, index: string) {
    searchState.selectedDocId = docId;
    searchState.selectedDocIndex = index;
  }

  let sidebarOpen = $derived(!!searchState.selectedDocId);

  let totalPages = $derived(Math.max(1, Math.ceil(searchState.total / searchState.pageSize)));

  let facets = $derived(searchState.facets);

  const DATE_FACET_KEYS = new Set(["Created"]);

  let filteredResults = $derived.by(() => {
    if (Object.keys(searchState.activeFacets).length === 0) return searchState.results;
    return searchState.results.filter((r) => {
      for (const [key, filterValues] of Object.entries(searchState.activeFacets)) {
        if (filterValues.length === 0) continue;
        const meta = r.metadata[key];
        if (DATE_FACET_KEYS.has(key)) {
          // Date range: filterValues is [minYear, maxYear]
          const year = Number(meta);
          const lo = Number(filterValues[0]);
          const hi = Number(filterValues[1] ?? filterValues[0]);
          if (isNaN(year) || year < lo || year > hi) return false;
        } else if (Array.isArray(meta)) {
          if (!meta.some((v) => filterValues.includes(v))) return false;
        } else {
          if (!filterValues.includes(meta ?? "")) return false;
        }
      }
      return true;
    });
  });

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

  let toolbarStuck = $state(false);
  let sentinel = $state<HTMLElement | null>(null);

  $effect(() => {
    if (!sentinel) return;
    const observer = new IntersectionObserver(
      ([entry]) => { toolbarStuck = !entry.isIntersecting; },
      { threshold: 1 }
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  });
</script>

<svelte:window onhashchange={onHashChange} />

<svelte:head>
  <title>{searchState.searched && searchState.query ? `aum - ${searchState.query}` : "aum"}</title>
</svelte:head>

{#snippet searchForm()}
  <form class="search-form" onsubmit={handleSubmit}>
    <input
      type="search"
      placeholder="Search documents..."
      bind:value={searchState.query}
      class="search-input"
    />
    {#if indices.length > 0}
      <select bind:value={searchState.selectedIndex} class="toolbar-select">
        {#each indices as idx}
          <option value={idx}>{idx}</option>
        {/each}
      </select>
    {/if}
    <select bind:value={searchState.searchType} class="toolbar-select">
      <option value="text">Full text</option>
      <option value="vector">Semantic</option>
      <option value="hybrid">Hybrid</option>
    </select>
    <button type="submit" disabled={loading || !searchState.query.trim()}>
      {loading ? "..." : "Search"}
    </button>
  </form>
{/snippet}

{@render header(searchForm)}

<main>
  {#if error}
    <div class="error">{error}</div>
  {/if}

  {#if searchState.searched}
    <div class="results-layout" class:sidebar-open={sidebarOpen}>
      {#if Object.keys(facets).length > 0}
        <aside class="facet-aside">
          <FacetPanel {facets} bind:activeFacets={searchState.activeFacets} dateFacets={["Created"]} />
        </aside>
      {/if}
      <div class="results-main">
        <div bind:this={sentinel} class="toolbar-sentinel"></div>
        <div class="results-toolbar" class:stuck={toolbarStuck}>
          <p class="result-count">
            {searchState.total} result{searchState.total !== 1 ? "s" : ""}
            {#if Object.keys(searchState.activeFacets).length > 0}
              ({filteredResults.length} shown after filter)
            {/if}
          </p>
          <div class="pagination-controls">
            <button
              class="page-btn"
              disabled={searchState.currentPage <= 1 || loading}
              onclick={() => doSearch(searchState.currentPage - 1)}
            >&lsaquo; Prev</button>

            {#each pageNumbers(searchState.currentPage, totalPages) as p}
              {#if p === "..."}
                <span class="page-ellipsis">…</span>
              {:else}
                <button
                  class="page-btn"
                  class:active={p === searchState.currentPage}
                  disabled={loading}
                  onclick={() => doSearch(p)}
                >{p}</button>
              {/if}
            {/each}

            <button
              class="page-btn"
              disabled={searchState.currentPage >= totalPages || loading}
              onclick={() => doSearch(searchState.currentPage + 1)}
            >Next &rsaquo;</button>

            <select
              class="page-size-select"
              bind:value={searchState.pageSize}
              onchange={handlePageSizeChange}
            >
              <option value={20}>20 / page</option>
              <option value={50}>50 / page</option>
              <option value={100}>100 / page</option>
            </select>
          </div>
        </div>
        <ResultList results={filteredResults} index={searchState.selectedIndex} />
      </div>

      {#if sidebarOpen}
        <aside class="doc-sidebar">
          {#key searchState.selectedDocId}
            <Document
              docId={searchState.selectedDocId}
              index={searchState.selectedDocIndex}
              onClose={closeSidebar}
              onNavigateDoc={navigateDoc}
            />
          {/key}
        </aside>
      {/if}
    </div>
  {/if}
</main>

<style>
  .search-form {
    flex: 1;
    display: flex;
    gap: 0.5rem;
    align-items: center;
    min-width: 0;
  }

  .search-input {
    flex: 1;
    padding: 0.45rem 0.65rem;
    border: none;
    border-radius: 4px;
    font-size: 0.95rem;
    background: rgba(255, 255, 255, 0.95);
    min-width: 0;
  }

  .search-input:focus {
    outline: 2px solid #4a7cf7;
  }

  .toolbar-select {
    padding: 0.45rem 0.5rem;
    border: none;
    border-radius: 4px;
    background: rgba(255, 255, 255, 0.9);
    font-size: 0.85rem;
    flex-shrink: 0;
  }

  button {
    padding: 0.45rem 1rem;
    background: #4a7cf7;
    color: white;
    border: none;
    border-radius: 4px;
    font-size: 0.9rem;
    cursor: pointer;
    flex-shrink: 0;
  }

  button:hover:not(:disabled) {
    background: #3a6ce7;
  }

  button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  main {
    padding: 0 1rem;
  }

  .error {
    background: #fee;
    color: #c33;
    padding: 0.75rem;
    border-radius: 4px;
    margin: 0.75rem 0;
  }

  .results-layout {
    display: flex;
    gap: 1rem;
    margin-top: 0.75rem;
  }

  .facet-aside {
    flex: 0 0 220px;
    max-width: 220px;
    min-width: 0;
    position: sticky;
    top: 3rem;
    align-self: flex-start;
    max-height: calc(100vh - 3.5rem);
    overflow-y: auto;
  }

  .results-main {
    flex: 1;
    min-width: 0;
  }

  .sidebar-open .results-main {
    flex: 0 0 35%;
    max-width: 35%;
  }

  .doc-sidebar {
    flex: 1;
    min-width: 0;
    background: #fafafa;
    border-left: 1px solid #ddd;
    border-radius: 6px;
    box-shadow: -2px 0 8px rgba(0, 0, 0, 0.05);
    position: sticky;
    top: 3rem;
    align-self: flex-start;
    max-height: calc(100vh - 3.5rem);
    overflow-y: auto;
  }

  .toolbar-sentinel {
    position: relative;
    top: -2.5rem;
    height: 0;
    pointer-events: none;
  }

  .results-toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
    flex-wrap: wrap;
    position: sticky;
    top: 2.5rem;
    background: #f5f5f5;
    z-index: 10;
    padding: 0.4rem 0;
  }

  .results-toolbar.stuck {
    padding: 0.65rem 0.75rem;
    margin: 0 -0.75rem 0.75rem;
  }


  .result-count {
    color: #888;
    font-size: 0.9rem;
    margin: 0;
  }

  .pagination-controls {
    display: flex;
    align-items: center;
    gap: 0.25rem;
    flex-wrap: wrap;
  }

  .page-btn {
    padding: 0.3rem 0.6rem;
    font-size: 0.85rem;
    background: #f0f0f0;
    color: #333;
    border: 1px solid #ddd;
    border-radius: 4px;
    cursor: pointer;
    flex-shrink: 0;
  }

  .page-btn:hover:not(:disabled) {
    background: #e0e8ff;
    border-color: #4a7cf7;
    color: #4a7cf7;
  }

  .page-btn.active {
    background: #4a7cf7;
    color: white;
    border-color: #4a7cf7;
  }

  .page-btn:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }

  .page-ellipsis {
    padding: 0.3rem 0.25rem;
    color: #888;
    font-size: 0.85rem;
  }

  .page-size-select {
    padding: 0.3rem 0.4rem;
    border: 1px solid #ddd;
    border-radius: 4px;
    background: #f0f0f0;
    font-size: 0.85rem;
    margin-left: 0.5rem;
    cursor: pointer;
  }
</style>
