<script lang="ts">
  import type { Snippet } from "svelte";
  import { search, listIndices } from "../lib/api";
  import { searchState } from "../lib/searchState.svelte";
  import ResultList from "../components/ResultList.svelte";
  import FacetPanel from "../components/FacetPanel.svelte";

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

  async function doSearch() {
    if (!searchState.query.trim()) return;
    loading = true;
    error = "";
    searchState.searched = true;
    try {
      const res = await search(searchState.query, searchState.searchType, 20, searchState.selectedIndex);
      searchState.results = res.results;
      searchState.total = res.total;
      searchState.activeFacets = {};
    } catch (err: any) {
      error = err.message || "Search failed";
      searchState.results = [];
      searchState.total = 0;
    } finally {
      loading = false;
    }
  }

  function handleSubmit(e: Event) {
    e.preventDefault();
    doSearch();
  }

  let facets = $derived.by(() => {
    const facetMap: Record<string, Set<string>> = {};
    for (const r of searchState.results) {
      for (const [key, value] of Object.entries(r.metadata)) {
        if (["Content-Type", "Author", "dc:creator"].includes(key) && value) {
          if (!facetMap[key]) facetMap[key] = new Set();
          facetMap[key].add(value);
        }
      }
    }
    const out: Record<string, string[]> = {};
    for (const [key, values] of Object.entries(facetMap)) {
      out[key] = [...values].sort();
    }
    return out;
  });

  let filteredResults = $derived.by(() => {
    if (Object.keys(searchState.activeFacets).length === 0) return searchState.results;
    return searchState.results.filter((r) => {
      for (const [key, values] of Object.entries(searchState.activeFacets)) {
        if (values.length > 0 && !values.includes(r.metadata[key] ?? "")) return false;
      }
      return true;
    });
  });
</script>

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
    <div class="results-layout">
      {#if Object.keys(facets).length > 0}
        <aside>
          <FacetPanel {facets} bind:activeFacets={searchState.activeFacets} />
        </aside>
      {/if}
      <div class="results-main">
        <p class="result-count">
          {filteredResults.length} result{filteredResults.length !== 1 ? "s" : ""}
          {#if Object.keys(searchState.activeFacets).length > 0}
            (filtered from {searchState.total})
          {/if}
        </p>
        <ResultList results={filteredResults} index={searchState.selectedIndex} />
      </div>
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

  aside {
    flex: 0 0 200px;
    max-width: 200px;
    min-width: 0;
  }

  .results-main {
    flex: 1;
    min-width: 0;
  }

  .result-count {
    color: #888;
    font-size: 0.9rem;
    margin: 0 0 0.75rem;
  }
</style>
