<script lang="ts">
  import type { Snippet } from "svelte";
  import { onMount, onDestroy, untrack } from "svelte";
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
  import { Input } from "$lib/components/ui/input/index.js";
  import { Slider } from "$lib/components/ui/slider/index.js";
  import { Toggle } from "$lib/components/ui/toggle/index.js";
  import * as Select from "$lib/components/ui/select/index.js";
  import * as Popover from "$lib/components/ui/popover/index.js";
  import ChevronLeftIcon from "@lucide/svelte/icons/chevron-left";
  import ChevronRightIcon from "@lucide/svelte/icons/chevron-right";
  import SearchIcon from "@lucide/svelte/icons/search";
  import Settings2Icon from "@lucide/svelte/icons/settings-2";
  import SlidersHorizontalIcon from "@lucide/svelte/icons/sliders-horizontal";
  import SparklesIcon from "@lucide/svelte/icons/sparkles";

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

  const SORT_OPTIONS = [
    { value: "relevance", label: "Best match" },
    { value: "date:desc", label: "Newest first" },
    { value: "date:asc", label: "Oldest first" },
    { value: "size:desc", label: "Largest first" },
    { value: "size:asc", label: "Smallest first" },
  ];
  const PAGE_SIZE_OPTIONS = [20, 50, 100];

  let sortLabel = $derived(SORT_OPTIONS.find((o) => o.value === searchState.sortBy)?.label ?? "Best match");

  let searchController: AbortController | undefined;
  onDestroy(() => searchController?.abort());

  async function doSearch(page: number = 1, resetFacets = true) {
    if (!searchState.query.trim()) return;
    searchController?.abort();
    const controller = new AbortController();
    searchController = controller;
    const query = searchState.query;
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
        controller.signal,
      );
      if (controller.signal.aborted) return;
      searchState.results = res.results;
      searchState.total = res.total;
      if (res.facets !== null) {
        searchState.facets = res.facets;
        if (resetFacets || Object.keys(searchState.baselineFacets).length === 0) {
          searchState.baselineFacets = res.facets;
          saveBaselineFacets(query, joinedIndex);
        }
      }
    } catch (err: any) {
      if (controller.signal.aborted) return;
      error = err.message || "Search failed";
      searchState.results = [];
      searchState.total = 0;
    } finally {
      if (!controller.signal.aborted) {
        loading = false;
        updateSearchUrl();
        window.scrollTo({ top: 0 });
      }
    }
  }

  function clearSearch() {
    searchController?.abort();
    loading = false;
    error = "";
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

  // Reset mark index and preview scroll when the selected document changes.
  $effect(() => {
    const _ = searchState.selectedDocId;
    currentMarkIndex = -1;
    previewAsideEl?.scrollTo({ top: 0, behavior: "instant" });
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

    if (!showKeyboardHelp && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
      if (!isEditableActive() || document.activeElement === searchInputEl) {
        e.preventDefault();
        navigateResult(e.key === "ArrowDown" ? 1 : -1);
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
  <form class="flex min-w-0 flex-1 items-center gap-2" onsubmit={handleSubmit}>
    <Input
      type="search"
      placeholder="Search documents…"
      bind:value={searchState.query}
      bind:ref={searchInputEl}
      aria-label="Search documents"
      class="h-9 min-w-0 flex-1 text-base"
    />
    {#if indices.length > 0}
      <IndexSelector {indices} selectedIndices={searchState.selectedIndices} onchange={handleIndicesChange} />
    {/if}
    {#if hybridEnabled}
      <div class="flex shrink-0 items-center gap-0.5">
        <Toggle
          size="sm"
          pressed={searchState.searchType === "hybrid"}
          onPressedChange={(pressed) => {
            searchState.searchType = pressed ? "hybrid" : "text";
            handleSearchTypeChange();
          }}
          aria-label="Blend keyword and semantic search"
          title="Blend keyword and semantic search"
          class="h-9 gap-1.5 px-2.5 font-mono text-[0.7rem] tracking-wider text-muted-foreground uppercase"
        >
          <SparklesIcon class="size-3.5" />Hybrid
        </Toggle>
        {#if searchState.searchType === "hybrid"}
          <Popover.Root>
            <Popover.Trigger>
              {#snippet child({ props })}
                <Button
                  {...props}
                  variant="ghost"
                  size="icon"
                  class="size-8 text-muted-foreground"
                >
                  <Settings2Icon class="size-4" />
                  <span class="sr-only">Adjust keyword / semantic balance</span>
                </Button>
              {/snippet}
            </Popover.Trigger>
            <Popover.Content class="w-64" align="end">
              <p class="m-0 mb-3 font-mono text-[0.7rem] tracking-[0.16em] uppercase text-muted-foreground">
                Ranking balance
              </p>
              <Slider
                type="single"
                min={0}
                max={1}
                step={0.05}
                value={searchState.semanticRatio}
                onValueChange={(v) => {
                  searchState.semanticRatio = v;
                  savePrefs();
                }}
                onValueCommit={() => {
                  if (searchState.searched) doSearch(1);
                }}
              />
              <div
                class="mt-2 flex justify-between font-mono text-[0.65rem] tracking-wider uppercase text-muted-foreground"
              >
                <span>Keyword</span>
                <span>Semantic</span>
              </div>
            </Popover.Content>
          </Popover.Root>
        {/if}
      </div>
    {/if}
    <Button
      type="submit"
      disabled={loading || !searchState.query.trim()}
      class="h-9 shrink-0 gap-1.5"
    >
      <SearchIcon class="size-4" />
      <span class="hidden sm:inline">{loading ? "Searching…" : "Search"}</span>
    </Button>
  </form>
{/snippet}

{@render header(searchForm, clearSearch)}

<main>
  {#if error}
    <div
      class="border-b border-destructive/25 bg-destructive/10 px-4 py-2.5 text-sm text-destructive"
      role="alert"
    >
      {error}
    </div>
  {/if}

  {#if searchState.searched}
    <div
      class="flex"
      bind:this={mainContainer}
      style={dragging ? "cursor: col-resize; user-select: none;" : undefined}
    >
      {#if Object.keys(facets).length > 0 && facetVisible && !previewFullscreen}
        <aside
          class="sticky top-12 h-[calc(100vh-3rem)] min-w-0 shrink-0 basis-[220px] self-start overflow-y-auto border-r border-border"
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
        class="min-h-[calc(100vh-3rem)] min-w-0 flex-1 {previewFullscreen ? 'hidden' : ''}"
        style={sidebarOpen && !previewFullscreen ? `flex: 0 0 ${resultsSplit}%; max-width: ${resultsSplit}%;` : ""}
      >
        <div
          class="sticky top-12 z-10 flex flex-wrap items-center justify-between gap-2 border-b border-border bg-background/95 px-3 py-1.5 backdrop-blur-sm"
        >
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
            <Select.Root
              type="single"
              value={searchState.sortBy}
              onValueChange={(v) => {
                searchState.sortBy = v;
                handleSortChange();
              }}
            >
              <Select.Trigger size="sm" class="mr-1 bg-card" aria-label="Sort results">{sortLabel}</Select.Trigger>
              <Select.Content>
                {#each SORT_OPTIONS as opt}
                  <Select.Item value={opt.value} label={opt.label} />
                {/each}
              </Select.Content>
            </Select.Root>
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

            <Select.Root
              type="single"
              value={String(searchState.pageSize)}
              onValueChange={(v) => {
                searchState.pageSize = Number(v);
                handlePageSizeChange();
              }}
            >
              <Select.Trigger size="sm" class="ml-1 bg-card font-mono tabular-nums" aria-label="Results per page"
                >{searchState.pageSize} / page</Select.Trigger
              >
              <Select.Content>
                {#each PAGE_SIZE_OPTIONS as n}
                  <Select.Item value={String(n)} label="{n} / page" />
                {/each}
              </Select.Content>
            </Select.Root>
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
          class="sticky top-12 h-[calc(100vh-3rem)] min-w-0 flex-1 self-start overflow-y-auto border-l border-border"
        >
          <!-- Not keyed on the doc id: the panel keeps the previous document
               on screen while the next one loads, instead of blanking. -->
          <Document
            docId={searchState.selectedDocId}
            index={searchState.selectedDocIndex}
            highlightQuery={searchState.submittedQuery}
            onClose={closeSidebar}
            onNavigateDoc={navigateDoc}
            onToggleFullscreen={() => (previewFullscreen = !previewFullscreen)}
            {previewFullscreen}
          />
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
    /* Straddles the column rule: 9px of grab area pulled back to zero width in
       flow, so the two panes still meet edge to edge. */
    flex: 0 0 9px;
    width: 9px;
    margin-inline: -4.5px;
    cursor: col-resize;
    position: sticky;
    top: 3rem;
    align-self: flex-start;
    height: calc(100vh - 3rem);
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
