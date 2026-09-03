<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import ResultCard from "./ResultCard.svelte";
  import { Skeleton } from "$lib/components/ui/skeleton/index.js";
  import { searchState } from "../lib/searchState.svelte";

  let {
    results = [],
    multiIndex = false,
    loading = false,
  }: { results: SearchResult[]; multiIndex: boolean; loading?: boolean } = $props();

  // Roving tabindex: Tab reaches the list once, landing on the selected row (or
  // the first, when nothing is selected); the arrow keys move within it.
  let selectedIdx = $derived(results.findIndex((r) => r.doc_id === searchState.selectedDocId));
  let tabbableIdx = $derived(selectedIdx >= 0 ? selectedIdx : 0);
</script>

<div class="divide-y divide-border" role="listbox" aria-label="Search results">
  {#if results.length === 0 && !loading}
    <p class="py-12 text-center font-mono text-sm tracking-wide text-muted-foreground">No results found.</p>
  {:else if results.length === 0 && loading}
    {#each { length: 8 } as _, i}
      <div class="skeleton-delayed py-1.5 pr-3 pl-3.5" style="opacity: {1 - i * 0.1}">
        <div class="flex items-baseline gap-2 py-0.5">
          <Skeleton class="h-3.5 w-1/4" />
          <Skeleton class="h-2.5 w-1/3" />
          <Skeleton class="ml-auto h-2.5 w-16" />
        </div>
        <Skeleton class="my-0.5 h-3 w-3/5" />
      </div>
    {/each}
  {:else}
    {#each results as result, i}
      <ResultCard {result} {multiIndex} tabbable={i === tabbableIdx} />
    {/each}
  {/if}
</div>
