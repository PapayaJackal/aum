<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import ResultCard from "./ResultCard.svelte";

  let {
    results = [],
    multiIndex = false,
    loading = false,
  }: { results: SearchResult[]; multiIndex: boolean; loading?: boolean } = $props();
</script>

<div class="flex flex-col gap-3">
  {#if results.length === 0 && !loading}
    <p class="text-gray-400 text-center py-8">No results found.</p>
  {:else if results.length === 0 && loading}
    <p class="text-gray-400 text-center py-8">Searching…</p>
  {:else}
    {#each results as result}
      <ResultCard {result} {multiIndex} />
    {/each}
  {/if}
</div>
