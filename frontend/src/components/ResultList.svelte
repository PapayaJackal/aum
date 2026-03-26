<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import ResultCard from "./ResultCard.svelte";

  let { results = [], multiIndex = false }: { results: SearchResult[]; multiIndex: boolean } = $props();
</script>

<div class="result-list">
  {#if results.length === 0}
    <p class="empty">No results found.</p>
  {:else}
    {#each results as result}
      <ResultCard {result} {multiIndex} />
    {/each}
  {/if}
</div>

<style>
  .result-list {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  .empty {
    color: #888;
    text-align: center;
    padding: 2rem;
  }
</style>
