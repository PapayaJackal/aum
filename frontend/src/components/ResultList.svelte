<script lang="ts">
  import type { SearchResult } from "../lib/api";
  import ResultCard from "./ResultCard.svelte";
  import { Skeleton } from "$lib/components/ui/skeleton/index.js";

  let {
    results = [],
    multiIndex = false,
    loading = false,
  }: { results: SearchResult[]; multiIndex: boolean; loading?: boolean } = $props();
</script>

<div class="flex flex-col gap-3">
  {#if results.length === 0 && !loading}
    <p class="py-12 text-center font-mono text-sm tracking-wide text-muted-foreground">No results found.</p>
  {:else if results.length === 0 && loading}
    {#each { length: 5 } as _, i}
      <div class="rounded-md border border-border/70 bg-card p-4 pl-5" style="opacity: {1 - i * 0.15}">
        <Skeleton class="mb-3 h-4 w-1/3" />
        <Skeleton class="mb-1.5 h-3 w-full" />
        <Skeleton class="mb-3 h-3 w-4/5" />
        <Skeleton class="h-2.5 w-1/2" />
      </div>
    {/each}
  {:else}
    {#each results as result}
      <ResultCard {result} {multiIndex} />
    {/each}
  {/if}
</div>
