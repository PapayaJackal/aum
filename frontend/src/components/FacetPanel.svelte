<script lang="ts">
  let {
    facets = {},
    activeFacets = $bindable({}),
    dateFacets = [] as string[],
  }: {
    facets: Record<string, string[]>;
    activeFacets: Record<string, string[]>;
    dateFacets?: string[];
  } = $props();

  const dateFacetSet = $derived(new Set(dateFacets));

  function toggleFacet(key: string, value: string) {
    const current = activeFacets[key] ?? [];
    if (current.includes(value)) {
      activeFacets = {
        ...activeFacets,
        [key]: current.filter((v) => v !== value),
      };
    } else {
      activeFacets = { ...activeFacets, [key]: [...current, value] };
    }
    // Clean up empty arrays
    if (activeFacets[key]?.length === 0) {
      const { [key]: _, ...rest } = activeFacets;
      activeFacets = rest;
    }
  }

  function isActive(key: string, value: string): boolean {
    return (activeFacets[key] ?? []).includes(value);
  }

  function clearAll() {
    activeFacets = {};
  }

  // Date range helpers
  function dateRange(key: string): { min: number; max: number; lo: number; hi: number } {
    const values = facets[key]?.map(Number).filter((n) => !isNaN(n)).sort((a, b) => a - b) ?? [];
    const min = values[0] ?? 2000;
    const max = values[values.length - 1] ?? new Date().getFullYear();
    const active = activeFacets[key];
    const lo = active?.[0] ? Number(active[0]) : min;
    const hi = active?.[1] ? Number(active[1]) : max;
    return { min, max, lo, hi };
  }

  function setDateRange(key: string, lo: number, hi: number, min: number, max: number) {
    if (lo <= min && hi >= max) {
      // Full range = no filter
      const { [key]: _, ...rest } = activeFacets;
      activeFacets = rest;
    } else {
      activeFacets = { ...activeFacets, [key]: [String(lo), String(hi)] };
    }
  }
</script>

<div class="bg-white rounded-md p-4 shadow-sm">
  <div class="flex justify-between items-center mb-3">
    <h3 class="m-0 text-sm font-semibold">Filters</h3>
    {#if Object.keys(activeFacets).length > 0}
      <button class="bg-transparent border-none text-indigo-500 text-xs cursor-pointer p-0 hover:underline" onclick={clearAll}>Clear</button>
    {/if}
  </div>

  {#each Object.entries(facets) as [key, values]}
    <div class="mb-4 last:mb-0">
      <h4 class="text-sm text-gray-500 m-0 mb-2 capitalize">{key}</h4>
      {#if dateFacetSet.has(key)}
        {@const dr = dateRange(key)}
        <div class="py-1">
          <div class="flex justify-center items-center gap-1 text-sm font-semibold text-gray-600 mb-1">
            <span>{dr.lo}</span>
            <span class="text-gray-400">&ndash;</span>
            <span>{dr.hi}</span>
          </div>
          <div class="slider-track">
            <input
              type="range"
              min={dr.min}
              max={dr.max}
              value={dr.lo}
              oninput={(e) => {
                const v = Math.min(Number((e.target as HTMLInputElement).value), dr.hi);
                setDateRange(key, v, dr.hi, dr.min, dr.max);
              }}
            />
            <input
              type="range"
              min={dr.min}
              max={dr.max}
              value={dr.hi}
              oninput={(e) => {
                const v = Math.max(Number((e.target as HTMLInputElement).value), dr.lo);
                setDateRange(key, dr.lo, v, dr.min, dr.max);
              }}
            />
          </div>
        </div>
      {:else}
        {#each values as value}
          <label class="flex items-center gap-1.5 text-sm py-0.5 cursor-pointer">
            <input
              type="checkbox"
              checked={isActive(key, value)}
              onchange={() => toggleFacet(key, value)}
              class="shrink-0"
            />
            <span class="overflow-hidden text-ellipsis whitespace-nowrap max-w-[170px]" title={value}>{value}</span>
          </label>
        {/each}
      {/if}
    </div>
  {/each}
</div>
