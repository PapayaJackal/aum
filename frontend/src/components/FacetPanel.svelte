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

<div class="facet-panel">
  <div class="facet-header">
    <h3>Filters</h3>
    {#if Object.keys(activeFacets).length > 0}
      <button class="clear-btn" onclick={clearAll}>Clear</button>
    {/if}
  </div>

  {#each Object.entries(facets) as [key, values]}
    <div class="facet-group">
      <h4>{key}</h4>
      {#if dateFacetSet.has(key)}
        {@const dr = dateRange(key)}
        <div class="date-range">
          <div class="date-labels">
            <span>{dr.lo}</span>
            <span class="date-sep">&ndash;</span>
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
          <label class="facet-option">
            <input
              type="checkbox"
              checked={isActive(key, value)}
              onchange={() => toggleFacet(key, value)}
            />
            <span class="facet-label" title={value}>{value}</span>
          </label>
        {/each}
      {/if}
    </div>
  {/each}
</div>

<style>
  .facet-panel {
    background: white;
    border-radius: 6px;
    padding: 1rem;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
  }

  .facet-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.75rem;
  }

  .facet-header h3 {
    margin: 0;
    font-size: 0.95rem;
  }

  .clear-btn {
    background: none;
    border: none;
    color: #66a;
    font-size: 0.8rem;
    cursor: pointer;
    padding: 0;
  }

  .clear-btn:hover {
    text-decoration: underline;
  }

  .facet-group {
    margin-bottom: 1rem;
  }

  .facet-group:last-child {
    margin-bottom: 0;
  }

  h4 {
    font-size: 0.85rem;
    color: #666;
    margin: 0 0 0.5rem;
    text-transform: capitalize;
  }

  .facet-option {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.85rem;
    padding: 0.15rem 0;
    cursor: pointer;
  }

  .facet-label {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 170px;
  }

  input[type="checkbox"] {
    flex-shrink: 0;
  }

  /* Date range slider */
  .date-range {
    padding: 0.25rem 0;
  }

  .date-labels {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.85rem;
    font-weight: 600;
    color: #444;
    margin-bottom: 0.35rem;
  }

  .date-sep {
    color: #999;
  }

  .slider-track {
    position: relative;
    height: 1.5rem;
  }

  .slider-track input[type="range"] {
    position: absolute;
    left: 0;
    top: 0;
    width: 100%;
    pointer-events: none;
    appearance: none;
    -webkit-appearance: none;
    background: transparent;
    margin: 0;
    height: 1.5rem;
  }

  .slider-track input[type="range"]::-webkit-slider-thumb {
    -webkit-appearance: none;
    pointer-events: auto;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    background: #4a7cf7;
    border: 2px solid white;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
    cursor: pointer;
    position: relative;
    z-index: 1;
  }

  .slider-track input[type="range"]::-moz-range-thumb {
    pointer-events: auto;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    background: #4a7cf7;
    border: 2px solid white;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
    cursor: pointer;
  }

  .slider-track input[type="range"]::-webkit-slider-runnable-track {
    height: 4px;
    background: #ddd;
    border-radius: 2px;
  }

  .slider-track input[type="range"]::-moz-range-track {
    height: 4px;
    background: #ddd;
    border-radius: 2px;
  }
</style>
