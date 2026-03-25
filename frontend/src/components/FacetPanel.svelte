<script lang="ts">
  let {
    facets = {},
    activeFacets = $bindable({}),
  }: {
    facets: Record<string, string[]>;
    activeFacets: Record<string, string[]>;
  } = $props();

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
    max-width: 150px;
  }

  input[type="checkbox"] {
    flex-shrink: 0;
  }
</style>
