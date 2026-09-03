<script lang="ts">
  import { Checkbox } from "$lib/components/ui/checkbox/index.js";
  import { Slider } from "$lib/components/ui/slider/index.js";
  import { Button } from "$lib/components/ui/button/index.js";

  let {
    facets = {},
    activeFacets = $bindable({}),
    dateFacets = [] as string[],
    valueLabelFn = undefined,
  }: {
    facets: Record<string, string[]>;
    activeFacets: Record<string, string[]>;
    dateFacets?: string[];
    valueLabelFn?: (facetKey: string, value: string) => string;
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

  // Date range helpers — local draft state so dragging the slider doesn't fire
  // a search on every tick; only commit to activeFacets on pointer release.
  let dateDrafts = $state<Record<string, { lo: number; hi: number }>>({});

  function dateRange(key: string): { min: number; max: number; lo: number; hi: number } {
    const values =
      facets[key]
        ?.map(Number)
        .filter((n) => !isNaN(n))
        .sort((a, b) => a - b) ?? [];
    const min = values[0] ?? 2000;
    const max = values[values.length - 1] ?? new Date().getFullYear();
    const draft = dateDrafts[key];
    if (draft) return { min, max, lo: draft.lo, hi: draft.hi };
    const active = activeFacets[key];
    const lo = active?.[0] ? Number(active[0]) : min;
    const hi = active?.[1] ? Number(active[1]) : max;
    return { min, max, lo, hi };
  }

  function updateDateDraft(key: string, lo: number, hi: number) {
    dateDrafts = { ...dateDrafts, [key]: { lo, hi } };
  }

  function commitDateRange(key: string, lo: number, hi: number, min: number, max: number) {
    const { [key]: _, ...rest } = dateDrafts;
    dateDrafts = rest;
    if (lo <= min && hi >= max) {
      const { [key]: __, ...restFacets } = activeFacets;
      activeFacets = restFacets;
    } else {
      activeFacets = { ...activeFacets, [key]: [String(lo), String(hi)] };
    }
  }
</script>

<div class="px-3 py-3">
  <div class="mb-3 flex items-center justify-between">
    <h3 class="m-0 font-mono text-[0.7rem] tracking-[0.18em] uppercase text-muted-foreground">Filters</h3>
    {#if Object.keys(activeFacets).length > 0}
      <Button variant="link" size="sm" class="h-auto p-0 text-xs" onclick={clearAll}>Clear</Button>
    {/if}
  </div>

  {#each Object.entries(facets) as [key, values]}
    <div
      class="mb-4 last:mb-0 [&:not(:first-of-type)]:border-t [&:not(:first-of-type)]:border-border/50 [&:not(:first-of-type)]:pt-3"
    >
      <h4 class="m-0 mb-2 text-sm font-medium text-foreground/80 capitalize">{key}</h4>
      {#if dateFacetSet.has(key)}
        {@const dr = dateRange(key)}
        <div class="pt-1 pb-2">
          <div
            class="mb-2 flex items-baseline justify-center gap-1.5 font-mono text-sm tabular-nums text-foreground/80"
          >
            <span>{dr.lo}</span>
            <span class="text-muted-foreground">&ndash;</span>
            <span>{dr.hi}</span>
          </div>
          <Slider
            type="multiple"
            min={dr.min}
            max={dr.max}
            step={1}
            value={[dr.lo, dr.hi]}
            onValueChange={(v) => updateDateDraft(key, v[0], v[1])}
            onValueCommit={(v) => commitDateRange(key, v[0], v[1], dr.min, dr.max)}
          />
        </div>
      {:else}
        {#each values as value}
          {@const label = valueLabelFn ? valueLabelFn(key, value) : value}
          <label
            class="-mx-1.5 flex cursor-pointer items-center gap-2 rounded px-1.5 py-1 text-sm transition-colors hover:bg-muted/70"
          >
            <Checkbox checked={isActive(key, value)} onCheckedChange={() => toggleFacet(key, value)} class="shrink-0" />
            <span class="min-w-0 overflow-hidden text-ellipsis whitespace-nowrap" title={label}>{label}</span>
          </label>
        {/each}
      {/if}
    </div>
  {/each}
</div>
