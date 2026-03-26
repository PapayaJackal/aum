<script lang="ts">
  import type { IndexInfo } from "../lib/api";

  let {
    indices = [],
    selectedIndices = [],
    onchange,
  }: {
    indices: IndexInfo[];
    selectedIndices: string[];
    onchange: (selected: string[]) => void;
  } = $props();

  let open = $state(false);
  let dropdownEl = $state<HTMLElement | null>(null);

  let label = $derived(
    selectedIndices.length === 0
      ? "Select dataset"
      : selectedIndices.length === 1
        ? selectedIndices[0]
        : `${selectedIndices.length} datasets`
  );

  function toggle(name: string) {
    const next = selectedIndices.includes(name)
      ? selectedIndices.filter((n) => n !== name)
      : [...selectedIndices, name];
    // Don't allow deselecting all
    if (next.length === 0) return;
    onchange(next);
  }

  let allSelected = $derived(
    indices.length > 0 && indices.every((i) => selectedIndices.includes(i.name))
  );

  function toggleAll() {
    if (allSelected) {
      // Deselect all except the first one (must keep at least one)
      onchange([indices[0].name]);
    } else {
      onchange(indices.map((i) => i.name));
    }
  }

  function handleClickOutside(e: MouseEvent) {
    if (dropdownEl && !dropdownEl.contains(e.target as Node)) {
      open = false;
    }
  }
</script>

<svelte:window onclick={handleClickOutside} />

<div class="relative shrink-0" bind:this={dropdownEl}>
  <button type="button"
    class="flex items-center gap-1 px-2 py-[0.45rem] border-none rounded bg-white/90 text-sm cursor-pointer whitespace-nowrap text-gray-800 hover:bg-white"
    onclick={() => (open = !open)}
  >
    <span>{label}</span>
    <span class="text-[0.7rem] text-gray-500">{open ? "\u25B4" : "\u25BE"}</span>
  </button>

  {#if open}
    <div class="absolute top-full left-0 mt-1 bg-white border border-gray-300 rounded-md shadow-lg z-50 min-w-[180px] max-h-[300px] overflow-y-auto py-1">
      {#if indices.length > 1}
        <button type="button"
          class="block w-full text-left px-3 py-1.5 border-none bg-transparent text-sm text-(--color-accent) cursor-pointer border-b border-b-gray-200 mb-0.5 hover:bg-blue-50"
          onclick={toggleAll}
        >
          {allSelected ? "Deselect all" : "Select all"}
        </button>
      {/if}
      {#each indices as idx}
        <label class="flex items-center gap-1.5 px-3 py-1.5 cursor-pointer text-sm text-gray-800 hover:bg-gray-100">
          <input
            type="checkbox"
            checked={selectedIndices.includes(idx.name)}
            onchange={() => toggle(idx.name)}
            class="m-0 cursor-pointer"
          />
          <span class="flex-1 min-w-0 overflow-hidden text-ellipsis whitespace-nowrap">{idx.name}</span>
          {#if idx.has_embeddings}
            <span class="text-[0.7rem] bg-green-50 text-green-700 px-1.5 py-0.5 rounded shrink-0" title="Has embeddings">hybrid</span>
          {/if}
        </label>
      {/each}
    </div>
  {/if}
</div>
