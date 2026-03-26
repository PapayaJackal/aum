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

<div class="index-selector" bind:this={dropdownEl}>
  <button type="button" class="selector-trigger" onclick={() => (open = !open)}>
    <span class="selector-label">{label}</span>
    <span class="selector-arrow">{open ? "\u25B4" : "\u25BE"}</span>
  </button>

  {#if open}
    <div class="selector-dropdown">
      {#if indices.length > 1}
        <button type="button" class="select-all-btn" onclick={toggleAll}>
          {allSelected ? "Deselect all" : "Select all"}
        </button>
      {/if}
      {#each indices as idx}
        <label class="selector-item">
          <input
            type="checkbox"
            checked={selectedIndices.includes(idx.name)}
            onchange={() => toggle(idx.name)}
          />
          <span class="item-name">{idx.name}</span>
          {#if idx.has_embeddings}
            <span class="embed-badge" title="Has embeddings">hybrid</span>
          {/if}
        </label>
      {/each}
    </div>
  {/if}
</div>

<style>
  .index-selector {
    position: relative;
    flex-shrink: 0;
  }

  .selector-trigger {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.45rem 0.5rem;
    border: none;
    border-radius: 4px;
    background: rgba(255, 255, 255, 0.9);
    font-size: 0.85rem;
    cursor: pointer;
    white-space: nowrap;
    color: #333;
  }

  .selector-trigger:hover {
    background: rgba(255, 255, 255, 1);
  }

  .selector-arrow {
    font-size: 0.7rem;
    color: #666;
  }

  .selector-dropdown {
    position: absolute;
    top: 100%;
    left: 0;
    margin-top: 4px;
    background: white;
    border: 1px solid #ddd;
    border-radius: 6px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    z-index: 100;
    min-width: 180px;
    max-height: 300px;
    overflow-y: auto;
    padding: 0.25rem 0;
  }

  .select-all-btn {
    display: block;
    width: 100%;
    text-align: left;
    padding: 0.4rem 0.75rem;
    border: none;
    background: none;
    font-size: 0.82rem;
    color: #4a7cf7;
    cursor: pointer;
    border-bottom: 1px solid #eee;
    margin-bottom: 0.15rem;
  }

  .select-all-btn:hover {
    background: #f0f4ff;
  }

  .selector-item {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.4rem 0.75rem;
    cursor: pointer;
    font-size: 0.85rem;
    color: #333;
  }

  .selector-item:hover {
    background: #f5f5f5;
  }

  .selector-item input[type="checkbox"] {
    margin: 0;
    cursor: pointer;
  }

  .item-name {
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .embed-badge {
    font-size: 0.7rem;
    background: #e8f5e9;
    color: #2e7d32;
    padding: 0.1rem 0.35rem;
    border-radius: 3px;
    flex-shrink: 0;
  }
</style>
