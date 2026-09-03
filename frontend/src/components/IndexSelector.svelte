<script lang="ts">
  import type { IndexInfo } from "../lib/api";
  import * as DropdownMenu from "$lib/components/ui/dropdown-menu/index.js";
  import { Badge } from "$lib/components/ui/badge/index.js";
  import ChevronDownIcon from "@lucide/svelte/icons/chevron-down";

  let {
    indices = [],
    selectedIndices = [],
    onchange,
  }: {
    indices: IndexInfo[];
    selectedIndices: string[];
    onchange: (selected: string[]) => void;
  } = $props();

  let label = $derived(
    selectedIndices.length === 0
      ? "Select dataset"
      : selectedIndices.length === 1
        ? selectedIndices[0]
        : `${selectedIndices.length} datasets`,
  );

  function toggle(name: string) {
    const next = selectedIndices.includes(name)
      ? selectedIndices.filter((n) => n !== name)
      : [...selectedIndices, name];
    // Don't allow deselecting all
    if (next.length === 0) return;
    onchange(next);
  }

  let allSelected = $derived(indices.length > 0 && indices.every((i) => selectedIndices.includes(i.name)));

  function toggleAll() {
    if (allSelected) {
      // Deselect all except the first one (must keep at least one)
      onchange([indices[0].name]);
    } else {
      onchange(indices.map((i) => i.name));
    }
  }
</script>

<DropdownMenu.Root>
  <DropdownMenu.Trigger
    class="flex shrink-0 items-center gap-1.5 rounded-md border border-primary-foreground/20 bg-primary-foreground/10 px-2.5 py-1.5 text-sm whitespace-nowrap text-primary-foreground transition-colors hover:bg-primary-foreground/20"
  >
    <span class="max-w-[12rem] truncate">{label}</span>
    <ChevronDownIcon class="size-3.5 opacity-60" />
  </DropdownMenu.Trigger>
  <DropdownMenu.Content class="max-h-[320px] min-w-[220px] overflow-y-auto" align="start">
    <DropdownMenu.Label class="font-mono text-[0.7rem] tracking-[0.16em] uppercase text-muted-foreground"
      >Datasets</DropdownMenu.Label
    >
    <DropdownMenu.Separator />
    {#if indices.length > 1}
      <DropdownMenu.Item closeOnSelect={false} onSelect={toggleAll} class="text-primary">
        {allSelected ? "Deselect all" : "Select all"}
      </DropdownMenu.Item>
      <DropdownMenu.Separator />
    {/if}
    {#each indices as idx}
      <DropdownMenu.CheckboxItem
        checked={selectedIndices.includes(idx.name)}
        closeOnSelect={false}
        onCheckedChange={() => toggle(idx.name)}
      >
        <span class="min-w-0 flex-1 truncate">{idx.name}</span>
        {#if idx.has_embeddings}
          <Badge
            variant="outline"
            class="ml-2 border-primary/30 font-mono text-[0.6rem] tracking-wider text-primary uppercase"
            title="Has embeddings">hybrid</Badge
          >
        {/if}
      </DropdownMenu.CheckboxItem>
    {/each}
  </DropdownMenu.Content>
</DropdownMenu.Root>
