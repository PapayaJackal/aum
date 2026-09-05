<script lang="ts">
  import { Skeleton } from "$lib/components/ui/skeleton/index.js";

  // Shared loading / error chrome for the rich preview components, so a PDF,
  // an image and an HTML document all fail and load the same way.
  let {
    loading = false,
    error = "",
    label,
    shape = "lines",
    hasContent = false,
  }: {
    loading?: boolean;
    error?: string;
    label: string;
    shape?: "lines" | "page";
    /** Previous render still on screen: hold it rather than showing skeletons. */
    hasContent?: boolean;
  } = $props();
</script>

{#if error}
  <div
    role="alert"
    class="rounded-md border border-destructive/25 bg-destructive/10 px-3 py-2 text-sm text-destructive"
  >
    {error}
  </div>
{:else if loading && !hasContent}
  <div class="skeleton-delayed flex flex-col gap-2" aria-busy="true" aria-label="Loading {label}">
    {#if shape === "page"}
      <Skeleton class="aspect-[1/1.294] w-full rounded-md" />
    {:else}
      <Skeleton class="h-3.5 w-2/5" />
      <Skeleton class="h-3 w-full" />
      <Skeleton class="h-3 w-full" />
      <Skeleton class="h-3 w-4/5" />
      <Skeleton class="mt-2 h-3 w-full" />
      <Skeleton class="h-3 w-3/5" />
    {/if}
  </div>
{/if}
