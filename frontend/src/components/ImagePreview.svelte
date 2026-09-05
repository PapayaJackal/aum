<script lang="ts">
  import { fetchPreviewBlob } from "../lib/api";
  import PreviewStatus from "./PreviewStatus.svelte";

  let {
    docId,
    index = "",
  }: {
    docId: string;
    index: string;
  } = $props();

  let objectUrl = $state<string | null>(null);
  let loading = $state(true);
  let error = $state("");

  $effect(() => {
    const id = docId;
    const idx = index;
    loading = true;
    error = "";
    let cancelled = false;

    fetchPreviewBlob(id, idx)
      .then((blob) => {
        if (cancelled) return;
        // Swap first, then release the previous image: the old one stays
        // visible right up to the frame the new one replaces it.
        const previous = objectUrl;
        objectUrl = URL.createObjectURL(blob);
        if (previous) URL.revokeObjectURL(previous);
      })
      .catch((err) => {
        if (!cancelled) {
          error = err.message || "Failed to load preview";
          objectUrl = null;
        }
      })
      .finally(() => {
        if (!cancelled) loading = false;
      });

    return () => {
      cancelled = true;
    };
  });

  // Release the last object URL when the panel itself goes away.
  $effect(() => () => {
    if (objectUrl) URL.revokeObjectURL(objectUrl);
  });
</script>

<PreviewStatus {loading} {error} label="image" shape="page" hasContent={!!objectUrl} />

{#if objectUrl}
  <div class="flex items-center justify-center overflow-hidden rounded-md border border-border/70 bg-muted/40 p-2">
    <img src={objectUrl} alt="Document preview" class="h-auto max-w-full rounded-sm" />
  </div>
{/if}
