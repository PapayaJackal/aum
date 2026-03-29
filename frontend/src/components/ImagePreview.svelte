<script lang="ts">
  import { fetchPreviewBlob } from "../lib/api";

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
    loading = true;
    error = "";
    let revoked = false;
    let url: string | null = null;

    fetchPreviewBlob(docId, index)
      .then((blob) => {
        if (revoked) return;
        url = URL.createObjectURL(blob);
        objectUrl = url;
      })
      .catch((err) => {
        if (!revoked) error = err.message || "Failed to load preview";
      })
      .finally(() => {
        if (!revoked) loading = false;
      });

    return () => {
      revoked = true;
      if (url) URL.revokeObjectURL(url);
    };
  });
</script>

{#if loading}
  <div class="flex items-center justify-center py-12 text-gray-400 text-sm">Loading preview...</div>
{:else if error}
  <div class="bg-red-50 text-red-600 p-3 rounded text-sm">{error}</div>
{:else if objectUrl}
  <div class="flex items-center justify-center">
    <img src={objectUrl} alt="Document preview" class="max-w-full h-auto rounded" />
  </div>
{/if}
