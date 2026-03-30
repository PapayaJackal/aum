<script lang="ts">
  let { onClose }: { onClose: () => void } = $props();

  function handleBackdropClick(e: MouseEvent) {
    if (e.target === e.currentTarget) onClose();
  }

  const bindings: { key: string; description: string }[] = [
    { key: "/", description: "Focus search bar" },
    { key: "j / k", description: "Scroll preview down / up" },
    { key: "n / b", description: "Next / previous highlight" },
    { key: "l / h", description: "Next / previous document" },
    { key: "?", description: "Toggle this help" },
    { key: "Esc", description: "Close preview or help" },
  ];
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div class="fixed inset-0 z-[100] flex items-center justify-center bg-black/40" onclick={handleBackdropClick}>
  <div class="bg-white rounded-lg shadow-xl max-w-sm w-full mx-4 overflow-hidden">
    <div class="flex items-center justify-between px-5 py-3 border-b border-gray-200">
      <h2 class="m-0 text-base font-semibold text-gray-800">Keyboard shortcuts</h2>
      <button
        class="bg-transparent border-none text-gray-400 text-lg cursor-pointer p-1 rounded leading-none hover:bg-gray-100 hover:text-gray-800"
        onclick={onClose}
        title="Close">&#x2715;</button
      >
    </div>
    <table class="w-full">
      <tbody>
        {#each bindings as b}
          <tr class="border-b border-gray-100 last:border-b-0">
            <td class="py-2.5 px-5 w-[100px]">
              <span class="inline-flex gap-1">
                {#each b.key.split(" / ") as k}
                  <kbd
                    class="inline-block px-2 py-0.5 bg-gray-100 border border-gray-300 rounded text-sm font-mono text-gray-700 shadow-[0_1px_0_rgba(0,0,0,0.1)]"
                    >{k}</kbd
                  >
                {/each}
              </span>
            </td>
            <td class="py-2.5 pr-5 text-sm text-gray-600">{b.description}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>
