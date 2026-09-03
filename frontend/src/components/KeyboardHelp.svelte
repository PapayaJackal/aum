<script lang="ts">
  import * as Dialog from "$lib/components/ui/dialog/index.js";

  let { onClose }: { onClose: () => void } = $props();

  const bindings: { key: string; description: string }[] = [
    { key: "/", description: "Focus search bar" },
    { key: "j / k", description: "Scroll preview down / up" },
    { key: "n / b", description: "Next / previous highlight" },
    { key: "\u2193 / \u2191", description: "Next / previous document" },
    { key: "l / h", description: "Next / previous document" },
    { key: "?", description: "Toggle this help" },
    { key: "Esc", description: "Close preview or help" },
  ];
</script>

<Dialog.Root open onOpenChange={(v) => !v && onClose()}>
  <Dialog.Content class="sm:max-w-sm">
    <Dialog.Header>
      <Dialog.Title class="font-display text-base">Keyboard shortcuts</Dialog.Title>
    </Dialog.Header>
    <dl class="m-0 divide-y divide-border/70">
      {#each bindings as b}
        <div class="flex items-center gap-4 py-2.5">
          <dt class="flex w-[104px] shrink-0 gap-1">
            {#each b.key.split(" / ") as k}
              <kbd
                class="inline-block rounded border border-border bg-muted px-2 py-0.5 font-mono text-xs text-foreground shadow-[0_1px_0_oklch(0_0_0/0.08)]"
                >{k}</kbd
              >
            {/each}
          </dt>
          <dd class="m-0 text-sm text-muted-foreground">{b.description}</dd>
        </div>
      {/each}
    </dl>
  </Dialog.Content>
</Dialog.Root>
