<script lang="ts">
  import "../app.css";
  import { ModeWatcher } from "mode-watcher";
  import Sun from "@tabler/icons-svelte/icons/sun";
  import Moon from "@tabler/icons-svelte/icons/moon";
  import { toggleMode } from "mode-watcher";
  import { Button } from "$lib/components/ui/button/index.js";
  let { children } = $props();
</script>

<ModeWatcher />
<div class="container mx-auto px-4 sm:px-6 lg:px-8">
  <div class="flex gap-4 py-2 items-center justify-between">
    <div class="text-4xl leading-none">aum</div>
    <Button on:click={toggleMode} variant="outline" size="icon">
      <Sun
        class="h-[1.2rem] w-[1.2rem] rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0"
      />
      <Moon
        class="absolute h-[1.2rem] w-[1.2rem] rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100"
      />
      <span class="sr-only">Toggle theme</span>
    </Button>
  </div>
  {@render children()}
</div>
