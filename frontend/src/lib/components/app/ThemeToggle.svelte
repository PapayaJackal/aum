<script lang="ts">
  import { onMount } from "svelte";
  import SunIcon from "@lucide/svelte/icons/sun";
  import MoonIcon from "@lucide/svelte/icons/moon";

  let dark = $state(false);

  onMount(() => {
    const stored = localStorage.getItem("aum-theme");
    dark = stored ? stored === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
    apply();
  });

  function apply() {
    document.documentElement.classList.toggle("dark", dark);
  }

  function toggle() {
    dark = !dark;
    apply();
    try {
      localStorage.setItem("aum-theme", dark ? "dark" : "light");
    } catch {
      // storage unavailable; the toggle still works for this session
    }
  }
</script>

<button
  type="button"
  onclick={toggle}
  title={dark ? "Switch to light" : "Switch to dark"}
  aria-label="Toggle colour scheme"
  class="inline-flex size-8 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
>
  {#if dark}
    <SunIcon class="size-4" />
  {:else}
    <MoonIcon class="size-4" />
  {/if}
</button>
