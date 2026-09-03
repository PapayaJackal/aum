<script lang="ts">
  import { onMount } from "svelte";
  import { isAuthenticated, clearAuth, isPublicMode, setPublicMode } from "./lib/auth";
  import { getProviders } from "./lib/api";
  import Login from "./routes/Login.svelte";
  import Invite from "./routes/Invite.svelte";
  import Search from "./routes/Search.svelte";
  import ThemeToggle from "./lib/components/app/ThemeToggle.svelte";

  let hash = $state(window.location.hash || "#/");
  let publicMode = $state(false);
  let ready = $state(false);

  function onHashChange() {
    hash = window.location.hash || "#/";
  }

  onMount(async () => {
    try {
      const config = await getProviders();
      if (config.public_mode) {
        setPublicMode(true);
        publicMode = true;
      }
    } catch {
      // If we can't reach the server, proceed with default (non-public) mode
    }
    ready = true;
  });

  $effect(() => {
    if (ready && !isAuthenticated() && !hash.startsWith("#/login") && !hash.startsWith("#/invite")) {
      window.location.hash = "#/login";
    }
  });

  function logout() {
    clearAuth();
    window.location.hash = "#/login";
  }
</script>

<svelte:window onhashchange={onHashChange} />

{#snippet wordmark(onclick?: () => void)}
  <a
    href="#/"
    {onclick}
    class="group flex shrink-0 items-baseline gap-2 no-underline"
    aria-label="aum — home"
  >
    <span class="font-display text-2xl leading-none text-primary-foreground transition-transform group-hover:-rotate-6"
      >&#x0950;</span
    >
    <span
      class="hidden font-display text-sm tracking-[0.28em] text-primary-foreground/55 uppercase sm:inline"
      >aum</span
    >
  </a>
{/snippet}

{#snippet chrome(inner: () => ReturnType<typeof wordmark>)}
  <header
    class="sticky top-0 z-50 flex items-center gap-3 border-b border-primary/25 bg-primary px-4 py-2 text-primary-foreground shadow-[0_1px_0_oklch(1_0_0/0.06)_inset,0_6px_20px_-12px_oklch(0_0_0/0.6)]"
  >
    {@render inner()}
  </header>
{/snippet}

{#if !ready}
  <!-- Wait for config check -->
{:else if hash.startsWith("#/invite")}
  {#snippet inviteBar()}
    {@render wordmark()}
    <div class="flex-1"></div>
    <ThemeToggle />
  {/snippet}
  {@render chrome(inviteBar)}
  <main class="px-4"><Invite /></main>
{:else if hash.startsWith("#/login") && !publicMode}
  {#snippet loginBar()}
    {@render wordmark()}
    <div class="flex-1"></div>
    <ThemeToggle />
  {/snippet}
  {@render chrome(loginBar)}
  <main class="px-4"><Login /></main>
{:else}
  <Search>
    {#snippet header(form, clearSearch)}
      {#snippet searchBar()}
        {@render wordmark(clearSearch)}
        {@render form()}
        <ThemeToggle />
        {#if !publicMode}
          <button
            onclick={logout}
            class="shrink-0 rounded-md border border-primary-foreground/25 px-3 py-1 text-sm text-primary-foreground/75 transition-colors hover:border-primary-foreground/60 hover:text-primary-foreground"
            >Logout</button
          >
        {/if}
      {/snippet}
      {@render chrome(searchBar)}
    {/snippet}
  </Search>
{/if}
