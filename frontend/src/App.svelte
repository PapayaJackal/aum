<script lang="ts">
  import { onMount } from "svelte";
  import { isAuthenticated, setPublicMode } from "./lib/auth";
  import { getProviders, logout as revokeSession } from "./lib/api";
  import Login from "./routes/Login.svelte";
  import Invite from "./routes/Invite.svelte";
  import Search from "./routes/Search.svelte";
  import ThemeToggle from "./lib/components/app/ThemeToggle.svelte";
  import { Button } from "$lib/components/ui/button/index.js";
  import LogOutIcon from "@lucide/svelte/icons/log-out";

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

  let signingOut = $state(false);
  let logoutError = $state("");

  async function logout() {
    signingOut = true;
    logoutError = "";
    try {
      await revokeSession();
      window.location.hash = "#/login";
    } catch (err) {
      logoutError = err instanceof Error ? err.message : "Could not sign out. Please try again.";
    } finally {
      signingOut = false;
    }
  }
</script>

<svelte:window onhashchange={onHashChange} />

{#snippet wordmark(onclick?: () => void)}
  <a href="#/" {onclick} class="group flex shrink-0 items-baseline gap-2 no-underline" aria-label="aum — home">
    <span class="font-display text-2xl leading-none text-foreground transition-transform group-hover:-rotate-6"
      >&#x0950;</span
    >
    <span class="hidden font-display text-sm tracking-[0.28em] text-muted-foreground uppercase sm:inline"
      >aum</span
    >
  </a>
{/snippet}

{#snippet chrome(inner: () => ReturnType<typeof wordmark>)}
  <header
    class="sticky top-0 z-50 flex items-center gap-3 border-b bg-masthead/95 px-4 py-2 text-masthead-foreground backdrop-blur supports-[backdrop-filter]:bg-masthead/80"
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
  {#if logoutError}
    <p role="alert" class="px-4 py-2 text-sm text-destructive">{logoutError}</p>
  {/if}
  <Search>
    {#snippet header(form, clearSearch)}
      {#snippet searchBar()}
        {@render wordmark(clearSearch)}
        {@render form()}
        <ThemeToggle />
        {#if !publicMode}
          <Button
            variant="outline"
            size="sm"
            onclick={logout}
            disabled={signingOut}
            class="shrink-0"
          >
            <LogOutIcon class="size-3.5" />
            <span class="hidden sm:inline">Logout</span>
          </Button>
        {/if}
      {/snippet}
      {@render chrome(searchBar)}
    {/snippet}
  </Search>
{/if}
