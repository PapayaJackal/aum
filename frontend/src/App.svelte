<script lang="ts">
  import { onMount } from "svelte";
  import { isAuthenticated, clearAuth, isPublicMode, setPublicMode } from "./lib/auth";
  import { getProviders } from "./lib/api";
  import Login from "./routes/Login.svelte";
  import Invite from "./routes/Invite.svelte";
  import Search from "./routes/Search.svelte";

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

{#if !ready}
  <!-- Wait for config check -->
{:else if hash.startsWith("#/invite")}
  <header class="bg-(--color-brand) text-white px-4 py-2 flex items-center gap-3 sticky top-0 z-50">
    <nav class="flex items-center gap-4 w-full">
      <a href="#/" class="font-bold text-xl leading-none text-white no-underline shrink-0">&#x0950;</a>
    </nav>
  </header>
  <main class="px-4"><Invite /></main>
{:else if hash.startsWith("#/login") && !publicMode}
  <header class="bg-(--color-brand) text-white px-4 py-2 flex items-center gap-3 sticky top-0 z-50">
    <nav class="flex items-center gap-4 w-full">
      <a href="#/" class="font-bold text-xl leading-none text-white no-underline shrink-0">&#x0950;</a>
    </nav>
  </header>
  <main class="px-4"><Login /></main>
{:else}
  <Search>
    {#snippet header(form, clearSearch)}
      <header class="bg-(--color-brand) text-white px-4 py-2 flex items-center gap-3 sticky top-0 z-50">
        <a href="#/" onclick={clearSearch} class="font-bold text-xl leading-none text-white no-underline shrink-0"
          >&#x0950;</a
        >
        {@render form()}
        {#if !publicMode}
          <button
            class="bg-transparent border border-gray-500 text-gray-300 px-3 py-1 rounded cursor-pointer shrink-0 text-sm hover:border-white hover:text-white"
            onclick={logout}>Logout</button
          >
        {/if}
      </header>
    {/snippet}
  </Search>
{/if}
