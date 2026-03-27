<script lang="ts">
  import { isAuthenticated, clearAuth } from "./lib/auth";
  import Login from "./routes/Login.svelte";
  import Search from "./routes/Search.svelte";

  let hash = $state(window.location.hash || "#/");

  function onHashChange() {
    hash = window.location.hash || "#/";
  }

  $effect(() => {
    if (!isAuthenticated() && !hash.startsWith("#/login")) {
      window.location.hash = "#/login";
    }
  });

  function logout() {
    clearAuth();
    window.location.hash = "#/login";
  }
</script>

<svelte:window onhashchange={onHashChange} />

{#if hash.startsWith("#/login")}
  <header class="bg-(--color-brand) text-white px-4 py-2 flex items-center gap-3 sticky top-0 z-50">
    <nav class="flex items-center gap-4 w-full">
      <a href="#/" class="font-bold text-xl leading-none text-white no-underline shrink-0">&#x0950;</a>
    </nav>
  </header>
  <main class="px-4"><Login /></main>
{:else}
  <Search>
    {#snippet header(form)}
      <header class="bg-(--color-brand) text-white px-4 py-2 flex items-center gap-3 sticky top-0 z-50">
        <a href="#/" class="font-bold text-xl leading-none text-white no-underline shrink-0">&#x0950;</a>
        {@render form()}
        <button
          class="bg-transparent border border-gray-500 text-gray-300 px-3 py-1 rounded cursor-pointer shrink-0 text-sm hover:border-white hover:text-white"
          onclick={logout}>Logout</button
        >
      </header>
    {/snippet}
  </Search>
{/if}
