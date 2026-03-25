<script lang="ts">
  import { isAuthenticated } from "./lib/auth";
  import Login from "./routes/Login.svelte";
  import Search from "./routes/Search.svelte";
  import Document from "./routes/Document.svelte";

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
    import("./lib/auth").then((m) => m.clearAuth());
    window.location.hash = "#/login";
  }
</script>

<svelte:window onhashchange={onHashChange} />

{#if hash.startsWith("#/login")}
  <header>
    <nav>
      <a href="#/" class="brand">aum</a>
    </nav>
  </header>
  <main><Login /></main>

{:else if hash.startsWith("#/document/")}
  {@const rest = hash.slice("#/document/".length)}
  {@const slashIdx = rest.indexOf("/")}
  {@const docIndex = slashIdx >= 0 ? decodeURIComponent(rest.slice(0, slashIdx)) : ""}
  {@const docId = slashIdx >= 0 ? rest.slice(slashIdx + 1) : rest}
  <header>
    <nav>
      <a href="#/" class="brand">aum</a>
      <a href="#/" class="back-link">← Back to search</a>
      <button class="logout-btn" onclick={logout}>Logout</button>
    </nav>
  </header>
  <main><Document {docId} index={docIndex} /></main>

{:else}
  <Search>
    {#snippet header(form)}
      <header>
        <a href="#/" class="brand">aum</a>
        {@render form()}
        <button class="logout-btn" onclick={logout}>Logout</button>
      </header>
    {/snippet}
  </Search>
{/if}

<style>
  :global(body) {
    margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #f5f5f5;
    color: #333;
  }

  header {
    background: #1a1a2e;
    color: white;
    padding: 0.5rem 1rem;
    display: flex;
    align-items: center;
    gap: 0.75rem;
    position: sticky;
    top: 0;
    z-index: 100;
  }

  nav {
    display: flex;
    align-items: center;
    gap: 1rem;
    width: 100%;
  }

  .brand {
    font-weight: 700;
    font-size: 1.1rem;
    color: white;
    text-decoration: none;
    flex-shrink: 0;
  }

  .back-link {
    color: #aac;
    text-decoration: none;
    font-size: 0.9rem;
    flex: 1;
  }

  .back-link:hover {
    color: white;
  }

  .logout-btn {
    background: none;
    border: 1px solid #666;
    color: #ccc;
    padding: 0.25rem 0.75rem;
    border-radius: 4px;
    cursor: pointer;
    flex-shrink: 0;
    font-size: 0.85rem;
  }

  .logout-btn:hover {
    border-color: white;
    color: white;
  }

  main {
    padding: 0 1rem;
  }
</style>
