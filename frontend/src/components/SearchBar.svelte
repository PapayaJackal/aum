<script lang="ts">
  let {
    query = $bindable(""),
    searchType = $bindable("text"),
    onSearch,
    loading = false,
  }: {
    query: string;
    searchType: string;
    onSearch: () => void;
    loading: boolean;
  } = $props();

  function handleSubmit(e: Event) {
    e.preventDefault();
    onSearch();
  }
</script>

<form class="search-bar" onsubmit={handleSubmit}>
  <input
    type="search"
    placeholder="Search documents..."
    bind:value={query}
    class="search-input"
  />

  <select bind:value={searchType} class="type-select">
    <option value="text">Full text</option>
    <option value="vector">Semantic</option>
    <option value="hybrid">Hybrid</option>
  </select>

  <button type="submit" disabled={loading || !query.trim()}>
    {loading ? "..." : "Search"}
  </button>
</form>

<style>
  .search-bar {
    display: flex;
    gap: 0.5rem;
    background: white;
    padding: 0.75rem;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  }

  .search-input {
    flex: 1;
    padding: 0.6rem 0.75rem;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 1rem;
  }

  .search-input:focus {
    outline: none;
    border-color: #1a1a2e;
  }

  .type-select {
    padding: 0.5rem;
    border: 1px solid #ddd;
    border-radius: 4px;
    background: white;
    font-size: 0.9rem;
  }

  button {
    padding: 0.6rem 1.25rem;
    background: #1a1a2e;
    color: white;
    border: none;
    border-radius: 4px;
    font-size: 0.95rem;
    cursor: pointer;
  }

  button:hover:not(:disabled) {
    background: #16213e;
  }

  button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
</style>
