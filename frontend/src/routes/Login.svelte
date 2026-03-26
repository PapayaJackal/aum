<script lang="ts">
  import { onMount } from "svelte";
  import { login, getProviders } from "../lib/api";
  import { setAuth } from "../lib/auth";

  let username = $state("");
  let password = $state("");
  let error = $state("");
  let loading = $state(false);
  let providers = $state<string[]>([]);

  onMount(() => {
    getProviders()
      .then((res) => (providers = res.providers))
      .catch(() => {});
  });

  async function handleLogin(e: Event) {
    e.preventDefault();
    error = "";
    loading = true;

    try {
      const res = await login(username, password);
      setAuth(res.access_token, res.refresh_token);
      window.location.hash = "#/";
    } catch (err: any) {
      error = err.message || "Login failed";
    } finally {
      loading = false;
    }
  }

  function oauthLogin(provider: string) {
    window.location.href = `/api/auth/oauth/${provider}/authorize`;
  }
</script>

<div class="max-w-sm mx-auto mt-16 p-8 bg-white rounded-lg shadow-md">
  <h1 class="m-0 mb-1 text-center text-(--color-brand) text-5xl">&#x0950;</h1>
  <p class="text-center text-gray-400 mt-0 mb-6">seek and ye shall find</p>

  <form onsubmit={handleLogin} class="flex flex-col gap-4">
    {#if error}
      <div class="bg-red-50 text-red-600 p-2 rounded text-sm">{error}</div>
    {/if}

    <label class="flex flex-col gap-1 text-sm font-medium">
      Username
      <input type="text" bind:value={username} required autocomplete="username"
        class="p-2 border border-gray-300 rounded text-base focus:outline-none focus:border-(--color-brand)" />
    </label>

    <label class="flex flex-col gap-1 text-sm font-medium">
      Password
      <input type="password" bind:value={password} required autocomplete="current-password"
        class="p-2 border border-gray-300 rounded text-base focus:outline-none focus:border-(--color-brand)" />
    </label>

    <button type="submit" disabled={loading}
      class="p-2.5 bg-(--color-brand) text-white border-none rounded text-base cursor-pointer hover:bg-(--color-brand-hover) disabled:opacity-60 disabled:cursor-not-allowed">
      {loading ? "Signing in..." : "Sign in"}
    </button>
  </form>

  {#if providers.length > 0}
    <div class="login-divider">or</div>
    <div class="flex flex-col gap-2">
      {#each providers as provider}
        <button onclick={() => oauthLogin(provider)}
          class="p-2.5 bg-white border border-gray-300 rounded text-base cursor-pointer capitalize hover:bg-gray-50 hover:border-gray-400">
          Sign in with {provider}
        </button>
      {/each}
    </div>
  {/if}
</div>
