<script lang="ts">
  import { login, getProviders } from "../lib/api";
  import { setAuth } from "../lib/auth";

  let username = $state("");
  let password = $state("");
  let error = $state("");
  let loading = $state(false);
  let providers = $state<string[]>([]);

  $effect(() => {
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

<div class="login-container">
  <h1>ॐ</h1>
  <p class="subtitle">seek and ye shall find</p>

  <form onsubmit={handleLogin}>
    {#if error}
      <div class="error">{error}</div>
    {/if}

    <label>
      Username
      <input type="text" bind:value={username} required autocomplete="username" />
    </label>

    <label>
      Password
      <input type="password" bind:value={password} required autocomplete="current-password" />
    </label>

    <button type="submit" disabled={loading}>
      {loading ? "Signing in..." : "Sign in"}
    </button>
  </form>

  {#if providers.length > 0}
    <div class="divider">or</div>
    <div class="oauth-buttons">
      {#each providers as provider}
        <button onclick={() => oauthLogin(provider)} class="oauth-btn">
          Sign in with {provider}
        </button>
      {/each}
    </div>
  {/if}
</div>

<style>
  .login-container {
    max-width: 360px;
    margin: 4rem auto;
    padding: 2rem;
    background: white;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  }

  h1 {
    margin: 0 0 0.25rem;
    text-align: center;
    color: #1a1a2e;
    font-size: 3.5rem;
  }

  .subtitle {
    text-align: center;
    color: #888;
    margin: 0 0 1.5rem;
  }

  form {
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  label {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    font-size: 0.9rem;
    font-weight: 500;
  }

  input {
    padding: 0.5rem;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 1rem;
  }

  button[type="submit"] {
    padding: 0.6rem;
    background: #1a1a2e;
    color: white;
    border: none;
    border-radius: 4px;
    font-size: 1rem;
    cursor: pointer;
  }

  button[type="submit"]:hover {
    background: #16213e;
  }

  button:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .error {
    background: #fee;
    color: #c33;
    padding: 0.5rem;
    border-radius: 4px;
    font-size: 0.9rem;
  }

  .divider {
    text-align: center;
    color: #999;
    margin: 1rem 0;
    position: relative;
  }

  .divider::before,
  .divider::after {
    content: "";
    position: absolute;
    top: 50%;
    width: 40%;
    height: 1px;
    background: #ddd;
  }

  .divider::before {
    left: 0;
  }

  .divider::after {
    right: 0;
  }

  .oauth-buttons {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .oauth-btn {
    padding: 0.6rem;
    background: white;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 0.95rem;
    cursor: pointer;
    text-transform: capitalize;
  }

  .oauth-btn:hover {
    background: #f9f9f9;
    border-color: #bbb;
  }
</style>
