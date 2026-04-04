<script lang="ts">
  import { onMount } from "svelte";
  import { validateInvite, redeemInvite } from "../lib/api";
  import { setAuth } from "../lib/auth";

  let token = $state("");
  let username = $state("");
  let password = $state("");
  let confirmPassword = $state("");
  let error = $state("");
  let loading = $state(false);
  let valid = $state<boolean | null>(null);

  onMount(async () => {
    const params = new URLSearchParams(window.location.hash.split("?")[1] || "");
    token = params.get("token") || "";
    if (!token) {
      error = "No invitation token provided";
      valid = false;
      return;
    }

    try {
      const inviteRes = await validateInvite(token);
      username = inviteRes.username;
      valid = true;
    } catch (err: any) {
      error = err.message || "Invalid or expired invitation";
      valid = false;
    }
  });

  async function handleSubmit(e: Event) {
    e.preventDefault();
    error = "";

    if (!password) {
      error = "Please set a password";
      return;
    }

    if (password !== confirmPassword) {
      error = "Passwords do not match";
      return;
    }

    loading = true;

    try {
      const res = await redeemInvite(token, password);
      setAuth(res.session_token);
      window.location.hash = "#/";
    } catch (err: any) {
      error = err.message || "Failed to create account";
    } finally {
      loading = false;
    }
  }
</script>

<div class="max-w-sm mx-auto mt-16 p-8 bg-white rounded-lg shadow-md">
  <h1 class="m-0 mb-1 text-center text-(--color-brand) text-5xl">&#x0950;</h1>
  <p class="text-center text-gray-400 mt-0 mb-6">set up your account</p>

  {#if error}
    <div class="bg-red-50 text-red-600 p-2 rounded text-sm mb-4">{error}</div>
  {/if}

  {#if valid === null}
    <p class="text-center text-gray-500">Validating invitation...</p>
  {:else if valid === false}
    <p class="text-center text-gray-500">This invitation link is invalid or has expired.</p>
  {:else}
    <p class="text-sm text-gray-600 mb-4">
      Welcome, <strong>{username}</strong>. Set a password to complete your account.
    </p>

    <form onsubmit={handleSubmit} class="flex flex-col gap-4">
      <label class="flex flex-col gap-1 text-sm font-medium">
        Password
        <input
          type="password"
          bind:value={password}
          required
          autocomplete="new-password"
          class="p-2 border border-gray-300 rounded text-base focus:outline-none focus:border-(--color-brand)"
        />
      </label>

      <label class="flex flex-col gap-1 text-sm font-medium">
        Confirm password
        <input
          type="password"
          bind:value={confirmPassword}
          required
          autocomplete="new-password"
          class="p-2 border border-gray-300 rounded text-base focus:outline-none focus:border-(--color-brand)"
        />
      </label>

      <button
        type="submit"
        disabled={loading}
        class="p-2.5 bg-(--color-brand) text-white border-none rounded text-base cursor-pointer hover:bg-(--color-brand-hover) disabled:opacity-60 disabled:cursor-not-allowed"
      >
        {loading ? "Creating account..." : "Create account"}
      </button>
    </form>
  {/if}
</div>
