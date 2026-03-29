<script lang="ts">
  import { onMount } from "svelte";
  import { validateInvite, beginInviteWebauthn, redeemInvite, getProviders } from "../lib/api";
  import { setAuth } from "../lib/auth";
  import { createPasskey } from "../lib/webauthn";

  let token = $state("");
  let username = $state("");
  let password = $state("");
  let confirmPassword = $state("");
  let error = $state("");
  let loading = $state(false);
  let valid = $state<boolean | null>(null);
  let passkeyRegistered = $state(false);
  let passkeyCredential = $state<object | null>(null);
  let passkeyEnabled = $state(false);

  onMount(async () => {
    const params = new URLSearchParams(window.location.hash.split("?")[1] || "");
    token = params.get("token") || "";
    if (!token) {
      error = "No invitation token provided";
      valid = false;
      return;
    }

    try {
      const [inviteRes, providerRes] = await Promise.all([validateInvite(token), getProviders()]);
      username = inviteRes.username;
      passkeyEnabled = providerRes.passkey_login_enabled;
      valid = true;
    } catch (err: any) {
      error = err.message || "Invalid or expired invitation";
      valid = false;
    }
  });

  async function handleRegisterPasskey() {
    error = "";
    loading = true;

    try {
      const { options } = await beginInviteWebauthn(token);
      passkeyCredential = await createPasskey(options);
      passkeyRegistered = true;

      // Auto-submit if no password was entered
      if (!password) {
        await submitAccount();
      }
    } catch (err: any) {
      error = err.message || "Passkey registration failed";
    } finally {
      loading = false;
    }
  }

  async function submitAccount() {
    const body: { password?: string; passkey_credential?: object } = {};
    if (password) body.password = password;
    if (passkeyCredential) body.passkey_credential = passkeyCredential;

    const res = await redeemInvite(token, body);
    setAuth(res.access_token, res.refresh_token);
    window.location.hash = "#/";
  }

  async function handleSubmit(e: Event) {
    e.preventDefault();
    error = "";

    if (!password && !passkeyCredential) {
      error = "Please set a password and/or register a passkey";
      return;
    }

    if (password && password !== confirmPassword) {
      error = "Passwords do not match";
      return;
    }

    loading = true;

    try {
      await submitAccount();
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
      Welcome, <strong>{username}</strong>.
      {#if passkeyEnabled}
        Set a password and/or register a passkey to complete your account.
      {:else}
        Set a password to complete your account.
      {/if}
    </p>

    <form onsubmit={handleSubmit} class="flex flex-col gap-4">
      <label class="flex flex-col gap-1 text-sm font-medium">
        Password
        <input
          type="password"
          bind:value={password}
          autocomplete="new-password"
          class="p-2 border border-gray-300 rounded text-base focus:outline-none focus:border-(--color-brand)"
        />
      </label>

      <label class="flex flex-col gap-1 text-sm font-medium">
        Confirm password
        <input
          type="password"
          bind:value={confirmPassword}
          autocomplete="new-password"
          class="p-2 border border-gray-300 rounded text-base focus:outline-none focus:border-(--color-brand)"
        />
      </label>

      {#if passkeyEnabled}
        <div class="border-t border-gray-200 pt-4">
          {#if passkeyRegistered}
            <div class="flex items-center gap-2 text-sm text-green-700">
              <span>&#10003;</span> Passkey registered
            </div>
          {:else}
            <button
              type="button"
              onclick={handleRegisterPasskey}
              disabled={loading}
              class="w-full p-2.5 bg-white border border-gray-300 rounded text-base cursor-pointer hover:bg-gray-50 hover:border-gray-400 disabled:opacity-60 disabled:cursor-not-allowed"
            >
              {loading ? "Registering..." : "Register a passkey"}
            </button>
          {/if}
        </div>
      {/if}

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
