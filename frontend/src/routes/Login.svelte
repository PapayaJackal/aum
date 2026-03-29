<script lang="ts">
  import { onMount } from "svelte";
  import {
    login,
    beginPasskeyLogin,
    completePasskeyLogin,
    beginPasskeyEnrollment,
    completePasskeyEnrollment,
    getProviders,
    type LoginResponse,
    type TokenResponse,
  } from "../lib/api";
  import { setAuth } from "../lib/auth";
  import { createPasskey, verifyPasskey } from "../lib/webauthn";

  let username = $state("");
  let password = $state("");
  let error = $state("");
  let loading = $state(false);
  let providers = $state<string[]>([]);
  let passkeyLoginEnabled = $state(false);

  // Enrollment flow state
  type Step = "login" | "enroll";
  let step = $state<Step>("login");
  let enrollmentToken = $state("");

  onMount(() => {
    getProviders()
      .then((res) => {
        providers = res.providers;
        passkeyLoginEnabled = res.passkey_login_enabled;
      })
      .catch(() => {});
  });

  function completeLogin(res: TokenResponse) {
    setAuth(res.access_token, res.refresh_token);
    window.location.hash = "#/";
  }

  async function handlePasskeyLogin() {
    error = "";
    loading = true;

    try {
      const { options, session_id } = await beginPasskeyLogin();
      const credential = await verifyPasskey(options);
      const res = await completePasskeyLogin(session_id, credential);
      completeLogin(res);
    } catch (err: any) {
      error = err.message || "Passkey login failed";
    } finally {
      loading = false;
    }
  }

  async function handlePasswordLogin(e: Event) {
    e.preventDefault();
    error = "";
    loading = true;

    try {
      const res: LoginResponse = await login(username, password);

      if ("passkey_enrollment_required" in res && res.passkey_enrollment_required) {
        enrollmentToken = res.enrollment_token;
        step = "enroll";
      } else {
        completeLogin(res as TokenResponse);
      }
    } catch (err: any) {
      error = err.message || "Login failed";
    } finally {
      loading = false;
    }
  }

  async function handleEnrollment() {
    error = "";
    loading = true;

    try {
      const { options } = await beginPasskeyEnrollment(enrollmentToken);
      const credential = await createPasskey(options);
      const res = await completePasskeyEnrollment(enrollmentToken, credential);
      completeLogin(res);
    } catch (err: any) {
      error = err.message || "Passkey registration failed";
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
  <p class="text-center text-gray-400 mt-0 mb-6">you know, for grep</p>

  {#if error}
    <div class="bg-red-50 text-red-600 p-2 rounded text-sm mb-4">{error}</div>
  {/if}

  {#if step === "login"}
    {#if passkeyLoginEnabled}
      <button
        onclick={handlePasskeyLogin}
        disabled={loading}
        class="w-full p-2.5 mb-4 bg-(--color-brand) text-white border-none rounded text-base cursor-pointer hover:bg-(--color-brand-hover) disabled:opacity-60 disabled:cursor-not-allowed"
      >
        {loading ? "Verifying..." : "Sign in with passkey"}
      </button>

      <div class="login-divider">or</div>
    {/if}

    <form onsubmit={handlePasswordLogin} class="flex flex-col gap-4">
      <label class="flex flex-col gap-1 text-sm font-medium">
        Username
        <input
          type="text"
          bind:value={username}
          required
          autocomplete="username"
          class="p-2 border border-gray-300 rounded text-base focus:outline-none focus:border-(--color-brand)"
        />
      </label>

      <label class="flex flex-col gap-1 text-sm font-medium">
        Password
        <input
          type="password"
          bind:value={password}
          required
          autocomplete="current-password"
          class="p-2 border border-gray-300 rounded text-base focus:outline-none focus:border-(--color-brand)"
        />
      </label>

      <button
        type="submit"
        disabled={loading}
        class="p-2.5 bg-(--color-brand) text-white border-none rounded text-base cursor-pointer hover:bg-(--color-brand-hover) disabled:opacity-60 disabled:cursor-not-allowed"
      >
        {loading ? "Signing in..." : "Sign in"}
      </button>
    </form>

    {#if providers.length > 0}
      <div class="login-divider">or</div>
      <div class="flex flex-col gap-2">
        {#each providers as provider}
          <button
            onclick={() => oauthLogin(provider)}
            class="p-2.5 bg-white border border-gray-300 rounded text-base cursor-pointer capitalize hover:bg-gray-50 hover:border-gray-400"
          >
            Sign in with {provider}
          </button>
        {/each}
      </div>
    {/if}
  {:else if step === "enroll"}
    <div class="flex flex-col gap-4 text-center">
      <p class="text-gray-600 text-sm">
        Your administrator requires passkey registration. Register a passkey to continue.
      </p>
      <button
        onclick={handleEnrollment}
        disabled={loading}
        class="p-2.5 bg-(--color-brand) text-white border-none rounded text-base cursor-pointer hover:bg-(--color-brand-hover) disabled:opacity-60 disabled:cursor-not-allowed"
      >
        {loading ? "Registering..." : "Register passkey"}
      </button>
      <button
        onclick={() => {
          step = "login";
          error = "";
        }}
        class="text-sm text-gray-500 bg-transparent border-none cursor-pointer hover:text-gray-700"
      >
        Back to login
      </button>
    </div>
  {/if}
</div>
