<script lang="ts">
  import { login, getProviders } from "../lib/api";
  import { setAuth } from "../lib/auth";
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button/index.js";
  import { Input } from "$lib/components/ui/input/index.js";
  import { Label } from "$lib/components/ui/label/index.js";
  import * as Card from "$lib/components/ui/card/index.js";

  let username = $state("");
  let password = $state("");
  let error = $state("");
  let loading = $state(false);

  onMount(() => {
    getProviders()
      .then((res) => {
        if (res.public_mode) {
          window.location.hash = "#/";
        }
      })
      .catch(() => {});
  });

  async function handleSubmit(e: Event) {
    e.preventDefault();
    error = "";
    loading = true;

    try {
      const res = await login(username, password);
      setAuth(res.session_token);
      window.location.hash = "#/";
    } catch (err: any) {
      error = err.message || "Login failed";
    } finally {
      loading = false;
    }
  }
</script>

<div class="mx-auto mt-24 w-full max-w-sm">
  <Card.Root class="border-border/70 shadow-[0_1px_0_oklch(1_0_0/0.6)_inset,0_18px_40px_-28px_oklch(0_0_0/0.45)]">
    <Card.Header class="items-center gap-1 text-center">
      <span class="font-display text-5xl leading-none text-primary">&#x0950;</span>
      <Card.Description class="font-mono text-xs tracking-[0.18em] uppercase"
        >you know, for grep</Card.Description
      >
    </Card.Header>

    <Card.Content>
      {#if error}
        <div
          class="mb-4 rounded-md border border-destructive/25 bg-destructive/10 px-3 py-2 text-sm text-destructive"
          role="alert"
        >
          {error}
        </div>
      {/if}

      <form onsubmit={handleSubmit} class="flex flex-col gap-4">
        <div class="flex flex-col gap-1.5">
          <Label for="username">Username</Label>
          <Input id="username" type="text" bind:value={username} required autocomplete="username" />
        </div>

        <div class="flex flex-col gap-1.5">
          <Label for="password">Password</Label>
          <Input id="password" type="password" bind:value={password} required autocomplete="current-password" />
        </div>

        <Button type="submit" disabled={loading} class="mt-1 w-full">
          {loading ? "Signing in…" : "Sign in"}
        </Button>
      </form>
    </Card.Content>
  </Card.Root>
</div>
