<script lang="ts">
  import { onMount } from "svelte";
  import { validateInvite, redeemInvite } from "../lib/api";
  import { setAuth } from "../lib/auth";
  import { Button } from "$lib/components/ui/button/index.js";
  import { Input } from "$lib/components/ui/input/index.js";
  import { Label } from "$lib/components/ui/label/index.js";
  import { Skeleton } from "$lib/components/ui/skeleton/index.js";
  import * as Card from "$lib/components/ui/card/index.js";

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

<div class="mx-auto mt-24 w-full max-w-sm">
  <Card.Root class="shadow-lg">
    <Card.Header class="items-center gap-1 text-center">
      <span class="font-display text-5xl leading-none text-primary">&#x0950;</span>
      <Card.Description class="font-mono text-xs tracking-[0.18em] uppercase">set up your account</Card.Description>
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

      {#if valid === null}
        <div class="flex flex-col gap-4" aria-busy="true">
          <Skeleton class="h-4 w-2/3" />
          <Skeleton class="h-9 w-full" />
          <Skeleton class="h-9 w-full" />
          <Skeleton class="h-9 w-full" />
        </div>
      {:else if valid === false}
        <p class="m-0 text-center text-sm text-muted-foreground">This invitation link is invalid or has expired.</p>
      {:else}
        <p class="m-0 mb-5 text-sm text-muted-foreground">
          Welcome, <strong class="font-display font-semibold text-foreground">{username}</strong>. Set a password to
          complete your account.
        </p>

        <form onsubmit={handleSubmit} class="flex flex-col gap-4">
          <div class="flex flex-col gap-1.5">
            <Label for="password">Password</Label>
            <Input id="password" type="password" bind:value={password} required autocomplete="new-password" />
          </div>

          <div class="flex flex-col gap-1.5">
            <Label for="confirm-password">Confirm password</Label>
            <Input
              id="confirm-password"
              type="password"
              bind:value={confirmPassword}
              required
              autocomplete="new-password"
            />
          </div>

          <Button type="submit" disabled={loading} class="mt-1 w-full">
            {loading ? "Creating account…" : "Create account"}
          </Button>
        </form>
      {/if}
    </Card.Content>
  </Card.Root>
</div>
