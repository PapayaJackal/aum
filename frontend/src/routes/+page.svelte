<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import { Button } from "$lib/components/ui/button";
  import Results from "$lib/components/Results.svelte";
  import { enhance } from "$app/forms";
  import type { ActionData } from "./$types";
  let { form }: { form: ActionData } = $props();

  let searchText = $state(form?.query ?? "");

  function isSuccess(data: ActionData): data is QueryResult {
    console.log(data)
    return data != null && !("error" in data);
  }

  // svelte-ignore non_reactive_update
  let inputField: HTMLInputElement;
</script>

<div class="flex flex-col gap-8">
  <form
    method="POST"
    use:enhance={() => {
      return async ({ update }) => {
        await update({ reset: false });
        inputField.focus();
      };
    }}
    class="w-96 mx-auto flex gap-2"
  >
    <Input bind:ref={inputField} bind:value={searchText} name="q" autofocus />
    <Button type="submit">Search</Button>
  </form>

  {#if isSuccess(form)}
    <Results data={form} />
  {:else if form?.error}
    <div class="text-red-500">{form.error}</div>
  {:else}
    <div>Search for a thing</div>
  {/if}
</div>
