<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  export let data: QueryResult;
</script>

<Table.Root>
  <Table.Caption>
    {data.estimatedTotalHits} hits in {data.processingTimeMs}ms
  </Table.Caption>
  <Table.Header>
    <Table.Row>
      <Table.Head class="w-[150px]">id</Table.Head>
      <Table.Head>title</Table.Head>
    </Table.Row>
  </Table.Header>
  <Table.Body>
    {#each data.hits as { id, content }}
      <Table.Row>
        <Table.Cell>{id}</Table.Cell>
        <Table.Cell>{content}</Table.Cell>
      </Table.Row>
    {/each}
  </Table.Body>
</Table.Root>
