<script lang="ts">
  import { onMount } from 'svelte';
  import { createEventDispatcher } from 'svelte';
  import type { AdapterSummary } from './types';
  import { t } from './i18n';
  import ListHeader from './ListHeader.svelte';
  import EmptyState from './EmptyState.svelte';

  const dispatch = createEventDispatcher();

  let {
    adapters = [],
    selectedAdapter,
  }: {
    adapters?: AdapterSummary[];
    selectedAdapter?: string | null;
  } = $props();

  function selectAdapter(name: string) {
    selectedAdapter = name;
    dispatch('select', name);
  }

  function handleCreate() {
    dispatch('create');
  }
</script>

<div class="adapter-list">
  <ListHeader
    titleKey="adapters.title"
    createKey="adapters.create"
    on:create={handleCreate}
  />

  <div class="list">
    {#each adapters as adapter}
      <button
        class="adapter-item"
        class:selected={selectedAdapter === adapter.name}
        onclick={() => selectAdapter(adapter.name)}
        type="button"
        aria-pressed={selectedAdapter === adapter.name}
        aria-label="Select adapter {adapter.name}"
      >
        <div class="adapter-name">{adapter.name}</div>
        <div class="adapter-meta">
          <span class="connection">{adapter.connection}</span>
          <span class="source-type type-{adapter.source_type}"
            >{adapter.source_type}</span
          >
        </div>
        {#if adapter.description}
          <div class="adapter-description">{adapter.description}</div>
        {/if}
      </button>
    {/each}

    {#if adapters.length === 0}
      <EmptyState
        messageKey="adapters.empty.message"
        actionKey="adapters.empty.action"
        on:create={handleCreate}
      />
    {/if}
  </div>
</div>

<style>
  .adapter-list {
    height: 100%;
    display: flex;
    flex-direction: column;
    background: white;
    border-right: 1px solid #e0e0e0;
  }

  .list {
    flex: 1;
    overflow-y: auto;
  }

  .adapter-item {
    padding: 12px 16px;
    border-bottom: 1px solid #f0f0f0;
    cursor: pointer;
    transition: background-color 0.2s;
    width: 100%;
    text-align: left;
    background: none;
    border-left: none;
    border-right: none;
    border-top: none;
    font-family: inherit;
    font-size: inherit;
  }

  .adapter-item:hover {
    background-color: #f8f9fa;
  }

  .adapter-item.selected {
    background-color: #e8f5e9;
    border-right: 3px solid #27ae60;
  }

  .adapter-name {
    font-weight: 500;
    margin-bottom: 4px;
    color: #333;
  }

  .adapter-meta {
    display: flex;
    gap: 8px;
    margin-bottom: 4px;
  }

  .connection {
    font-size: 0.85rem;
    color: #666;
    background: #f0f0f0;
    padding: 2px 6px;
    border-radius: 3px;
  }

  .source-type {
    font-size: 0.85rem;
    padding: 2px 6px;
    border-radius: 3px;
    color: white;
  }

  .type-file {
    background: #3498db;
  }

  .type-database {
    background: #9b59b6;
  }

  .adapter-description {
    font-size: 0.85rem;
    color: #666;
    line-height: 1.3;
  }
</style>
