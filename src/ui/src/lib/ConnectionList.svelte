<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ConnectionSummary } from './types';
  import { t } from './i18n';
  import ListHeader from './ListHeader.svelte';
  import EmptyState from './EmptyState.svelte';

  const dispatch = createEventDispatcher();

  let {
    connections = [],
    selectedConnection,
  }: {
    connections?: ConnectionSummary[];
    selectedConnection?: string | null;
  } = $props();

  function selectConnection(name: string) {
    selectedConnection = name;
    dispatch('select', name);
  }

  function handleCreate() {
    dispatch('create');
  }

  function getConnectionIcon(type: string) {
    switch (type) {
      case 'sqlite':
        return '🗄️';
      case 'mysql':
        return '🐬';
      case 'postgresql':
        return '🐘';
      case 's3':
        return '☁️';
      case 'localfile':
        return '📁';
      default:
        return '🔗';
    }
  }
</script>

<div class="connection-list">
  <ListHeader
    titleKey="connections.title"
    createKey="connections.create"
    on:create={handleCreate}
  />

  <div class="list">
    {#each connections as connection}
      <button
        class="connection-item"
        class:selected={selectedConnection === connection.name}
        onclick={() => selectConnection(connection.name)}
        type="button"
        aria-pressed={selectedConnection === connection.name}
        aria-label="Select connection {connection.name}"
      >
        <div class="connection-header">
          <span class="connection-icon"
            >{getConnectionIcon(connection.connection_type)}</span
          >
          <div class="connection-info">
            <div class="connection-name">{connection.name}</div>
            <div class="connection-type">{connection.connection_type}</div>
          </div>
        </div>
        <div class="connection-details">{connection.details}</div>
      </button>
    {/each}

    {#if connections.length === 0}
      <EmptyState
        messageKey="connections.empty.message"
        actionKey="connections.empty.action"
        on:create={handleCreate}
      />
    {/if}
  </div>
</div>

<style>
  .connection-list {
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

  .connection-item {
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

  .connection-item:hover {
    background-color: #f8f9fa;
  }

  .connection-item.selected {
    background-color: #e8f5e9;
    border-right: 3px solid #27ae60;
  }

  .connection-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 4px;
  }

  .connection-icon {
    font-size: 1.5rem;
  }

  .connection-info {
    flex: 1;
  }

  .connection-name {
    font-weight: 500;
    color: #333;
  }

  .connection-type {
    font-size: 0.8rem;
    color: #666;
    text-transform: uppercase;
  }

  .connection-details {
    font-size: 0.85rem;
    color: #999;
    margin-left: 34px;
  }
</style>
