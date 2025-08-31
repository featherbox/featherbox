<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ConnectionSummary } from './types';

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
  <div class="header">
    <h3>Connections</h3>
    <button class="create-btn" onclick={handleCreate}>新規作成</button>
  </div>

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
      <div class="empty-state">
        <p>接続がありません</p>
        <button onclick={handleCreate}>最初の接続を作成</button>
      </div>
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

  .header {
    padding: 16px;
    border-bottom: 1px solid #e0e0e0;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .header h3 {
    margin: 0;
    font-size: 1.1rem;
    font-weight: 600;
    color: #333;
  }

  .create-btn {
    background: #27ae60;
    color: white;
    border: none;
    padding: 6px 12px;
    border-radius: 4px;
    font-size: 0.9rem;
    cursor: pointer;
    transition: background 0.2s;
  }

  .create-btn:hover {
    background: #2ecc71;
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

  .empty-state {
    padding: 40px 16px;
    text-align: center;
    color: #666;
  }

  .empty-state p {
    margin-bottom: 16px;
  }

  .empty-state button {
    background: #27ae60;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
  }

  .empty-state button:hover {
    background: #2ecc71;
  }
</style>
