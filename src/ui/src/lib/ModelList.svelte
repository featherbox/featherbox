<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { FileText } from 'lucide-svelte';
  import type { ModelSummary } from './types';

  const dispatch = createEventDispatcher();

  let {
    models = [],
    selectedModel,
  }: {
    models?: ModelSummary[];
    selectedModel?: string | null;
  } = $props();

  function selectModel(path: string) {
    selectedModel = path;
    dispatch('select', path);
  }

  function handleCreate() {
    dispatch('create');
  }

  function getModelFolder(path: string) {
    const parts = path.split('/');
    return parts.length > 1 ? parts.slice(0, -1).join('/') : '';
  }

  function getModelName(path: string) {
    const parts = path.split('/');
    return parts[parts.length - 1];
  }

  const groupedModels = $derived(
    models.reduce(
      (acc, model) => {
        const folder = getModelFolder(model.path);
        if (!acc[folder]) {
          acc[folder] = [];
        }
        acc[folder].push(model);
        return acc;
      },
      {} as Record<string, ModelSummary[]>,
    ),
  );
</script>

<div class="model-list">
  <div class="header">
    <h3>Models</h3>
    <button class="create-btn" onclick={handleCreate}>新規作成</button>
  </div>

  <div class="list">
    {#each Object.entries(groupedModels) as [folder, folderModels]}
      {#if folder}
        <div class="folder-section">
          <div class="folder-header">{folder}/</div>
        </div>
      {/if}
      {#each folderModels as model}
        <button
          class="model-item"
          class:selected={selectedModel === model.path}
          onclick={() => selectModel(model.path)}
          type="button"
          aria-pressed={selectedModel === model.path}
          aria-label="Select model {model.path}"
        >
          <div class="model-header">
            <FileText size={18} />
            <div class="model-info">
              <div class="model-name">{getModelName(model.path)}</div>
              <div class="model-path">{model.path}</div>
            </div>
          </div>
          {#if model.description}
            <div class="model-description">{model.description}</div>
          {/if}
        </button>
      {/each}
    {/each}

    {#if models.length === 0}
      <div class="empty-state">
        <p>モデルがありません</p>
        <button onclick={handleCreate}>最初のモデルを作成</button>
      </div>
    {/if}
  </div>
</div>

<style>
  .model-list {
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

  .folder-section {
    background: #f8f9fa;
    border-bottom: 1px solid #e0e0e0;
  }

  .folder-header {
    padding: 8px 16px;
    font-size: 0.85rem;
    font-weight: 500;
    color: #666;
  }

  .model-item {
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

  .model-item:hover {
    background-color: #f8f9fa;
  }

  .model-item.selected {
    background-color: #e8f5e9;
    border-right: 3px solid #27ae60;
  }

  .model-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 4px;
    color: #3498db;
  }

  .model-info {
    flex: 1;
  }

  .model-name {
    font-weight: 500;
    color: #333;
  }

  .model-path {
    font-size: 0.8rem;
    color: #666;
    font-family: monospace;
  }

  .model-description {
    font-size: 0.85rem;
    color: #666;
    line-height: 1.3;
    margin-left: 28px;
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
