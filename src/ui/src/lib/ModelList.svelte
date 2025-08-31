<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { FileText } from 'lucide-svelte';
  import type { ModelSummary } from './types';
  import { t } from './i18n';
  import ListHeader from './ListHeader.svelte';
  import EmptyState from './EmptyState.svelte';

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
  <ListHeader
    titleKey="models.title"
    createKey="models.create"
    on:create={handleCreate}
  />

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
          aria-label={$t('common.select_model_aria', {
            values: { path: model.path },
          })}
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
      <EmptyState
        messageKey="models.empty.message"
        actionKey="models.empty.action"
        on:create={handleCreate}
      />
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
</style>
