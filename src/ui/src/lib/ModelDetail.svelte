<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { FileText, Code, Play } from 'lucide-svelte';
  import type { ModelDetails } from './types';
  import { t } from './i18n';

  const dispatch = createEventDispatcher();

  let {
    model,
  }: {
    model?: ModelDetails | null;
  } = $props();

  function handleEdit() {
    if (model) {
      dispatch('edit', model);
    }
  }

  function handleDelete() {
    if (model && confirm(`モデル「${model.name}」を削除しますか？`)) {
      dispatch('delete', model.path);
    }
  }

  function handleRun() {
    if (model) {
      dispatch('run', model.path);
    }
  }
</script>

<div class="model-detail">
  {#if model}
    <div class="header">
      <div class="title-section">
        <FileText size={24} color="#3498db" />
        <div>
          <h2>{model.name}</h2>
          <p class="model-path">{model.path}</p>
        </div>
      </div>
      <div class="actions">
        <button class="run-btn" onclick={handleRun}>
          <Play size={16} />
          実行
        </button>
        <button class="edit-btn" onclick={handleEdit}>編集</button>
        <button class="delete-btn" onclick={handleDelete}>削除</button>
      </div>
    </div>

    <div class="content">
      {#if model.config.description}
        <div class="section">
          <h3>説明</h3>
          <p class="description">{model.config.description}</p>
        </div>
      {/if}

      <div class="section">
        <h3>
          <Code size={18} />
          SQL クエリ
        </h3>
        <div class="sql-code">
          <pre><code>{model.config.sql}</code></pre>
        </div>
      </div>

      {#if model.config.depends && model.config.depends.length > 0}
        <div class="section">
          <h3>依存関係</h3>
          <div class="dependencies">
            {#each model.config.depends as dep}
              <div class="dependency-item">{dep}</div>
            {/each}
          </div>
        </div>
      {/if}
    </div>
  {:else}
    <div class="empty-state">
      <p>{$t('common.select_model')}</p>
    </div>
  {/if}
</div>

<style>
  .model-detail {
    height: 100%;
    display: flex;
    flex-direction: column;
    background: white;
  }

  .header {
    padding: 24px;
    border-bottom: 1px solid #e0e0e0;
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
  }

  .title-section {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .title-section h2 {
    margin: 0;
    font-size: 1.5rem;
    color: #333;
  }

  .model-path {
    margin: 4px 0 0 0;
    color: #666;
    font-size: 0.9rem;
    font-family: monospace;
  }

  .actions {
    display: flex;
    gap: 8px;
  }

  .run-btn {
    background: #e67e22;
    color: white;
    border: none;
    padding: 8px 12px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.9rem;
    display: flex;
    align-items: center;
    gap: 4px;
  }

  .run-btn:hover {
    background: #d35400;
  }

  .edit-btn {
    background: #3498db;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.9rem;
  }

  .edit-btn:hover {
    background: #2980b9;
  }

  .delete-btn {
    background: #e74c3c;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.9rem;
  }

  .delete-btn:hover {
    background: #c0392b;
  }

  .content {
    flex: 1;
    padding: 24px;
    overflow-y: auto;
  }

  .section {
    margin-bottom: 32px;
  }

  .section h3 {
    margin: 0 0 16px 0;
    font-size: 1.1rem;
    color: #333;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .description {
    color: #666;
    line-height: 1.5;
    margin: 0;
  }

  .sql-code {
    background: #f8f9fa;
    border: 1px solid #e0e0e0;
    border-radius: 4px;
    overflow-x: auto;
  }

  .sql-code pre {
    margin: 0;
    padding: 16px;
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: 0.9rem;
    line-height: 1.4;
    color: #333;
  }

  .dependencies {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }

  .dependency-item {
    background: #e8f5e9;
    color: #27ae60;
    padding: 4px 8px;
    border-radius: 3px;
    font-size: 0.9rem;
    font-family: monospace;
  }

  .empty-state {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #999;
    font-size: 1.1rem;
  }
</style>
