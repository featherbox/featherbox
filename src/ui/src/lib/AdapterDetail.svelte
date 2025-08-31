<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { AdapterDetails } from './types';

  const dispatch = createEventDispatcher();

  let {
    adapter,
  }: {
    adapter?: AdapterDetails | null;
  } = $props();

  function handleEdit() {
    if (adapter) {
      dispatch('edit', adapter);
    }
  }

  function handleDelete() {
    if (adapter && confirm(`アダプター「${adapter.name}」を削除しますか？`)) {
      dispatch('delete', adapter.name);
    }
  }
</script>

<div class="adapter-detail">
  {#if adapter}
    <div class="header">
      <h2>{adapter.name}</h2>
      <div class="actions">
        <button class="edit-btn" onclick={handleEdit}>編集</button>
        <button class="delete-btn" onclick={handleDelete}>削除</button>
      </div>
    </div>

    <div class="content">
      <div class="section">
        <h3>基本情報</h3>
        <div class="info-grid">
          <div class="info-item">
            <div class="label">接続名</div>
            <span>{adapter.config.connection}</span>
          </div>
          {#if adapter.config.description}
            <div class="info-item full-width">
              <div class="label">説明</div>
              <span>{adapter.config.description}</span>
            </div>
          {/if}
        </div>
      </div>

      <div class="section">
        <h3>データソース</h3>
        {#if adapter.config.source.type === 'file'}
          <div class="info-grid">
            <div class="info-item">
              <div class="label">ファイルパス</div>
              <span class="path">{adapter.config.source.file?.path}</span>
            </div>
            <div class="info-item">
              <div class="label">フォーマット</div>
              <span class="format">{adapter.config.source.format?.type}</span>
            </div>
            {#if adapter.config.source.file?.compression}
              <div class="info-item">
                <div class="label">圧縮</div>
                <span>{adapter.config.source.file?.compression}</span>
              </div>
            {/if}
            {#if adapter.config.source.format?.delimiter}
              <div class="info-item">
                <div class="label">区切り文字</div>
                <span>"{adapter.config.source.format?.delimiter}"</span>
              </div>
            {/if}
            {#if adapter.config.source.format?.has_header !== null && adapter.config.source.format?.has_header !== undefined}
              <div class="info-item">
                <div class="label">ヘッダー行</div>
                <span
                  >{adapter.config.source.format?.has_header
                    ? 'あり'
                    : 'なし'}</span
                >
              </div>
            {/if}
          </div>
        {:else if adapter.config.source.type === 'database'}
          <div class="info-grid">
            <div class="info-item">
              <div class="label">テーブル名</div>
              <span class="table-name">{adapter.config.source.table_name}</span>
            </div>
          </div>
        {/if}
      </div>

      <div class="section">
        <h3>カラム定義</h3>
        <div class="columns-table">
          <div class="table-header">
            <span>カラム名</span>
            <span>型</span>
            <span>説明</span>
          </div>
          {#each adapter.config.columns as column}
            <div class="table-row">
              <span class="column-name">{column.name}</span>
              <span class="column-type">{column.type}</span>
              <span class="column-description">{column.description || '-'}</span
              >
            </div>
          {/each}
        </div>
      </div>
    </div>
  {:else}
    <div class="empty-state">
      <p>アダプターを選択してください</p>
    </div>
  {/if}
</div>

<style>
  .adapter-detail {
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
    align-items: center;
  }

  .header h2 {
    margin: 0;
    font-size: 1.5rem;
    color: #333;
  }

  .actions {
    display: flex;
    gap: 8px;
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

  .edit-btn:hover:not(:disabled) {
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

  .delete-btn:hover:not(:disabled) {
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
  }

  .info-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
  }

  .info-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .info-item.full-width {
    grid-column: 1 / -1;
  }

  .info-item .label {
    font-size: 0.85rem;
    font-weight: 500;
    color: #666;
  }

  .info-item span {
    color: #333;
    word-break: break-all;
  }

  .path {
    font-family: monospace;
    background: #f8f9fa;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 0.9rem;
  }

  .format,
  .table-name {
    display: inline-block;
    background: #e8f5e9;
    color: #27ae60;
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 0.9rem;
    font-weight: 500;
  }

  .columns-table {
    border: 1px solid #e0e0e0;
    border-radius: 4px;
    overflow: hidden;
  }

  .table-header {
    display: grid;
    grid-template-columns: 1fr 120px 2fr;
    background: #f8f9fa;
    padding: 12px 16px;
    font-weight: 600;
    font-size: 0.9rem;
    color: #666;
    border-bottom: 1px solid #e0e0e0;
  }

  .table-row {
    display: grid;
    grid-template-columns: 1fr 120px 2fr;
    padding: 12px 16px;
    border-bottom: 1px solid #f0f0f0;
  }

  .table-row:last-child {
    border-bottom: none;
  }

  .column-name {
    font-family: monospace;
    font-weight: 500;
    color: #333;
  }

  .column-type {
    color: #9b59b6;
    font-weight: 500;
    font-size: 0.9rem;
  }

  .column-description {
    color: #666;
    font-size: 0.9rem;
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
