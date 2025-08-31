<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { AdapterConfig } from './types';

  const dispatch = createEventDispatcher();

  let {
    isOpen = $bindable(false),
    mode = 'create',
    initialData = null,
  }: {
    isOpen?: boolean;
    mode?: 'create' | 'edit';
    initialData?: { name: string; config: AdapterConfig } | null;
  } = $props();

  let name = $state('');
  let connection = $state('local');
  let description = $state('');
  let sourceType = $state<'file' | 'database'>('file');
  let filePath = $state('');
  let formatType = $state('csv');
  let tableName = $state('');

  $effect(() => {
    if (initialData && mode === 'edit') {
      name = initialData.name;
      connection = initialData.config.connection;
      description = initialData.config.description || '';
      sourceType = initialData.config.source.type;
      if (sourceType === 'file' && initialData.config.source.file) {
        filePath = initialData.config.source.file.path;
        formatType = initialData.config.source.format?.type || 'csv';
      } else if (
        sourceType === 'database' &&
        initialData.config.source.table_name
      ) {
        tableName = initialData.config.source.table_name;
      }
    }
  });

  function handleSubmit() {
    const config: AdapterConfig = {
      connection,
      description: description || undefined,
      source:
        sourceType === 'file'
          ? {
              type: 'file',
              file: {
                path: filePath,
              },
              format: {
                type: formatType,
                has_header: formatType === 'csv' ? true : undefined,
              },
            }
          : {
              type: 'database',
              table_name: tableName,
            },
      columns: [],
    };

    dispatch('submit', { name, config });
    handleClose();
  }

  function handleClose() {
    isOpen = false;
    name = '';
    connection = 'local';
    description = '';
    sourceType = 'file';
    filePath = '';
    formatType = 'csv';
    tableName = '';
    dispatch('close');
  }
</script>

{#if isOpen}
  <div
    class="modal-overlay"
    onclick={handleClose}
    onkeydown={(e) => e.key === 'Escape' && handleClose()}
    tabindex="0"
    role="button"
    aria-label="Close modal"
  >
    <div
      class="modal"
      onclick={(e) => e.stopPropagation()}
      onkeydown={(e) => e.stopPropagation()}
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-title"
      tabindex="-1"
    >
      <div class="modal-header">
        <h2 id="modal-title">
          {mode === 'create' ? '新規アダプター作成' : 'アダプター編集'}
        </h2>
        <button class="close-btn" onclick={handleClose}>×</button>
      </div>

      <div class="modal-content">
        <div class="form-group">
          <label for="name">アダプター名</label>
          <input
            id="name"
            type="text"
            bind:value={name}
            placeholder="例: sales_data"
            disabled={mode === 'edit'}
          />
        </div>

        <div class="form-group">
          <label for="connection">接続名</label>
          <input
            id="connection"
            type="text"
            bind:value={connection}
            placeholder="例: local"
          />
        </div>

        <div class="form-group">
          <label for="description">説明（任意）</label>
          <textarea
            id="description"
            bind:value={description}
            placeholder="このアダプターの説明を入力"
            rows="3"
          ></textarea>
        </div>

        <div class="form-group">
          <fieldset>
            <legend>データソースタイプ</legend>
            <div class="radio-group">
              <label class="radio-label">
                <input type="radio" bind:group={sourceType} value="file" />
                ファイル
              </label>
              <label class="radio-label">
                <input type="radio" bind:group={sourceType} value="database" />
                データベース
              </label>
            </div>
          </fieldset>
        </div>

        {#if sourceType === 'file'}
          <div class="form-group">
            <label for="filePath">ファイルパス</label>
            <input
              id="filePath"
              type="text"
              bind:value={filePath}
              placeholder="例: data/sales_*.csv"
            />
          </div>

          <div class="form-group">
            <label for="formatType">フォーマット</label>
            <select id="formatType" bind:value={formatType}>
              <option value="csv">CSV</option>
              <option value="json">JSON</option>
              <option value="parquet">Parquet</option>
            </select>
          </div>
        {:else}
          <div class="form-group">
            <label for="tableName">テーブル名</label>
            <input
              id="tableName"
              type="text"
              bind:value={tableName}
              placeholder="例: users"
            />
          </div>
        {/if}
      </div>

      <div class="modal-footer">
        <button class="btn-cancel" onclick={handleClose}>キャンセル</button>
        <button
          class="btn-submit"
          onclick={handleSubmit}
          disabled={!name || (sourceType === 'file' ? !filePath : !tableName)}
        >
          {mode === 'create' ? '作成' : '更新'}
        </button>
      </div>
    </div>
  </div>
{/if}

<style>
  .modal-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.5);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
    cursor: pointer;
  }

  .modal-overlay:focus {
    outline: 2px solid #27ae60;
    outline-offset: -2px;
  }

  .modal {
    background: white;
    border-radius: 8px;
    width: 90%;
    max-width: 500px;
    max-height: 90vh;
    display: flex;
    flex-direction: column;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  }

  .modal-header {
    padding: 20px;
    border-bottom: 1px solid #e0e0e0;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .modal-header h2 {
    margin: 0;
    font-size: 1.25rem;
    color: #333;
  }

  .close-btn {
    background: none;
    border: none;
    font-size: 1.5rem;
    color: #666;
    cursor: pointer;
    padding: 0;
    width: 30px;
    height: 30px;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .close-btn:hover {
    color: #333;
  }

  .modal-content {
    padding: 20px;
    overflow-y: auto;
    flex: 1;
  }

  .form-group {
    margin-bottom: 20px;
  }

  .form-group label,
  .form-group legend {
    display: block;
    margin-bottom: 6px;
    font-size: 0.9rem;
    font-weight: 500;
    color: #333;
  }

  .form-group input,
  .form-group textarea,
  .form-group select {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 0.95rem;
  }

  .form-group input:focus,
  .form-group textarea:focus,
  .form-group select:focus {
    outline: none;
    border-color: #27ae60;
  }

  .form-group input:disabled {
    background: #f5f5f5;
    cursor: not-allowed;
  }

  fieldset {
    border: none;
    padding: 0;
    margin: 0;
  }

  .radio-group {
    display: flex;
    gap: 20px;
  }

  .radio-label {
    display: flex;
    align-items: center;
    font-weight: normal;
    cursor: pointer;
  }

  .radio-label input {
    width: auto;
    margin-right: 6px;
  }

  .modal-footer {
    padding: 20px;
    border-top: 1px solid #e0e0e0;
    display: flex;
    justify-content: flex-end;
    gap: 10px;
  }

  .btn-cancel,
  .btn-submit {
    padding: 8px 16px;
    border-radius: 4px;
    font-size: 0.95rem;
    cursor: pointer;
    border: none;
  }

  .btn-cancel {
    background: #f5f5f5;
    color: #666;
  }

  .btn-cancel:hover {
    background: #e0e0e0;
  }

  .btn-submit {
    background: #27ae60;
    color: white;
  }

  .btn-submit:hover:not(:disabled) {
    background: #2ecc71;
  }

  .btn-submit:disabled {
    background: #bdc3c7;
    cursor: not-allowed;
  }
</style>
