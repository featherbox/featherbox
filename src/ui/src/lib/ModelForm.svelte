<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { t } from './i18n';
  import type { ModelDetails } from './types';

  const dispatch = createEventDispatcher();

  let {
    isOpen = $bindable(false),
    mode = 'create',
    initialData = null,
  }: {
    isOpen?: boolean;
    mode?: 'create' | 'edit';
    initialData?: ModelDetails | null;
  } = $props();

  let name = $state('');
  let path = $state('');
  let description = $state('');
  let sql = $state('');
  let depends = $state('');

  $effect(() => {
    if (initialData && mode === 'edit') {
      name = initialData.name;
      path = initialData.path;
      description = initialData.config.description || '';
      sql = initialData.config.sql || '';
      depends = initialData.config.depends?.join(', ') || '';
    }
  });

  function handleSubmit() {
    const config: any = {
      sql: sql.trim(),
    };

    if (description.trim()) {
      config.description = description.trim();
    }

    if (depends.trim()) {
      config.depends = depends
        .split(',')
        .map((d) => d.trim())
        .filter((d) => d);
    }

    const modelPath = mode === 'create' ? path.trim() : initialData?.path || '';
    const modelName = mode === 'create' ? name.trim() : initialData?.name || '';

    dispatch('submit', {
      name: modelName,
      path: modelPath,
      config,
    });
    handleClose();
  }

  function handleClose() {
    isOpen = false;
    name = '';
    path = '';
    description = '';
    sql = '';
    depends = '';
    dispatch('close');
  }

  function isFormValid() {
    if (mode === 'create') {
      return name.trim() && path.trim() && sql.trim();
    } else {
      return sql.trim();
    }
  }
</script>

{#if isOpen}
  <div
    class="modal-overlay"
    onclick={handleClose}
    onkeydown={(e) => e.key === 'Escape' && handleClose()}
    tabindex="0"
    role="button"
    aria-label={$t('common.close_modal_aria')}
  >
    <div
      class="modal large"
      onclick={(e) => e.stopPropagation()}
      onkeydown={(e) => e.stopPropagation()}
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-title"
      tabindex="-1"
    >
      <div class="modal-header">
        <h2 id="modal-title">
          {mode === 'create' ? '新規モデル作成' : 'モデル編集'}
        </h2>
        <button class="close-btn" onclick={handleClose}>×</button>
      </div>

      <div class="modal-content">
        {#if mode === 'create'}
          <div class="form-row">
            <div class="form-group">
              <label for="name">モデル名</label>
              <input
                id="name"
                type="text"
                bind:value={name}
                placeholder="例: user_stats"
              />
            </div>
            <div class="form-group">
              <label for="path">パス</label>
              <input
                id="path"
                type="text"
                bind:value={path}
                placeholder="例: marts/user_stats"
              />
            </div>
          </div>
        {/if}

        <div class="form-group">
          <label for="description">説明（任意）</label>
          <textarea
            id="description"
            bind:value={description}
            placeholder="このモデルの説明を入力"
            rows="2"
          ></textarea>
        </div>

        <div class="form-group">
          <label for="sql">SQLクエリ</label>
          <textarea
            id="sql"
            bind:value={sql}
            placeholder="SELECT * FROM users WHERE created_at >= '2024-01-01'"
            rows="10"
            class="sql-editor"
          ></textarea>
        </div>

        <div class="form-group">
          <label for="depends">依存関係（任意）</label>
          <input
            id="depends"
            type="text"
            bind:value={depends}
            placeholder="例: staging.users, staging.orders（カンマ区切り）"
          />
        </div>
      </div>

      <div class="modal-footer">
        <button class="btn-cancel" onclick={handleClose}>キャンセル</button>
        <button
          class="btn-submit"
          onclick={handleSubmit}
          disabled={!isFormValid()}
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
    max-width: 800px;
    max-height: 90vh;
    display: flex;
    flex-direction: column;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  }

  .modal.large {
    max-width: 900px;
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

  .form-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 20px;
  }

  .form-group {
    margin-bottom: 20px;
  }

  .form-group label {
    display: block;
    margin-bottom: 6px;
    font-size: 0.9rem;
    font-weight: 500;
    color: #333;
  }

  .form-group input,
  .form-group textarea {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 0.95rem;
    font-family: inherit;
  }

  .form-group input:focus,
  .form-group textarea:focus {
    outline: none;
    border-color: #27ae60;
  }

  .sql-editor {
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: 0.9rem;
    line-height: 1.4;
    resize: vertical;
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
