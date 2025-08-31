<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ConnectionDetails } from './types';
  import { t } from './i18n';

  const dispatch = createEventDispatcher();

  let {
    connection,
  }: {
    connection?: ConnectionDetails | null;
  } = $props();

  function handleEdit() {
    if (connection) {
      dispatch('edit', connection);
    }
  }

  function handleDelete() {
    if (connection && confirm(`接続「${connection.name}」を削除しますか？`)) {
      dispatch('delete', connection.name);
    }
  }
</script>

<div class="connection-detail">
  {#if connection}
    <div class="header">
      <h2>{connection.name}</h2>
      <div class="actions">
        <button class="edit-btn" onclick={handleEdit}>編集</button>
        <button class="delete-btn" onclick={handleDelete}>削除</button>
      </div>
    </div>

    <div class="content">
      <div class="section">
        <h3>接続情報</h3>
        <div class="info-grid">
          <div class="info-item">
            <div class="label">タイプ</div>
            <span class="connection-type">{connection.type}</span>
          </div>

          {#if connection.type === 'sqlite'}
            <div class="info-item full-width">
              <div class="label">データベースパス</div>
              <span class="path">{connection.path}</span>
            </div>
          {:else if connection.type === 'localfile'}
            <div class="info-item full-width">
              <div class="label">ベースパス</div>
              <span class="path">{connection.base_path}</span>
            </div>
          {:else if connection.type === 'mysql' || connection.type === 'postgresql'}
            <div class="info-item">
              <div class="label">ホスト</div>
              <span>{connection.host}</span>
            </div>
            <div class="info-item">
              <div class="label">ポート</div>
              <span>{connection.port}</span>
            </div>
            <div class="info-item">
              <div class="label">データベース</div>
              <span>{connection.database}</span>
            </div>
            <div class="info-item">
              <div class="label">ユーザー名</div>
              <span>{connection.username}</span>
            </div>
          {:else if connection.type === 's3'}
            <div class="info-item">
              <div class="label">バケット</div>
              <span>{connection.bucket}</span>
            </div>
            <div class="info-item">
              <div class="label">リージョン</div>
              <span>{connection.region || 'デフォルト'}</span>
            </div>
            {#if connection.endpoint_url}
              <div class="info-item full-width">
                <div class="label">エンドポイントURL</div>
                <span class="path">{connection.endpoint_url}</span>
              </div>
            {/if}
          {/if}
        </div>
      </div>

      {#if connection.type === 'sqlite'}
        <div class="section">
          <h3>接続テスト</h3>
          <div class="test-info">
            <p>SQLiteデータベースファイルへの接続をテストできます。</p>
            <button class="test-btn">接続テスト</button>
          </div>
        </div>
      {/if}
    </div>
  {:else}
    <div class="empty-state">
      <p>{$t('common.select_connection')}</p>
    </div>
  {/if}
</div>

<style>
  .connection-detail {
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
  }

  .connection-type {
    display: inline-block;
    background: #3498db;
    color: white;
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 0.9rem;
    font-weight: 500;
    text-transform: uppercase;
  }

  .path {
    font-family: monospace;
    background: #f8f9fa;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 0.9rem;
  }

  .test-info {
    background: #f8f9fa;
    padding: 16px;
    border-radius: 4px;
  }

  .test-info p {
    margin: 0 0 12px 0;
    color: #666;
  }

  .test-btn {
    background: #27ae60;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.9rem;
  }

  .test-btn:hover {
    background: #2ecc71;
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
