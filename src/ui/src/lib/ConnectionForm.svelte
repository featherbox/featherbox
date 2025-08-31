<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { Database, Folder, Cloud, Server } from 'lucide-svelte';
  import { t } from './i18n';
  import type { ConnectionDetails } from './types';
  import {
    createSecret,
    updateSecret,
    deleteSecret,
    getSecretInfo,
    generateUniqueSecretKey,
    extractSecretKey,
    createSecretReference,
    isSecretReference,
  } from './secretUtils';

  const dispatch = createEventDispatcher();

  let {
    isOpen = $bindable(false),
    mode = 'create',
    initialData = null,
  }: {
    isOpen?: boolean;
    mode?: 'create' | 'edit';
    initialData?: ConnectionDetails | null;
  } = $props();

  let name = $state('');
  let connectionType = $state('sqlite');

  let sqlitePath = $state('');
  let localfilePath = $state('');
  let mysqlHost = $state('localhost');
  let mysqlPort = $state(3306);
  let mysqlDatabase = $state('');
  let mysqlUsername = $state('');
  let mysqlPassword = $state('');
  let postgresHost = $state('localhost');
  let postgresPort = $state(5432);
  let postgresDatabase = $state('');
  let postgresUsername = $state('');
  let postgresPassword = $state('');
  let s3Bucket = $state('');
  let s3Region = $state('us-east-1');
  let s3EndpointUrl = $state('');
  let s3AuthMethod = $state<'credential_chain' | 'explicit'>(
    'credential_chain',
  );
  let s3AccessKeyId = $state('');
  let s3SecretAccessKey = $state('');
  let s3PathStyleAccess = $state(false);

  let mysqlPasswordSecret = $state('');
  let postgresPasswordSecret = $state('');
  let s3SecretAccessKeySecret = $state('');

  let mysqlPasswordChanged = $state(false);
  let postgresPasswordChanged = $state(false);
  let s3SecretAccessKeyChanged = $state(false);

  $effect(() => {
    if (initialData && mode === 'edit') {
      name = initialData.name;
      connectionType = initialData.type;

      if (connectionType === 'sqlite') {
        sqlitePath = initialData.path || '';
      } else if (connectionType === 'localfile') {
        localfilePath = initialData.base_path || '';
      } else if (connectionType === 'mysql') {
        mysqlHost = initialData.host || 'localhost';
        mysqlPort = initialData.port || 3306;
        mysqlDatabase = initialData.database || '';
        mysqlUsername = initialData.username || '';

        const passwordValue = initialData.password || '';
        if (isSecretReference(passwordValue)) {
          mysqlPasswordSecret = extractSecretKey(passwordValue) || '';
          mysqlPassword = '';
          loadSecretValue('mysql', 'password');
        } else {
          mysqlPassword = passwordValue;
          mysqlPasswordSecret = '';
        }
      } else if (connectionType === 'postgresql') {
        postgresHost = initialData.host || 'localhost';
        postgresPort = initialData.port || 5432;
        postgresDatabase = initialData.database || '';
        postgresUsername = initialData.username || '';

        const passwordValue = initialData.password || '';
        if (isSecretReference(passwordValue)) {
          postgresPasswordSecret = extractSecretKey(passwordValue) || '';
          postgresPassword = '';
          loadSecretValue('postgresql', 'password');
        } else {
          postgresPassword = passwordValue;
          postgresPasswordSecret = '';
        }
      } else if (connectionType === 's3') {
        s3Bucket = initialData.bucket || '';
        s3Region = initialData.region || 'us-east-1';
        s3EndpointUrl = initialData.endpoint_url || '';
        s3AuthMethod = initialData.auth_method || 'credential_chain';
        s3AccessKeyId = initialData.access_key_id || '';

        const secretAccessKeyValue = initialData.secret_access_key || '';
        if (isSecretReference(secretAccessKeyValue)) {
          s3SecretAccessKeySecret =
            extractSecretKey(secretAccessKeyValue) || '';
          s3SecretAccessKey = '';
          loadSecretValue('s3', 'secret_access_key');
        } else {
          s3SecretAccessKey = secretAccessKeyValue;
          s3SecretAccessKeySecret = '';
        }

        s3PathStyleAccess = initialData.path_style_access || false;
      }
    }
  });

  async function loadSecretValue(connType: string, field: string) {
    let secretKey = '';
    if (connType === 'mysql' && field === 'password') {
      secretKey = mysqlPasswordSecret;
    } else if (connType === 'postgresql' && field === 'password') {
      secretKey = postgresPasswordSecret;
    } else if (connType === 's3' && field === 'secret_access_key') {
      secretKey = s3SecretAccessKeySecret;
    }

    if (secretKey) {
      const secretInfo = await getSecretInfo(secretKey);
      if (secretInfo) {
        if (connType === 'mysql' && field === 'password') {
          mysqlPassword = `[${secretInfo.masked_value}]`;
        } else if (connType === 'postgresql' && field === 'password') {
          postgresPassword = `[${secretInfo.masked_value}]`;
        } else if (connType === 's3' && field === 'secret_access_key') {
          s3SecretAccessKey = `[${secretInfo.masked_value}]`;
        }
      }
    }
  }

  async function handleSecretCreation() {
    const secretsToProcess = [];

    if (connectionType === 'mysql' && mysqlPassword && mysqlPasswordChanged) {
      const secretKey = await generateUniqueSecretKey(
        name,
        'mysql',
        'password',
      );
      secretsToProcess.push({
        key: secretKey,
        value: mysqlPassword,
        field: 'mysql_password',
      });
    }

    if (
      connectionType === 'postgresql' &&
      postgresPassword &&
      postgresPasswordChanged
    ) {
      const secretKey = await generateUniqueSecretKey(
        name,
        'postgresql',
        'password',
      );
      secretsToProcess.push({
        key: secretKey,
        value: postgresPassword,
        field: 'postgres_password',
      });
    }

    if (
      connectionType === 's3' &&
      s3AuthMethod === 'explicit' &&
      s3SecretAccessKey &&
      s3SecretAccessKeyChanged
    ) {
      const secretKey = await generateUniqueSecretKey(
        name,
        's3',
        'secret_access_key',
      );
      secretsToProcess.push({
        key: secretKey,
        value: s3SecretAccessKey,
        field: 's3_secret_access_key',
      });
    }

    for (const secret of secretsToProcess) {
      if (mode === 'edit') {
        let existingKey = '';
        if (secret.field === 'mysql_password') {
          existingKey = mysqlPasswordSecret;
        } else if (secret.field === 'postgres_password') {
          existingKey = postgresPasswordSecret;
        } else if (secret.field === 's3_secret_access_key') {
          existingKey = s3SecretAccessKeySecret;
        }

        if (existingKey) {
          await updateSecret(existingKey, secret.value);
          continue;
        }
      }

      const success = await createSecret(secret.key, secret.value);
      if (success) {
        if (secret.field === 'mysql_password') {
          mysqlPasswordSecret = secret.key;
        } else if (secret.field === 'postgres_password') {
          postgresPasswordSecret = secret.key;
        } else if (secret.field === 's3_secret_access_key') {
          s3SecretAccessKeySecret = secret.key;
        }
      }
    }
  }

  async function handleSubmit() {
    let config: any = { type: connectionType };

    if (connectionType === 'sqlite') {
      config.path = sqlitePath;
    } else if (connectionType === 'localfile') {
      config.base_path = localfilePath;
    } else if (connectionType === 'mysql') {
      config.host = mysqlHost;
      config.port = mysqlPort;
      config.database = mysqlDatabase;
      config.username = mysqlUsername;
      await handleSecretCreation();
      config.password = mysqlPasswordSecret
        ? createSecretReference(mysqlPasswordSecret)
        : mysqlPassword;
    } else if (connectionType === 'postgresql') {
      config.host = postgresHost;
      config.port = postgresPort;
      config.database = postgresDatabase;
      config.username = postgresUsername;
      await handleSecretCreation();
      config.password = postgresPasswordSecret
        ? createSecretReference(postgresPasswordSecret)
        : postgresPassword;
    } else if (connectionType === 's3') {
      config.bucket = s3Bucket;
      config.region = s3Region;
      if (s3EndpointUrl) config.endpoint_url = s3EndpointUrl;
      config.auth_method = s3AuthMethod;
      if (s3AuthMethod === 'explicit') {
        config.access_key_id = s3AccessKeyId;
        await handleSecretCreation();
        config.secret_access_key = s3SecretAccessKeySecret
          ? createSecretReference(s3SecretAccessKeySecret)
          : s3SecretAccessKey;
      }
      if (s3PathStyleAccess) config.path_style_access = true;
    }

    dispatch('submit', { name, config });
    handleClose();
  }

  function handleClose() {
    isOpen = false;
    name = '';
    connectionType = 'sqlite';
    sqlitePath = '';
    localfilePath = '';
    mysqlHost = 'localhost';
    mysqlPort = 3306;
    mysqlDatabase = '';
    mysqlUsername = '';
    mysqlPassword = '';
    postgresHost = 'localhost';
    postgresPort = 5432;
    postgresDatabase = '';
    postgresUsername = '';
    postgresPassword = '';
    s3Bucket = '';
    s3Region = 'us-east-1';
    s3EndpointUrl = '';
    s3AuthMethod = 'credential_chain';
    s3AccessKeyId = '';
    s3SecretAccessKey = '';
    s3PathStyleAccess = false;
    dispatch('close');
  }

  function isFormValid() {
    if (!name.trim()) return false;

    switch (connectionType) {
      case 'sqlite':
        return !!sqlitePath.trim();
      case 'localfile':
        return !!localfilePath.trim();
      case 'mysql':
        return !!mysqlDatabase.trim() && !!mysqlUsername.trim();
      case 'postgresql':
        return !!postgresDatabase.trim() && !!postgresUsername.trim();
      case 's3':
        return (
          !!s3Bucket.trim() &&
          (s3AuthMethod === 'credential_chain' ||
            (!!s3AccessKeyId.trim() && !!s3SecretAccessKey.trim()))
        );
      default:
        return false;
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
          {mode === 'create' ? '新規接続作成' : '接続編集'}
        </h2>
        <button class="close-btn" onclick={handleClose}>×</button>
      </div>

      <div class="modal-content">
        <div class="form-group">
          <label for="name">接続名</label>
          <input
            id="name"
            type="text"
            bind:value={name}
            placeholder="例: my_database"
            disabled={mode === 'edit'}
          />
        </div>

        <div class="form-group">
          <fieldset>
            <legend>接続タイプ</legend>
            <div class="connection-types">
              <button
                type="button"
                class="type-btn"
                class:active={connectionType === 'sqlite'}
                onclick={() => (connectionType = 'sqlite')}
              >
                <Database size={20} />
                <span>SQLite</span>
              </button>
              <button
                type="button"
                class="type-btn"
                class:active={connectionType === 'localfile'}
                onclick={() => (connectionType = 'localfile')}
              >
                <Folder size={20} />
                <span>ローカルファイル</span>
              </button>
              <button
                type="button"
                class="type-btn"
                class:active={connectionType === 'mysql'}
                onclick={() => (connectionType = 'mysql')}
              >
                <Server size={20} />
                <span>MySQL</span>
              </button>
              <button
                type="button"
                class="type-btn"
                class:active={connectionType === 'postgresql'}
                onclick={() => (connectionType = 'postgresql')}
              >
                <Server size={20} />
                <span>PostgreSQL</span>
              </button>
              <button
                type="button"
                class="type-btn"
                class:active={connectionType === 's3'}
                onclick={() => (connectionType = 's3')}
              >
                <Cloud size={20} />
                <span>S3</span>
              </button>
            </div>
          </fieldset>
        </div>

        {#if connectionType === 'sqlite'}
          <div class="form-group">
            <label for="sqlitePath">データベースパス</label>
            <input
              id="sqlitePath"
              type="text"
              bind:value={sqlitePath}
              placeholder="例: ./database.db"
            />
            <div class="help-text">
              SQLiteデータベースファイルへのパスを入力してください
            </div>
          </div>
        {:else if connectionType === 'mysql'}
          <div class="form-group">
            <label for="mysqlHost">ホスト</label>
            <input
              id="mysqlHost"
              type="text"
              bind:value={mysqlHost}
              placeholder="例: localhost"
            />
          </div>
          <div class="form-group">
            <label for="mysqlPort">ポート</label>
            <input
              id="mysqlPort"
              type="number"
              bind:value={mysqlPort}
              placeholder="3306"
            />
          </div>
          <div class="form-group">
            <label for="mysqlDatabase">データベース名</label>
            <input
              id="mysqlDatabase"
              type="text"
              bind:value={mysqlDatabase}
              placeholder="例: myapp_db"
            />
          </div>
          <div class="form-group">
            <label for="mysqlUsername">ユーザー名</label>
            <input
              id="mysqlUsername"
              type="text"
              bind:value={mysqlUsername}
              placeholder="例: root"
            />
          </div>
          <div class="form-group">
            <label for="mysqlPassword">パスワード</label>
            <input
              id="mysqlPassword"
              type="password"
              bind:value={mysqlPassword}
              placeholder="パスワード"
              oninput={() => (mysqlPasswordChanged = true)}
            />
            {#if mysqlPasswordSecret && !mysqlPasswordChanged}
              <div class="help-text secret-info">
                🔒 シークレットで管理されています
              </div>
            {/if}
          </div>
        {:else if connectionType === 'postgresql'}
          <div class="form-group">
            <label for="postgresHost">ホスト</label>
            <input
              id="postgresHost"
              type="text"
              bind:value={postgresHost}
              placeholder="例: localhost"
            />
          </div>
          <div class="form-group">
            <label for="postgresPort">ポート</label>
            <input
              id="postgresPort"
              type="number"
              bind:value={postgresPort}
              placeholder="5432"
            />
          </div>
          <div class="form-group">
            <label for="postgresDatabase">データベース名</label>
            <input
              id="postgresDatabase"
              type="text"
              bind:value={postgresDatabase}
              placeholder="例: myapp_db"
            />
          </div>
          <div class="form-group">
            <label for="postgresUsername">ユーザー名</label>
            <input
              id="postgresUsername"
              type="text"
              bind:value={postgresUsername}
              placeholder="例: postgres"
            />
          </div>
          <div class="form-group">
            <label for="postgresPassword">パスワード</label>
            <input
              id="postgresPassword"
              type="password"
              bind:value={postgresPassword}
              placeholder="パスワード"
              oninput={() => (postgresPasswordChanged = true)}
            />
            {#if postgresPasswordSecret && !postgresPasswordChanged}
              <div class="help-text secret-info">
                🔒 シークレットで管理されています
              </div>
            {/if}
          </div>
        {:else if connectionType === 's3'}
          <div class="form-group">
            <label for="s3Bucket">バケット名</label>
            <input
              id="s3Bucket"
              type="text"
              bind:value={s3Bucket}
              placeholder="例: my-data-bucket"
            />
          </div>
          <div class="form-group">
            <label for="s3Region">リージョン</label>
            <input
              id="s3Region"
              type="text"
              bind:value={s3Region}
              placeholder="例: us-east-1"
            />
          </div>
          <div class="form-group">
            <label for="s3EndpointUrl">エンドポイントURL（オプション）</label>
            <input
              id="s3EndpointUrl"
              type="text"
              bind:value={s3EndpointUrl}
              placeholder="例: http://localhost:9000 (MinIOなど)"
            />
          </div>
          <div class="form-group">
            <fieldset>
              <legend>認証方法</legend>
              <div class="radio-group">
                <label class="radio-label">
                  <input
                    type="radio"
                    bind:group={s3AuthMethod}
                    value="credential_chain"
                  />
                  AWS認証チェーン
                </label>
                <label class="radio-label">
                  <input
                    type="radio"
                    bind:group={s3AuthMethod}
                    value="explicit"
                  />
                  アクセスキー指定
                </label>
              </div>
            </fieldset>
          </div>
          {#if s3AuthMethod === 'explicit'}
            <div class="form-group">
              <label for="s3AccessKeyId">アクセスキーID</label>
              <input
                id="s3AccessKeyId"
                type="text"
                bind:value={s3AccessKeyId}
                placeholder="AKIA..."
              />
            </div>
            <div class="form-group">
              <label for="s3SecretAccessKey">シークレットアクセスキー</label>
              <input
                id="s3SecretAccessKey"
                type="password"
                bind:value={s3SecretAccessKey}
                placeholder="シークレットキー"
                oninput={() => (s3SecretAccessKeyChanged = true)}
              />
              {#if s3SecretAccessKeySecret && !s3SecretAccessKeyChanged}
                <div class="help-text secret-info">
                  🔒 シークレットで管理されています
                </div>
              {/if}
            </div>
          {/if}
          <div class="form-group">
            <label class="checkbox-label">
              <input type="checkbox" bind:checked={s3PathStyleAccess} />
              パススタイルアクセスを使用（MinIOなど）
            </label>
          </div>
        {:else if connectionType === 'localfile'}
          <div class="form-group">
            <label for="localfilePath">ベースパス</label>
            <input
              id="localfilePath"
              type="text"
              bind:value={localfilePath}
              placeholder="例: ./data"
            />
            <div class="help-text">
              ファイルが格納されているディレクトリへのパスを入力してください
            </div>
          </div>
        {/if}
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

  .form-group input {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 0.95rem;
  }

  .form-group input:focus {
    outline: none;
    border-color: #27ae60;
  }

  .form-group input:disabled {
    background: #f5f5f5;
    cursor: not-allowed;
  }

  .connection-types {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    gap: 10px;
  }

  .type-btn {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 12px;
    border: 2px solid #ddd;
    border-radius: 8px;
    background: white;
    cursor: pointer;
    transition: all 0.2s;
    gap: 4px;
  }

  .type-btn:hover {
    border-color: #27ae60;
    background: #f8f9fa;
  }

  .type-btn.active {
    border-color: #27ae60;
    background: #e8f5e9;
    color: #27ae60;
  }

  .type-btn span {
    font-size: 0.85rem;
    font-weight: 500;
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

  .checkbox-label {
    display: flex;
    align-items: center;
    cursor: pointer;
  }

  .checkbox-label input {
    width: auto;
    margin-right: 6px;
  }

  .help-text {
    margin-top: 4px;
    font-size: 0.85rem;
    color: #666;
  }

  .secret-info {
    color: #27ae60;
    font-weight: 500;
    background: #e8f5e9;
    padding: 4px 8px;
    border-radius: 4px;
    display: inline-block;
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
