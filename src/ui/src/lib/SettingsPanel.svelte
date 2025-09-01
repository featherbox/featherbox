<script lang="ts">
  import { t, locale } from './i18n';
  import { API_BASE_URL } from './config';

  interface ChatConfig {
    api_key: string;
    model: string;
  }

  let config = $state<ChatConfig>({
    api_key: '',
    model: 'gemini-2.5-flash',
  });

  let isSaving = $state(false);
  let saveStatus = $state<string | null>(null);
  let currentLocale = $state($locale);

  const availableModels = [
    'gemini-2.5-pro',
    'gemini-2.5-flash',
    'gemini-2.5-flash-lite',
    'gemini-2.0-flash',
    'gemini-2.0-flash-lite',
    'gemini-1.5-pro',
    'gemini-1.5-flash',
    'gemini-1.5-flash-8b',
  ];

  $effect(() => {
    loadConfig();
  });

  $effect(() => {
    currentLocale = $locale;
  });

  async function loadConfig() {
    try {
      const response = await fetch(`${API_BASE_URL}/api/chat/config`);
      if (response.ok) {
        config = await response.json();
      }
    } catch (error) {
      console.error('Failed to load config:', error);
    }
  }

  async function saveConfig() {
    if (!config.api_key.trim()) {
      saveStatus = $t('settings.api_key_required');
      return;
    }

    isSaving = true;
    saveStatus = null;

    try {
      const response = await fetch(`${API_BASE_URL}/api/chat/config`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
      });

      if (response.ok) {
        saveStatus = $t('settings.save_success');
        setTimeout(() => {
          saveStatus = null;
        }, 3000);
      } else {
        saveStatus = $t('settings.save_failed');
      }
    } catch (error) {
      console.error('Config save error:', error);
      saveStatus = `${$t('settings.save_error')}: ${error}`;
    } finally {
      isSaving = false;
    }
  }
</script>

<div class="settings-panel">
  <div class="page-header">
    <h1 class="page-title">{$t('settings.title')}</h1>
    <p class="page-description">{$t('settings.description')}</p>
  </div>

  <div class="settings-content">
    <div class="settings-section">
      <h2 class="section-title">{$t('settings.language.title')}</h2>
      <div class="setting-group">
        <label for="language">{$t('settings.language.description')}</label>
        <select
          id="language"
          bind:value={currentLocale}
          onchange={() => locale.set(currentLocale)}
        >
          <option value="ja">日本語</option>
          <option value="en">English</option>
        </select>
      </div>
    </div>

    <div class="settings-section">
      <h2 class="section-title">{$t('settings.ai_config.title')}</h2>

      <div class="setting-group">
        <label for="model">{$t('settings.ai_config.model')}</label>
        <select id="model" bind:value={config.model}>
          {#each availableModels as model}
            <option value={model}>{model}</option>
          {/each}
        </select>
        <div class="setting-help">{$t('settings.ai_config.model_help')}</div>
      </div>

      <div class="setting-group">
        <label for="apikey">{$t('settings.ai_config.api_key')}</label>
        <input
          id="apikey"
          type="password"
          bind:value={config.api_key}
          placeholder={$t('settings.ai_config.api_key_placeholder')}
        />
        <div class="setting-help">
          {$t('settings.ai_config.api_key_help')}:
          <a
            href="https://aistudio.google.com/app/apikey"
            target="_blank"
            rel="noopener noreferrer"
          >
            https://aistudio.google.com/app/apikey
          </a>
        </div>
      </div>

      <div class="setting-group">
        <button
          onclick={saveConfig}
          class="save-button"
          disabled={isSaving || !config.api_key.trim()}
        >
          {isSaving ? $t('settings.saving') : $t('settings.save_settings')}
        </button>

        {#if saveStatus}
          <div
            class="save-status"
            class:success={saveStatus === $t('settings.save_success')}
            class:error={saveStatus !== $t('settings.save_success')}
          >
            {saveStatus}
          </div>
        {/if}
      </div>
    </div>
  </div>
</div>

<style>
  .settings-panel {
    padding: 2rem;
    max-width: 800px;
    margin: 0 auto;
  }

  .page-header {
    margin-bottom: 2rem;
  }

  .page-title {
    font-size: 2rem;
    margin-bottom: 0.5rem;
    color: #2c3e50;
  }

  .page-description {
    color: #7f8c8d;
    margin: 0;
  }

  .settings-section {
    background: white;
    border-radius: 8px;
    padding: 2rem;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    margin-bottom: 1.5rem;
  }

  .section-title {
    font-size: 1.5rem;
    margin-bottom: 1.5rem;
    color: #2c3e50;
  }

  .setting-group {
    margin-bottom: 1.5rem;
  }

  label {
    display: block;
    margin-bottom: 0.5rem;
    font-weight: 500;
    color: #2c3e50;
  }

  select,
  input {
    width: 100%;
    padding: 0.75rem;
    border: 1px solid #e0e0e0;
    border-radius: 6px;
    font-size: 1rem;
    transition: border-color 0.2s ease;
    font-family: inherit;
  }

  select:focus,
  input:focus {
    outline: none;
    border-color: #27ae60;
    box-shadow: 0 0 0 2px rgba(39, 174, 96, 0.2);
  }

  .setting-help {
    margin-top: 0.5rem;
    font-size: 0.9rem;
    color: #7f8c8d;
  }

  .setting-help a {
    color: #27ae60;
    text-decoration: none;
  }

  .setting-help a:hover {
    text-decoration: underline;
  }

  .save-button {
    background-color: #27ae60;
    color: white;
    border: none;
    padding: 0.75rem 1.5rem;
    border-radius: 6px;
    font-size: 1rem;
    cursor: pointer;
    transition: background-color 0.2s ease;
    font-family: inherit;
  }

  .save-button:hover:not(:disabled) {
    background-color: #219a52;
  }

  .save-button:disabled {
    background-color: #95a5a6;
    cursor: not-allowed;
  }

  .save-status {
    margin-top: 1rem;
    padding: 0.75rem;
    border-radius: 6px;
    font-size: 0.9rem;
  }

  .save-status.success {
    background-color: #d5edda;
    color: #155724;
    border: 1px solid #c3e6cb;
  }

  .save-status.error {
    background-color: #f8d7da;
    color: #721c24;
    border: 1px solid #f5c6cb;
  }
</style>
