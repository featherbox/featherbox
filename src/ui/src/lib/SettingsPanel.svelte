<script lang="ts">
  import { t, locale } from './i18n';

  let currentLocale = $state($locale);

  $effect(() => {
    currentLocale = $locale;
  });
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

  select {
    width: 100%;
    padding: 0.75rem;
    border: 1px solid #e0e0e0;
    border-radius: 6px;
    font-size: 1rem;
    transition: border-color 0.2s ease;
    font-family: inherit;
  }

  select:focus {
    outline: none;
    border-color: #27ae60;
    box-shadow: 0 0 0 2px rgba(39, 174, 96, 0.2);
  }
</style>
