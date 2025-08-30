<script lang="ts">
  import Navigation from './lib/Navigation.svelte';
  
  let activeSection = 'adapters';
</script>

<div class="app">
  <Navigation bind:activeSection />
  
  <main class="main-content">
    {#if activeSection === 'adapters'}
      <div class="placeholder">
        <h2>Adapters</h2>
        <p>アダプター管理機能は今後実装予定です</p>
      </div>
    {:else if activeSection === 'models'}
      <div class="placeholder">
        <h2>Models</h2>
        <p>モデル管理機能は今後実装予定です</p>
      </div>
    {:else if activeSection === 'analysis'}
      <div class="placeholder">
        <h2>Analysis</h2>
        <p>分析機能は今後実装予定です</p>
      </div>
    {:else if activeSection === 'settings'}
      <div class="placeholder">
        <h2>Settings</h2>
        <p>設定機能は今後実装予定です</p>
      </div>
    {/if}
  </main>
</div>

<style>
  .app {
    display: flex;
    height: 100vh;
    background-color: #f5f5f5;
  }

  .main-content {
    flex: 1;
    overflow-y: auto;
  }

  .placeholder {
    padding: 32px;
    max-width: 800px;
    margin: 0 auto;
  }

  .placeholder h2 {
    margin: 0 0 16px 0;
    font-size: 1.75rem;
    color: #333;
  }

  .placeholder p {
    color: #666;
    font-size: 1rem;
  }
</style>
