<script lang="ts">
  import { Database, FileText, BarChart3, Settings } from 'lucide-svelte';
  
  export let activeSection: string = 'adapters';
</script>

<nav class="navigation">
  <ul class="nav-items">
    <li class="nav-item" class:active={activeSection === 'adapters'}>
      <button on:click={() => activeSection = 'adapters'} title="Adapters">
        <Database class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'models'}>
      <button on:click={() => activeSection = 'models'} title="Models">
        <FileText class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'analysis'}>
      <button on:click={() => activeSection = 'analysis'} title="Analysis">
        <BarChart3 class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'settings'}>
      <button on:click={() => activeSection = 'settings'} title="Settings">
        <Settings class="icon" />
      </button>
    </li>
  </ul>
</nav>

<style>
  .navigation {
    width: 60px;
    height: 100vh;
    background-color: #f5f5f5;
    border-right: 1px solid #e0e0e0;
    display: flex;
    flex-direction: column;
  }

  .nav-items {
    list-style: none;
    padding: 8px 0;
    margin: 0;
  }

  .nav-item {
    margin: 2px 0;
  }

  .nav-item button {
    width: 100%;
    height: 60px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: none;
    border: none;
    cursor: pointer;
    transition: all 0.2s;
    color: #666;
    position: relative;
  }

  .nav-item button:hover {
    background-color: #e8f5e9;
    color: #27ae60;
  }

  .nav-item.active button {
    background-color: #e8f5e9;
    color: #27ae60;
  }

  .nav-item.active button::before {
    content: '';
    position: absolute;
    left: 0;
    top: 50%;
    transform: translateY(-50%);
    width: 3px;
    height: 24px;
    background-color: #27ae60;
    border-radius: 0 2px 2px 0;
  }

  :global(.icon) {
    width: 24px;
    height: 24px;
    flex-shrink: 0;
  }
</style>