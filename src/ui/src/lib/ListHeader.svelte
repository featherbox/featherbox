<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { t } from './i18n';

  const dispatch = createEventDispatcher();

  let {
    titleKey,
    createKey,
  }: {
    titleKey: string;
    createKey: string;
  } = $props();

  function handleCreate() {
    dispatch('create');
  }
</script>

<div class="header">
  <h3>{$t(titleKey)}</h3>
  <button class="create-btn" onclick={handleCreate}>{$t(createKey)}</button>
</div>

<style>
  .header {
    padding: 16px;
    border-bottom: 1px solid #e0e0e0;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .header h3 {
    margin: 0;
    font-size: 1.1rem;
    font-weight: 600;
    color: #333;
  }

  .create-btn {
    background: #27ae60;
    color: white;
    border: none;
    padding: 6px 12px;
    border-radius: 4px;
    font-size: 0.9rem;
    cursor: pointer;
    transition: background 0.2s;
  }

  .create-btn:hover:not(:disabled) {
    background: #2ecc71;
  }
</style>
