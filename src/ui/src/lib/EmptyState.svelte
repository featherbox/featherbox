<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { t } from './i18n';

  const dispatch = createEventDispatcher();

  let {
    messageKey,
    actionKey,
  }: {
    messageKey: string;
    actionKey: string;
  } = $props();

  function handleCreate() {
    dispatch('create');
  }
</script>

<div class="empty-state">
  <p>{$t(messageKey)}</p>
  <button onclick={handleCreate}>{$t(actionKey)}</button>
</div>

<style>
  .empty-state {
    padding: 40px 16px;
    text-align: center;
    color: #666;
  }

  .empty-state p {
    margin-bottom: 16px;
  }

  .empty-state button {
    background: #27ae60;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
  }

  .empty-state button:hover:not(:disabled) {
    background: #2ecc71;
  }
</style>
