<script lang="ts">
  import { onMount } from 'svelte';
  import './lib/i18n';
  import { t, isLoading } from './lib/i18n';
  import Navigation from './lib/Navigation.svelte';
  import AdapterList from './lib/AdapterList.svelte';
  import AdapterDetail from './lib/AdapterDetail.svelte';
  import AdapterForm from './lib/AdapterForm.svelte';
  import ConnectionList from './lib/ConnectionList.svelte';
  import ConnectionDetail from './lib/ConnectionDetail.svelte';
  import ConnectionForm from './lib/ConnectionForm.svelte';
  import ModelList from './lib/ModelList.svelte';
  import ModelDetail from './lib/ModelDetail.svelte';
  import ModelForm from './lib/ModelForm.svelte';
  import PipelinePanel from './lib/PipelinePanel.svelte';
  import SettingsPanel from './lib/SettingsPanel.svelte';
  import AnalysisSession from './lib/AnalysisSession.svelte';
  import QueryPanel from './lib/QueryPanel.svelte';
  import type {
    AdapterSummary,
    AdapterDetails,
    ModelSummary,
    ModelDetails,
    ConnectionSummary,
    ConnectionDetails,
  } from './lib/types';

  let activeSection = $state('connections');
  let adapters = $state<AdapterSummary[]>([]);
  let selectedAdapter = $state<string | null>(null);
  let selectedAdapterDetails = $state<AdapterDetails | null>(null);
  let showAdapterForm = $state(false);
  let adapterFormMode = $state<'create' | 'edit'>('create');
  let adapterFormData = $state<{ name: string; config: any } | null>(null);

  let connections = $state<ConnectionSummary[]>([]);
  let selectedConnection = $state<string | null>(null);
  let selectedConnectionDetails = $state<ConnectionDetails | null>(null);
  let showConnectionForm = $state(false);
  let connectionFormMode = $state<'create' | 'edit'>('create');
  let connectionFormData = $state<ConnectionDetails | null>(null);

  let models = $state<ModelSummary[]>([]);
  let selectedModel = $state<string | null>(null);
  let selectedModelDetails = $state<ModelDetails | null>(null);
  let showModelForm = $state(false);
  let modelFormMode = $state<'create' | 'edit'>('create');
  let modelFormData = $state<ModelDetails | null>(null);

  onMount(() => {
    if (activeSection === 'adapters') {
      loadAdapters();
    } else if (activeSection === 'connections') {
      loadConnections();
    } else if (activeSection === 'models') {
      loadModels();
    }
  });

  $effect(() => {
    if (activeSection === 'adapters') {
      loadAdapters();
    } else if (activeSection === 'connections') {
      loadConnections();
    } else if (activeSection === 'models') {
      loadModels();
    }
  });

  async function loadAdapters() {
    try {
      const response = await fetch('http://localhost:3000/api/adapters');
      if (response.ok) {
        adapters = await response.json();
      }
    } catch (error) {
      console.error('Failed to load adapters:', error);
    }
  }

  async function loadAdapterDetails(name: string) {
    try {
      const response = await fetch(
        `http://localhost:3000/api/adapters/${name}`,
      );
      if (response.ok) {
        selectedAdapterDetails = await response.json();
      }
    } catch (error) {
      console.error('Failed to load adapter details:', error);
    }
  }

  function handleAdapterSelect(event: CustomEvent<string>) {
    selectedAdapter = event.detail;
    loadAdapterDetails(event.detail);
  }

  function handleAdapterCreate() {
    adapterFormMode = 'create';
    adapterFormData = null;
    showAdapterForm = true;
  }

  function handleAdapterEdit(event: CustomEvent<AdapterDetails>) {
    adapterFormMode = 'edit';
    adapterFormData = event.detail;
    showAdapterForm = true;
  }

  async function handleAdapterDelete(event: CustomEvent<string>) {
    const name = event.detail;
    if (!confirm($t('adapters.delete.confirm', { values: { name } }))) {
      return;
    }

    try {
      const response = await fetch(
        `http://localhost:3000/api/adapters/${name}`,
        {
          method: 'DELETE',
        },
      );
      if (response.ok) {
        await loadAdapters();
        if (selectedAdapter === name) {
          selectedAdapter = null;
          selectedAdapterDetails = null;
        }
      }
    } catch (error) {
      console.error('Failed to delete adapter:', error);
    }
  }

  async function handleAdapterFormSubmit(
    event: CustomEvent<{ name: string; config: any }>,
  ) {
    const { name, config } = event.detail;

    try {
      const url =
        adapterFormMode === 'create'
          ? 'http://localhost:3000/api/adapters'
          : `http://localhost:3000/api/adapters/${name}`;

      const method = adapterFormMode === 'create' ? 'POST' : 'PUT';
      const body =
        adapterFormMode === 'create'
          ? JSON.stringify({ name, config })
          : JSON.stringify({ config });

      const response = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json',
        },
        body,
      });

      if (response.ok) {
        await loadAdapters();
        if (adapterFormMode === 'create') {
          selectedAdapter = name;
          await loadAdapterDetails(name);
        } else if (selectedAdapter === name) {
          await loadAdapterDetails(name);
        }
      }
    } catch (error) {
      console.error('Failed to save adapter:', error);
    }
  }

  async function loadConnections() {
    try {
      const response = await fetch('http://localhost:3000/api/connections');
      if (response.ok) {
        connections = await response.json();
      }
    } catch (error) {
      console.error('Failed to load connections:', error);
    }
  }

  async function loadConnectionDetails(name: string) {
    try {
      const response = await fetch(
        `http://localhost:3000/api/connections/${name}`,
      );
      if (response.ok) {
        const config = await response.json();
        selectedConnectionDetails = { name, ...config };
      }
    } catch (error) {
      console.error('Failed to load connection details:', error);
    }
  }

  function handleConnectionSelect(event: CustomEvent<string>) {
    selectedConnection = event.detail;
    loadConnectionDetails(event.detail);
  }

  function handleConnectionCreate() {
    connectionFormMode = 'create';
    connectionFormData = null;
    showConnectionForm = true;
  }

  function handleConnectionEdit(event: CustomEvent<any>) {
    connectionFormMode = 'edit';
    connectionFormData = event.detail;
    showConnectionForm = true;
  }

  async function handleConnectionDelete(event: CustomEvent<string>) {
    const name = event.detail;
    if (!confirm($t('connections.delete.confirm', { values: { name } }))) {
      return;
    }

    try {
      const response = await fetch(
        `http://localhost:3000/api/connections/${name}`,
        {
          method: 'DELETE',
        },
      );
      if (response.ok) {
        await loadConnections();
        if (selectedConnection === name) {
          selectedConnection = null;
          selectedConnectionDetails = null;
        }
      }
    } catch (error) {
      console.error('Failed to delete connection:', error);
    }
  }

  async function handleConnectionFormSubmit(
    event: CustomEvent<{ name: string; config: any }>,
  ) {
    const { name, config } = event.detail;

    try {
      const url =
        connectionFormMode === 'create'
          ? 'http://localhost:3000/api/connections'
          : `http://localhost:3000/api/connections/${name}`;

      const method = connectionFormMode === 'create' ? 'POST' : 'PUT';
      const body =
        connectionFormMode === 'create'
          ? JSON.stringify({ name, config })
          : JSON.stringify({ config });

      const response = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json',
        },
        body,
      });

      if (response.ok) {
        await loadConnections();
        if (connectionFormMode === 'create') {
          selectedConnection = name;
          await loadConnectionDetails(name);
        } else if (selectedConnection === name) {
          await loadConnectionDetails(name);
        }
      }
    } catch (error) {
      console.error('Failed to save connection:', error);
    }
  }

  async function loadModels() {
    try {
      const response = await fetch('http://localhost:3000/api/models');
      if (response.ok) {
        models = await response.json();
      }
    } catch (error) {
      console.error('Failed to load models:', error);
    }
  }

  async function loadModelDetails(path: string) {
    try {
      const response = await fetch(
        `http://localhost:3000/api/models/${encodeURIComponent(path)}`,
      );
      if (response.ok) {
        selectedModelDetails = await response.json();
      }
    } catch (error) {
      console.error('Failed to load model details:', error);
    }
  }

  function handleModelSelect(event: CustomEvent<string>) {
    selectedModel = event.detail;
    loadModelDetails(event.detail);
  }

  function handleModelCreate() {
    modelFormMode = 'create';
    modelFormData = null;
    showModelForm = true;
  }

  function handleModelEdit(event: CustomEvent<any>) {
    modelFormMode = 'edit';
    modelFormData = event.detail;
    showModelForm = true;
  }

  async function handleModelDelete(event: CustomEvent<string>) {
    const path = event.detail;
    if (!confirm($t('models.delete.confirm'))) {
      return;
    }

    try {
      const response = await fetch(
        `http://localhost:3000/api/models/${encodeURIComponent(path)}`,
        {
          method: 'DELETE',
        },
      );
      if (response.ok) {
        await loadModels();
        if (selectedModel === path) {
          selectedModel = null;
          selectedModelDetails = null;
        }
      }
    } catch (error) {
      console.error('Failed to delete model:', error);
    }
  }

  function handleModelRun(event: CustomEvent<string>) {
    const path = event.detail;
    console.log('Run model:', path);
  }

  async function handleModelFormSubmit(
    event: CustomEvent<{ name: string; path: string; config: any }>,
  ) {
    const { name, path, config } = event.detail;

    try {
      const url =
        modelFormMode === 'create'
          ? 'http://localhost:3000/api/models'
          : `http://localhost:3000/api/models/${encodeURIComponent(modelFormData?.path || '')}`;

      const method = modelFormMode === 'create' ? 'POST' : 'PUT';
      const body =
        modelFormMode === 'create'
          ? JSON.stringify({ name, path, config })
          : JSON.stringify({ config });

      const response = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json',
        },
        body,
      });

      if (response.ok) {
        await loadModels();
        const targetPath =
          modelFormMode === 'create' ? path : modelFormData?.path || '';
        selectedModel = targetPath;
        await loadModelDetails(targetPath);
      }
    } catch (error) {
      console.error('Failed to save model:', error);
    }
  }
</script>

{#if !$isLoading}
  <div class="app">
    <Navigation bind:activeSection />

    <main class="main-content">
      {#if activeSection === 'connections'}
        <div class="connections-section">
          <div class="connection-list-panel">
            <ConnectionList
              {connections}
              {selectedConnection}
              on:select={handleConnectionSelect}
              on:create={handleConnectionCreate}
            />
          </div>
          <div class="connection-detail-panel">
            <ConnectionDetail
              connection={selectedConnectionDetails}
              on:edit={handleConnectionEdit}
              on:delete={handleConnectionDelete}
            />
          </div>
        </div>
      {:else if activeSection === 'adapters'}
        <div class="adapters-section">
          <div class="adapter-list-panel">
            <AdapterList
              {adapters}
              {selectedAdapter}
              on:select={handleAdapterSelect}
              on:create={handleAdapterCreate}
            />
          </div>
          <div class="adapter-detail-panel">
            <AdapterDetail
              adapter={selectedAdapterDetails}
              on:edit={handleAdapterEdit}
              on:delete={handleAdapterDelete}
            />
          </div>
        </div>
      {:else if activeSection === 'models'}
        <div class="models-section">
          <div class="model-list-panel">
            <ModelList
              {models}
              {selectedModel}
              on:select={handleModelSelect}
              on:create={handleModelCreate}
            />
          </div>
          <div class="model-detail-panel">
            <ModelDetail
              model={selectedModelDetails}
              on:edit={handleModelEdit}
              on:delete={handleModelDelete}
              on:run={handleModelRun}
            />
          </div>
        </div>
      {:else if activeSection === 'query'}
        <QueryPanel />
      {:else if activeSection === 'pipeline'}
        <PipelinePanel />
      {:else if activeSection === 'analysis'}
        <AnalysisSession />
      {:else if activeSection === 'settings'}
        <SettingsPanel />
      {/if}
    </main>
  </div>

  <AdapterForm
    bind:isOpen={showAdapterForm}
    mode={adapterFormMode}
    initialData={adapterFormData}
    on:submit={handleAdapterFormSubmit}
    on:close={() => (showAdapterForm = false)}
  />

  <ConnectionForm
    bind:isOpen={showConnectionForm}
    mode={connectionFormMode}
    initialData={connectionFormData}
    on:submit={handleConnectionFormSubmit}
    on:close={() => (showConnectionForm = false)}
  />

  <ModelForm
    bind:isOpen={showModelForm}
    mode={modelFormMode}
    initialData={modelFormData}
    on:submit={handleModelFormSubmit}
    on:close={() => (showModelForm = false)}
  />
{:else}
  <div class="loading">Loading...</div>
{/if}

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

  .connections-section,
  .adapters-section,
  .models-section {
    display: flex;
    height: 100vh;
  }

  .connection-list-panel,
  .adapter-list-panel,
  .model-list-panel {
    width: 320px;
    flex-shrink: 0;
  }

  .connection-detail-panel,
  .adapter-detail-panel,
  .model-detail-panel {
    flex: 1;
  }

  .loading {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100vh;
    font-size: 1.2rem;
    color: #666;
  }
</style>
