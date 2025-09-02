<script lang="ts">
  import { onMount } from 'svelte';
  import { Plus, Edit3 } from 'lucide-svelte';
  import { t } from 'svelte-i18n';
  import DashboardList from './DashboardList.svelte';
  import DashboardForm from './DashboardForm.svelte';
  import ChartComponent from './ChartComponent.svelte';
  import ListHeader from './ListHeader.svelte';

  let selectedDashboard: string | null = null;
  let showForm = false;
  let editingDashboard: any = null;
  let queries: string[] = [];
  let dashboardData: any = null;
  let dashboardConfig: any = null;
  let loading = false;

  onMount(async () => {
    await loadQueries();
  });

  async function loadQueries() {
    try {
      const response = await fetch('/api/queries');
      if (response.ok) {
        const queryData = await response.json();
        queries = Object.keys(queryData.queries || {});
      }
    } catch (error) {
      console.error('Error loading queries:', error);
    }
  }

  async function loadDashboardData() {
    if (!selectedDashboard) return;

    loading = true;
    try {
      const [configResponse, dataResponse] = await Promise.all([
        fetch(`/api/dashboards/${selectedDashboard}`),
        fetch(`/api/dashboards/${selectedDashboard}/data`),
      ]);

      if (configResponse.ok && dataResponse.ok) {
        dashboardConfig = await configResponse.json();
        dashboardData = await dataResponse.json();
      }
    } catch (error) {
      console.error('Error loading dashboard data:', error);
    } finally {
      loading = false;
    }
  }

  function handleNewDashboard() {
    editingDashboard = null;
    showForm = true;
  }

  function handleEditDashboard() {
    if (dashboardConfig) {
      editingDashboard = dashboardConfig;
      showForm = true;
    }
  }

  function handleFormClose() {
    showForm = false;
    editingDashboard = null;
  }

  function handleFormSaved() {
    if (selectedDashboard) {
      loadDashboardData();
    }
  }

  $: if (selectedDashboard) {
    loadDashboardData();
  } else {
    dashboardData = null;
    dashboardConfig = null;
  }
</script>

<div class="dashboard-panel">
  <div class="dashboard-sidebar">
    <ListHeader
      titleKey="navigation.dashboards"
      createKey="dashboards.new.button"
      on:create={handleNewDashboard}
    />

    <DashboardList
      bind:selectedDashboard
      on:refresh={() => {
        if (selectedDashboard) {
          loadDashboardData();
        }
      }}
    />
  </div>

  <div class="dashboard-content">
    {#if selectedDashboard}
      <div class="content-header">
        <div class="dashboard-info">
          <h3 class="dashboard-title">{selectedDashboard}</h3>
          {#if dashboardConfig?.description}
            <p class="dashboard-description">{dashboardConfig.description}</p>
          {/if}
        </div>
        <button
          class="edit-btn"
          on:click={handleEditDashboard}
          aria-label={$t('dashboards.edit.label')}
        >
          <Edit3 size={16} />
          {$t('common.edit')}
        </button>
      </div>

      <div class="chart-container">
        {#if loading}
          <div class="loading-state">
            <div class="spinner"></div>
            <p>{$t('dashboards.loading_chart')}</p>
          </div>
        {:else if dashboardData && dashboardConfig}
          <ChartComponent
            chartType={dashboardConfig.chart.chart_type}
            labels={dashboardData.labels.map((l: any) => String(l))}
            values={dashboardData.values.map((v: any) => Number(v))}
            title={dashboardConfig.name}
          />
        {:else}
          <div class="empty-chart">
            <p>{$t('dashboards.no_data')}</p>
          </div>
        {/if}
      </div>
    {:else}
      <div class="empty-state">
        <div class="empty-icon">📊</div>
        <h3>{$t('dashboards.select_dashboard')}</h3>
        <p>{$t('dashboards.select_dashboard_description')}</p>
      </div>
    {/if}
  </div>
</div>

<DashboardForm
  bind:isVisible={showForm}
  bind:editingDashboard
  {queries}
  on:close={handleFormClose}
  on:saved={handleFormSaved}
/>

<style>
  .dashboard-panel {
    display: flex;
    height: 100%;
    background: #f8fafc;
  }

  .dashboard-sidebar {
    width: 400px;
    background: white;
    border-right: 1px solid #e2e8f0;
    display: flex;
    flex-direction: column;
  }


  .dashboard-content {
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  .content-header {
    background: white;
    padding: 1.5rem;
    border-bottom: 1px solid #e2e8f0;
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
  }

  .dashboard-info {
    flex: 1;
  }

  .dashboard-title {
    margin: 0 0 0.5rem 0;
    font-size: 1.5rem;
    font-weight: 600;
    color: #1e293b;
  }

  .dashboard-description {
    margin: 0;
    color: #64748b;
    line-height: 1.5;
  }

  .edit-btn {
    background: white;
    border: 1px solid #d1d5db;
    color: #374151;
    padding: 0.5rem 1rem;
    border-radius: 6px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .edit-btn:hover {
    background: #f9fafb;
    border-color: #9ca3af;
  }

  .chart-container {
    flex: 1;
    padding: 1.5rem;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .loading-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 1rem;
    color: #64748b;
  }

  .spinner {
    width: 32px;
    height: 32px;
    border: 3px solid #e2e8f0;
    border-top: 3px solid #3b82f6;
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    0% {
      transform: rotate(0deg);
    }
    100% {
      transform: rotate(360deg);
    }
  }

  .empty-chart {
    text-align: center;
    color: #64748b;
  }

  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
    height: 100%;
    color: #64748b;
  }

  .empty-icon {
    font-size: 4rem;
    margin-bottom: 1rem;
    opacity: 0.5;
  }

  .empty-state h3 {
    margin: 0 0 0.5rem 0;
    font-size: 1.25rem;
    font-weight: 600;
    color: #374151;
  }

  .empty-state p {
    margin: 0;
    max-width: 400px;
    line-height: 1.5;
  }

  @media (max-width: 1024px) {
    .dashboard-sidebar {
      width: 300px;
    }
  }

  @media (max-width: 768px) {
    .dashboard-panel {
      flex-direction: column;
    }

    .dashboard-sidebar {
      width: 100%;
      height: 300px;
    }
  }
</style>
