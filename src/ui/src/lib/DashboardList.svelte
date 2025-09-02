<script lang="ts">
  import { onMount } from 'svelte';
  import { Trash2, BarChart3, TrendingUp } from 'lucide-svelte';
  import { t } from 'svelte-i18n';
  import EmptyState from './EmptyState.svelte';

  interface Dashboard {
    name: string;
    description?: string;
    query: string;
    chart_type: string;
  }

  export let selectedDashboard: string | null = null;

  let dashboards: Dashboard[] = [];
  let loading = true;

  onMount(async () => {
    await loadDashboards();
  });

  async function loadDashboards() {
    try {
      const response = await fetch('/api/dashboards');
      if (response.ok) {
        dashboards = await response.json();
      }
    } catch (error) {
      console.error('Error loading dashboards:', error);
    } finally {
      loading = false;
    }
  }

  async function deleteDashboard(name: string) {
    if (!confirm($t('dashboards.delete.confirm'))) return;

    try {
      const response = await fetch(`/api/dashboards/${name}`, {
        method: 'DELETE',
      });
      if (response.ok) {
        dashboards = dashboards.filter((d) => d.name !== name);
        if (selectedDashboard === name) {
          selectedDashboard = null;
        }
      }
    } catch (error) {
      console.error('Error deleting dashboard:', error);
    }
  }

  function handleDashboardSelect(name: string) {
    selectedDashboard = selectedDashboard === name ? null : name;
  }

  function getChartIcon(chartType: string) {
    return chartType === 'line' ? TrendingUp : BarChart3;
  }
</script>

<div class="dashboard-list">
  {#if loading}
    <div class="loading">{$t('common.loading')}</div>
  {:else if dashboards.length === 0}
    <EmptyState
      messageKey="dashboards.empty"
      actionKey="dashboards.new.button"
      on:create={() => {}}
    />
  {:else}
    <div class="dashboard-grid">
      {#each dashboards as dashboard}
        <div
          class="dashboard-card"
          class:selected={selectedDashboard === dashboard.name}
          on:click={() => handleDashboardSelect(dashboard.name)}
          on:keydown={(e) =>
            e.key === 'Enter' && handleDashboardSelect(dashboard.name)}
          role="button"
          tabindex="0"
        >
          <div class="dashboard-header">
            <div class="dashboard-icon">
              <svelte:component
                this={getChartIcon(dashboard.chart_type)}
                size={20}
              />
            </div>
            <h3 class="dashboard-name">{dashboard.name}</h3>
            <button
              class="delete-btn"
              on:click|stopPropagation={() => deleteDashboard(dashboard.name)}
              aria-label={$t('dashboards.delete.label', {
                values: { name: dashboard.name },
              })}
            >
              <Trash2 size={16} />
            </button>
          </div>

          {#if dashboard.description}
            <p class="dashboard-description">{dashboard.description}</p>
          {/if}

          <div class="dashboard-meta">
            <span class="chart-type">{dashboard.chart_type}</span>
            <span class="query-name"
              >{$t('dashboards.query')}: {dashboard.query}</span
            >
          </div>
        </div>
      {/each}
    </div>
  {/if}
</div>

<style>
  .dashboard-list {
    height: 100%;
    display: flex;
    flex-direction: column;
  }

  .loading {
    padding: 2rem;
    text-align: center;
    color: #666;
  }

  .dashboard-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 1rem;
    padding: 1rem;
    overflow-y: auto;
  }

  .dashboard-card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 1rem;
    cursor: pointer;
    transition: all 0.2s ease;
  }

  .dashboard-card:hover {
    border-color: #3b82f6;
    box-shadow: 0 2px 8px rgba(59, 130, 246, 0.1);
  }

  .dashboard-card.selected {
    border-color: #3b82f6;
    background: #f8fafc;
  }

  .dashboard-header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.5rem;
  }

  .dashboard-icon {
    color: #3b82f6;
  }

  .dashboard-name {
    font-size: 1rem;
    font-weight: 600;
    margin: 0;
    flex: 1;
    color: #1e293b;
  }

  .delete-btn {
    background: none;
    border: none;
    color: #64748b;
    cursor: pointer;
    padding: 0.25rem;
    border-radius: 4px;
    transition: all 0.2s ease;
  }

  .delete-btn:hover {
    color: #dc2626;
    background: #fef2f2;
  }

  .dashboard-description {
    font-size: 0.875rem;
    color: #64748b;
    margin: 0 0 0.75rem 0;
    line-height: 1.4;
  }

  .dashboard-meta {
    display: flex;
    align-items: center;
    gap: 1rem;
    font-size: 0.75rem;
    color: #64748b;
  }

  .chart-type {
    background: #f1f5f9;
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    font-weight: 500;
  }

  .query-name {
    flex: 1;
  }
</style>
