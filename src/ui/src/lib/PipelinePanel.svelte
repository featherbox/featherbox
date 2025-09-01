<script lang="ts">
  import { onMount } from 'svelte';
  import {
    Play,
    RefreshCw,
    Database,
    AlertCircle,
    CheckCircle,
    Loader2,
  } from 'lucide-svelte';
  import { _ } from 'svelte-i18n';
  import PipelineGraph from './PipelineGraph.svelte';
  import type { GraphNode, GraphEdge, PipelineStatus } from './types';
  import { API_BASE_URL } from './config';

  let loading = $state(false);
  let error = $state<string | null>(null);
  let nodes = $state<GraphNode[]>([]);
  let edges = $state<GraphEdge[]>([]);
  let pipelineStatus = $state<PipelineStatus | null>(null);
  let currentPipelineId = $state<number | null>(null);
  let pollingInterval: ReturnType<typeof setInterval> | null = null;
  let selectedNode = $state<string | null>(null);

  onMount(() => {
    loadGraph();
    return () => {
      if (pollingInterval) {
        clearInterval(pollingInterval);
      }
    };
  });

  async function loadGraph() {
    try {
      loading = true;
      error = null;

      const response = await fetch(`${API_BASE_URL}/api/graph`);
      if (!response.ok) {
        throw new Error('Failed to load graph');
      }

      const data = await response.json();
      nodes = data.nodes || [];
      edges = data.edges || [];
    } catch (e) {
      error = e instanceof Error ? e.message : 'Unknown error';
    } finally {
      loading = false;
    }
  }

  async function runMigrate() {
    try {
      loading = true;
      error = null;

      const response = await fetch(`${API_BASE_URL}/api/pipeline/migrate`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      const result = await response.json();
      if (!result.success) {
        throw new Error(result.message);
      }

      await loadGraph(); // Reload graph after migration
    } catch (e) {
      error = e instanceof Error ? e.message : 'Migration failed';
    } finally {
      loading = false;
    }
  }

  async function runPipeline() {
    try {
      loading = true;
      error = null;

      const response = await fetch(`${API_BASE_URL}/api/pipeline/run`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          project_path: '.', // Use current directory
        }),
      });

      const result = await response.json();
      if (!result.success) {
        throw new Error(result.message);
      }

      if (result.pipeline_id) {
        currentPipelineId = result.pipeline_id;
        startPolling();
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Pipeline execution failed';
    } finally {
      loading = false;
    }
  }

  function startPolling() {
    if (pollingInterval) {
      clearInterval(pollingInterval);
    }

    pollingInterval = setInterval(async () => {
      if (currentPipelineId) {
        await loadPipelineStatus(currentPipelineId);
      }
    }, 1000); // Poll every 1 second
  }

  function stopPolling() {
    if (pollingInterval) {
      clearInterval(pollingInterval);
      pollingInterval = null;
    }
  }

  async function loadPipelineStatus(pipelineId: number) {
    try {
      const response = await fetch(
        `${API_BASE_URL}/api/pipeline/${pipelineId}/status`,
      );
      if (!response.ok) {
        throw new Error('Failed to load pipeline status');
      }

      const data = await response.json();
      pipelineStatus = data.pipeline;

      // Update node statuses based on task statuses
      updateNodeStatuses();

      // Stop polling if pipeline is complete
      if (
        pipelineStatus &&
        (pipelineStatus.status === 'COMPLETED' ||
          pipelineStatus.status === 'FAILED')
      ) {
        stopPolling();
      }
    } catch (e) {
      console.error('Failed to load pipeline status:', e);
    }
  }

  function updateNodeStatuses() {
    if (!pipelineStatus || !pipelineStatus.tasks) return;

    const taskStatusMap = new Map();
    for (const task of pipelineStatus.tasks) {
      taskStatusMap.set(task.table_name, task.status.toLowerCase());
    }

    nodes = nodes.map((node) => ({
      ...node,
      status: taskStatusMap.get(node.name) || 'pending',
    }));
  }

  async function runNode(nodeName: string) {
    try {
      loading = true;
      error = null;

      const response = await fetch(
        `${API_BASE_URL}/api/pipeline/run-nodes/${nodeName}`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            project_path: '.',
          }),
        },
      );

      const result = await response.json();
      if (!result.success) {
        throw new Error(result.message);
      }

      if (result.pipeline_id) {
        currentPipelineId = result.pipeline_id;
        startPolling();
      }
    } catch (e) {
      error = e instanceof Error ? e.message : `Failed to run node ${nodeName}`;
    } finally {
      loading = false;
    }
  }

  function onNodeClick(nodeName: string) {
    selectedNode = selectedNode === nodeName ? null : nodeName;
  }

  function getSelectedNodeInfo() {
    if (!selectedNode) return null;
    const node = nodes.find((n) => n.name === selectedNode);
    const taskStatus = pipelineStatus?.tasks.find(
      (t) => t.table_name === selectedNode,
    );
    return { node, taskStatus };
  }

  function getStatusIcon(status: string) {
    switch (status.toLowerCase()) {
      case 'running':
        return Loader2;
      case 'completed':
        return CheckCircle;
      case 'failed':
        return AlertCircle;
      default:
        return Database;
    }
  }

  function getStatusColor(status: string) {
    switch (status.toLowerCase()) {
      case 'running':
        return 'text-orange-500';
      case 'completed':
        return 'text-green-500';
      case 'failed':
        return 'text-red-500';
      default:
        return 'text-gray-500';
    }
  }
</script>

<div class="pipeline-panel">
  <div class="header">
    <h2 class="title">{$_('pipeline.title')}</h2>

    <div class="actions">
      <button
        onclick={runMigrate}
        disabled={loading}
        class="btn btn-secondary"
        title={$_('pipeline.migrate_tooltip')}
      >
        <Database size={16} />
        {$_('pipeline.migrate')}
      </button>

      <button
        onclick={runPipeline}
        disabled={loading || nodes.length === 0}
        class="btn btn-primary"
        title={$_('pipeline.run_tooltip')}
      >
        <Play size={16} />
        {$_('pipeline.run')}
      </button>

      <button
        onclick={loadGraph}
        disabled={loading}
        class="btn btn-secondary"
        title={$_('pipeline.refresh_tooltip')}
      >
        <RefreshCw size={16} class={loading ? 'animate-spin' : ''} />
        {$_('pipeline.refresh')}
      </button>
    </div>
  </div>

  {#if error}
    <div class="error">
      <AlertCircle size={16} />
      {error}
    </div>
  {/if}

  {#if pipelineStatus}
    <div class="status-bar">
      <div class="status-item">
        {#if pipelineStatus}
          {@const StatusIcon = getStatusIcon(pipelineStatus.status)}
          <StatusIcon size={16} class={getStatusColor(pipelineStatus.status)} />
        {/if}
        <span>Pipeline {pipelineStatus.status}</span>
        {#if pipelineStatus.status === 'RUNNING'}
          <span class="text-sm text-gray-500">
            ({pipelineStatus.tasks.filter((t) => t.status === 'COMPLETED')
              .length} / {pipelineStatus.tasks.length} completed)
          </span>
        {/if}
      </div>

      {#if pipelineStatus.started_at}
        <div class="status-item text-sm text-gray-500">
          Started: {new Date(pipelineStatus.started_at).toLocaleString()}
        </div>
      {/if}
    </div>
  {/if}

  <div class="graph-section">
    {#if nodes.length === 0}
      <div class="empty-state">
        <Database size={48} class="text-gray-400" />
        <h3>No Pipeline Found</h3>
        <p>
          Run migrate to create a pipeline graph from your adapters and models.
        </p>
        <button onclick={runMigrate} class="btn btn-primary" disabled={loading}>
          <Database size={16} />
          Run Migrate
        </button>
      </div>
    {:else}
      <div class="graph-layout">
        <div class="graph-container">
          <PipelineGraph {nodes} {edges} {onNodeClick} />
        </div>

        {#if selectedNode}
          {@const nodeInfo = getSelectedNodeInfo()}
          {#if nodeInfo}
            <div class="node-panel">
              <div class="node-panel-header">
                <h3>{selectedNode}</h3>
                <button
                  onclick={() => (selectedNode = null)}
                  class="close-button"
                >
                  ×
                </button>
              </div>

              <div class="node-details">
                {#if nodeInfo.node?.last_updated_at}
                  <div class="detail-item">
                    <span class="label">Last Updated:</span>
                    <span class="value"
                      >{new Date(
                        nodeInfo.node.last_updated_at,
                      ).toLocaleString()}</span
                    >
                  </div>
                {/if}

                {#if nodeInfo.taskStatus}
                  <div class="detail-item">
                    <span class="label">Status:</span>
                    <span
                      class="value status-{nodeInfo.taskStatus.status.toLowerCase()}"
                    >
                      {nodeInfo.taskStatus.status}
                    </span>
                  </div>

                  {#if nodeInfo.taskStatus.started_at}
                    <div class="detail-item">
                      <span class="label">Started:</span>
                      <span class="value"
                        >{new Date(
                          nodeInfo.taskStatus.started_at,
                        ).toLocaleString()}</span
                      >
                    </div>
                  {/if}

                  {#if nodeInfo.taskStatus.completed_at}
                    <div class="detail-item">
                      <span class="label">Completed:</span>
                      <span class="value"
                        >{new Date(
                          nodeInfo.taskStatus.completed_at,
                        ).toLocaleString()}</span
                      >
                    </div>
                  {/if}
                {/if}
              </div>

              <div class="node-actions">
                <button
                  onclick={() => runNode(selectedNode!)}
                  disabled={loading}
                  class="btn btn-primary btn-small"
                >
                  <Play size={14} />
                  Run Node
                </button>
              </div>
            </div>
          {/if}
        {/if}
      </div>
    {/if}
  </div>
</div>

<style>
  .pipeline-panel {
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 1rem;
    gap: 1rem;
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid #e0e0e0;
    padding-bottom: 1rem;
  }

  .title {
    font-size: 1.25rem;
    font-weight: 600;
    margin: 0;
  }

  .actions {
    display: flex;
    gap: 0.5rem;
  }

  .btn {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    border-radius: 0.375rem;
    border: 1px solid transparent;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
  }

  .btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .btn-primary {
    background-color: #0066cc;
    color: white;
    border-color: #0066cc;
  }

  .btn-primary:hover:not(:disabled) {
    background-color: #005bb5;
    border-color: #005bb5;
  }

  .btn-secondary {
    background-color: white;
    color: #374151;
    border-color: #d1d5db;
  }

  .btn-secondary:hover:not(:disabled) {
    background-color: #f9fafb;
    border-color: #9ca3af;
  }

  .error {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.75rem 1rem;
    background-color: #fef2f2;
    border: 1px solid #fecaca;
    border-radius: 0.375rem;
    color: #dc2626;
    font-size: 0.875rem;
  }

  .status-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.75rem 1rem;
    background-color: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 0.375rem;
  }

  .status-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .graph-section {
    flex: 1;
    display: flex;
    flex-direction: column;
  }

  .graph-layout {
    flex: 1;
    display: flex;
    gap: 1rem;
  }

  .graph-container {
    flex: 1;
    min-width: 0;
  }

  .node-panel {
    width: 300px;
    flex-shrink: 0;
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 0.5rem;
    display: flex;
    flex-direction: column;
  }

  .node-panel-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 1rem;
    border-bottom: 1px solid #e2e8f0;
    background: #f8fafc;
    border-radius: 0.5rem 0.5rem 0 0;
  }

  .node-panel-header h3 {
    margin: 0;
    font-size: 1rem;
    font-weight: 600;
    color: #1e293b;
  }

  .close-button {
    background: none;
    border: none;
    font-size: 1.5rem;
    cursor: pointer;
    color: #64748b;
    padding: 0;
    width: 24px;
    height: 24px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 0.25rem;
  }

  .close-button:hover {
    background: #e2e8f0;
    color: #1e293b;
  }

  .node-details {
    flex: 1;
    padding: 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  .detail-item {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .detail-item .label {
    font-size: 0.75rem;
    font-weight: 600;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .detail-item .value {
    font-size: 0.875rem;
    color: #1e293b;
  }

  .status-running {
    color: #ea580c !important;
    font-weight: 600;
  }

  .status-completed {
    color: #16a34a !important;
    font-weight: 600;
  }

  .status-failed {
    color: #dc2626 !important;
    font-weight: 600;
  }

  .status-pending {
    color: #64748b !important;
  }

  .node-actions {
    padding: 1rem;
    border-top: 1px solid #e2e8f0;
    background: #f8fafc;
    border-radius: 0 0 0.5rem 0.5rem;
  }

  .btn-small {
    padding: 0.5rem 0.75rem;
    font-size: 0.75rem;
    width: 100%;
    justify-content: center;
  }

  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    flex: 1;
    gap: 1rem;
    text-align: center;
    color: #6b7280;
  }

  .empty-state h3 {
    margin: 0;
    font-size: 1.125rem;
    font-weight: 600;
  }

  .empty-state p {
    margin: 0;
    font-size: 0.875rem;
  }

  :global(.animate-spin) {
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    from {
      transform: rotate(0deg);
    }
    to {
      transform: rotate(360deg);
    }
  }
</style>
