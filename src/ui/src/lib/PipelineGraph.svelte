<script lang="ts">
  import { onMount } from 'svelte';
  import cytoscape from 'cytoscape';
  import dagre from 'cytoscape-dagre';
  import type { GraphNode, GraphEdge } from './types';

  type Props = {
    nodes: GraphNode[];
    edges: GraphEdge[];
    onNodeClick?: (nodeName: string) => void;
  };

  let { nodes, edges, onNodeClick }: Props = $props();

  let container: HTMLDivElement;
  let cy: cytoscape.Core | null = null;

  onMount(() => {
    cytoscape.use(dagre);
    initializeGraph();
    return () => {
      if (cy) {
        cy.destroy();
      }
    };
  });

  function initializeGraph() {
    if (!container) return;

    const elements = [
      ...nodes.map((node) => ({
        data: {
          id: node.name,
          label: node.name,
          status: node.status || 'pending',
          lastUpdated: node.last_updated_at,
        },
      })),
      ...edges.map((edge) => ({
        data: {
          id: `${edge.from}-${edge.to}`,
          source: edge.from,
          target: edge.to,
        },
      })),
    ];

    cy = cytoscape({
      container,
      elements,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': getNodeColor,
            label: 'data(label)',
            'text-valign': 'center',
            'text-halign': 'center',
            color: '#fff',
            'font-size': '12px',
            width: '80px',
            height: '40px',
            'border-width': '2px',
            'border-color': '#333',
            shape: 'roundrectangle',
          },
        },
        {
          selector: 'edge',
          style: {
            width: 2,
            'line-color': '#666',
            'target-arrow-color': '#666',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
          },
        },
        {
          selector: 'node:selected',
          style: {
            'border-color': '#0066cc',
            'border-width': '3px',
          },
        },
      ],
      layout: {
        name: 'dagre',
        rankDir: 'TB',
        padding: 10,
      } as any,
    });

    cy.on('tap', 'node', (event) => {
      const nodeName = event.target.data('id');
      onNodeClick?.(nodeName);
    });
  }

  function getNodeColor(node: any) {
    const status = node.data('status');
    switch (status) {
      case 'running':
        return '#ffa500'; // Orange
      case 'completed':
        return '#28a745'; // Green
      case 'failed':
        return '#dc3545'; // Red
      default:
        return '#6c757d'; // Gray (pending)
    }
  }

  $effect(() => {
    if (cy && nodes && edges) {
      updateGraph();
    }
  });

  function updateGraph() {
    if (!cy) return;

    const elements = [
      ...nodes.map((node) => ({
        data: {
          id: node.name,
          label: node.name,
          status: node.status || 'pending',
          lastUpdated: node.last_updated_at,
        },
      })),
      ...edges.map((edge) => ({
        data: {
          id: `${edge.from}-${edge.to}`,
          source: edge.from,
          target: edge.to,
        },
      })),
    ];

    cy.json({ elements });
    cy.layout({
      name: 'dagre',
      rankDir: 'TB',
      padding: 10,
    } as any).run();
  }
</script>

<div class="graph-container">
  <div bind:this={container} class="cytoscape-container"></div>
</div>

<style>
  .graph-container {
    width: 100%;
    height: 100%;
    min-height: 500px;
    position: relative;
  }

  .cytoscape-container {
    width: 100%;
    height: 100%;
    border: 1px solid #e0e0e0;
    border-radius: 4px;
  }
</style>
