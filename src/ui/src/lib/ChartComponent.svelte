<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import {
    Chart as ChartJS,
    Title,
    Tooltip,
    Legend,
    BarElement,
    CategoryScale,
    LinearScale,
    LineElement,
    PointElement,
    LineController,
    BarController,
  } from 'chart.js';

  ChartJS.register(
    Title,
    Tooltip,
    Legend,
    BarElement,
    CategoryScale,
    LinearScale,
    LineElement,
    PointElement,
    LineController,
    BarController,
  );

  export let chartType: 'line' | 'bar' = 'line';
  export let labels: string[] = [];
  export let values: number[] = [];
  export let title: string = '';

  let canvasElement: HTMLCanvasElement;
  let chart: ChartJS | null = null;

  $: data = {
    labels,
    datasets: [
      {
        label: title,
        data: values,
        backgroundColor:
          chartType === 'bar'
            ? 'rgba(59, 130, 246, 0.5)'
            : 'rgba(59, 130, 246, 0.1)',
        borderColor: 'rgba(59, 130, 246, 1)',
        borderWidth: 2,
        fill: chartType === 'line',
      },
    ],
  };

  $: options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: !!title,
        text: title,
      },
      legend: {
        display: true,
        position: 'top' as const,
      },
    },
    scales: {
      y: {
        beginAtZero: true,
      },
    },
  };

  function createChart() {
    if (chart) {
      chart.destroy();
    }

    if (canvasElement) {
      chart = new ChartJS(canvasElement, {
        type: chartType,
        data,
        options,
      });
    }
  }

  $: if (canvasElement && (chartType || labels.length || values.length)) {
    createChart();
  }

  onMount(() => {
    createChart();
  });

  onDestroy(() => {
    if (chart) {
      chart.destroy();
    }
  });
</script>

<div class="chart-container">
  <canvas bind:this={canvasElement}></canvas>
</div>

<style>
  .chart-container {
    width: 100%;
    height: 400px;
    position: relative;
  }
</style>
