<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { X, Save } from 'lucide-svelte';
  import { t } from 'svelte-i18n';

  export let isVisible = false;
  export let editingDashboard: any = null;
  export let queries: string[] = [];

  const dispatch = createEventDispatcher();

  let formData = {
    name: '',
    description: '',
    query: '',
    chart: {
      type: 'line',
      x_column: '',
      y_column: '',
    },
  };

  let isEditing = false;

  $: {
    if (editingDashboard) {
      isEditing = true;
      formData = {
        name: editingDashboard.name,
        description: editingDashboard.description || '',
        query: editingDashboard.query,
        chart: {
          type: editingDashboard.chart.chart_type,
          x_column: editingDashboard.chart.x_column,
          y_column: editingDashboard.chart.y_column,
        },
      };
    } else {
      isEditing = false;
      formData = {
        name: '',
        description: '',
        query: '',
        chart: {
          type: 'line',
          x_column: '',
          y_column: '',
        },
      };
    }
  }

  function closeForm() {
    dispatch('close');
  }

  async function handleSubmit() {
    if (
      !formData.name ||
      !formData.query ||
      !formData.chart.x_column ||
      !formData.chart.y_column
    ) {
      return;
    }

    try {
      const url = isEditing
        ? `/api/dashboards/${editingDashboard.name}`
        : '/api/dashboards';
      const method = isEditing ? 'PUT' : 'POST';

      const response = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(formData),
      });

      if (response.ok) {
        dispatch('saved');
        closeForm();
      } else {
        console.error('Failed to save dashboard');
      }
    } catch (error) {
      console.error('Error saving dashboard:', error);
    }
  }

  function handleKeydown(event: KeyboardEvent) {
    if (event.key === 'Escape') {
      closeForm();
    }
  }
</script>

<svelte:window on:keydown={handleKeydown} />

{#if isVisible}
  <div
    class="modal-overlay"
    role="button"
    tabindex="-1"
    on:click={closeForm}
    on:keydown={(e) => e.key === 'Escape' && closeForm()}
  >
    <div
      class="modal-content"
      role="dialog"
      tabindex="0"
      on:click|stopPropagation
      on:keydown|stopPropagation
    >
      <div class="modal-header">
        <h2>
          {isEditing ? $t('dashboards.edit.title') : $t('dashboards.new.title')}
        </h2>
        <button
          class="close-btn"
          on:click={closeForm}
          aria-label={$t('common.close')}
        >
          <X size={20} />
        </button>
      </div>

      <form on:submit|preventDefault={handleSubmit}>
        <div class="form-group">
          <label for="name">{$t('dashboards.form.name')}</label>
          <input
            id="name"
            type="text"
            bind:value={formData.name}
            required
            disabled={isEditing}
            placeholder={$t('dashboards.form.name_placeholder')}
          />
        </div>

        <div class="form-group">
          <label for="description">{$t('dashboards.form.description')}</label>
          <textarea
            id="description"
            bind:value={formData.description}
            placeholder={$t('dashboards.form.description_placeholder')}
            rows="3"
          ></textarea>
        </div>

        <div class="form-group">
          <label for="query">{$t('dashboards.form.query')}</label>
          <select id="query" bind:value={formData.query} required>
            <option value="">{$t('dashboards.form.select_query')}</option>
            {#each queries as query}
              <option value={query}>{query}</option>
            {/each}
          </select>
        </div>

        <div class="form-group">
          <label for="chart-type">{$t('dashboards.form.chart_type')}</label>
          <select id="chart-type" bind:value={formData.chart.type}>
            <option value="line">{$t('dashboards.chart_types.line')}</option>
            <option value="bar">{$t('dashboards.chart_types.bar')}</option>
          </select>
        </div>

        <div class="form-row">
          <div class="form-group">
            <label for="x-column">{$t('dashboards.form.x_column')}</label>
            <input
              id="x-column"
              type="text"
              bind:value={formData.chart.x_column}
              required
              placeholder={$t('dashboards.form.x_column_placeholder')}
            />
          </div>

          <div class="form-group">
            <label for="y-column">{$t('dashboards.form.y_column')}</label>
            <input
              id="y-column"
              type="text"
              bind:value={formData.chart.y_column}
              required
              placeholder={$t('dashboards.form.y_column_placeholder')}
            />
          </div>
        </div>

        <div class="form-actions">
          <button type="button" class="cancel-btn" on:click={closeForm}>
            {$t('common.cancel')}
          </button>
          <button type="submit" class="save-btn">
            <Save size={16} />
            {$t('common.save')}
          </button>
        </div>
      </form>
    </div>
  </div>
{/if}

<style>
  .modal-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.5);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
  }

  .modal-content {
    background: white;
    border-radius: 8px;
    width: 90%;
    max-width: 600px;
    max-height: 90vh;
    overflow-y: auto;
  }

  .modal-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 1.5rem;
    border-bottom: 1px solid #e2e8f0;
  }

  .modal-header h2 {
    margin: 0;
    font-size: 1.25rem;
    font-weight: 600;
    color: #1e293b;
  }

  .close-btn {
    background: none;
    border: none;
    color: #64748b;
    cursor: pointer;
    padding: 0.25rem;
    border-radius: 4px;
    transition: all 0.2s ease;
  }

  .close-btn:hover {
    color: #374151;
    background: #f1f5f9;
  }

  form {
    padding: 1.5rem;
  }

  .form-group {
    margin-bottom: 1rem;
  }

  .form-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1rem;
  }

  label {
    display: block;
    margin-bottom: 0.5rem;
    font-weight: 500;
    color: #374151;
  }

  input,
  textarea,
  select {
    width: 100%;
    padding: 0.75rem;
    border: 1px solid #d1d5db;
    border-radius: 6px;
    font-size: 0.875rem;
    transition: border-color 0.2s ease;
  }

  input:focus,
  textarea:focus,
  select:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
  }

  input:disabled {
    background: #f9fafb;
    color: #6b7280;
    cursor: not-allowed;
  }

  textarea {
    resize: vertical;
    min-height: 80px;
  }

  .form-actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.75rem;
    margin-top: 1.5rem;
    padding-top: 1.5rem;
    border-top: 1px solid #e2e8f0;
  }

  .cancel-btn {
    background: white;
    border: 1px solid #d1d5db;
    color: #374151;
    padding: 0.75rem 1.5rem;
    border-radius: 6px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
  }

  .cancel-btn:hover {
    background: #f9fafb;
  }

  .save-btn {
    background: #3b82f6;
    border: 1px solid #3b82f6;
    color: white;
    padding: 0.75rem 1.5rem;
    border-radius: 6px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .save-btn:hover {
    background: #2563eb;
  }

  .save-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
</style>
