<script lang="ts">
  interface QueryResult {
    results: string[][];
    column_count: number;
  }

  interface SavedQuery {
    name: string;
    description: string | null;
    sql: string;
  }

  interface SavedQueriesResponse {
    queries: Record<string, SavedQuery>;
  }

  let sql = $state('SELECT * FROM users LIMIT 10');
  let results = $state<QueryResult | null>(null);
  let error = $state<string | null>(null);
  let loading = $state(false);
  let savedQueries = $state<Record<string, SavedQuery>>({});
  let showSaveDialog = $state(false);
  let saveQueryName = $state('');
  let saveQueryDescription = $state('');
  let selectedQuery = $state<string | null>(null);

  async function executeQuery() {
    if (!sql.trim()) {
      error = 'SQL query is required';
      return;
    }

    loading = true;
    error = null;
    results = null;

    try {
      const response = await fetch('http://localhost:3000/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sql }),
      });

      if (response.ok) {
        results = await response.json();
      } else {
        const errorData = await response.json();
        error = errorData.error || 'Query execution failed';
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Network error occurred';
    } finally {
      loading = false;
    }
  }

  function handleKeydown(event: KeyboardEvent) {
    if (event.ctrlKey && event.key === 'Enter') {
      event.preventDefault();
      executeQuery();
    }
  }

  async function loadSavedQueries() {
    try {
      const response = await fetch('http://localhost:3000/api/queries');
      if (response.ok) {
        const data: SavedQueriesResponse = await response.json();
        savedQueries = data.queries;
      } else {
        console.error('Failed to load saved queries');
      }
    } catch (e) {
      console.error('Failed to load saved queries:', e);
    }
  }

  async function saveQuery() {
    if (!saveQueryName.trim() || !sql.trim()) {
      error = 'Query name and SQL are required';
      return;
    }

    try {
      const response = await fetch('http://localhost:3000/api/queries', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          name: saveQueryName,
          sql: sql,
          description: saveQueryDescription || null,
        }),
      });

      if (response.ok) {
        showSaveDialog = false;
        saveQueryName = '';
        saveQueryDescription = '';
        await loadSavedQueries();
      } else {
        const errorData = await response.json();
        error = errorData.error || 'Failed to save query';
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Network error occurred';
    }
  }

  function loadQuery(queryName: string) {
    const query = savedQueries[queryName];
    if (query) {
      sql = query.sql;
      selectedQuery = queryName;
    }
  }

  async function deleteQuery(queryName: string) {
    if (!confirm(`Are you sure you want to delete query "${queryName}"?`)) {
      return;
    }

    try {
      const response = await fetch(
        `http://localhost:3000/api/queries/${queryName}`,
        {
          method: 'DELETE',
        },
      );

      if (response.ok) {
        await loadSavedQueries();
        if (selectedQuery === queryName) {
          selectedQuery = null;
        }
      } else {
        const errorData = await response.json();
        error = errorData.error || 'Failed to delete query';
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Network error occurred';
    }
  }

  function openSaveDialog() {
    showSaveDialog = true;
    saveQueryName = '';
    saveQueryDescription = '';
  }

  function closeSaveDialog() {
    showSaveDialog = false;
    saveQueryName = '';
    saveQueryDescription = '';
  }

  loadSavedQueries();
</script>

<div class="query-panel">
  <div class="sidebar">
    <div class="saved-queries">
      <div class="section-header">
        <h3>Saved Queries</h3>
        <button class="btn-small" on:click={loadSavedQueries} title="Refresh">
          ↻
        </button>
      </div>
      <div class="queries-list">
        {#each Object.entries(savedQueries) as [name, query]}
          <div class="query-item" class:selected={selectedQuery === name}>
            <div class="query-info" on:click={() => loadQuery(name)}>
              <div class="query-name">{query.name}</div>
              {#if query.description}
                <div class="query-description">{query.description}</div>
              {/if}
            </div>
            <button
              class="delete-btn"
              on:click={() => deleteQuery(name)}
              title="Delete"
            >
              ×
            </button>
          </div>
        {:else}
          <div class="empty-state">No saved queries</div>
        {/each}
      </div>
    </div>
  </div>

  <div class="main-content">
    <div class="query-editor">
      <div class="editor-header">
        <h2>SQL Query</h2>
        <div class="editor-actions">
          <button
            class="btn-secondary"
            on:click={openSaveDialog}
            disabled={!sql.trim()}
          >
            Save Query
          </button>
        </div>
      </div>
      <div class="editor-container">
        <textarea
          bind:value={sql}
          on:keydown={handleKeydown}
          placeholder="Enter your SQL query here... (Ctrl+Enter to execute)"
          rows="8"
          disabled={loading}
        ></textarea>
      </div>
      <div class="actions">
        <button on:click={executeQuery} disabled={loading}>
          {loading ? 'Executing...' : 'Execute Query'}
        </button>
        <span class="hint">Ctrl+Enter</span>
      </div>
    </div>

    <div class="results-container">
      {#if error}
        <div class="error">
          <h3>Error</h3>
          <p>{error}</p>
        </div>
      {:else if results}
        <div class="results">
          <h3>Results ({results.results.length} rows)</h3>
          {#if results.results.length === 0}
            <p class="no-results">No results found.</p>
          {:else}
            <div class="table-container">
              <table>
                <thead>
                  {#if results.results.length > 0}
                    <tr>
                      {#each Array(results.column_count) as _, i}
                        <th>Column {i + 1}</th>
                      {/each}
                    </tr>
                  {/if}
                </thead>
                <tbody>
                  {#each results.results as row}
                    <tr>
                      {#each row as cell}
                        <td>{cell}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {/if}
        </div>
      {:else if !loading}
        <div class="placeholder">
          <p>Execute a query to see results here.</p>
        </div>
      {/if}

      {#if loading}
        <div class="loading">
          <p>Executing query...</p>
        </div>
      {/if}
    </div>
  </div>
</div>

{#if showSaveDialog}
  <div class="modal-overlay" on:click={closeSaveDialog}>
    <div class="modal" on:click|stopPropagation>
      <div class="modal-header">
        <h3>Save Query</h3>
        <button class="close-btn" on:click={closeSaveDialog}>×</button>
      </div>
      <div class="modal-body">
        <div class="form-group">
          <label for="query-name">Query Name *</label>
          <input
            id="query-name"
            type="text"
            bind:value={saveQueryName}
            placeholder="Enter query name"
            required
          />
        </div>
        <div class="form-group">
          <label for="query-description">Description</label>
          <input
            id="query-description"
            type="text"
            bind:value={saveQueryDescription}
            placeholder="Enter description (optional)"
          />
        </div>
      </div>
      <div class="modal-footer">
        <button class="btn-secondary" on:click={closeSaveDialog}>
          Cancel
        </button>
        <button
          class="btn-primary"
          on:click={saveQuery}
          disabled={!saveQueryName.trim()}
        >
          Save
        </button>
      </div>
    </div>
  </div>
{/if}

<style>
  .query-panel {
    display: flex;
    height: 100vh;
    background: white;
  }

  .sidebar {
    width: 300px;
    border-right: 1px solid #e5e7eb;
    padding: 1rem;
    background: #f9fafb;
    overflow-y: auto;
  }

  .main-content {
    flex: 1;
    display: flex;
    flex-direction: column;
    padding: 1rem;
    gap: 1rem;
  }

  .section-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
  }

  .section-header h3 {
    margin: 0;
    font-size: 1.125rem;
    font-weight: 600;
    color: #374151;
  }

  .btn-small {
    padding: 0.25rem 0.5rem;
    background: white;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    cursor: pointer;
    font-size: 0.875rem;
  }

  .btn-small:hover {
    background: #f3f4f6;
  }

  .queries-list {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .query-item {
    display: flex;
    align-items: center;
    padding: 0.75rem;
    background: white;
    border: 1px solid #e5e7eb;
    border-radius: 0.375rem;
    cursor: pointer;
  }

  .query-item:hover {
    border-color: #d1d5db;
  }

  .query-item.selected {
    border-color: #3b82f6;
    background: #eff6ff;
  }

  .query-info {
    flex: 1;
  }

  .query-name {
    font-weight: 500;
    color: #374151;
  }

  .query-description {
    font-size: 0.875rem;
    color: #6b7280;
    margin-top: 0.25rem;
  }

  .delete-btn {
    padding: 0.25rem 0.5rem;
    background: none;
    border: none;
    color: #ef4444;
    cursor: pointer;
    font-size: 1.25rem;
    line-height: 1;
  }

  .delete-btn:hover {
    background: #fef2f2;
    border-radius: 0.25rem;
  }

  .empty-state {
    text-align: center;
    color: #9ca3af;
    font-style: italic;
    padding: 2rem;
  }

  .editor-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .btn-secondary {
    padding: 0.5rem 1rem;
    background: white;
    color: #374151;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    cursor: pointer;
    font-weight: 500;
  }

  .btn-secondary:hover:not(:disabled) {
    background: #f9fafb;
  }

  .btn-secondary:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

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

  .modal {
    background: white;
    border-radius: 0.5rem;
    width: 90%;
    max-width: 500px;
    box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
  }

  .modal-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 1.5rem;
    border-bottom: 1px solid #e5e7eb;
  }

  .modal-header h3 {
    margin: 0;
    font-size: 1.25rem;
    font-weight: 600;
    color: #374151;
  }

  .close-btn {
    background: none;
    border: none;
    font-size: 1.5rem;
    color: #6b7280;
    cursor: pointer;
    padding: 0;
    width: 2rem;
    height: 2rem;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .close-btn:hover {
    color: #374151;
  }

  .modal-body {
    padding: 1.5rem;
  }

  .form-group {
    margin-bottom: 1rem;
  }

  .form-group:last-child {
    margin-bottom: 0;
  }

  .form-group label {
    display: block;
    margin-bottom: 0.5rem;
    font-weight: 500;
    color: #374151;
  }

  .form-group input {
    width: 100%;
    padding: 0.75rem;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    font-size: 0.875rem;
  }

  .form-group input:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
  }

  .modal-footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.75rem;
    padding: 1.5rem;
    border-top: 1px solid #e5e7eb;
  }

  .btn-primary {
    padding: 0.5rem 1rem;
    background: #3b82f6;
    color: white;
    border: none;
    border-radius: 0.375rem;
    cursor: pointer;
    font-weight: 500;
  }

  .btn-primary:hover:not(:disabled) {
    background: #2563eb;
  }

  .btn-primary:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .query-editor {
    flex-shrink: 0;
  }

  .query-editor h2 {
    margin: 0 0 0.5rem 0;
    font-size: 1.25rem;
    font-weight: 600;
    color: #374151;
  }

  .editor-container {
    margin-bottom: 0.5rem;
  }

  textarea {
    width: 100%;
    padding: 0.75rem;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 0.875rem;
    line-height: 1.5;
    resize: vertical;
    min-height: 120px;
  }

  textarea:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
  }

  textarea:disabled {
    background-color: #f9fafb;
    cursor: not-allowed;
  }

  .actions {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  button {
    padding: 0.5rem 1rem;
    background-color: #3b82f6;
    color: white;
    border: none;
    border-radius: 0.375rem;
    cursor: pointer;
    font-weight: 500;
  }

  button:hover:not(:disabled) {
    background-color: #2563eb;
  }

  button:disabled {
    background-color: #9ca3af;
    cursor: not-allowed;
  }

  .hint {
    font-size: 0.875rem;
    color: #6b7280;
  }

  .results-container {
    flex: 1;
    overflow-y: auto;
    min-height: 0;
  }

  .results h3,
  .error h3 {
    margin: 0 0 0.75rem 0;
    font-size: 1.125rem;
    font-weight: 600;
  }

  .results h3 {
    color: #059669;
  }

  .error h3 {
    color: #dc2626;
  }

  .error {
    padding: 1rem;
    background-color: #fef2f2;
    border: 1px solid #fecaca;
    border-radius: 0.375rem;
  }

  .error p {
    margin: 0;
    color: #991b1b;
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 0.875rem;
    white-space: pre-wrap;
  }

  .table-container {
    border: 1px solid #e5e7eb;
    border-radius: 0.375rem;
    overflow: auto;
    max-height: 400px;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.875rem;
  }

  th {
    background-color: #f9fafb;
    padding: 0.75rem;
    text-align: left;
    font-weight: 600;
    color: #374151;
    border-bottom: 1px solid #e5e7eb;
    position: sticky;
    top: 0;
  }

  td {
    padding: 0.75rem;
    border-bottom: 1px solid #f3f4f6;
    vertical-align: top;
    max-width: 200px;
    word-wrap: break-word;
  }

  tr:hover {
    background-color: #f9fafb;
  }

  .no-results {
    text-align: center;
    color: #6b7280;
    font-style: italic;
    padding: 2rem;
  }

  .placeholder {
    text-align: center;
    color: #9ca3af;
    padding: 3rem;
    font-style: italic;
  }

  .loading {
    text-align: center;
    color: #6b7280;
    padding: 2rem;
  }
</style>
