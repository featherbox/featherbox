<script lang="ts">
  import ChatPanel from './ChatPanel.svelte';
  import { onMount } from 'svelte';
  import { t } from './i18n';
  import { API_BASE_URL } from './config';

  interface AnalysisSession {
    id: string;
    name: string;
    createdAt: string;
    updatedAt: string;
    sessionId?: number | null;
  }

  let sessions = $state<AnalysisSession[]>([
    {
      id: 'session-1',
      name: '売上分析レポート',
      createdAt: '2024-01-20T10:30:00Z',
      updatedAt: '2024-01-20T15:45:00Z',
    },
    {
      id: 'session-2',
      name: 'ユーザー行動分析',
      createdAt: '2024-01-19T14:20:00Z',
      updatedAt: '2024-01-19T16:30:00Z',
    },
    {
      id: 'session-3',
      name: '商品パフォーマンス',
      createdAt: '2024-01-18T09:15:00Z',
      updatedAt: '2024-01-18T11:45:00Z',
    },
  ]);

  let selectedSession: AnalysisSession | null = $state(null);
  let isLoading = $state(false);
  let currentSessionId = $state<number | null>(null);

  async function handleSessionSelect(session: AnalysisSession) {
    selectedSession = session;
    if (selectedSession.sessionId) {
      currentSessionId = selectedSession.sessionId;
    } else {
      try {
        const response = await fetch(`${API_BASE_URL}/api/chat/sessions`, {
          method: 'POST',
        });
        if (response.ok) {
          const data = await response.json();
          currentSessionId = data.session_id;
          selectedSession.sessionId = data.session_id;
        }
      } catch (error) {
        console.error('Session start error:', error);
      }
    }
  }

  async function handleNewSession() {
    try {
      const response = await fetch(`${API_BASE_URL}/api/chat/sessions`, {
        method: 'POST',
      });
      if (response.ok) {
        const data = await response.json();
        const newSession: AnalysisSession = {
          id: `session-${Date.now()}`,
          name: $t('analysis.new_session_name'),
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          sessionId: data.session_id,
        };
        sessions = [newSession, ...sessions];
        selectedSession = newSession;
        currentSessionId = data.session_id;
      }
    } catch (error) {
      console.error('Session creation error:', error);
    }
  }

  function formatDate(dateString: string): string {
    return new Date(dateString).toLocaleDateString('ja-JP', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  }

  onMount(() => {
    if (sessions.length > 0 && !selectedSession) {
      handleSessionSelect(sessions[0]);
    }
  });
</script>

<div class="analysis-container">
  <div class="analysis-sidebar">
    <div class="sidebar-header">
      <h2>{$t('analysis.title')}</h2>
      <button
        onclick={handleNewSession}
        class="new-session-btn"
        aria-label={$t('analysis.new_session')}
      >
        <svg
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        >
          <line x1="12" y1="5" x2="12" y2="19"></line>
          <line x1="5" y1="12" x2="19" y2="12"></line>
        </svg>
      </button>
    </div>

    <div class="sessions-list">
      {#each sessions as session}
        <button
          class="session-item"
          class:active={selectedSession?.id === session.id}
          onclick={() => handleSessionSelect(session)}
        >
          <div class="session-name">{session.name}</div>
          <div class="session-date">{formatDate(session.updatedAt)}</div>
        </button>
      {/each}
    </div>
  </div>

  <div class="analysis-main">
    {#if selectedSession}
      <div class="analysis-content">
        <div class="content-header">
          <h1>{selectedSession.name}</h1>
          <span class="last-updated"
            >{$t('analysis.last_updated')}: {formatDate(
              selectedSession.updatedAt,
            )}</span
          >
        </div>

        <div class="chat-container">
          <ChatPanel {isLoading} sessionId={currentSessionId} />
        </div>
      </div>
    {:else}
      <div class="empty-selection">
        <div class="empty-content">
          <h2>{$t('analysis.select_session')}</h2>
          <p>{$t('analysis.select_description')}</p>
        </div>
      </div>
    {/if}
  </div>
</div>

<style>
  .analysis-container {
    display: flex;
    height: calc(100vh - 60px);
    overflow: hidden;
  }

  .analysis-sidebar {
    width: 300px;
    height: 100%;
    background-color: #f8f9fa;
    border-right: 1px solid #e0e0e0;
    overflow: hidden;
    display: flex;
    flex-direction: column;
  }

  .sidebar-header {
    padding: 1rem;
    border-bottom: 1px solid #e0e0e0;
    background-color: white;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .sidebar-header h2 {
    margin: 0;
    font-size: 1.25rem;
    color: #2c3e50;
  }

  .new-session-btn {
    background-color: #27ae60;
    color: white;
    border: none;
    border-radius: 6px;
    padding: 0.5rem;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: background-color 0.2s ease;
  }

  .new-session-btn:hover {
    background-color: #219a52;
  }

  .sessions-list {
    flex: 1;
    overflow-y: auto;
    padding: 0.5rem;
  }

  .session-item {
    padding: 0.75rem;
    margin-bottom: 0.25rem;
    border-radius: 6px;
    cursor: pointer;
    transition: all 0.2s ease;
    background-color: white;
    border: 1px solid #e0e0e0;
    width: 100%;
    text-align: left;
  }

  .session-item:hover {
    background-color: #f0f8f0;
    border-color: #27ae60;
  }

  .session-item.active {
    background-color: #e8f5e9;
    border-color: #27ae60;
  }

  .session-name {
    font-weight: 500;
    color: #2c3e50;
    margin-bottom: 0.25rem;
    font-size: 0.95rem;
  }

  .session-date {
    font-size: 0.8rem;
    color: #7f8c8d;
  }

  .analysis-main {
    flex: 1;
    height: 100%;
    overflow: hidden;
    background-color: white;
  }

  .analysis-content {
    display: flex;
    flex-direction: column;
    height: 100%;
  }

  .content-header {
    padding: 1rem 1.5rem;
    border-bottom: 1px solid #e0e0e0;
    background-color: white;
    flex-shrink: 0;
  }

  .content-header h1 {
    margin: 0 0 0.5rem 0;
    font-size: 1.5rem;
    color: #2c3e50;
  }

  .last-updated {
    font-size: 0.85rem;
    color: #7f8c8d;
  }

  .chat-container {
    flex: 1;
    overflow: hidden;
  }

  .empty-selection {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100%;
  }

  .empty-content {
    text-align: center;
    color: #7f8c8d;
  }

  .empty-content h2 {
    margin: 0 0 0.5rem 0;
    color: #2c3e50;
  }
</style>
