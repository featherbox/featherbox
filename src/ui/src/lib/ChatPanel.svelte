<script lang="ts">
  import { t } from './i18n';

  interface Props {
    isLoading: boolean;
    sessionId?: number | null;
  }

  const { isLoading, sessionId = null }: Props = $props();

  let inputValue = $state('');
  let chatHistory = $state<
    Array<{ type: 'user' | 'ai'; message: string; timestamp: string }>
  >([]);
  let currentSessionId = $state<number | null>(null);
  let isProcessing = $state(false);
  let showApiKeyWarning = $state(false);

  async function startNewSession() {
    try {
      const response = await fetch('http://localhost:3000/api/chat/sessions', {
        method: 'POST',
      });
      if (response.ok) {
        const data = await response.json();
        currentSessionId = data.session_id;
        chatHistory = [];
      } else {
        console.error('Session start failed:', response.statusText);
      }
    } catch (error) {
      console.error('Session start error:', error);
    }
  }

  async function loadChatHistory() {
    if (currentSessionId === null) return;

    try {
      const response = await fetch(
        `http://localhost:3000/api/chat/sessions/${currentSessionId}/messages`,
      );
      if (response.ok) {
        const data = await response.json();
        chatHistory = data.messages;
      } else {
        console.error('Failed to load chat history:', response.statusText);
        chatHistory = [];
      }
    } catch (error) {
      console.error('Chat history load error:', error);
      chatHistory = [];
    }
  }

  $effect(() => {
    if (sessionId !== null && sessionId !== currentSessionId) {
      currentSessionId = sessionId;
      loadChatHistory();
    } else if (currentSessionId === null && sessionId === null) {
      startNewSession();
    }
  });

  async function handleSubmit() {
    if (!inputValue.trim() || isProcessing || currentSessionId === null) return;

    try {
      const response = await fetch('http://localhost:3000/api/chat/config');
      if (response.ok) {
        const config = await response.json();
        if (!config.api_key || config.api_key.trim() === '') {
          showApiKeyWarning = true;
          return;
        }
      } else {
        showApiKeyWarning = true;
        return;
      }
    } catch (error) {
      console.error('Config load error:', error);
      showApiKeyWarning = true;
      return;
    }

    const userMessage = {
      type: 'user' as const,
      message: inputValue.trim(),
      timestamp: new Date().toISOString(),
    };

    chatHistory.push(userMessage);
    const currentInput = inputValue.trim();
    inputValue = '';
    isProcessing = true;

    try {
      const response = await fetch(
        `http://localhost:3000/api/chat/sessions/${currentSessionId}/messages`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ message: currentInput }),
        },
      );

      if (response.ok) {
        const data = await response.json();
        const aiMessage = {
          type: 'ai' as const,
          message: data.response,
          timestamp: new Date().toISOString(),
        };
        chatHistory.push(aiMessage);
      } else {
        const errorMessage = {
          type: 'ai' as const,
          message: `${$t('chat.error_occurred')}: ${response.statusText}`,
          timestamp: new Date().toISOString(),
        };
        chatHistory.push(errorMessage);
      }
      chatHistory = [...chatHistory];
    } catch (error) {
      console.error('Message send error:', error);
      const errorMessage = {
        type: 'ai' as const,
        message: `${$t('chat.error_occurred')}: ${error}`,
        timestamp: new Date().toISOString(),
      };
      chatHistory.push(errorMessage);
      chatHistory = [...chatHistory];
    } finally {
      isProcessing = false;
    }
  }

  function handleKeyDown(event: KeyboardEvent) {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      handleSubmit();
    }
  }

  function formatTime(timestamp: string): string {
    return new Date(timestamp).toLocaleTimeString('ja-JP', {
      hour: '2-digit',
      minute: '2-digit',
    });
  }
</script>

<div class="chat-panel">
  <div class="chat-messages">
    {#if chatHistory.length === 0}
      <div class="welcome-message">
        <p>{$t('chat.welcome')}</p>
        <div class="example-queries">
          <p><strong>{$t('chat.examples.title')}:</strong></p>
          <ul>
            <li>{$t('chat.examples.sales')}</li>
            <li>{$t('chat.examples.users')}</li>
            <li>{$t('chat.examples.popular')}</li>
          </ul>
        </div>
      </div>
    {/if}

    {#each chatHistory as message}
      <div class="message {message.type}">
        <div class="message-content">
          <div class="message-text">{message.message}</div>
          <div class="message-time">{formatTime(message.timestamp)}</div>
        </div>
      </div>
    {/each}

    {#if isProcessing}
      <div class="message ai loading">
        <div class="message-content">
          <div class="message-text">
            <div class="typing-indicator">
              <span></span>
              <span></span>
              <span></span>
            </div>
            {$t('chat.analyzing')}
          </div>
        </div>
      </div>
    {/if}
  </div>

  <div class="chat-input-area">
    {#if showApiKeyWarning}
      <div class="api-key-warning">
        <div class="warning-content">
          <div class="warning-text">
            <strong>{$t('chat.api_key_warning.title')}</strong>
            <p>
              {$t('chat.api_key_warning.description')}
            </p>
          </div>
          <div class="warning-actions">
            <button
              onclick={() => (window.location.hash = '#settings')}
              class="settings-link"
            >
              {$t('chat.api_key_warning.go_to_settings')}
            </button>
            <button
              onclick={() => (showApiKeyWarning = false)}
              class="dismiss-btn"
            >
              {$t('chat.api_key_warning.close')}
            </button>
          </div>
        </div>
      </div>
    {/if}
    <div class="input-container">
      <textarea
        bind:value={inputValue}
        onkeydown={handleKeyDown}
        placeholder={$t('chat.placeholder')}
        class="chat-input"
        rows="3"
        disabled={isProcessing}
      ></textarea>
      <button
        onclick={handleSubmit}
        class="send-button"
        disabled={!inputValue.trim() || isProcessing}
      >
        {isProcessing ? $t('chat.sending') : $t('chat.send')}
      </button>
    </div>
  </div>
</div>

<style>
  .chat-panel {
    display: flex;
    flex-direction: column;
    height: 100%;
  }

  .chat-messages {
    flex: 1;
    overflow-y: auto;
    padding: 1rem;
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .welcome-message {
    text-align: center;
    color: #7f8c8d;
    padding: 2rem;
  }

  .example-queries {
    text-align: left;
    margin-top: 1.5rem;
    background-color: white;
    padding: 1rem;
    border-radius: 8px;
    border: 1px solid #e0e0e0;
  }

  .example-queries ul {
    margin: 0.5rem 0 0 0;
    padding-left: 1.5rem;
  }

  .example-queries li {
    margin-bottom: 0.5rem;
    color: #27ae60;
    cursor: pointer;
  }

  .example-queries li:hover {
    text-decoration: underline;
  }

  .message {
    display: flex;
    max-width: 80%;
  }

  .message.user {
    align-self: flex-end;
  }

  .message.user .message-content {
    background-color: #27ae60;
    color: white;
  }

  .message.ai .message-content {
    background-color: white;
    color: #2c3e50;
    border: 1px solid #e0e0e0;
  }

  .message-content {
    padding: 1rem;
    border-radius: 12px;
    position: relative;
  }

  .message-text {
    line-height: 1.5;
    white-space: pre-wrap;
  }

  .message-time {
    font-size: 0.75rem;
    opacity: 0.7;
    margin-top: 0.5rem;
  }

  .message.user .message-time {
    color: rgba(255, 255, 255, 0.8);
  }

  .loading .message-text {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .typing-indicator {
    display: flex;
    gap: 0.25rem;
  }

  .typing-indicator span {
    width: 8px;
    height: 8px;
    background-color: #27ae60;
    border-radius: 50%;
    animation: typing 1.4s infinite;
  }

  .typing-indicator span:nth-child(2) {
    animation-delay: 0.2s;
  }

  .typing-indicator span:nth-child(3) {
    animation-delay: 0.4s;
  }

  @keyframes typing {
    0%,
    60%,
    100% {
      transform: translateY(0);
    }
    30% {
      transform: translateY(-10px);
    }
  }

  .chat-input-area {
    padding: 1rem;
    background-color: white;
    border-top: 1px solid #e0e0e0;
  }

  .input-container {
    display: flex;
    gap: 0.5rem;
    align-items: flex-end;
  }

  .chat-input {
    flex: 1;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 0.75rem;
    font-size: 0.95rem;
    resize: none;
    font-family: inherit;
  }

  .chat-input:focus {
    outline: none;
    border-color: #27ae60;
    box-shadow: 0 0 0 2px rgba(39, 174, 96, 0.2);
  }

  .chat-input:disabled {
    background-color: #f5f5f5;
    color: #999;
  }

  .send-button {
    padding: 0.75rem 1.5rem;
    background-color: #27ae60;
    color: white;
    border: none;
    border-radius: 8px;
    font-size: 0.95rem;
    font-weight: 500;
    cursor: pointer;
    transition: background-color 0.2s ease;
    white-space: nowrap;
    font-family: inherit;
  }

  .send-button:hover:not(:disabled) {
    background-color: #219a52;
  }

  .send-button:disabled {
    background-color: #95a5a6;
    cursor: not-allowed;
  }

  .api-key-warning {
    background-color: #fff3cd;
    border: 1px solid #ffeaa7;
    border-radius: 8px;
    padding: 1rem;
    margin-bottom: 1rem;
    color: #856404;
  }

  .warning-content {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }

  .warning-text strong {
    display: block;
    margin-bottom: 0.5rem;
    color: #6c5ce7;
  }

  .warning-text p {
    margin: 0;
    font-size: 0.9rem;
    line-height: 1.4;
  }

  .warning-actions {
    display: flex;
    gap: 0.5rem;
    flex-shrink: 0;
  }

  .settings-link {
    background-color: #6c5ce7;
    color: white;
    text-decoration: none;
    border: none;
    padding: 0.5rem 1rem;
    border-radius: 6px;
    font-size: 0.9rem;
    font-weight: 500;
    transition: background-color 0.2s ease;
    cursor: pointer;
    font-family: inherit;
  }

  .settings-link:hover {
    background-color: #5a4fcf;
  }

  .dismiss-btn {
    background: none;
    border: 1px solid #ddd;
    color: #666;
    padding: 0.5rem 1rem;
    border-radius: 6px;
    font-size: 0.9rem;
    cursor: pointer;
    transition: all 0.2s ease;
    font-family: inherit;
  }

  .dismiss-btn:hover {
    background-color: #f1f1f1;
  }
</style>
