use anyhow::Result;
use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::SystemTime,
    time::UNIX_EPOCH,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatConfig {
    pub api_key: String,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageRole {
    #[serde(rename = "user")]
    User,
    #[serde(rename = "ai")]
    AI,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub r#type: MessageRole,
    pub message: String,
    pub timestamp: String,
}

#[derive(Debug)]
pub struct ChatSession {
    pub messages: Vec<ChatMessage>,
    pub config: ChatConfig,
}

#[derive(Clone)]
pub struct AppState {
    sessions: Arc<Mutex<HashMap<u64, ChatSession>>>,
    next_session_id: Arc<Mutex<u64>>,
    http_client: Client,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: Arc::new(Mutex::new(1)),
            http_client: Client::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StartSessionResponse {
    session_id: u64,
}

#[derive(Serialize, Deserialize)]
struct SendMessageRequest {
    message: String,
}

#[derive(Serialize, Deserialize)]
struct SendMessageResponse {
    response: String,
}

#[derive(Serialize, Deserialize)]
struct GetMessagesResponse {
    messages: Vec<ChatMessage>,
}

#[derive(Serialize, Deserialize)]
struct GeminiRequest {
    contents: Vec<GeminiContent>,
}

#[derive(Serialize, Deserialize)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Serialize, Deserialize)]
struct GeminiPart {
    text: String,
}

#[derive(Serialize, Deserialize)]
struct GeminiResponse {
    candidates: Vec<GeminiCandidate>,
}

#[derive(Serialize, Deserialize)]
struct GeminiCandidate {
    content: GeminiContent,
}

fn get_config_path() -> Result<std::path::PathBuf, String> {
    dirs::config_dir()
        .ok_or("Config directory not found".to_string())
        .map(|dir| dir.join("featherbox").join("chat_config.json"))
}

async fn load_config() -> Result<ChatConfig, String> {
    let config_path = get_config_path()?;

    if !config_path.exists() {
        return Ok(ChatConfig {
            api_key: String::new(),
            model: "gemini-2.0-flash-exp".to_string(),
        });
    }

    let json_data = std::fs::read_to_string(&config_path)
        .map_err(|e| format!("Failed to read config file: {e}"))?;

    let config: ChatConfig =
        serde_json::from_str(&json_data).map_err(|e| format!("JSON parsing error: {e}"))?;

    Ok(config)
}

async fn save_config_impl(config: &ChatConfig) -> Result<(), String> {
    let config_path = get_config_path()?;

    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config directory: {e}"))?;
    }

    let json_data = serde_json::to_string_pretty(&config)
        .map_err(|e| format!("JSON serialization error: {e}"))?;

    std::fs::write(&config_path, json_data)
        .map_err(|e| format!("Failed to save config file: {e}"))?;

    Ok(())
}

async fn get_chat_config() -> Result<Json<ChatConfig>, (StatusCode, String)> {
    match load_config().await {
        Ok(config) => Ok(Json(config)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

async fn save_chat_config(
    Json(config): Json<ChatConfig>,
) -> Result<StatusCode, (StatusCode, String)> {
    match save_config_impl(&config).await {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

async fn start_session(
    State(state): State<AppState>,
) -> Result<Json<StartSessionResponse>, (StatusCode, String)> {
    let config = match load_config().await {
        Ok(config) => config,
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    };

    if config.api_key.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "API key not configured".to_string(),
        ));
    }

    let session_id = {
        let mut next_id = state.next_session_id.lock().unwrap();
        let id = *next_id;
        *next_id += 1;
        id
    };

    let session = ChatSession {
        messages: Vec::new(),
        config,
    };

    {
        let mut sessions = state.sessions.lock().unwrap();
        sessions.insert(session_id, session);
    }

    Ok(Json(StartSessionResponse { session_id }))
}

async fn send_message(
    Path(session_id): Path<u64>,
    State(state): State<AppState>,
    Json(request): Json<SendMessageRequest>,
) -> Result<Json<SendMessageResponse>, (StatusCode, String)> {
    let mut session = {
        let mut sessions = state.sessions.lock().unwrap();
        match sessions.remove(&session_id) {
            Some(session) => session,
            None => return Err((StatusCode::BAD_REQUEST, "Invalid session ID".to_string())),
        }
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let timestamp_str = chrono::DateTime::from_timestamp_millis(timestamp)
        .unwrap()
        .to_rfc3339();

    let user_message = ChatMessage {
        r#type: MessageRole::User,
        message: request.message.clone(),
        timestamp: timestamp_str,
    };
    session.messages.push(user_message);

    let response =
        match call_gemini_api(&state.http_client, &session.config, &session.messages).await {
            Ok(response) => response,
            Err(e) => {
                let mut sessions = state.sessions.lock().unwrap();
                sessions.insert(session_id, session);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Gemini API error: {e}"),
                ));
            }
        };

    let ai_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let ai_timestamp_str = chrono::DateTime::from_timestamp_millis(ai_timestamp)
        .unwrap()
        .to_rfc3339();

    let ai_message = ChatMessage {
        r#type: MessageRole::AI,
        message: response.clone(),
        timestamp: ai_timestamp_str,
    };
    session.messages.push(ai_message);

    {
        let mut sessions = state.sessions.lock().unwrap();
        sessions.insert(session_id, session);
    }

    Ok(Json(SendMessageResponse { response }))
}

async fn get_messages(
    Path(session_id): Path<u64>,
    State(state): State<AppState>,
) -> Result<Json<GetMessagesResponse>, (StatusCode, String)> {
    let sessions = state.sessions.lock().unwrap();
    match sessions.get(&session_id) {
        Some(session) => Ok(Json(GetMessagesResponse {
            messages: session.messages.clone(),
        })),
        None => Err((StatusCode::BAD_REQUEST, "Invalid session ID".to_string())),
    }
}

async fn call_gemini_api(
    client: &Client,
    config: &ChatConfig,
    messages: &[ChatMessage],
) -> Result<String, String> {
    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        config.model, config.api_key
    );

    let mut contents = Vec::new();

    for message in messages {
        let role = match message.r#type {
            MessageRole::User => "user",
            MessageRole::AI => "model",
        };

        contents.push(GeminiContent {
            role: role.to_string(),
            parts: vec![GeminiPart {
                text: message.message.clone(),
            }],
        });
    }

    let request_body = GeminiRequest { contents };

    let response = client
        .post(&url)
        .json(&request_body)
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {e}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(format!("API error {status}: {text}"));
    }

    let gemini_response: GeminiResponse = response
        .json()
        .await
        .map_err(|e| format!("JSON parsing failed: {e}"))?;

    gemini_response
        .candidates
        .first()
        .and_then(|candidate| candidate.content.parts.first())
        .map(|part| part.text.clone())
        .ok_or_else(|| "No response from Gemini API".to_string())
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/sessions", post(start_session)).route(
        "/sessions/{session_id}/messages",
        get(get_messages).post(send_message),
    )
}

pub fn config_routes() -> Router {
    Router::new().route("/chat/config", get(get_chat_config).post(save_chat_config))
}
