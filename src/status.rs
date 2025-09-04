use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    #[serde(flatten)]
    pub states: HashMap<String, State>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub phase: Phase,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<ErrorInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub message: String,
    pub code: String,
    pub at: DateTime<Utc>,
}

impl Status {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    pub async fn create_new(project_dir: &Path) -> Result<(PathBuf, Self)> {
        let status_dir = Self::get_status_dir(project_dir);
        fs::create_dir_all(&status_dir).await?;

        let now = Utc::now();
        let filename = now.format("%Y-%m-%d-%H-%M-%S.json").to_string();
        let path = status_dir.join(filename);

        let status = Self::new();
        status.save(&path).await?;

        Ok((path, status))
    }

    pub async fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }

        let content = fs::read_to_string(path).await?;
        let status: Self = serde_json::from_str(&content)?;
        Ok(status)
    }

    pub async fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content).await?;
        Ok(())
    }

    pub fn get_status_dir(project_dir: &Path) -> PathBuf {
        project_dir.join(".data").join("status")
    }

    pub async fn get_latest(project_dir: &Path) -> Result<Option<(PathBuf, Self)>> {
        let status_dir = Self::get_status_dir(project_dir);

        if !status_dir.exists() {
            return Ok(None);
        }

        let mut entries = fs::read_dir(&status_dir).await?;
        let mut latest_file = None;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                match latest_file {
                    None => latest_file = Some(path),
                    Some(ref current) => {
                        if path > *current {
                            latest_file = Some(path);
                        }
                    }
                }
            }
        }

        if let Some(path) = latest_file {
            let status = Self::load(&path).await?;
            Ok(Some((path, status)))
        } else {
            Ok(None)
        }
    }

    pub fn start_task(&mut self, table_name: String) {
        self.states.insert(
            table_name,
            State {
                phase: Phase::Running,
                started_at: Utc::now(),
                completed_at: None,
                error: None,
            },
        );
    }

    pub fn complete_task(&mut self, table_name: &str) {
        if let Some(state) = self.states.get_mut(table_name) {
            state.phase = Phase::Completed;
            state.completed_at = Some(Utc::now());
            state.error = None;
        }
    }

    pub fn fail_task(&mut self, table_name: &str, error_message: String, error_code: String) {
        if let Some(state) = self.states.get_mut(table_name) {
            state.phase = Phase::Failed;
            state.error = Some(ErrorInfo {
                message: error_message,
                code: error_code,
                at: Utc::now(),
            });
        }
    }

    pub fn is_running(&self) -> bool {
        self.states
            .values()
            .any(|state| state.phase == Phase::Running)
    }

    pub fn all_completed(&self) -> bool {
        !self.states.is_empty()
            && self
                .states
                .values()
                .all(|state| state.phase == Phase::Completed)
    }

    pub fn has_failures(&self) -> bool {
        self.states
            .values()
            .any(|state| state.phase == Phase::Failed)
    }

    pub fn get_failed_tables(&self) -> Vec<String> {
        self.states
            .iter()
            .filter(|(_, state)| state.phase == Phase::Failed)
            .map(|(name, _)| name.clone())
            .collect()
    }

    pub fn get_completed_tables(&self) -> HashMap<String, DateTime<Utc>> {
        self.states
            .iter()
            .filter(|(_, state)| state.phase == Phase::Completed)
            .filter_map(|(name, state)| state.completed_at.map(|ts| (name.clone(), ts)))
            .collect()
    }
}

impl Default for Status {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatusInfo {
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub tasks: Vec<TaskStatusInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusInfo {
    pub table_name: String,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

impl Status {
    pub fn to_pipeline_info(&self) -> PipelineStatusInfo {
        let overall_status = if self.is_running() {
            "running"
        } else if self.has_failures() {
            "failed"
        } else if self.all_completed() {
            "completed"
        } else {
            "pending"
        };

        let started_at = self.states.values().map(|s| s.started_at).min();

        let completed_at = if !self.is_running() {
            self.states.values().filter_map(|s| s.completed_at).max()
        } else {
            None
        };

        let tasks = self
            .states
            .iter()
            .map(|(name, state)| TaskStatusInfo {
                table_name: name.clone(),
                status: match state.phase {
                    Phase::Running => "running",
                    Phase::Completed => "completed",
                    Phase::Failed => "failed",
                }
                .to_string(),
                started_at: Some(state.started_at),
                completed_at: state.completed_at,
                error_message: state.error.as_ref().map(|e| e.message.clone()),
            })
            .collect();

        PipelineStatusInfo {
            status: overall_status.to_string(),
            started_at,
            completed_at,
            tasks,
        }
    }
}
