use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    pub phase: Phase,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<ErrorInfo>,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskStatus {
    pub fn start(&mut self, started_at: DateTime<Utc>) {
        self.phase = Phase::Running;
        self.started_at = Some(started_at);
    }

    pub fn new() -> Self {
        Self {
            started_at: None,
            phase: Phase::Waiting,
            completed_at: None,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Running,
    Completed,
    Failed,
    Waiting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub message: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatus {
    pub phase: Phase,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub tasks: HashMap<String, TaskStatus>,
}

pub struct StatusManager {
    pub path: PathBuf,
}

impl StatusManager {
    pub fn new(project_dir: &Path) -> Self {
        let status_dir = Self::get_status_dir(project_dir);
        let now = Utc::now();
        let filename = now.format("%Y-%m-%d-%H-%M-%S.json").to_string();
        let path = status_dir.join(filename);

        Self { path }
    }

    pub async fn start(
        &self,
        started_at: DateTime<Utc>,
        table_list: &[String],
    ) -> Result<PipelineStatus> {
        let status = PipelineStatus {
            phase: Phase::Running,
            started_at: Some(started_at),
            completed_at: None,
            tasks: HashMap::from_iter(
                table_list
                    .iter()
                    .cloned()
                    .map(|table| (table, TaskStatus::new())),
            ),
        };

        self.save(&status).await?;

        Ok(status)
    }

    async fn load(&self) -> Result<PipelineStatus> {
        let content = fs::read_to_string(&self.path).await?;
        let status: PipelineStatus = serde_json::from_str(&content)?;
        Ok(status)
    }

    async fn save(&self, pipeline_status: &PipelineStatus) -> Result<()> {
        let content = serde_json::to_string_pretty(pipeline_status)?;
        fs::write(&self.path, content).await?;
        Ok(())
    }

    pub fn get_status_dir(project_dir: &Path) -> PathBuf {
        project_dir.join(".data").join("status")
    }

    pub async fn find_latest_status(project_dir: &Path) -> Result<Option<PipelineStatus>> {
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
            let content = fs::read_to_string(&path).await?;
            let status: PipelineStatus = serde_json::from_str(&content)?;

            Ok(Some(status))
        } else {
            Ok(None)
        }
    }

    pub async fn start_tasks(&mut self, tables: &[String]) -> Result<()> {
        let mut status = self.load().await?;

        for table in tables {
            status.tasks.get_mut(table).unwrap().start(Utc::now());
        }

        self.save(&status).await?;

        Ok(())
    }

    pub async fn is_waiting(&self, table_name: &str) -> Result<bool> {
        let status = self.load().await?;

        Ok(status.tasks.get(table_name).unwrap().phase == Phase::Waiting)
    }

    pub async fn complete_task(&mut self, table_name: &str) -> Result<()> {
        let mut status = self.load().await?;

        if let Some(task) = status.tasks.get_mut(table_name) {
            task.phase = Phase::Completed;
            task.completed_at = Some(Utc::now());
            task.error = None;
        }

        self.save(&status).await?;

        Ok(())
    }

    pub async fn completed_tasks(&self) -> Result<Vec<(String, DateTime<Utc>)>> {
        let status = self.load().await?;

        Ok(status
            .tasks
            .clone()
            .into_iter()
            .filter(|(_, task)| task.phase == Phase::Completed)
            .map(|(table, task)| (table, task.completed_at.unwrap()))
            .collect())
    }

    pub async fn fail_task(&mut self, table_name: &str, error_message: String) -> Result<()> {
        let mut status = self.load().await?;
        if let Some(task) = status.tasks.get_mut(table_name) {
            task.phase = Phase::Failed;
            task.error = Some(ErrorInfo {
                message: error_message,
                at: Utc::now(),
            });
        }
        self.save(&status).await?;
        Ok(())
    }
}
