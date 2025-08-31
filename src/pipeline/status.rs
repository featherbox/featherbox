use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl std::fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineStatus::Pending => write!(f, "PENDING"),
            PipelineStatus::Running => write!(f, "RUNNING"),
            PipelineStatus::Completed => write!(f, "COMPLETED"),
            PipelineStatus::Failed => write!(f, "FAILED"),
        }
    }
}

impl From<String> for PipelineStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "PENDING" => PipelineStatus::Pending,
            "RUNNING" => PipelineStatus::Running,
            "COMPLETED" => PipelineStatus::Completed,
            "FAILED" => PipelineStatus::Failed,
            _ => PipelineStatus::Pending,
        }
    }
}

impl From<&str> for PipelineStatus {
    fn from(s: &str) -> Self {
        match s {
            "PENDING" => PipelineStatus::Pending,
            "RUNNING" => PipelineStatus::Running,
            "COMPLETED" => PipelineStatus::Completed,
            "FAILED" => PipelineStatus::Failed,
            _ => PipelineStatus::Pending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "PENDING"),
            TaskStatus::Running => write!(f, "RUNNING"),
            TaskStatus::Completed => write!(f, "COMPLETED"),
            TaskStatus::Failed => write!(f, "FAILED"),
        }
    }
}

impl From<String> for TaskStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "PENDING" => TaskStatus::Pending,
            "RUNNING" => TaskStatus::Running,
            "COMPLETED" => TaskStatus::Completed,
            "FAILED" => TaskStatus::Failed,
            _ => TaskStatus::Pending,
        }
    }
}

impl From<&str> for TaskStatus {
    fn from(s: &str) -> Self {
        match s {
            "PENDING" => TaskStatus::Pending,
            "RUNNING" => TaskStatus::Running,
            "COMPLETED" => TaskStatus::Completed,
            "FAILED" => TaskStatus::Failed,
            _ => TaskStatus::Pending,
        }
    }
}
