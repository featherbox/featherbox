use crate::{
    database::entities::{pipeline_actions, pipelines},
    pipeline::status::{PipelineStatus, TaskStatus},
};
use anyhow::Result;
use chrono::Utc;
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use serde::{Deserialize, Serialize};

pub struct StateManager {
    db: DatabaseConnection,
}

impl StateManager {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn update_pipeline_status(
        &self,
        pipeline_id: i32,
        status: PipelineStatus,
    ) -> Result<()> {
        let pipeline = pipelines::Entity::find_by_id(pipeline_id)
            .one(&self.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Pipeline not found"))?;

        let mut pipeline_active: pipelines::ActiveModel = pipeline.into();
        pipeline_active.status = Set(status.to_string());

        match status {
            PipelineStatus::Running => {
                pipeline_active.started_at = Set(Some(Utc::now().naive_utc()));
            }
            PipelineStatus::Completed | PipelineStatus::Failed => {
                pipeline_active.completed_at = Set(Some(Utc::now().naive_utc()));
            }
            _ => {}
        }

        pipeline_active.update(&self.db).await.map_err(|e| {
            anyhow::anyhow!("Failed to update pipeline status to {}: {}", status, e)
        })?;
        Ok(())
    }

    pub async fn update_task_status(
        &self,
        pipeline_id: i32,
        table_name: &str,
        status: TaskStatus,
        error_message: Option<&str>,
    ) -> Result<()> {
        let action = pipeline_actions::Entity::find()
            .filter(pipeline_actions::Column::PipelineId.eq(pipeline_id))
            .filter(pipeline_actions::Column::TableName.eq(table_name))
            .one(&self.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Pipeline action not found"))?;

        let mut action_active: pipeline_actions::ActiveModel = action.into();
        action_active.status = Set(status.to_string());
        action_active.error_message = Set(error_message.map(|s| s.to_string()));

        match status {
            TaskStatus::Running => {
                action_active.started_at = Set(Some(Utc::now().naive_utc()));
            }
            TaskStatus::Completed | TaskStatus::Failed => {
                action_active.completed_at = Set(Some(Utc::now().naive_utc()));
            }
            _ => {}
        }

        action_active.update(&self.db).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to update task status for {} to {}: {}",
                table_name,
                status,
                e
            )
        })?;
        Ok(())
    }

    pub async fn get_pipeline_status(&self, pipeline_id: i32) -> Result<PipelineStatus> {
        let pipeline = pipelines::Entity::find_by_id(pipeline_id)
            .one(&self.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Pipeline not found"))?;

        Ok(PipelineStatus::from(pipeline.status))
    }

    pub async fn get_tasks_status(&self, pipeline_id: i32) -> Result<Vec<TaskStatusInfo>> {
        let actions = pipeline_actions::Entity::find()
            .filter(pipeline_actions::Column::PipelineId.eq(pipeline_id))
            .all(&self.db)
            .await?;

        let tasks = actions
            .into_iter()
            .map(|action| TaskStatusInfo {
                table_name: action.table_name,
                status: TaskStatus::from(action.status),
                execution_order: action.execution_order,
                started_at: action.started_at,
                completed_at: action.completed_at,
                error_message: action.error_message,
            })
            .collect();

        Ok(tasks)
    }

    pub async fn get_pipeline_info(&self, pipeline_id: i32) -> Result<PipelineInfo> {
        let pipeline = pipelines::Entity::find_by_id(pipeline_id)
            .one(&self.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Pipeline not found"))?;

        let tasks = self.get_tasks_status(pipeline_id).await?;

        Ok(PipelineInfo {
            id: pipeline.id,
            graph_id: pipeline.graph_id,
            status: PipelineStatus::from(pipeline.status),
            created_at: pipeline.created_at,
            started_at: pipeline.started_at,
            completed_at: pipeline.completed_at,
            tasks,
        })
    }

    pub async fn mark_downstream_tasks_failed(
        &self,
        pipeline_id: i32,
        failed_table: &str,
        downstream_tables: &[String],
    ) -> Result<()> {
        for table_name in downstream_tables {
            self.update_task_status(
                pipeline_id,
                table_name,
                TaskStatus::Failed,
                Some(&format!("Upstream task {} failed", failed_table)),
            )
            .await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusInfo {
    pub table_name: String,
    pub status: TaskStatus,
    pub execution_order: i32,
    pub started_at: Option<chrono::NaiveDateTime>,
    pub completed_at: Option<chrono::NaiveDateTime>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PipelineInfo {
    pub id: i32,
    pub graph_id: i32,
    pub status: PipelineStatus,
    pub created_at: chrono::NaiveDateTime,
    pub started_at: Option<chrono::NaiveDateTime>,
    pub completed_at: Option<chrono::NaiveDateTime>,
    pub tasks: Vec<TaskStatusInfo>,
}
