use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(flatten)]
    pub nodes: HashMap<String, Node>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub last_updated_at: Option<DateTime<Utc>>,
    pub referenced: Vec<String>,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    pub async fn load(project_dir: &Path) -> Result<Self> {
        let path = Self::get_path(project_dir);

        if !path.exists() {
            return Ok(Self::new());
        }

        let content = fs::read_to_string(&path).await?;
        let metadata: Self = serde_json::from_str(&content)?;
        Ok(metadata)
    }

    pub async fn save(&self, project_dir: &Path) -> Result<()> {
        let path = Self::get_path(project_dir);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let content = serde_json::to_string_pretty(self)?;
        fs::write(&path, content).await?;
        Ok(())
    }

    pub fn get_path(project_dir: &Path) -> PathBuf {
        project_dir.join(".data").join("metadata.json")
    }

    pub fn get_node(&self, table_name: &str) -> Option<&Node> {
        self.nodes.get(table_name)
    }

    pub fn get_node_mut(&mut self, table_name: &str) -> &mut Node {
        self.nodes
            .entry(table_name.to_string())
            .or_insert_with(|| Node {
                last_updated_at: None,
                referenced: Vec::new(),
            })
    }

    pub fn update_node_timestamp(&mut self, table_name: &str, timestamp: DateTime<Utc>) {
        let node = self.get_node_mut(table_name);
        node.last_updated_at = Some(timestamp);
    }

    pub fn set_dependencies(&mut self, dependencies: HashMap<String, Vec<String>>) {
        self.nodes.clear();

        let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();

        for (table, deps) in &dependencies {
            for dep in deps {
                reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .push(table.clone());
            }
        }

        for table in dependencies.keys() {
            let referenced = reverse_deps.get(table).cloned().unwrap_or_default();

            self.nodes.insert(
                table.clone(),
                Node {
                    last_updated_at: self.nodes.get(table).and_then(|n| n.last_updated_at),
                    referenced,
                },
            );
        }

        for (table, referenced_by) in reverse_deps {
            self.nodes.entry(table).or_insert(Node {
                last_updated_at: None,
                referenced: referenced_by,
            });
        }
    }

    pub fn get_impacted_tables(&self, changed_tables: &[String]) -> Vec<String> {
        let mut impacted = changed_tables.to_vec();
        let mut visited = std::collections::HashSet::new();
        let mut queue = changed_tables.to_vec();

        while let Some(table) = queue.pop() {
            if !visited.insert(table.clone()) {
                continue;
            }

            if let Some(node) = self.get_node(&table) {
                for referenced in &node.referenced {
                    if !visited.contains(referenced) {
                        queue.push(referenced.clone());
                        impacted.push(referenced.clone());
                    }
                }
            }
        }

        impacted
    }

    pub fn get_oldest_dependency_timestamp(
        &self,
        table_name: &str,
        dependencies: &[String],
    ) -> Option<DateTime<Utc>> {
        dependencies
            .iter()
            .filter_map(|dep| self.get_node(dep))
            .filter_map(|node| node.last_updated_at)
            .min()
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self::new()
    }
}
