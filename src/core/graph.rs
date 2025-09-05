use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub updated_at: Option<DateTime<Utc>>,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Graph {
    nodes: HashMap<String, Node>,
}

impl Graph {
    pub async fn load(project_dir: &Path) -> Result<Self> {
        let path = Self::get_path(project_dir);

        if !path.exists() {
            return Ok(Self::default());
        }

        let content = fs::read_to_string(&path).await?;
        Ok(Graph {
            nodes: serde_json::from_str(&content)?,
        })
    }

    pub async fn save(&self, project_dir: &Path) -> Result<()> {
        let path = Self::get_path(project_dir);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let content = serde_json::to_string_pretty(&self.nodes)?;
        fs::write(&path, content).await?;
        Ok(())
    }

    fn get_path(project_dir: &Path) -> PathBuf {
        project_dir.join(".data").join("metadata.json")
    }

    pub fn upstream(&self, name: &str) -> Vec<String> {
        let mut visited = std::collections::HashSet::new();
        self.all_upstream(name, &mut visited)
    }

    fn all_upstream(
        &self,
        name: &str,
        visited: &mut std::collections::HashSet<String>,
    ) -> Vec<String> {
        if let Some(node) = self.nodes.get(name) {
            let mut all_upstream = Vec::new();

            for dep in &node.dependencies {
                if !visited.contains(dep) {
                    visited.insert(dep.clone());
                    all_upstream.push(dep.clone());

                    let nested_upstream = self.all_upstream(dep, visited);
                    all_upstream.extend(nested_upstream);
                }
            }

            all_upstream
        } else {
            Vec::new()
        }
    }

    pub fn direct_downstream(&self, name: &str) -> Vec<String> {
        let mut downstream_nodes = Vec::new();

        for node in self.nodes.values() {
            if node.dependencies.contains(&name.to_string()) {
                downstream_nodes.push(node.name.clone());
            }
        }

        downstream_nodes
    }

    pub fn delete_node(&mut self, name: &str) {
        self.nodes.remove(name);
    }

    pub fn update_node(&mut self, name: &str) {
        self.reset_updated_at(name);

        let downstream_nodes = self.downstream(name);
        for node_name in downstream_nodes {
            self.reset_updated_at(&node_name);
        }
    }

    pub fn has_node(&self, name: &str) -> bool {
        self.nodes.contains_key(name)
    }

    fn reset_updated_at(&mut self, name: &str) {
        if let Some(node) = self.nodes.get_mut(name) {
            node.updated_at = None;
        }
    }

    pub fn create_node(&mut self, name: &str, dependencies: &[&str]) {
        self.nodes.insert(
            name.to_string(),
            Node {
                name: name.to_string(),
                updated_at: None,
                dependencies: Vec::new(),
            },
        );
        self.update_dependencies(name, dependencies);
    }

    pub fn update_dependencies(&mut self, name: &str, dependencies: &[&str]) {
        if let Some(node) = self.nodes.get_mut(name) {
            node.dependencies = dependencies.iter().map(|s| s.to_string()).collect();
        }
    }

    pub fn set_current_time(&mut self, name: &str) {
        if let Some(node) = self.nodes.get_mut(name) {
            node.updated_at = Some(Utc::now());
        }
    }

    pub fn downstream(&self, name: &str) -> Vec<String> {
        let mut visited = std::collections::HashSet::new();
        self.all_downstream(name, &mut visited)
    }

    fn all_downstream(
        &self,
        name: &str,
        visited: &mut std::collections::HashSet<String>,
    ) -> Vec<String> {
        let mut all_downstream = Vec::new();
        let direct_downstream = self.direct_downstream(name);

        for downstream_name in direct_downstream {
            if !visited.contains(&downstream_name) {
                visited.insert(downstream_name.clone());
                all_downstream.push(downstream_name.clone());

                let nested_downstream = self.all_downstream(&downstream_name, visited);
                all_downstream.extend(nested_downstream);
            }
        }

        all_downstream
    }

    pub fn get_node(&self, name: &str) -> Option<&Node> {
        self.nodes.get(name)
    }
}

use sqlparser::{
    ast::{Statement, TableFactor},
    dialect::DuckDbDialect,
    parser::Parser,
};

pub fn dependent_tables(sql: &str) -> Result<Vec<String>, String> {
    let dialect = DuckDbDialect {};
    let ast = match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => ast,
        Err(e) => return Err(e.to_string()),
    };

    if let Some(Statement::Query(query)) = ast.first()
        && let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref()
    {
        let mut tables = Vec::new();

        for table in &select.from {
            collect_table_names(&table.relation, &mut tables);

            for join in &table.joins {
                collect_table_names(&join.relation, &mut tables);
            }
        }

        return Ok(tables);
    }

    Ok(vec![])
}

pub fn collect_table_names(table_factor: &TableFactor, tables: &mut Vec<String>) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            tables.push(name.to_string());
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_names(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                collect_table_names(&join.relation, tables);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_graph() -> Result<()> {
        let tempdir = tempdir().unwrap();
        let testdir = tempdir.path();
        let mut graph = Graph::load(testdir).await?;

        // Create Node
        graph.create_node("a", &[]);
        graph.create_node("e", &[]);
        graph.create_node("b", &["a"]);
        graph.create_node("c", &["b"]);
        graph.create_node("d", &["b", "e"]);
        println!("{:#?}", graph);

        // a-->b-->c
        //     |
        // e---+-->d

        graph.save(testdir).await?;

        // Load Graph
        let mut graph = Graph::load(testdir).await?;
        assert!(graph.has_node("a"));
        assert!(graph.has_node("b"));
        assert!(graph.has_node("c"));
        assert!(graph.has_node("d"));
        assert!(graph.has_node("e"));

        // Downstream
        assert_eq!(graph.downstream("a"), vec!["b", "c", "d"]);
        assert_eq!(graph.downstream("c"), vec![] as Vec<String>);
        assert_eq!(graph.downstream("b"), vec!["c", "d"]);
        assert_eq!(graph.downstream("d"), vec![] as Vec<String>);
        assert_eq!(graph.downstream("e"), vec!["d"]);

        // Upstream
        assert_eq!(graph.upstream("a"), vec![] as Vec<String>);
        assert_eq!(graph.upstream("b"), vec!["a"]);
        assert_eq!(graph.upstream("c"), vec!["b", "a"]);
        assert_eq!(graph.upstream("d"), vec!["b", "a", "e"]);
        assert_eq!(graph.upstream("e"), vec![] as Vec<String>);

        // Update Node
        graph.set_current_time("a");
        graph.set_current_time("b");
        graph.set_current_time("c");
        graph.set_current_time("d");
        graph.set_current_time("e");

        graph.update_node("a");

        // update itself
        assert!(graph.get_node("a").unwrap().updated_at.is_none());
        // referenced node
        assert!(graph.get_node("b").unwrap().updated_at.is_none());
        assert!(graph.get_node("c").unwrap().updated_at.is_none());
        assert!(graph.get_node("d").unwrap().updated_at.is_none());
        // non-referenced node
        assert!(graph.get_node("e").unwrap().updated_at.is_some());

        // Delete Node
        graph.delete_node("c");
        graph.delete_node("d");

        // a-->b
        //
        // e

        graph.save(testdir).await?;

        let graph = Graph::load(testdir).await?;
        assert!(graph.has_node("a"));
        assert!(graph.has_node("b"));
        assert!(!graph.has_node("c"));
        assert!(!graph.has_node("d"));
        assert!(graph.has_node("e"));

        Ok(())
    }

    #[test]
    fn test_dependent_tables() {
        let sql = "SELECT * FROM users";
        let tables = dependent_tables(sql).unwrap();
        assert_eq!(tables, vec!["users"]);

        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let tables = dependent_tables(sql).unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_collect_table_names() {
        let sql = "SELECT * FROM test_table";
        let tables = dependent_tables(sql).unwrap();
        assert_eq!(tables, vec!["test_table"]);
    }
}
