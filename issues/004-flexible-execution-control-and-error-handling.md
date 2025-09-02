# 柔軟な実行制御とエラーハンドリングの改善

## 目的
部分的な失敗に対してより柔軟な対応ができる実行制御機能を実装し、パイプラインの堅牢性と運用性を向上させる。

## 背景
現在のパイプライン実行は単純な依存関係ベースのスキップロジックのみを提供しており、複雑な実行シナリオに対応できない制限がある。

### 現在の制限
1. **単純なスキップロジック** (`src/pipeline/execution.rs:286-298`)
   ```rust
   fn should_skip_task(&self, table_name: &str, graph: &Graph, failed_tasks: &HashSet<String>) -> bool {
       // 親が失敗したら子を無条件にスキップ
       for edge in &graph.edges {
           if edge.to == table_name && failed_tasks.contains(&edge.from) {
               return true;
           }
       }
       false
   }
   ```

2. **全面的な失敗伝播**
   - 一つのタスクが失敗すると、その下流がすべてスキップされる
   - 部分的なデータでも処理可能なケースが考慮されていない

3. **再実行機能の不足**
   - 失敗したタスクの個別再実行ができない
   - 一時的な問題（ネットワーク障害等）での自動再試行がない

### 実運用での問題
- 大規模パイプラインで一箇所の失敗が全体を停止させる
- データ品質の問題で部分的に処理を続行したいケースに対応できない
- デバッグとトラブルシューティングが困難

## 期待する結果

### 1. 依存関係の種類分け
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum DependencyType {
    Required,    // 必須依存（現在の動作）
    Optional,    // 任意依存（失敗しても続行可能）
    Conditional, // 条件付き依存（データ品質による）
}

#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub dependency_type: DependencyType, // 新しいフィールド
}
```

### 2. 柔軟なスキップロジック
```rust
fn should_skip_task(&self, table_name: &str, graph: &Graph, failed_tasks: &HashSet<String>) -> SkipDecision {
    let dependencies = graph.get_dependencies(table_name);
    
    // 必須依存関係の確認
    let required_failures = dependencies.iter()
        .filter(|dep| dep.dependency_type == DependencyType::Required)
        .filter(|dep| failed_tasks.contains(&dep.from))
        .count();
    
    if required_failures > 0 {
        return SkipDecision::Skip("Required dependencies failed".to_string());
    }
    
    // 任意依存関係の確認
    let optional_failures = dependencies.iter()
        .filter(|dep| dep.dependency_type == DependencyType::Optional)
        .filter(|dep| failed_tasks.contains(&dep.from))
        .count();
    
    if optional_failures > 0 {
        return SkipDecision::ExecuteWithWarning("Some optional dependencies failed".to_string());
    }
    
    SkipDecision::Execute
}

#[derive(Debug)]
enum SkipDecision {
    Execute,
    ExecuteWithWarning(String),
    Skip(String),
}
```

### 3. 再実行・再試行機能
```rust
pub struct ExecutionConfig {
    pub retry_attempts: usize,        // 最大再試行回数
    pub retry_delay_ms: u64,          // 再試行間隔
    pub continue_on_failure: bool,    // 失敗時の続行可否
    pub partial_execution: bool,      // 部分実行モード
}
```

### 4. 実行モードの選択肢
```bash
# 通常実行（現在の動作）
featherbox run

# 部分的な失敗を許容して続行
featherbox run --continue-on-failure

# 特定のテーブルのみ強制実行
featherbox run --force --tables user_stats,reports

# 失敗したタスクのみ再実行
featherbox run --retry-failed

# 依存関係を無視して実行
featherbox run --ignore-dependencies --tables problem_table
```

### 5. エラー分類と対応
```rust
#[derive(Debug, Clone)]
pub enum ExecutionError {
    DataSourceUnavailable { table: String, retryable: bool },
    DataQualityIssue { table: String, severity: Severity },
    ConfigurationError { table: String, retryable: false },
    ResourceConstraint { table: String, retryable: true },
    Unknown { table: String, error: String },
}

#[derive(Debug, Clone)]
pub enum Severity {
    Warning,  // ログ出力して続行
    Error,    // 失敗としてマークするが下流は条件付きで続行
    Critical, // 即座に実行停止
}
```

### 6. 実行状態の詳細追跡
```rust
#[derive(Debug, Clone)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed { reason: String, retryable: bool },
    Skipped { reason: String },
    CompletedWithWarnings { warnings: Vec<String> },
}
```

### 7. 設定での制御
```yaml
# project.yml
execution:
  retry_policy:
    max_attempts: 3
    delay_ms: 5000
    exponential_backoff: true
  
  failure_policy:
    continue_on_optional_failure: true
    max_failed_ratio: 0.3  # 30%まで失敗許容
  
  dependency_overrides:
    user_stats:
      user_data: optional      # デフォルトはrequiredだがoptionalに変更
    reports:
      error_logs: optional
```

### 8. 実行サマリーの改善
```
=== Pipeline Execution Summary ===
✅ Completed: 8 tasks
⚠️  Completed with warnings: 2 tasks  
❌ Failed: 1 task
⏭️  Skipped: 3 tasks (dependency failures)

Failed tasks:
  - external_api_data: Network timeout (retryable)

Skipped tasks:
  - api_analysis: Required dependency 'external_api_data' failed
  - daily_report: Required dependency 'external_api_data' failed
  - weekly_summary: Required dependency 'daily_report' failed

Warnings:
  - user_stats: 5% of records had missing email addresses
  - sales_summary: Data freshness warning (last update: 2 hours ago)

💡 Suggestion: Run 'featherbox run --retry-failed' to retry failed tasks
=======================================
```