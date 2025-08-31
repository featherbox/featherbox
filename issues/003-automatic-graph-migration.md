# 自動グラフマイグレーション機能の実装

## 目的
`fbox run`実行時に設定変更を自動検出し、手動でのグラフマイグレーションを不要にして、シームレスなパイプライン実行を実現する。

## 背景
現在のワークフローでは、ユーザーがパイプラインを実行する前に手動で`fbox migrate`を実行する必要がある。これにより以下の問題が発生している：

### 現在の問題点
1. **手動ワークフロー**
   ```bash
   # 現在の必須ワークフロー
   fbox migrate  # グラフ更新
   fbox run      # パイプライン実行
   ```

2. **エラーによる実行停止**
   - `src/commands/run.rs:39-44`でグラフIDが存在しない場合にエラー終了
   - 初回実行時や設定変更後にユーザーが困惑する

3. **実行履歴の複雑性**
   - グラフIDと実行履歴の管理が分離されている
   - どのグラフバージョンで何が実行されたかが追跡しにくい

### ユーザー体験の課題
- パイプライン実行の度にマイグレーションの必要性を確認する手間
- 設定変更後に実行が失敗する可能性
- 複数ステップの実行が必要

## 期待する結果

### 1. シームレスな実行体験
```bash
# 理想的なワークフロー
fbox run      # 設定変更の自動検出 → 必要に応じてマイグレーション → 実行
```

### 2. 自動マイグレーション機能の実装

#### `run`コマンドの改善
```rust
// src/commands/run.rs での実装イメージ
pub async fn run(project_path: &Path) -> Result<()> {
    let config = Config::load_from_directory(&project_root)?;
    let app_db = connect_app_db(&config.project).await?;
    
    // 自動的にグラフの状態をチェック・更新
    let graph_id = ensure_current_graph(&app_db, &config).await?;
    
    // 以下、現在の実行ロジック
    // ...
}

async fn ensure_current_graph(db: &DatabaseConnection, config: &Config) -> Result<i32> {
    // 1. 最新グラフIDを取得（存在しない場合は初回実行）
    // 2. 設定変更を検出
    // 3. 変更があれば新しいグラフを作成
    // 4. グラフIDを返す
}
```

### 3. 段階的な自動化レベル
1. **警告付き自動実行**（初期実装）
   - 変更検出時に「グラフを更新しています...」と表示
   - ユーザーに何が起こっているかを明確に伝える

2. **完全自動実行**（将来実装）
   - 設定ファイル変更時の自動検出
   - サイレントなグラフ更新

### 4. 実行履歴の統合管理
- グラフ作成と実行を一つのトランザクションとして扱う
- 実行ログにどのグラフバージョンが使用されたかを記録
- ロールバック機能のための履歴保持

### 5. エラーハンドリングの改善
```rust
// 現在のエラーハンドリングの改善
let graph_id = match latest_graph_id(&app_db).await? {
    Some(id) => {
        // 変更チェックと更新
        if has_configuration_changes(&app_db, &config).await? {
            println!("設定変更を検出しました。グラフを更新しています...");
            migrate_from_config(&config, &app_db).await?.unwrap_or(id)
        } else {
            id
        }
    }
    None => {
        println!("初回実行です。グラフを作成しています...");
        migrate_from_config(&config, &app_db).await?
            .ok_or_else(|| anyhow!("グラフの作成に失敗しました"))?
    }
};
```

### 6. 下位互換性の維持
- `fbox migrate`コマンドは引き続き利用可能
- 明示的なマイグレーションが必要な場合の選択肢として保持
- CI/CDパイプラインでの explicit な制御が可能

### 7. 設定オプション
```yaml
# project.yml
auto_migration: true  # デフォルト: true
migration_strategy: "auto" | "prompt" | "manual"
```