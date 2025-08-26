# 削除されたノードのDuckLakeテーブルクリーンアップ処理

## 目的

現在、Adapter・Modelファイルを削除した場合、対応するDuckLakeテーブルが自動削除されずに残存する問題がある。削除されたノードのテーブルを適切にクリーンアップし、データの整合性を保つ。

## 期待する結果

### Adapter削除時のクリーンアップ
- **現状**: `adapters/users.yml`を削除してもDuckLakeの`users`テーブルが残存
- **期待**: Adapterが削除されたら、対応するDuckLakeテーブルも削除される

### Model削除時のクリーンアップ
- **現状**: `models/user_stats.yml`を削除してもDuckLakeの`user_stats`テーブルが残存
- **期待**: Modelが削除されたら、対応するDuckLakeテーブルも削除される

### 具体的な問題例
```bash
# Adapterを削除した場合
rm adapters/users.yml
fbox migrate  # → removed_nodes: ["users"] として検知
fbox run      # → users テーブルは削除されずDuckLakeに残存

# Modelを削除した場合  
rm models/user_stats.yml
fbox migrate  # → removed_nodes: ["user_stats"] として検知
fbox run      # → user_stats テーブルは削除されずDuckLakeに残存
```

## 実装方針

### 1. クリーンアップフェーズの追加
- `src/pipeline/execution.rs`でパイプライン実行前にクリーンアップフェーズを追加
- 削除されたノードに対応するテーブルの`DROP TABLE`処理を実装

### 2. 既存機能の活用
- **変更検知**: `detect_changes()`による削除ノードの検出（既存）
- **一時テーブル削除**: `drop_temp_table()`の機能拡張
- **テーブル存在確認**: `table_exists()`の活用

### 3. 安全性の考慮
- クリーンアップ処理の詳細ログ記録

### 4. 実装箇所
- パイプライン実行開始時のクリーンアップ処理
- `GraphChanges::removed_nodes`を利用した削除対象テーブルの特定
- DuckLakeでの`DROP TABLE IF EXISTS`実行

### 5. ノード処理の実行順序
既存のLevel概念を活用し、各レベル内で処理順序を統一：

```
Level 1: Adapterノード（依存関係の上流）
  1. 削除: 削除されたAdapterのテーブルをクリーンアップ
  2. 追加: 新しく追加されたAdapterのテーブルを作成  
  3. 修正: 設定変更されたAdapterのテーブルを再作成

Level 2: Level 1に依存するModelノード
  1. 削除: 削除されたModelのテーブルをクリーンアップ
  2. 追加: 新しく追加されたModelのテーブルを作成
  3. 修正: 設定変更されたModelのテーブルを再作成

Level 3: Level 2に依存するModelノード
  ... (同様の処理)
```

### 利点
- **依存関係の安全性**: 上流から下流への順序処理で依存関係エラーを回避
- **既存設計との整合性**: 現在の並列実行Levelと整合
- **効率性**: 各レベル内の同じ操作種別は並列実行可能