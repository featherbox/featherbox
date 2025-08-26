# Adapter、Model設定内容の変更検知機能の実装

## 目的

現在のmigrateコマンドはノード名とエッジ（依存関係）の変更のみを検知しているが、Adapter・Modelの設定内容の変更は検知されない。設定内容が変更された場合も、そのノードと下流ノードの再実行を可能にする。

## 期待する結果

以下のような設定変更時に、該当するAdapterまたはModelと、その下流ノードが再実行対象となる：

### Adapter設定の変更検知
- **ファイルパス変更**: `path: "data/users.csv"` → `path: "data/customers.csv"`
- **ファイル形式変更**: `type: csv` → `type: json`
- **カラム定義変更**: カラム名、データ型、カラムの追加・削除
- **接続先変更**: `connection: old_db` → `connection: new_db`
- **データベース設定変更**: `table_name: users` → `table_name: customers`

### Model設定の変更検知
- **SQL内容変更**: SQLの内容に少しでも変化があった場合（依存関係が同じでも）

### 変更検知の対象外
- `description`フィールドの変更は検知しない（実行に影響しないため）

## 実装方針

### 1. 設定内容の直接比較
- 前回実行時の`AdapterConfig`・`ModelConfig`構造体をメタデータDBに保存
- 現在の設定と前回の設定をRustの構造体比較（`PartialEq`）で変更を検知
- `description`フィールドは比較対象から除外

### 2. データベーススキーマの拡張
- `__fbox_nodes`テーブルに`config_json`カラムを追加
- 設定内容をJSON形式でシリアライズして保存

### 3. 変更検知ロジックの拡張
- 現在の構造変更検知に加えて、設定内容変更検知を実装
- `GraphChanges`構造体に設定変更されたノードの情報を追加

### 4. 影響範囲分析の拡張
- 設定変更されたノードとその下流ノードを再実行対象に含める
- 既存の`calculate_affected_nodes()`を拡張