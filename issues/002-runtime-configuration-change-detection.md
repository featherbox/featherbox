# 実行環境設定の変更検出機能の追加

## 目的
プロジェクト設定（project.yml）の変更を検出し、実行環境に影響する変更時に適切にパイプラインを再実行する。

## 背景
現在の差分検出システムは、アダプターとモデルの設定変更のみを検出し、実行環境に関する設定変更を考慮していない。しかし、以下の設定変更は実行結果に大きな影響を与える可能性がある：

### 現在検出されない実行環境の変更
1. **ストレージ設定の変更**
   - `storage`の変更（ローカル ↔ S3の切り替え）
   - データの読み書き場所が変わる

2. **データベース設定の変更**
   - `database`の接続先変更
   - カタログデータベースの変更

3. **外部接続設定の変更**
   - `connections`の追加・削除・変更
   - S3、API等の外部リソースへのアクセス方法の変更

### 現在のコード制限
- `src/dependency.rs:16-128`では`AdapterConfig`と`ModelConfig`のみを比較
- `ProjectConfig`の変更が`detect_changes`で考慮されていない
- プロジェクト設定の変更は全体再実行を促すべきだが、現在は無視される

## 期待する結果

### 1. ProjectConfig変更検出の実装
- `src/config/project.rs`に`has_changed`メソッドの追加
- 実行に影響する設定変更の適切な検出

### 2. 検出すべき設定項目
```rust
impl ProjectConfig {
    pub fn has_changed(&self, other: &Self) -> bool {
        // ストレージ設定
        self.storage != other.storage ||
        // データベース設定（接続情報）
        self.database.has_connection_changed(&other.database) ||
        // 外部接続設定
        self.connections != other.connections
    }
}
```

### 3. 影響範囲の考慮
- **ストレージ変更**: データの保存場所変更 → 全体再実行
- **DB設定変更**: カタログ変更 → 全体再実行
- **接続設定変更**: 影響を受けるアダプターのみ再実行

### 4. 実装方針
- `detect_changes`でプロジェクト設定も比較
- プロジェクト設定の変更履歴をデータベースに保存
- 設定変更の種類に応じた適切な影響範囲計算

### 5. 変更タイプの分類
```rust
pub enum ConfigChangeType {
    ExecutionEnvironment,  // 全体再実行が必要
    ConnectionOnly,        // 関連アダプターのみ再実行
    NonFunctional,        // 再実行不要（説明文の変更等）
}
```

### 6. descriptionフィールドの除外継続
- `description`フィールドの変更は引き続き無視
- 機能に影響しないメタデータの変更として扱う
