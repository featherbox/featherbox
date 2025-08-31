export interface ProjectConfig {
  storage: StorageConfig;
  database: DatabaseConfig;
  connections: Record<string, ConnectionConfig>;
  secret_key_path?: string;
}

export interface StorageConfig {
  type: 'local' | 's3';
  path?: string;
  bucket?: string;
  region?: string;
  endpoint_url?: string;
  auth_method?: 'credential_chain' | 'explicit';
  access_key_id?: string;
  secret_access_key?: string;
  session_token?: string;
  path_style_access?: boolean;
}

export interface DatabaseConfig {
  type: 'sqlite' | 'mysql' | 'postgresql';
  path?: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
}

export interface ConnectionConfig {
  type: 'localfile' | 's3' | 'sqlite' | 'mysql' | 'postgresql';
  base_path?: string;
  path?: string;
  bucket?: string;
  region?: string;
  endpoint_url?: string;
  auth_method?: 'credential_chain' | 'explicit';
  access_key_id?: string;
  secret_access_key?: string;
  session_token?: string;
  path_style_access?: boolean;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
}

export interface CreateProjectRequest {
  project_name: string;
  config: ProjectConfig;
}

export interface CreateProjectResponse {
  project_path: string;
}

export interface ApiError {
  message: string;
  status?: number;
}

export interface AdapterSummary {
  name: string;
  description?: string;
  connection: string;
  source_type: string;
}

export interface AdapterConfig {
  connection: string;
  description?: string;
  source: AdapterSource;
  columns: ColumnConfig[];
}

export interface AdapterSource {
  type: 'file' | 'database';
  file?: FileConfig;
  format?: FormatConfig;
  table_name?: string;
}

export interface FileConfig {
  path: string;
  compression?: string;
  max_batch_size?: string;
}

export interface FormatConfig {
  type: string;
  delimiter?: string;
  null_value?: string;
  has_header?: boolean;
}

export interface ColumnConfig {
  name: string;
  type: string;
  description?: string;
}

export interface AdapterDetails {
  name: string;
  config: AdapterConfig;
}

export interface ModelSummary {
  name: string;
  path: string;
  description?: string;
}

export interface ModelConfig {
  sql: string;
  description?: string;
  depends?: string[];
}

export interface ModelDetails {
  name: string;
  path: string;
  config: ModelConfig;
}

export interface ConnectionSummary {
  name: string;
  connection_type: string;
  details: string;
}

export interface ConnectionDetails {
  name: string;
  type: string;
  [key: string]: any; // 接続タイプによって異なるプロパティ
}
