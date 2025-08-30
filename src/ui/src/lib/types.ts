export interface ProjectConfig {
  storage: StorageConfig;
  database: DatabaseConfig;
  deployments: DeploymentsConfig;
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

export interface DeploymentsConfig {
  timeout: number;
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