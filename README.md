# FeatherBox

FeatherBox is a lightweight, easy-to-use data pipeline framework designed for developers.

## Features and Benefits
- Extract, Load, Transform data from various sources.
- Use duckdb (Fast!)
- Automatic pipeline management
  - You only need to define adapters and models.
- Single binary for all operations
  - No need to install dependencies, just run the binary.
  - CI/CD friendly.

## Command

```bash
fbox init                  # Initialize a new FeatherBox project
fbox adapter   new/delete  # Create or delete an adapter
fbox model     new/delete  # Create or delete a model
fbox run                   # Generate pipelines and run them
```

## Examples
```
project.yml

./adapters
├── app_logs.yml
└── app_db.yml

./models
├── app_db
│   ├── users.yml
│   └── orders.yml
├── app_logs
│   ├── app_log.yml
│   ├── task_log.yml
│   └── job_log.yml
├── staging
│   ├── users.yml
│   ├── access_logs.yml
│   ├── task_logs.yml
│   ├── job_logs.yml
│   └── orders.yml
└── datastore
    ├── active_users.yml
    ├── sales.yml
    └── orders.yml
```

### Project Settings
```yaml
storage:
  type: local
  path: /home/user/featherbox/storage

database:
  type: sqlite # or 'mysql', 'postgresql'
  path: /home/user/featherbox/database.db

deployments:
  timeout: 10m
```

### Adapters

- Time Series Data
```yaml
type: s3 # or 'localfile', 'gcs', 'azure'
config:
  access_key: $YOUR_ACCESS_KEY
  secret_key: $YOUR_SECRET_KEY
  bucket: $YOUR_BUCKET_NAME
  region: $YOUR_REGION
  endpoint: https://s3.amazonaws.com
  path_style: true # optional, for S3 compatible services
  ssl: true # optional, for secure connections
path: [YYYY]/[MM]/[DD]/foo.bar_[YYYY][MM][DD]T[HH][MM].log.gz # use glob patterns or time-based patterns
range:
  since: 2023-01-01T00:00:00Z
update_strategy: 'filename' # or 'content'
```

- Relational data
```yaml
type: mysql # or 'postgresql', 'sqlite'
config:
  host: localhost
  port: 3306 # or 5432 for Postgre
  user: $YOUR_USERNAME
  password: $YOUR_PASSWORD
  dbname: $YOUR_DATABASE_NAME
incremental:
  type: cdc # or 'columner'
  ...
```

### Models
```yaml
name: active_users
sql: |
  SELECT
    COUNT(*) AS active_users,
    DATE(created_at) AS date
  FROM users
  WHERE last_login >= NOW() - INTERVAL '30 days'
  GROUP BY DATE(created_at)
  ORDER BY DATE(created_at) DESC
max_age: 1d
```

## Architecture

- TODO

```mermaid
TDOO
```

## Database Schema

### Adapters

- TODO

```sql

```

### Models

- TODO

```sql
TODO
```

### Nodes

- TODO

```sql
CREATE TABLE IF NOT EXISTS nodes (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  type VARCHAR(50) NOT NULL, -- 'adapter', 'model', 'pipeline'
  config JSONB, -- configuration for the node
  last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(name)
);
```

```sql
CREATE TABLE IF NOT EXISTS edges (
  id SERIAL PRIMARY KEY,
  from_node_id INT NOT NULL,
  to_node_id INT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (from_node_id) REFERENCES nodes(id),
  FOREIGN KEY (to_node_id) REFERENCES nodes(id),
  UNIQUE(from_node_id, to_node_id)
);
```

### Pipelines

- TODO

```sql
CREATE TABLE IF NOT EXISTS pipelines (
  id SERIAL PRIMARY KEY,
  type VARCHAR(50) NOT NULL, -- 'migrate' or 'sync'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(50) NOT NULL, -- 'pending', 'running', 'completed', 'failed'
  action VARCHAR(50) NOT NULL,
);

CREATE TABLE IF NOT EXISTS destroy_actions (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  pipeline_id INT NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL, -- 'pending', 'running', 'completed', 'failed'
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);

CREATE TABLE IF NOT EXISTS create_actions (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  pipeline_id INT NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL, -- 'pending', 'running', 'completed', 'failed'
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);

CREATE TABLE IF NOT EXISTS update_actions (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  pipeline_id INT NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  update_until TIMESTAMP NOT NULL,
  status VARCHAR(50) NOT NULL, -- 'pending', 'running', 'completed', 'failed'
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);
```

### Delta

```sql
CREATE TABLE IF NOT EXISTS delta (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  table_name VARCHAR(255) NOT NULL,
  type VARCHAR(50) NOT NULL, -- 'create', 'update', 'delete'
  column name VARCHAR(255) NOT NULL,
  value TEXT NOT NULL,
);
```

### Deployments

```sql
CREATE TABLE IF NOT EXISTS deployments (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(50) NOT NULL, -- 'running', 'completed', 'failed'
  pipeline_id INT NOT NULL,
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);

CREATE TABLE IF NOT EXISTS deployment_logs (
  id SERIAL PRIMARY KEY,
  deployment_id INT NOT NULL,
  message TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (deployment_id) REFERENCES deployments(id)
);
```

## Functional Design

### Initialization

### Create adapters

### Delete adapters

### Create models

### Delete models

### Generate pipelines and run them

