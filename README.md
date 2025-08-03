# FeatherBox

FeatherBox is a lightweight, easy-to-use data pipeline framework designed for developers.

## Features and Benefits

- All-in-one data pipeline framework
  - Extract, Load, Transform data from various sources.
- Use Duckdb and DuckLake (Catalog format)
  - Very Fast!
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

connections:
  app_logs:
    type: s3
    access_key: $YOUR_ACCESS_KEY
    secret_key: $YOUR_SECRET_KEY
    bucket: $YOUR_BUCKET_NAME
    region: $YOUR_REGION
    endpoint: https://s3.amazonaws.com
    path_style: true
    ssl: true
```

### Adapters

- Time Series Data

```yaml
name: app_logs
connection: app_logs
file:
  path: <YYYY>/<MM>/<DD>/*_<YYYY><MM><DD>T<HH><MM>.log.gz
  compression: gzip
  max_batch_size: 100MB

update_strategy:
  detection: filename
  timestamp_from: path
  range:
    since: 2023-01-01 00:00:00

format:
  type: 'csv'
  delim: ' '
  nullstr: '-'
  header: false

columns:
  - name: timestamp
    type: datetime
  - name: level
    type: string
  - name: message
    type: string
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
max_batch_records: 10000
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

FeatherBox adopts a delta-based update architecture to achieve efficient data pipeline processing.

### Core Components

1. **Adapter**: Defines connections to external data sources (S3, MySQL, PostgreSQL, etc.)
2. **Model**: Defines data transformation logic using SQL
3. **Delta**: Manages differential data and propagates changes between models
4. **Pipeline**: Automatically generates DAG from adapter and model dependencies
5. **Deployment**: Executes pipeline actions

### Data Flow

```mermaid
graph LR
    subgraph "Data Sources"
        S3[S3/GCS]
        DB[(MySQL/PostgreSQL)]
    end

    subgraph "FeatherBox Core"
        A1[Adapter 1]
        A2[Adapter 2]
        D1[Delta]
        M1[Model 1]
        M2[Model 2]
        M3[Model 3]
        D2[Delta]
        D3[Delta]
    end

    subgraph "Output"
        DW[(Data Warehouse)]
    end

    S3 -->|Extract| A1
    DB -->|CDC/Incremental| A2
    A1 -->|Convert to Delta| D1
    A2 -->|Convert to Delta| D1
    D1 -->|Update| M1
    D1 -->|Update| M2
    M1 -->|Propagate Delta| D2
    M2 -->|Propagate Delta| D3
    D2 -->|Update| M3
    D3 -->|Update| M3
    M3 --> DW
```

### Pipeline Generation Process

```mermaid
graph TD
    Start[fbox run]
    GH{Git Hash Changed?}
    MIG[Migration Pipeline]
    SYNC[Sync Pipeline]
    DAG[Generate DAG]
    DEPLOY[Deploy Actions]

    Start --> GH
    GH -->|Yes| MIG
    GH -->|No| SYNC
    MIG --> DAG
    SYNC --> DAG
    DAG --> DEPLOY

    MIG -.->|Actions| CREATE[Create Models]
    MIG -.->|Actions| DELETE[Delete Models]
    SYNC -.->|Actions| UPDATE[Update Data]
```

### Delta Update Mechanism

1. **Delta Extraction from Data Sources**
   - S3: Detects file path changes (filename-based or content-based)
   - RDB: CDC (Change Data Capture) or column-based incremental updates

2. **Delta Propagation**
   - Records changes from upstream models as Delta
   - Downstream models receive Delta and update their data
   - Generates new Delta from update results and propagates further downstream

3. **Delta Processing Flow**
   - Extract changes from adapters (new files, CDC events, etc.)
   - Convert to standardized Delta format
   - Apply Delta to target model tables
   - Generate new Delta for downstream models

## Database Schema

### Nodes

```sql
CREATE TABLE IF NOT EXISTS nodes (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

```sql
CREATE TABLE IF NOT EXISTS pipelines (
  id SERIAL PRIMARY KEY,
  commit_hash VARCHAR(64) NOT NULL, -- Git commit hash
  type VARCHAR(50) NOT NULL, -- 'migrate' or 'sync'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(50) NOT NULL, -- 'queued', 'running', 'completed'
  action VARCHAR(50) NOT NULL,
);

CREATE TABLE IF NOT EXISTS tasks (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  pipeline_id INT NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  update_since TIMESTAMP NOT NULL,
  update_until TIMESTAMP NOT NULL,
  execution_order INT NOT NULL, -- order of execution in the pipeline
  execution_time_ms INT,
  status VARCHAR(50) NOT NULL, -- 'queued', 'running', 'completed', 'failed'
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);
```

### Delta

```sql
CREATE TABLE IF NOT EXISTS delta (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  pipeline_id INT NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  update_since TIMESTAMP NOT NULL,
  update_until TIMESTAMP NOT NULL,
  delta_records_path VARCHAR(255) NOT NULL, -- path to delta records file
);

CREATE TABLE IF NOT EXISTS delta_records (
  id SERIAL PRIMARY KEY,
  type VARCHAR(50) NOT NULL, -- 'create', 'update', 'delete'
  column_name VARCHAR(255) NOT NULL,
  value TEXT NOT NULL
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

Creates a new FeatherBox project structure.

```bash
fbox init
```

1. **Project Setup**
   - Interactive prompts
     - project name
     - Storage configuration (local/S3/GCS)
     - Database configuration (SQLite/MySQL/PostgreSQL)
   - Creates `project.yml` with default configuration
   - Creates directory structure:

     ```
     ./adapters/    # Adapter definitions
     ./models/      # Model definitions
     ```

   - Initializes database schema

### Create Adapters

Defines new data source connections.

```bash
fbox adapter new <adapter_name>
```

1. **Adapter Creation**
   - Creates YAML file in `./adapters/` directory
   - Validates configuration syntax
   - Tests connection to data source
   - Registers adapter in database

2. **Supported Types**
   - **Time-series data**: S3, GCS, local files with pattern matching
   - **Relational data**: MySQL, PostgreSQL with CDC or incremental updates

### Delete Adapters

Removes adapter and associated data.

```bash
fbox adapter delete <adapter_name>
```

1. **Validation**
   - Checks for dependent models
   - Delete dependencies

2. **Delete Files**

### Create Models

Defines data transformation logic.

```bash
fbox model new <model_name>
```

1. **Model Creation**
   - Creates YAML file in appropriate subdirectory under `./models/`
   - Validates SQL syntax

2. **Dependency Detection**
   - Parses SQL to identify referenced tables
   - Validates circular dependencies

### Delete Models

Removes model and downstream dependencies.

```bash
fbox model delete <model_name>
```

1. **Validation**
   - Checks for dependent models
   - Delete dependencies

2. **Delete Files**

### Generate Pipelines and Run Them

Core execution engine for data processing.

```bash
fbox run
```

0. **Validate Untracked Changes**
   - Checks for uncommitted changes in `./adapters/` and `./models/`
   - Exit if untracked changes are found

1. **Pipeline Generation**
  1.1 **Git Hash Check**:
  1.2 **Pipeline Type Decision**:
     - If Git hash has changed, run migration pipeline
     - If Git hash is unchanged, run sync pipeline
  1.3 **DAG Generation**:
     - Constructs Directed Acyclic Graph (DAG) from adapters and models
     - Identifies dependencies and execution order
  1.4 **Action Creation**:

2. **Migration Pipeline** (Git hash changed)
   - **Analyze Changes**:
     - Compares current configuration with last deployed state
     - Identifies added/modified/deleted adapters and models

   - **Generate Actions**:
     - `destroy` actions for deleted models (reverse topological order)
     - `create` actions for new models (topological order)
     - `update` actions for all models to refresh data

3. **Sync Pipeline** (Git hash unchanged)
   - **Freshness Check**:
     - Evaluates each model's `max_age` setting
     - Identifies stale data based on last execution time

   - **Generate Update Actions**:
     - Creates `update` actions only for stale models
     - Propagates updates through dependency chain

4. **Deployment Execution**
   - **Action Scheduling**:
     - Sorts actions by priority and dependencies
     - Executes actions in parallel where possible
     - Respects deployment timeout

   - **Delta Processing**:

     ```
     Adapter → Extract Changes → Convert to Delta → Apply to Model → Propagate Delta
     ```

   - **Error Handling**:
     - Retries transient failures
     - Rolls back on critical errors
     - Logs all operations

5. **Monitoring**
   - Real-time progress updates
   - Detailed logging to `deployment_logs`
   - Performance metrics collection


TODO

```yaml
connection: test_data
description: 'Configuration for processing web server logs'
file:
  path: <YYYY>/<MM>/<DD>/*_<YYYY><MM><DD>T<HH><MM>.log.gz
  compression: gzip
  max_batch_size: 100MB
update_strategy:
  detection: filename
  timestamp_from: path
  range:
    since: 2023-01-01 00:00:00
format:
  type: 'json'
columns:
  - name: timestamp
    type: DATETIME
    description: 'The timestamp of the log entry'
  - name: path
    type: STRING
    description: 'The path of the request'
  - name: method
    type: STRING
    description: 'The HTTP method of the request'
  - name: status
    type: INTEGER
    description: 'The HTTP status code of the response'
  - name: response_time
    type: INTEGER
    description: 'The time taken to process the request in milliseconds'
  - name: user_agent
    type: STRING
    description: 'The user agent of the client making the request'
```
