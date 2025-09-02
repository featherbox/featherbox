# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FeatherBox is a lightweight data pipeline framework built in Rust that enables Extract, Load, Transform (ELT) operations using DuckDB and DuckLake. It provides a single binary CLI tool for creating and managing data pipelines with automatic dependency resolution and differential execution, along with a web-based UI for visual management and monitoring.

## Key Commands

```bash
# Development
cargo build                    # Build the featherbox binary
nix develop --command cargo test  # ALWAYS use this for testing - required for proper dependencies
cargo test                     # DO NOT use this directly - may fail due to missing dependencies

# Task Commands (Quality Assurance)
nix develop --command task lint    # Run linter to check code quality
nix develop --command task format  # Run formatter to fix code style
nix develop --command task test    # Run tests to verify functionality
nix develop --command task dev     # Start development server

# CLI Usage
target/debug/featherbox new <project>           # Initialize new project
target/debug/featherbox start <project>         # Start web UI and API server for project
target/debug/featherbox server                  # Start API server only (port 3015)
target/debug/featherbox connection new          # Create connection configuration
target/debug/featherbox connection delete       # Delete connection configuration
target/debug/featherbox adapter new             # Create adapter configuration
target/debug/featherbox adapter delete <name>   # Delete adapter configuration
target/debug/featherbox model new               # Create model configuration
target/debug/featherbox model delete            # Delete model configuration
target/debug/featherbox secret new              # Create new secret
target/debug/featherbox secret edit             # Edit existing secret
target/debug/featherbox secret delete           # Delete secret
target/debug/featherbox secret list             # List all secrets
target/debug/featherbox secret gen-key          # Generate new encryption key
target/debug/featherbox query execute "<sql>"   # Execute SQL query for verification
target/debug/featherbox query list              # List saved queries
target/debug/featherbox query save <name> "<sql>" # Save query with name
target/debug/featherbox query run <name>        # Run saved query by name
target/debug/featherbox query delete <name>     # Delete saved query
target/debug/featherbox query update <name>     # Update saved query
target/debug/featherbox migrate                 # Run database migrations and save graph
target/debug/featherbox run                     # Execute pipeline with differential execution
```

## Architecture Overview

### Domain-Based Architecture

FeatherBox follows domain-driven design principles with clear separation of concerns:

1. **CLI Commands Domain (`src/commands/`)**: Command interface and workspace management
   - `workspace.rs`: Project directory detection and workspace management
   - `init.rs`, `adapter.rs`, `model.rs`, `run.rs`, `migrate.rs`, `connection.rs`, `secret.rs`, `query.rs`, `start.rs`: Individual command implementations
   - `templates/`: YAML templates for configuration generation

2. **Configuration Domain (`src/config/`)**: YAML-based configuration management
   - `project.rs`: Project-wide settings structure (storage, database, connections)
   - `adapter.rs`: Data source configuration structures (CSV, JSON, Parquet)
   - `model.rs`: SQL transformation configuration structures
   - `query.rs`: SQL query configuration structures

3. **Pipeline Execution Domain (`src/pipeline/`)**: Data processing pipeline
   - `execution.rs`: Pipeline orchestration with topological sorting
   - `ducklake.rs`: DuckDB integration for ELT operations
   - `adapter.rs`, `model.rs`: Type-specific pipeline execution
   - `build.rs`: Build pipeline management
   - `database.rs`: Database operations
   - `file_processor.rs`: File processing utilities
   - `logger.rs`: Pipeline logging
   - `status.rs`: Pipeline execution status management
   - `state_manager.rs`: Pipeline state coordination

4. **Dependency Resolution Domain (`src/dependency/`)**: Graph analysis and change detection
   - `graph.rs`: Dependency analysis and DAG generation from SQL parsing

5. **Database Layer (`src/database/`)**: Persistence and migrations
   - `connection.rs`: Database connection management with automatic migrations
   - `entities/`: Sea-ORM entity definitions for metadata storage
   - `migration/`: Database schema migrations

6. **Supporting Infrastructure**:
   - `s3_client.rs`: AWS S3 integration for remote storage
   - `secret.rs`: Encrypted secret management using age encryption
   - `api.rs`: RESTful API server for web UI integration

7. **Web UI (`src/ui/`)**: Svelte-based web interface
   - Svelte frontend application for visual management
   - Provides forms for adapter, model, connection, and secret configuration
   - Real-time pipeline execution monitoring with visual graph representation
   - Interactive SQL query panel for data exploration
   - Pipeline execution controls and status monitoring
   - Accessible at http://localhost:5173 when started

### Data Flow

```
Data Sources → Configuration → Dependency Resolution → Pipeline Execution → Results
     ↓              ↓                    ↓                      ↓
  Adapters    →  Graph Analysis  →  Impact Analysis  →  DuckDB Processing
```


### Database Schema

Uses Sea-ORM with SQLite for metadata management. Tables are prefixed with `__featherbox_` to avoid conflicts with user data:
- `__featherbox_graphs`: Graph version history
- `__featherbox_nodes`: Node (table) information  
- `__featherbox_edges`: Edge (dependency) relationships
- `__featherbox_pipelines`: Pipeline execution records
- `__featherbox_pipeline_actions`: Action execution details

## Development Patterns

### Implementation Approach
- **Incremental implementation**: Implement features step-by-step with testing at each stage
- **No premature optimization**: Focus on correctness first, optimize later when needed
- **No backward compatibility**: Since not yet released, break changes are acceptable for better design

### Error Handling
- Uses `anyhow::Result<T>` throughout for unified error handling
- Detailed error messages with context for user-facing operations
- Proper error propagation in async contexts

### Testing Strategy
- Comprehensive unit tests for each module
- Integration tests using `tempfile` for isolation
- E2E tests in `tests/integration_test.rs` with fixtures-based setup
- Test both success and error cases
- Use `#[tokio::test]` for async test functions

### Configuration Management
- YAML-based configuration with structured validation
- Separate concerns: project settings, adapters, and models
- Configuration loading with proper error reporting

### Migration System
- Sea-ORM migrations in `src/database/migration/`
- `featherbox migrate` command for graph migration and schema management
- `connect_app_db()` automatically runs pending database schema migrations
- `featherbox run` requires `featherbox migrate` to be run first to establish graph
- Embedded migrations for single-binary distribution

## Key Implementation Details

### Dependency Detection
Implemented in `src/dependency/graph.rs` using `sqlparser` crate to extract table references and build dependency graphs. Circular dependency detection prevents invalid configurations.

### Differential Execution
Implemented in the Dependency Resolution Domain:
- Graph structure comparison with previously executed graphs stored in database
- Only executes affected parts of the pipeline when changes are detected
- Uses database entities to track execution history and changes


### Async Architecture
Uses Tokio runtime throughout with proper async/await patterns. Database operations and file I/O are async.

### Single Binary Design
All functionality is embedded in a single binary including:
- Templates for adapter/model creation (`src/commands/templates/`)
- Database migrations (`src/database/migration/`)
- All dependencies statically linked

### Secret Management
Implemented using age encryption for secure credential storage:
- Automatic encryption key generation (`secret_key.txt`)
- Encrypted secrets storage (`secrets.enc`)
- Secret expansion in connection configurations using `{{SECRET_NAME}}` syntax
- Command-line interface for secret management (new, edit, delete, list)
- Integration with database connections for secure credential handling

### Query Management
Structured SQL query management system:
- Save frequently used queries with descriptive names
- Query templates with parameter substitution
- Direct SQL execution for ad-hoc analysis
- Integration with web UI for interactive query development
- YAML-based query configuration for version control

### Pipeline Status Management
Real-time pipeline execution monitoring:
- Pipeline execution status tracking (running, completed, failed)
- Visual dependency graph representation in web UI
- Individual task status monitoring
- Failed task isolation and error reporting
- Pipeline restart and recovery capabilities

## Configuration Structure

### Project Layout
```
project.yml           # Main configuration
secret_key.txt        # Encryption key for secrets (auto-generated)
secrets.enc           # Encrypted secrets storage
adapters/            # Data source definitions
├── source1.yml
└── source2.yml
models/              # Transformation definitions
├── staging/
│   └── clean_data.yml
└── marts/
    └── aggregated.yml
queries/             # Saved SQL queries
├── analysis.yml
└── validation.yml
```


## Testing Guidelines

### Unit Tests
- Use `tempfile::tempdir()` for test isolation
- Create complete project structures in tests using direct data structure construction rather than YAML parsing
- Test both CLI commands and library functions
- Ensure proper cleanup of test resources
- Database migrations are handled automatically by `connect_app_db()`
- Verify graph structure in database after migrations using entity queries

### E2E Integration Tests
- Location: `tests/integration_test.rs` for comprehensive end-to-end workflow testing
- Test fixtures: `tests/fixtures/` directory contains all test data and configurations
  - `tests/fixtures/project.yml`: Complete project configuration with connections
  - `tests/fixtures/test_data/`: Test data files (JSON format)
  - `tests/fixtures/adapters/`: Adapter configuration files
  - `tests/fixtures/models/`: Model configuration files
- Workflow testing: Validates complete CLI workflow from `featherbox init` through `featherbox run`
- External behavior verification: Tests focus on command success/failure and SQL query results only

## Code Style Guidelines

- **ABSOLUTELY NO COMMENTS IN CODE**: Code must be self-explanatory through clear function names and structure. Comments are strictly forbidden in all circumstances.
- **Refactor for clarity**: If code is unclear, extract functions with descriptive names instead of adding comments
- **Function extraction**: Break down complex logic into small, well-named functions
- **Comment prohibition enforcement**: Any code containing comments will be rejected. Use descriptive variable names, function names, and clear code structure instead.
- **YAML string indentation**: In tests and template strings, use consistent indentation that matches the surrounding code context. Multi-line YAML strings should be indented to align with the code structure for better readability
- **Domain-driven organization**: Keep related functionality within the same domain directory; cross-domain communication should be explicit and minimal

## Internationalization Guidelines

The web UI supports multiple languages (English and Japanese) using svelte-i18n. All user-facing text must be internationalized:

### Translation Files
- **Location**: `src/ui/src/lib/locales/`
- **Languages**: `en.json` (English), `ja.json` (Japanese)
- **Structure**: Hierarchical JSON with domain-based organization (e.g., `navigation.connections`, `query.execute`)

### Implementation Rules
- **NO hardcoded strings**: All user-visible text must use `$t('translation.key')` function
- **Translation keys**: Use descriptive, hierarchical keys that reflect the UI structure
- **Parameter interpolation**: Use `$t('key', { values: { param: value } })` for dynamic content
- **Consistency**: Maintain parallel structure between English and Japanese translation files
- **aria-label attributes**: All accessibility labels must be internationalized using `$t()` function
- **Placeholders and tooltips**: All form placeholders and button tooltips must use translation keys

### Translation Key Structure
```
{
  "navigation": { ... },      // Navigation menu items
  "connections": { ... },     // Connection management
  "adapters": { ... },        // Adapter management  
  "models": { ... },          // Model management
  "query": { ... },          // Query panel functionality
  "pipeline": { ... },        // Pipeline operations
  "analysis": { ... },        // Analysis sessions
  "chat": { ... },           // Chat functionality
  "settings": { ... },        // Settings panel
  "common": { ... }          // Shared UI elements
}
```

### Validation Requirements
- **Complete coverage**: Every user-visible string must have translations in both languages
- **Parameter consistency**: Interpolation parameters must match between language files
- **Testing**: Verify UI functionality in both languages during development
- **No fallback strings**: Avoid hardcoded fallback text; use proper translation keys

## Directory Structure Guidelines

### Domain Organization
```
src/
├── main.rs                      # Application entry point
├── commands.rs                  # CLI coordination and shared utilities
├── config.rs                    # Configuration integration
├── api.rs                       # RESTful API server
├── commands/                    # CLI Commands Domain
│   ├── workspace.rs            # Project workspace management
│   ├── start.rs               # Web UI and server startup
│   ├── [command].rs            # Individual command implementations
│   └── templates/              # Configuration templates
├── config/                      # Configuration Domain
│   └── [type].rs              # Configuration structure definitions
├── pipeline/                    # Pipeline Execution Domain
│   ├── execution.rs           # Pipeline orchestration
│   ├── ducklake.rs           # Data processing engine
│   ├── adapter.rs            # Adapter-specific execution
│   ├── model.rs              # Model-specific execution
│   ├── build.rs              # Build pipeline management
│   ├── database.rs           # Database operations
│   ├── file_processor.rs     # File processing utilities
│   └── logger.rs             # Pipeline logging
├── dependency/                  # Dependency Resolution Domain
│   └── graph.rs              # Dependency graph construction
├── database/                    # Database Layer
│   ├── connection.rs         # Database connection management
│   ├── entities/             # ORM entity definitions  
│   └── migration/            # Schema migrations
├── ui/                          # Web UI (Svelte application)
│   ├── src/                   # Frontend source code
│   ├── public/                # Static assets
│   └── package.json           # Frontend dependencies
├── s3_client.rs              # AWS S3 integration
└── secret.rs                 # Encrypted secret management
```

### Module Naming Conventions
- Domain directories use plural nouns (`commands/`, `config/`)
- Domain coordination files use singular nouns (`commands.rs`, `config.rs`)
- **IMPORTANT**: Uses Rust 2024 edition - NO `mod.rs` files, use coordination files instead
- Avoid module inception (file names matching directory names)
- Use descriptive names that indicate responsibility (`workspace.rs` vs `project.rs`)

## Important Notes

- The binary requires `libstdc++.so.6` - use `nix develop` environment if missing
- **CRITICAL**: ALWAYS use `nix develop --command cargo test` for testing - direct `cargo test` may fail due to missing dependencies
- **CRITICAL**: Before any code changes, run `nix develop --command cargo test` to establish baseline - ensure existing tests are not broken
- Configuration changes trigger full dependency graph recalculation
- All user data operations go through DuckDB for performance
- Metadata operations use SQLite via Sea-ORM for reliability
- Graph migration (`featherbox migrate`) must be run before pipeline execution (`featherbox run`)
- Web UI runs on port 5173, API server runs on port 3015
- Use `featherbox start <project>` to launch both UI and API server together

## Quality Assurance Workflow

After completing any task, always run these commands to ensure code quality:
```bash
nix develop --command task lint
nix develop --command task format
```

Then run tests to verify functionality:
```bash
nix develop --command task test
```

Finally, start the development server to verify operation:
```bash
nix develop --command task dev
```

## API Endpoints

The RESTful API provides the following endpoints:

### Adapters
- `GET /api/adapters` - List all adapters
- `GET /api/adapters/{name}` - Get adapter details
- `POST /api/adapters` - Create new adapter
- `PUT /api/adapters/{name}` - Update adapter
- `DELETE /api/adapters/{name}` - Delete adapter

### Models
- `GET /api/models` - List all models
- `GET /api/models/{path}` - Get model details
- `POST /api/models` - Create new model
- `PUT /api/models/{path}` - Update model
- `DELETE /api/models/{path}` - Delete model

### Connections
- `GET /api/connections` - List all connections
- `GET /api/connections/{name}` - Get connection details
- `POST /api/connections` - Create new connection
- `PUT /api/connections/{name}` - Update connection
- `DELETE /api/connections/{name}` - Delete connection

### Secrets
- `GET /api/secrets` - List all secrets (masked)
- `POST /api/secrets` - Create new secret
- `PUT /api/secrets/{name}` - Update secret
- `DELETE /api/secrets/{name}` - Delete secret
- `POST /api/secrets/generate-key` - Generate encryption key

### Queries
- `GET /api/queries` - List saved queries
- `GET /api/queries/{name}` - Get query details
- `POST /api/queries` - Save new query
- `PUT /api/queries/{name}` - Update query
- `DELETE /api/queries/{name}` - Delete query
- `POST /api/queries/{name}/execute` - Execute saved query

### Pipeline
- `GET /api/pipeline/status` - Get pipeline execution status
- `POST /api/pipeline/run` - Start pipeline execution
- `POST /api/pipeline/stop` - Stop running pipeline
- `GET /api/pipeline/graph` - Get dependency graph visualization data

### Chat/Analysis
- `POST /api/chat/message` - Send analysis message
- `GET /api/chat/config` - Get chat configuration