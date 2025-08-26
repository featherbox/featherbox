# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FeatherBox is a lightweight data pipeline framework built in Rust that enables Extract, Load, Transform (ELT) operations using DuckDB and DuckLake. It provides a single binary CLI tool for creating and managing data pipelines with automatic dependency resolution and differential execution.

## Key Commands

```bash
# Development
cargo build                    # Build the fbox binary
nix develop --command cargo test  # ALWAYS use this for testing - required for proper dependencies
cargo test                     # DO NOT use this directly - may fail due to missing dependencies

# CLI Usage
target/debug/fbox init <project>          # Initialize new project
target/debug/fbox connection new <name>   # Create connection configuration
target/debug/fbox adapter new <name>      # Create adapter configuration
target/debug/fbox model new <name>        # Create model configuration
target/debug/fbox secret set <key> <value> # Manage encrypted secrets
target/debug/fbox migrate                 # Run database migrations and save graph
target/debug/fbox run                     # Execute pipeline with differential execution
target/debug/fbox query "<sql>"           # Execute SQL query for verification
```

## Architecture Overview

### Domain-Based Architecture

FeatherBox follows domain-driven design principles with clear separation of concerns:

1. **CLI Commands Domain (`src/commands/`)**: Command interface and workspace management
   - `workspace.rs`: Project directory detection and workspace management
   - `init.rs`, `adapter.rs`, `model.rs`, `run.rs`, `migrate.rs`, `connection.rs`, `secret.rs`, `query.rs`: Individual command implementations
   - `templates/`: YAML templates for configuration generation

2. **Configuration Domain (`src/config/`)**: YAML-based configuration management
   - `project.rs`: Project-wide settings structure (storage, database, connections)
   - `adapter.rs`: Data source configuration structures (CSV, JSON, Parquet)
   - `model.rs`: SQL transformation configuration structures

3. **Pipeline Execution Domain (`src/pipeline/`)**: Data processing pipeline
   - `execution.rs`: Pipeline orchestration with topological sorting
   - `ducklake.rs`: DuckDB integration for ELT operations
   - `adapter.rs`, `model.rs`: Type-specific pipeline execution
   - `build.rs`: Build pipeline management
   - `database.rs`: Database operations
   - `file_processor.rs`: File processing utilities
   - `logger.rs`: Pipeline logging

4. **Dependency Resolution Domain (`src/dependency/`)**: Graph analysis and change detection
   - `graph.rs`: Dependency analysis and DAG generation from SQL parsing

5. **Database Layer (`src/database/`)**: Persistence and migrations
   - `connection.rs`: Database connection management with automatic migrations
   - `entities/`: Sea-ORM entity definitions for metadata storage
   - `migration/`: Database schema migrations

6. **Supporting Infrastructure**:
   - `s3_client.rs`: AWS S3 integration for remote storage
   - `secret.rs`: Encrypted secret management using age encryption

### Data Flow

```
Data Sources → Configuration → Dependency Resolution → Pipeline Execution → Results
     ↓              ↓                    ↓                      ↓
  Adapters    →  Graph Analysis  →  Impact Analysis  →  DuckDB Processing
```


### Database Schema

Uses Sea-ORM with SQLite for metadata management. Tables are prefixed with `__fbox_` to avoid conflicts with user data:
- `__fbox_graphs`: Graph version history
- `__fbox_nodes`: Node (table) information  
- `__fbox_edges`: Edge (dependency) relationships
- `__fbox_pipelines`: Pipeline execution records
- `__fbox_pipeline_actions`: Action execution details

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
- `fbox migrate` command for graph migration and schema management
- `connect_app_db()` automatically runs pending database schema migrations
- `fbox run` requires `fbox migrate` to be run first to establish graph
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


## Configuration Structure

### Project Layout
```
project.yml           # Main configuration
adapters/            # Data source definitions
├── source1.yml
└── source2.yml
models/              # Transformation definitions
├── staging/
│   └── clean_data.yml
└── marts/
    └── aggregated.yml
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
- Workflow testing: Validates complete CLI workflow from `fbox init` through `fbox run`
- External behavior verification: Tests focus on command success/failure and SQL query results only

## Code Style Guidelines

- **ABSOLUTELY NO COMMENTS IN CODE**: Code must be self-explanatory through clear function names and structure. Comments are strictly forbidden in all circumstances.
- **Refactor for clarity**: If code is unclear, extract functions with descriptive names instead of adding comments
- **Function extraction**: Break down complex logic into small, well-named functions
- **Comment prohibition enforcement**: Any code containing comments will be rejected. Use descriptive variable names, function names, and clear code structure instead.
- **YAML string indentation**: In tests and template strings, use consistent indentation that matches the surrounding code context. Multi-line YAML strings should be indented to align with the code structure for better readability
- **Domain-driven organization**: Keep related functionality within the same domain directory; cross-domain communication should be explicit and minimal

## Directory Structure Guidelines

### Domain Organization
```
src/
├── main.rs                      # Application entry point
├── commands.rs                  # CLI coordination and shared utilities
├── config.rs                    # Configuration integration
├── commands/                    # CLI Commands Domain
│   ├── workspace.rs            # Project workspace management
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
- Graph migration (`fbox migrate`) must be run before pipeline execution (`fbox run`)