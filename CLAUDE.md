# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FeatherBox is a lightweight data pipeline framework built in Rust that enables Extract, Load, Transform (ELT) operations using DuckDB and DuckLake. It provides a single binary CLI tool for creating and managing data pipelines with automatic dependency resolution and differential execution.

## Key Commands

```bash
# Development
cargo build                    # Build the fbox binary
cargo test                     # Run all tests
nix develop --command cargo test  # Run tests in Nix environment (if dependencies missing)

# CLI Usage
target/debug/fbox init <project>          # Initialize new project
target/debug/fbox adapter new <name>      # Create adapter configuration
target/debug/fbox model new <name>        # Create model configuration
target/debug/fbox migrate up              # Run database migrations
target/debug/fbox migrate status          # Check migration status
target/debug/fbox run                     # Execute pipeline
```

## Architecture Overview

### Core Components

1. **CLI (`src/main.rs`)**: Command routing and argument parsing using clap
2. **Configuration System (`src/config/`)**: YAML-based configuration loading
   - `project.rs`: Project-wide settings (storage, database, connections)
   - `adapter.rs`: Data source definitions (CSV, JSON, Parquet)
   - `model.rs`: SQL-based transformation definitions
3. **Graph Engine (`src/graph.rs`)**: Dependency analysis and DAG generation
4. **Pipeline (`src/pipeline.rs`)**: Execution orchestration with topological sorting
5. **DuckLake (`src/ducklake.rs`)**: DuckDB integration for data processing
6. **Metadata (`src/metadata.rs`)**: Change detection and execution history using Sea-ORM

### Data Flow

```
Data Sources → Adapters → Graph → Pipeline → DuckLake → Results
```

The system automatically builds a dependency graph from SQL analysis, performs topological sorting for execution order, and uses differential execution to avoid unnecessary reprocessing.

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
- Test both success and error cases
- Use `#[tokio::test]` for async test functions

### Configuration Management
- YAML-based configuration with structured validation
- Separate concerns: project settings, adapters, and models
- Configuration loading with proper error reporting

### Migration System
- Sea-ORM migrations in `src/migration/`
- Separate `fbox migrate` command for schema management
- `fbox run` fails if pending migrations exist
- Embedded migrations for single-binary distribution

## Key Implementation Details

### Dependency Detection
SQL parsing using `sqlparser` crate to extract table references and build dependency graphs. Circular dependency detection prevents invalid configurations.

### Differential Execution
Compares current graph structure with previously executed graphs to determine if processing is needed. Only executes when changes are detected.

### Async Architecture
Uses Tokio runtime throughout with proper async/await patterns. Database operations and file I/O are async.

### Single Binary Design
All functionality is embedded in a single binary including:
- Templates for adapter/model creation
- Database migrations
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

### Adapter Configuration
Defines data sources with connection details, file formats, and schema information.

### Model Configuration  
Contains SQL transformations with dependency resolution and caching settings (`max_age`).

## Testing Guidelines

- Use `tempfile::tempdir()` for test isolation
- Create complete project structures in tests
- Test both CLI commands and library functions
- Ensure proper cleanup of test resources
- Run migrations in integration tests before testing pipeline execution

## Code Style Guidelines

- **No comments in code**: Code should be self-explanatory through clear function names and structure
- **Refactor for clarity**: If code is unclear, extract functions with descriptive names instead of adding comments
- **Function extraction**: Break down complex logic into small, well-named functions
- **YAML string indentation**: In tests and template strings, use consistent indentation that matches the surrounding code context. Multi-line YAML strings should be indented to align with the code structure for better readability

## Important Notes

- The binary requires `libstdc++.so.6` - use `nix develop` environment if missing
- Configuration changes trigger full dependency graph recalculation
- All user data operations go through DuckDB for performance
- Metadata operations use SQLite via Sea-ORM for reliability