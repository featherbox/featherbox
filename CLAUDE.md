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
target/debug/fbox migrate                 # Run database migrations and save graph
target/debug/fbox run                     # Execute pipeline with incremental imports
target/debug/fbox query "<sql>"           # Execute SQL query for verification
```

## Architecture Overview

### Domain-Based Architecture

FeatherBox follows domain-driven design principles with clear separation of concerns:

1. **CLI Commands Domain (`src/commands/`)**: Command interface and workspace management
   - `workspace.rs`: Project directory detection and workspace management
   - `init.rs`, `adapter.rs`, `model.rs`, `run.rs`, `migrate.rs`: Individual command implementations
   - `templates/`: YAML templates for configuration generation

2. **Configuration Domain (`src/config/`)**: YAML-based configuration management
   - `project.rs`: Project-wide settings structure (storage, database, connections)
   - `adapter.rs`: Data source configuration structures (CSV, JSON, Parquet)
   - `model.rs`: SQL transformation configuration structures

3. **Pipeline Execution Domain (`src/pipeline/`)**: Data processing pipeline
   - `execution.rs`: Pipeline orchestration with topological sorting and incremental imports
   - `ducklake.rs`: DuckDB integration for ELT operations with time-based file filtering

4. **Dependency Resolution Domain (`src/dependency/`)**: Graph analysis and change detection
   - `graph.rs`: Dependency analysis and DAG generation from SQL parsing
   - `impact_analysis.rs`: Change impact analysis for differential execution  
   - `metadata.rs`: Change detection, execution history management, and incremental import tracking

5. **Database Layer (`src/database/`)**: Persistence and migrations
   - `connection.rs`: Database connection management with automatic migrations
   - `entities/`: Sea-ORM entity definitions for metadata storage
   - `migration/`: Database schema migrations

### Data Flow

```
Data Sources → Configuration → Dependency Resolution → Pipeline Execution → Results
     ↓              ↓                    ↓                      ↓
  Adapters    →  Graph Analysis  →  Impact Analysis  →  DuckDB Processing
```

The system follows a domain-driven approach:
1. **Configuration Domain** loads and validates YAML settings
2. **Dependency Resolution Domain** builds dependency graphs from SQL analysis and detects changes
3. **Pipeline Execution Domain** performs topological sorting and executes ELT operations
4. **Database Layer** handles metadata persistence and change tracking for differential execution

### Database Schema

Uses Sea-ORM with SQLite for metadata management. Tables are prefixed with `__fbox_` to avoid conflicts with user data:
- `__fbox_graphs`: Graph version history
- `__fbox_nodes`: Node (table) information  
- `__fbox_edges`: Edge (dependency) relationships
- `__fbox_pipelines`: Pipeline execution records
- `__fbox_pipeline_actions`: Action execution details with time range tracking (`since`, `until`)

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
Implemented across the Dependency Resolution Domain:
- `src/dependency/metadata.rs`: Compares current graph structure with previously executed graphs
- `src/dependency/impact_analysis.rs`: Calculates downstream impact of changes  
- Only executes affected parts of the pipeline when changes are detected

### Incremental Data Import
High-water mark pattern implementation for efficient data processing:
- `Action` struct with `since`/`until` fields tracks execution time ranges
- `ExecutedRange` struct provides clean abstraction for time range handling
- `get_executed_ranges_for_graph()` retrieves previous execution history
- `calculate_remaining_range()` determines new data to process
- File filtering based on filename patterns and time ranges
- Only processes new files since last successful execution
- Supports period extension by updating adapter range configuration

### Async Architecture
Uses Tokio runtime throughout with proper async/await patterns. Database operations and file I/O are async.

### Single Binary Design
All functionality is embedded in a single binary including:
- Templates for adapter/model creation (`src/commands/templates/`)
- Database migrations (`src/database/migration/`)
- All dependencies statically linked

### Domain Interaction Patterns
- **Commands Domain** coordinates with all other domains for CLI operations
- **Configuration Domain** provides validated settings to Pipeline and Dependency domains
- **Dependency Resolution Domain** informs Pipeline Domain about what needs execution
- **Database Layer** serves all domains for persistence needs
- **Pipeline Execution Domain** focuses solely on data processing execution

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
Defines data sources with connection details, file formats, and schema information. Supports incremental import via `update_strategy` with time range configuration:

```yaml
update_strategy:
  detection: filename  # Extract timestamp from filename patterns
  range:
    since: "2024-01-01"
    until: "2024-01-31"
```

### Model Configuration  
Contains SQL transformations with dependency resolution and caching settings (`max_age`).

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

- **No comments in code**: Code should be self-explanatory through clear function names and structure
- **Refactor for clarity**: If code is unclear, extract functions with descriptive names instead of adding comments
- **Function extraction**: Break down complex logic into small, well-named functions
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
│   └── ducklake.rs           # Data processing engine
├── dependency/                  # Dependency Resolution Domain
│   ├── graph.rs              # Dependency graph construction
│   ├── impact_analysis.rs    # Change impact analysis
│   └── metadata.rs           # Change detection & execution history
└── database/                    # Database Layer
    ├── connection.rs         # Database connection management
    ├── entities/             # ORM entity definitions  
    └── migration/            # Schema migrations
```

### Module Naming Conventions
- Domain directories use plural nouns (`commands/`, `config/`)
- Domain coordination files use singular nouns (`commands.rs`, `config.rs`)
- **IMPORTANT**: Uses Rust 2024 edition - NO `mod.rs` files, use coordination files instead
- Avoid module inception (file names matching directory names)
- Use descriptive names that indicate responsibility (`workspace.rs` vs `project.rs`)

## Important Notes

- The binary requires `libstdc++.so.6` - use `nix develop` environment if missing
- Configuration changes trigger full dependency graph recalculation
- All user data operations go through DuckDB for performance
- Metadata operations use SQLite via Sea-ORM for reliability
- Incremental imports require adapters to have `update_strategy.range` configuration
- Graph migration (`fbox migrate`) must be run before pipeline execution (`fbox run`)
- Time range calculations support both date (`2024-01-01`) and datetime (`2024-01-01 12:00:00`) formats