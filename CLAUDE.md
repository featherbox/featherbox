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
   - `execution.rs`: Pipeline orchestration with topological sorting
   - `ducklake.rs`: DuckDB integration for ELT operations

4. **Dependency Resolution Domain (`src/dependency/`)**: Graph analysis and change detection
   - `graph.rs`: Dependency analysis and DAG generation from SQL parsing
   - `impact_analysis.rs`: Change impact analysis for differential execution  
   - `metadata.rs`: Change detection and execution history management

5. **Database Layer (`src/database/`)**: Persistence and migrations
   - `connection.rs`: Database connection management
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
- Sea-ORM migrations in `src/database/migration/`
- Separate `fbox migrate` command for schema management
- `fbox run` fails if pending migrations exist
- Embedded migrations for single-binary distribution

## Key Implementation Details

### Dependency Detection
Implemented in `src/dependency/graph.rs` using `sqlparser` crate to extract table references and build dependency graphs. Circular dependency detection prevents invalid configurations.

### Differential Execution
Implemented across the Dependency Resolution Domain:
- `src/dependency/metadata.rs`: Compares current graph structure with previously executed graphs
- `src/dependency/impact_analysis.rs`: Calculates downstream impact of changes  
- Only executes affected parts of the pipeline when changes are detected

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
- Avoid module inception (file names matching directory names)
- Use descriptive names that indicate responsibility (`workspace.rs` vs `project.rs`)

## Important Notes

- The binary requires `libstdc++.so.6` - use `nix develop` environment if missing
- Configuration changes trigger full dependency graph recalculation
- All user data operations go through DuckDB for performance
- Metadata operations use SQLite via Sea-ORM for reliability