# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a synthetic data generation repository containing two main projects for creating realistic fake data:
1. **Sales Database** (`/sales`) - International sales operations with PostgreSQL
2. **Chemical Process Monitoring** (`/chemical`) - Industrial IoT sensor data with authentication

## Common Development Commands

### Sales Project
```bash
# Setup and run
cd sales
uv sync                                          # Install dependencies
uv run init_database.py --populate-ref --verbose # Initialize database
uv run generate_data.py --preset medium          # Generate synthetic data
uv run test_connection.py                        # Test database connection

# Database operations
psql -h localhost -U postgres -d sales           # Connect to database
uv run python sample_queries.sql                 # Run sample queries
```

### Chemical Project
```bash
# Setup and run
cd chemical
docker-compose up -d                             # Start PostgreSQL container
python data_ingestion.py                         # Ingest sensor data
cd general/db_setup && ./03_run_setup.sh        # Setup auth database

# Database operations
docker exec -it chemical_db psql -U postgres -d chemical  # Connect to database
docker-compose down                              # Stop containers
docker-compose logs -f                           # View logs
```

## High-Level Architecture

### Sales Database Architecture
- **Technology**: PostgreSQL with Python data generation
- **Pattern**: Traditional relational database with foreign keys
- **Key Components**:
  - `generators/` - Modular data generation classes
  - `schema.sql` - Complete database schema
  - `generate_data.py` - Orchestrates data generation
  - Uses `uv` package manager for dependency management

### Chemical Monitoring Architecture
- **Technology**: PostgreSQL + Docker + Qdrant vector DB
- **Pattern**: Time-series data with multi-tenant authentication
- **Key Components**:
  - `data_ingestion.py` - Parquet to PostgreSQL pipeline
  - `general/db_setup/` - Authentication system
  - `semantic/` - Vector search capabilities
  - Docker Compose for service orchestration

## Key Design Decisions

1. **Data Realism**: Both projects prioritize realistic data patterns (business cycles, sensor fluctuations)
2. **Modularity**: Generators are separate modules for easy extension
3. **Scalability**: Configurable data volumes from small to enterprise scale
4. **Multi-tenancy**: Chemical project includes organization-based isolation
5. **Time-series**: Chemical data uses aggregation levels for performance

## Important Notes

- Both projects use PostgreSQL as the primary database
- Sales project requires `uv` package manager
- Chemical project requires Docker and Docker Compose
- Default credentials are for development only - change in production
- Data generation can be resource-intensive for large datasets