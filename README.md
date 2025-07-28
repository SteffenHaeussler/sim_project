# Simulation Data Project

A comprehensive repository for generating realistic synthetic data for multiple domains including international sales operations and industrial chemical process monitoring.

## Overview

This repository contains two main data generation projects:

1. **Sales Database** - International sales operations with multi-currency support
2. **Chemical Process Monitoring** - Industrial IoT sensor data with authentication

Both projects are designed to create realistic fake data for development, testing, and demonstration purposes.

## Projects

### 📊 Sales Database (`/sales`)

A PostgreSQL-based international sales database with comprehensive business operations modeling.

**Features:**
- Multi-currency support with historical exchange rates
- Global operations across countries and territories
- Complete product catalog with hierarchical categories
- Supplier management and purchase orders
- Sales operations with tax calculations
- Inventory control by territory
- Employee hierarchy management
- Financial tracking and profitability analysis

**Quick Start:**
```bash
cd sales
./setup.sh  # Interactive setup
# Or manually:
uv sync
uv run init_database.py --populate-ref --verbose
uv run generate_data.py --preset medium
```

### 🏭 Chemical Process Monitoring (`/chemical`)

Industrial process monitoring system with time-series sensor data and authentication.

**Features:**
- Time-series sensor data ingestion from Parquet files
- Multiple aggregation levels (minute, hourly, daily)
- Asset management and tracking
- Organization-based authentication system
- API usage tracking for billing
- PostgreSQL with Docker support
- Semantic search capabilities

**Components:**
- PostgreSQL database for time-series data
- Authentication system with organization management
- API usage logging for billing integration
- Qdrant vector database for semantic search
- Data ingestion pipeline for sensor readings

**Quick Start:**
```bash
cd chemical
docker-compose up -d  # Start PostgreSQL
python data_ingestion.py  # Ingest sensor data
cd general/db_setup
./03_run_setup.sh  # Setup authentication DB
```

## Repository Structure

```
sim_project/
├── sales/                  # International sales database
│   ├── generators/         # Data generation modules
│   ├── schema.sql         # Database schema
│   ├── generate_data.py   # Synthetic data generator
│   └── README.md          # Detailed sales documentation
│
└── chemical/              # Chemical process monitoring
    ├── data_ingestion.py  # Sensor data ingestion
    ├── semantic/          # Semantic search components
    ├── general/db_setup/  # Authentication database
    └── compose.yaml       # Docker configuration
```

## Prerequisites

- **PostgreSQL 12+** for both projects
- **Python 3.8+**
- **Docker & Docker Compose** (for chemical project)
- **uv** package manager (for sales project)

## Use Cases

### Sales Database
- E-commerce platform development
- Business intelligence and reporting
- Sales performance analytics
- Multi-currency transaction testing
- Supply chain management systems
- Financial reporting applications

### Chemical Process Monitoring
- Industrial IoT applications
- Time-series data processing
- Real-time monitoring dashboards
- Predictive maintenance systems
- API billing and usage tracking
- Multi-tenant SaaS development

## Data Characteristics

Both projects generate:
- **Realistic patterns**: Seasonal trends, business cycles, sensor fluctuations
- **Comprehensive relationships**: Proper foreign keys and data integrity
- **Historical data**: Multiple years of time-series information
- **Scalable volumes**: Configurable data sizes from small to enterprise

## Getting Started

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd sim_project
   ```

2. Choose a project:
   - For sales data: `cd sales && ./setup.sh`
   - For chemical monitoring: `cd chemical && docker-compose up -d`

3. Follow the project-specific README for detailed instructions

## License

This project is designed for creating synthetic data for development and testing purposes only.