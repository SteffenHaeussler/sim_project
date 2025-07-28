# Chemical Process Monitoring System

A comprehensive industrial IoT data simulation system for chemical process monitoring with time-series sensor data, authentication, and API usage tracking.

## Overview

This system simulates a real-world industrial monitoring platform with:
- Time-series sensor data from chemical processing equipment
- Multi-tenant authentication and organization management
- API usage tracking for billing purposes
- Semantic search capabilities for asset discovery
- PostgreSQL-based data storage with Docker support

## Architecture

```
chemical/
├── data_ingestion.py      # Sensor data ingestion pipeline
├── ingestion.py          # Alternative ingestion module
├── assets.json           # Asset definitions and metadata
├── id2assets.json        # Asset ID mappings
├── graph.json            # Asset relationship graph
├── compose.yaml          # Docker Compose configuration
├── pgdata/               # PostgreSQL data directory
├── qdrant_storage/       # Vector database storage
├── semantic/             # Semantic search components
│   ├── bi_encoder.py     # Bi-encoder for embeddings
│   ├── cross_encoder.py  # Cross-encoder for ranking
│   └── semantic.py       # Main semantic search module
└── general/db_setup/     # Authentication database setup
    ├── setup_db.py
    ├── setup_evaluation_db.py
    └── README.md         # Auth system documentation
```

## Features

### 1. Time-Series Data Management
- **Multi-resolution aggregation**: Minute, hourly, and daily averages
- **Efficient storage**: Parquet file ingestion with automatic resampling
- **Asset tracking**: UUID-based asset identification
- **Scalable design**: Handles millions of sensor readings

### 2. Authentication System
- **Multi-tenant support**: Organization-based user management
- **User limits**: Configurable user counts per organization
- **API tracking**: Complete logging of all API calls
- **Billing ready**: Usage reports for invoicing

### 3. Semantic Search
- **Asset discovery**: Find equipment by description
- **Vector embeddings**: Qdrant-based similarity search
- **Bi-encoder/Cross-encoder**: Advanced ranking system
- **Real-time indexing**: Automatic asset indexing

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- PostgreSQL client tools (optional)

### 1. Start the Database
```bash
docker-compose up -d
```

This starts:
- PostgreSQL on port 5432
- Database name: `chemical`
- Username: `postgres`
- Password: `example`

### 2. Set Up Authentication Database
```bash
cd general/db_setup
./03_run_setup.sh
```

This creates:
- Organization management tables
- User authentication system
- API usage tracking tables
- Demo organization and admin user

### 3. Ingest Sensor Data
```bash
python data_ingestion.py
```

This will:
- Read Parquet files from `raw/data/`
- Create time-series tables with different aggregations
- Index data by asset ID and timestamp

## Database Schema

### Main Database (`chemical`)

#### Assets Table
```sql
CREATE TABLE assets (
    id UUID PRIMARY KEY,
    name VARCHAR(255),
    type VARCHAR(100),
    location VARCHAR(255),
    specifications JSONB
);
```

#### Time-Series Tables
```sql
-- Created for each aggregation level (min, h, d)
CREATE TABLE data_{aggregation} (
    pk_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID REFERENCES assets(id),
    timestamp TIMESTAMP,
    value FLOAT,
    INDEX asset_timestamp_idx (asset_id, timestamp)
);
```

### Authentication Database (`organisation`)

#### Core Tables
- `organizations`: Company profiles with user limits
- `users`: User authentication and profiles
- `api_usage_logs`: API call tracking for billing

## Data Generation

### Sensor Data Format
The system expects Parquet files with:
- `id`: Asset UUID
- `timestamp`: Reading timestamp
- `value`: Sensor reading value

### Sample Data Generation
```python
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate sample sensor data
dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='min')
data = {
    'id': 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    'timestamp': dates,
    'value': np.random.normal(75.0, 5.0, len(dates))  # Temperature readings
}
df = pd.DataFrame(data)
df.to_parquet('raw/data/sensor_001.parquet')
```

## API Usage Examples

### Query Time-Series Data
```sql
-- Get hourly averages for a specific asset
SELECT asset_id, timestamp, value
FROM data_h
WHERE asset_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
  AND timestamp >= '2024-01-01'
  AND timestamp < '2024-02-01'
ORDER BY timestamp;
```

### Generate Usage Reports
```sql
-- Monthly API usage by organization
SELECT * FROM get_monthly_usage_report('org-uuid', '2024-01-01');
```

### Asset Search
```python
# Using the semantic search module
from semantic.semantic import search_assets

results = search_assets("high-temperature reactor vessel")
```

## Configuration

### Environment Variables
Create a `.env` file:
```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=chemical
DB_USER=postgres
DB_PASSWORD=example

# Authentication Database
auth_host=localhost
auth_port=5432
auth_db=organisation
auth_user=postgres
auth_password=example
```

### Docker Compose Settings
Modify `compose.yaml` for production:
- Change default passwords
- Add volume persistence
- Configure resource limits
- Enable SSL/TLS

## Monitoring and Maintenance

### Database Health
```bash
# Check database size
docker exec -it chemical_db psql -U postgres -d chemical -c "\l+"

# View table sizes
docker exec -it chemical_db psql -U postgres -d chemical -c "\dt+"

# Monitor active connections
docker exec -it chemical_db psql -U postgres -c "SELECT * FROM pg_stat_activity;"
```

### Data Retention
```sql
-- Delete old minute-level data (keep 30 days)
DELETE FROM data_min 
WHERE timestamp < CURRENT_DATE - INTERVAL '30 days';

-- Archive old hourly data
INSERT INTO data_h_archive 
SELECT * FROM data_h 
WHERE timestamp < CURRENT_DATE - INTERVAL '1 year';
```

## Use Cases

1. **Industrial IoT Development**: Test monitoring dashboards and alerting systems
2. **Time-Series Analytics**: Develop and test analytical algorithms
3. **Multi-Tenant SaaS**: Build billing and usage tracking systems
4. **Machine Learning**: Train predictive maintenance models
5. **Performance Testing**: Benchmark query performance with realistic data

## Troubleshooting

### Common Issues

**Database Connection Failed**
- Ensure Docker containers are running: `docker-compose ps`
- Check port availability: `lsof -i :5432`
- Verify credentials in connection string

**Data Ingestion Errors**
- Check Parquet file format matches expected schema
- Ensure `raw/data/` directory exists and contains files
- Verify database tables were created successfully

**Authentication Issues**
- Run setup script: `./general/db_setup/03_run_setup.sh`
- Check organization and user creation in logs
- Verify environment variables are set

## Security Considerations

- Change default passwords before production use
- Implement proper network isolation
- Enable SSL for database connections
- Regularly backup authentication database
- Monitor API usage for anomalies
- Implement rate limiting for API endpoints

## Next Steps

1. **Extend the Schema**: Add equipment maintenance records, alarm thresholds
2. **Build APIs**: Create RESTful endpoints for data access
3. **Add Visualization**: Integrate with Grafana or custom dashboards
4. **Implement Alerts**: Add real-time monitoring and notifications
5. **Scale Horizontally**: Implement TimescaleDB for better time-series performance