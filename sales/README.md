# International Sales Database

A comprehensive PostgreSQL database schema for managing international sales operations with multi-currency support, territory management, and complete order-to-cash processes.

## Features

- **Multi-currency Support**: Historical exchange rates and currency conversion
- **Global Operations**: Countries, territories, and region-based management
- **Complete Product Catalog**: Hierarchical categories with pricing and cost tracking
- **Supplier Management**: Purchase orders with landed cost calculations
- **Sales Operations**: Complete order management with tax calculations
- **Inventory Control**: Territory-based inventory tracking
- **HR Management**: Employee hierarchy with territory assignments
- **Financial Tracking**: Comprehensive cost tracking and profitability analysis

## Quick Start

### Prerequisites

- PostgreSQL 12+ installed and running
- Python 3.8+
- [uv](https://github.com/astral-sh/uv) package manager
- Git (optional)

Install uv if you haven't already:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```


### Manual Setup

If you prefer to set up manually:

```bash
# 1. Install dependencies with uv
uv sync

# 2. Configure environment
# Create .env file with your database settings:
cat > .env << EOF
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sales
DB_USER=postgres
PGPASSWORD=your_password
EOF

# 3. Initialize database
uv run init_database.py --populate-ref --verbose

# 4. Test connection
uv run test_connection.py
```

## Database Schema

### Core Tables

**Geographic & Currency**
- `countries` - Country master data with currency
- `territories` - Territories within countries
- `currencies` - Currency definitions
- `exchange_rates` - Historical exchange rates
- `tax_types` & `tax_rates` - Tax management

**Human Resources**
- `roles` - Job roles and descriptions
- `employees` - Employee master with territory assignments

**Product Management**
- `product_categories` - Hierarchical product categories
- `products` - Product master data
- `product_prices` - Multi-currency pricing with history
- `product_costs` - Comprehensive cost tracking
- `inventory` - Territory-based inventory levels

**Supplier Management**
- `suppliers` - Supplier master data
- `product_suppliers` - Product-supplier relationships
- `purchase_orders` - Purchase order headers
- `purchase_order_details` - Purchase order line items
- `purchase_order_line_costs` - Landed cost breakdown

**Customer & Sales**
- `customers` - Customer master data
- `addresses` - Address management
- `customer_addresses` - Customer address relationships
- `shipping_methods` - Shipping options
- `orders` - Sales order headers
- `order_details` - Sales order line items
- `sales_targets` - Sales target management

## Usage Examples

### Basic Connection Test

```python
from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('PGPASSWORD')
)

cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM countries;")
print(f"Countries in database: {cursor.fetchone()[0]}")
```

### Sample Queries

The `sample_queries.sql` file contains extensive examples including:

- **Data Exploration**: Table sizes, row counts, relationships
- **Geographic Analysis**: Countries, territories, currency rates
- **Product Catalog**: Categories, pricing, inventory levels
- **Sales Analysis**: Performance metrics, trends, profitability
- **Supplier Analysis**: Purchase orders, delivery performance
- **Financial Reports**: Cost analysis, margin calculations

### Common Operations

```sql
-- Get current exchange rates
SELECT from_currency, to_currency, rate, effective_date
FROM exchange_rates
WHERE effective_date = (
    SELECT MAX(effective_date)
    FROM exchange_rates er2
    WHERE er2.from_currency = exchange_rates.from_currency
);

-- Product profitability analysis
SELECT p.name,
       pp.price as selling_price,
       pc.amount as cost,
       (pp.price - pc.amount) as profit
FROM products p
JOIN product_prices pp ON p.product_id = pp.product_id
JOIN product_costs pc ON p.product_id = pc.product_id
WHERE pp.end_date IS NULL AND pc.end_date IS NULL;

-- Territory sales performance
SELECT t.name as territory,
       COUNT(o.order_id) as orders,
       SUM(o.grand_total_amount) as total_sales
FROM territories t
JOIN employees e ON t.territory_id = e.territory_id
JOIN orders o ON e.employee_id = o.employee_id
GROUP BY t.territory_id, t.name
ORDER BY total_sales DESC;
```

## File Structure

```
sales/
├── .env                     # Database configuration
├── README.md               # This file
├── pyproject.toml          # Python project configuration for uv
├── requirements.txt        # Python dependencies (legacy)
├── setup.sh               # Automated setup script
├── schema.sql             # Complete database schema
├── init_database.py       # Database initialization script
├── test_connection.py     # Simple connection tester
├── sample_queries.sql     # Example queries and reports
└── .python-version        # Python version (managed by uv)
```

## Scripts Reference

### init_database.py

The main initialization script with options:

```bash
# Full initialization with reference data
uv run init_database.py --populate-ref --verbose

# Schema only (no reference data)
uv run init_database.py --verbose

# Test connection only
uv run init_database.py --test-connection

# Drop and recreate everything
uv run init_database.py --drop-existing --populate-ref --verbose

# Custom schema file
uv run init_database.py --schema-file custom_schema.sql
```

### test_connection.py

Simple connection and health check:

```bash
uv run test_connection.py
```

### setup.sh

Interactive setup with multiple options:

```bash
./setup.sh
# Choose from:
# 1) Full initialization (database + schema + reference data)
# 2) Schema only (database + schema)
# 3) Test connection only
# 4) Drop and recreate everything
```

## Configuration

### Environment Variables

Required in `.env` file:

```bash
DB_HOST=localhost          # Database host
DB_PORT=5432              # Database port
DB_NAME=sales             # Database name
DB_USER=postgres          # Database user
PGPASSWORD=your_password  # Database password
```

### Database Requirements

- PostgreSQL 12+
- Sufficient disk space (schema creates ~30 tables)
- User with CREATE DATABASE privileges
- UUID extension support (automatically enabled)

## Data Generation

After database initialization, generate realistic synthetic data:

### Quick Start with Presets

```bash
# Small dataset (2K orders, 6 months)
uv run generate_data.py --preset small

# Medium dataset (5K orders, 18 months) - Recommended for testing
uv run generate_data.py --preset medium

# Large dataset (10K orders, 3 years) - Production-like volume
uv run generate_data.py --preset large

# Enterprise dataset (25K orders, 5 years) - Full-scale simulation
uv run generate_data.py --preset enterprise
```

### Custom Data Generation

```bash
# Custom volumes
uv run generate_data.py --customers 500 --orders 2000 --products 1000

# Specific date range
uv run generate_data.py --date-range 2023-2024 --preset medium

# Generate only specific data types
uv run generate_data.py --only products,customers,sales

# Clear existing data and regenerate
uv run generate_data.py --preset medium --clear-existing
```

### Generated Data Features

- **Realistic Business Patterns**: Seasonal sales trends, customer segments, employee hierarchies
- **Multi-Currency Operations**: Historical exchange rates with realistic volatility
- **Geographic Distribution**: Customers and suppliers across multiple countries/territories
- **Product Complexity**: Hierarchical categories, multi-supplier sourcing, landed costs
- **Purchase/Sales Cycle**: Complete order-to-cash and procure-to-pay workflows
- **Inventory Management**: Stock levels, reorder points, territory distribution
- **Financial Accuracy**: Tax calculations, currency conversions, profit margins

## Next Steps

After data generation, you can:

1. **Explore with Sample Queries**: `psql -f sample_queries.sql`
2. **Build Applications**: Use the schema for web apps, reports, etc.
3. **Add Business Logic**: Create stored procedures, triggers, views
4. **Performance Tuning**: Add additional indexes based on usage patterns
5. **Data Integration**: Import real data from existing systems

## Troubleshooting

### Common Issues

**Connection Failed**
- Verify PostgreSQL is running: `pg_ctl status`
- Check credentials in `.env` file
- Test with psql: `psql -h localhost -U postgres`

**Permission Denied**
- Ensure user has CREATE DATABASE privileges
- Check PostgreSQL authentication (pg_hba.conf)

**Import Errors**
- Install dependencies: `uv sync`
- Check uv installation: `uv --version`

**Schema Creation Failed**
- Check for existing database conflicts
- Use `--drop-existing` flag to recreate
- Review PostgreSQL logs for detailed errors

### Getting Help

1. Check the logs for detailed error messages
2. Verify all prerequisites are installed
3. Test with the simple connection script first
4. Review the sample queries for usage examples

## Schema Design Principles

- **Normalization**: Proper 3NF design with clear relationships
- **Auditability**: Created/updated timestamps on all master data
- **Flexibility**: JSONB fields for extensible specifications
- **Performance**: Strategic indexes on frequently queried columns
- **Scalability**: Partitioning-ready design for large datasets
- **Internationalization**: Multi-currency and multi-territory support
- **Data Integrity**: Comprehensive foreign key constraints and checks
