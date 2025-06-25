# Simplified Database Setup Scripts

This directory contains simplified database setup scripts for the industrial process monitoring application with basic authentication and API usage tracking for billing.

## Files

### Database Scripts
- `01_init_database_simple.sql` - Creates simplified database tables (3 tables only)
- `02_seed_data_simple.sql` - Inserts Demo organization and admin user
- `03_run_setup_simple.sh` - Automated setup script

### Legacy Scripts (Complex)
- `01_init_database.sql` - Original complex database schema (8+ tables)
- `02_seed_data.sql` - Original seed data with roles and multiple users
- `03_run_setup.sh` - Original setup script

## Simplified Database Schema

### Tables Created:
1. **organizations** - Company info and user limits for billing
2. **users** - Basic user authentication 
3. **api_usage_logs** - API call tracking for invoicing

### Demo Data:
- **Organization**: "Demo" (max 50 users)
- **Admin User**: admin@demo.ai (password: admin123!)

## Quick Setup

1. **Ensure PostgreSQL is running**
2. **Set environment variables in .env file:**
   ```bash
   auth_host=localhost
   auth_port=5432
   auth_db=organisation
   auth_user=postgres
   auth_password=your_password
   ```

3. **Run the setup script:**
   ```bash
   ./scripts/03_run_setup_simple.sh
   ```

## Manual Setup

If you prefer to run the SQL scripts manually:

```bash
# Connect to PostgreSQL
psql -h localhost -p 5432 -U postgres

# Create database
CREATE DATABASE organisation;
\c organisation;

# Run the scripts
\i scripts/01_init_database_simple.sql
\i scripts/02_seed_data_simple.sql
```

## Usage Tracking for Billing

The `api_usage_logs` table tracks every API call with:
- User and organization info
- Endpoint and method
- Timestamp and duration
- Query parameters (optional)

### Example Billing Queries:

```sql
-- Monthly usage by organization
SELECT COUNT(*) as total_calls, 
       endpoint,
       AVG(CAST(duration_ms AS NUMERIC)) as avg_duration
FROM api_usage_logs 
WHERE organization_id = 'org-uuid'
AND timestamp >= '2025-01-01'
AND timestamp < '2025-02-01'
GROUP BY endpoint;

-- Usage report function
SELECT * FROM get_monthly_usage_report('org-uuid', '2025-01-01');
```

## Login Credentials

After setup, you can login with:
- **Email**: admin@demo.ai
- **Password**: admin123!

**⚠️ Important**: Change the default password in production!

## Environment Variables

The scripts read database connection info from these environment variables:
- `auth_host` - Database host (default: localhost)
- `auth_port` - Database port (default: 5432) 
- `auth_db` - Database name (default: organisation)
- `auth_user` - Database user (default: postgres)
- `auth_password` - Database password (required)

## Billing Integration

The simplified schema is designed for easy billing integration:
- Track API usage per organization
- Generate monthly/usage reports
- Monitor API performance
- Export billing data

Perfect for SaaS industrial monitoring applications!