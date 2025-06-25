# Database Setup Scripts

This directory contains scripts to initialize the PostgreSQL database for the industrial process monitoring application with authentication and user management.

## Files

- `01_init_database.sql` - Creates all database tables, indexes, and functions
- `02_seed_data.sql` - Inserts default roles, demo organization, and test users
- `03_run_setup.sh` - Automated setup script that runs everything
- `README.md` - This documentation

## Quick Start

1. **Make sure PostgreSQL is running** (via docker compose):
   ```bash
   docker compose up -d db
   ```

2. **Run the setup script**:
   ```bash
   ./scripts/03_run_setup.sh
   ```

3. **Or run with options**:
   ```bash
   # Create backup before setup
   ./scripts/03_run_setup.sh --backup

   # Skip confirmation prompts
   ./scripts/03_run_setup.sh --force
   ```

## Environment Variables

The setup script uses these environment variables (all optional):

```bash
export DB_HOST=localhost      # Database host
export DB_PORT=5432          # Database port
export DB_NAME=organisation # Database name
export DB_USER=postgres      # Database user
export PGPASSWORD= # PostgreSQL password
```

## Manual Setup

If you prefer to run the SQL scripts manually:

```bash
# Connect to PostgreSQL
psql -h localhost -p 5432 -U postgres

# Create database
CREATE DATABASE organisation;

# Connect to the new database
\c organisation;

# Run the init script
\i scripts/01_init_database.sql

# Run the seed data script
\i scripts/02_seed_data.sql
```

## Database Schema

### Core Tables

- **organizations** - Multi-tenant organizations for commercial use
- **roles** - User roles with permissions for industrial process monitoring
- **users** - Application users with authentication and profile information
- **user_sessions** - JWT session tracking and management
- **audit_logs** - Security and compliance audit trail
- **password_reset_tokens** - Secure password reset token management
- **api_keys** - API keys for programmatic access

### Demo Users Created

| Email | Password | Role | Description |
|-------|----------|------|-------------|
| admin@demo.local | admin123! | System Administrator | Full system access |
| manager@demo.local | manager123! | Manager | User management + full monitoring |
| engineer@demo.local | engineer123! | Process Engineer | Full monitoring + analysis |
| operator@demo.local | operator123! | Plant Operator | Basic monitoring access |

**⚠️ Important**: Change these default passwords in production!

## Role Permissions

### Administrator
- Full system access
- User and organization management
- System configuration
- Audit log access

### Manager
- User management within organization
- Full monitoring and asset access
- Report generation and export
- API read/write access

### Process Engineer
- Full monitoring and analysis capabilities
- Asset read/write access
- Report generation and export
- API read/write access

### Plant Operator
- Read-only monitoring access
- Alert acknowledgment
- Basic reporting
- API read access

### Viewer
- Read-only access to monitoring data
- Basic reporting

## Maintenance Functions

The database includes automated maintenance functions:

```sql
-- Clean up expired sessions (run periodically)
SELECT cleanup_expired_sessions();

-- Clean up expired password reset tokens
SELECT cleanup_expired_password_tokens();
```

## Security Features

- UUID primary keys for all tables
- Bcrypt password hashing
- JWT token tracking with expiration
- Session management with refresh tokens
- Audit logging for compliance
- Role-based permissions (JSONB)
- Account lockout after failed attempts
- Secure password reset workflow

## Next Steps

After setting up the database:

1. Install Python dependencies for SQLAlchemy and authentication
2. Create database models in your FastAPI application
3. Implement JWT authentication middleware
4. Add login/logout endpoints
5. Protect existing routes with authentication decorators

The database is now ready for integration with your FastAPI application!
