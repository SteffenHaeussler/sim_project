# Standalone Database Scripts

This folder contains completely self-contained database management scripts that can be copied and run in any project folder without dependencies on the main application.

## Scripts

### `init_database.py`
**Purpose**: Initialize a complete authentication database schema including users, organisations, password resets, and API usage logging.

**Usage**:
```bash
python init_database.py
```

**Creates**:
- `organisation` table with billing info
- `users` table with authentication data  
- `password_resets` table for forgot password functionality
- `api_usage_logs` table for tracking API calls

### `create_password_reset_table.py`
**Purpose**: Create specifically the `password_resets` table with proper indexes and foreign key constraints.

**Usage**:
```bash
python create_password_reset_table.py
```

**Features**:
- Checks if `users` table exists before adding foreign key
- Creates optimized indexes for performance
- Safe to run multiple times (checks if table exists)

## Environment Variables

Both scripts support multiple ways to configure database connection:

### Option 1: Single DATABASE_URL
```bash
export DATABASE_URL="postgresql+asyncpg://user:password@localhost:5432/dbname"
```

### Option 2: Individual variables
```bash
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_USER="your_user"
export DB_PASSWORD="your_password"
export DB_NAME="your_database"
```

### Alternative variable names
The scripts also check for these alternative names:
- `DB_USERNAME` (alternative to `DB_USER`)
- `DB_PASS` (alternative to `DB_PASSWORD`)
- `DATABASE_NAME` (alternative to `DB_NAME`)

## Prerequisites

1. **PostgreSQL Database**: Scripts are designed for PostgreSQL with UUID support
2. **Python Dependencies**: 
   - `sqlalchemy[asyncio]`
   - `asyncpg`
3. **Database Permissions**: User must have CREATE TABLE and CREATE INDEX permissions

## Example Usage

### Complete new database setup:
```bash
export DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/mydb"
python init_database.py
```

### Adding password reset to existing database:
```bash
export DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/mydb"
python create_password_reset_table.py
```

## Safety Features

Both scripts include multiple safety features:
- ✅ Check if tables already exist before creating
- ✅ Hide database passwords in console output
- ✅ Comprehensive error handling and clear error messages
- ✅ Graceful handling of missing dependencies
- ✅ Transaction safety (rollback on errors)

## Portability

These scripts are **completely standalone** and can be:
- ✅ Copied to any folder and run independently
- ✅ Used in different projects without modification
- ✅ Run without any knowledge of the main application structure
- ✅ Executed from any directory

Just copy the `.py` files and run them with the appropriate environment variables!

## Dependencies

Only requires these Python packages:
```bash
pip install sqlalchemy[asyncio] asyncpg
```

No project-specific dependencies or imports needed.