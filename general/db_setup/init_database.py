#!/usr/bin/env python3
"""
Standalone database initialization script

This script creates a complete database schema for a typical authentication system.
It's completely self-contained and can be copied to any project folder.

Usage:
    python init_database.py

Environment Variables Required:
    DATABASE_URL - PostgreSQL connection string (e.g., postgresql+asyncpg://user:pass@host:port/db)
"""

import asyncio
import os
import sys
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text


def get_database_url():
    """Get database URL from environment variables"""
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        # Try alternative environment variable names
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_user = os.getenv("DB_USER", os.getenv("DB_USERNAME"))
        db_password = os.getenv("DB_PASSWORD", os.getenv("DB_PASS"))
        db_name = os.getenv("DB_NAME", os.getenv("DATABASE_NAME"))
        
        if db_user and db_password and db_name:
            database_url = f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        else:
            print("❌ Error: Database configuration not found!")
            print("Set DATABASE_URL environment variable or DB_HOST, DB_USER, DB_PASSWORD, DB_NAME")
            print("Example: export DATABASE_URL='postgresql+asyncpg://user:pass@localhost:5432/dbname'")
            sys.exit(1)
    
    return database_url


async def table_exists(conn, table_name):
    """Check if a table exists"""
    query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = %s
    );
    """
    result = await conn.execute(text(query), (table_name,))
    return result.scalar()


async def create_organisation_table(conn):
    """Create organisation table"""
    if await table_exists(conn, 'organisation'):
        print("✅ Table 'organisation' already exists")
        return
    
    print("Creating organisation table...")
    query = """
    CREATE TABLE organisation (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) NOT NULL UNIQUE,
        display_name VARCHAR(255) NOT NULL,
        max_users INTEGER NOT NULL DEFAULT 50,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        billing_email VARCHAR(255),
        billing_company VARCHAR(255)
    );
    """
    await conn.execute(text(query))
    
    # Create indexes
    indexes = [
        "CREATE INDEX idx_organisation_name ON organisation(name);",
        "CREATE INDEX idx_organisation_is_active ON organisation(is_active);"
    ]
    for index_query in indexes:
        await conn.execute(text(index_query))


async def create_users_table(conn):
    """Create users table"""
    if await table_exists(conn, 'users'):
        print("✅ Table 'users' already exists")
        return
    
    print("Creating users table...")
    query = """
    CREATE TABLE users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        email VARCHAR(255) NOT NULL UNIQUE,
        password_hash VARCHAR(255) NOT NULL,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        organisation_id UUID NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        FOREIGN KEY (organisation_id) REFERENCES organisation(id)
    );
    """
    await conn.execute(text(query))
    
    # Create indexes
    indexes = [
        "CREATE INDEX idx_users_email ON users(email);",
        "CREATE INDEX idx_users_organisation_id ON users(organisation_id);",
        "CREATE INDEX idx_users_is_active ON users(is_active);"
    ]
    for index_query in indexes:
        await conn.execute(text(index_query))


async def create_password_resets_table(conn):
    """Create password_resets table"""
    if await table_exists(conn, 'password_resets'):
        print("✅ Table 'password_resets' already exists")
        return
    
    print("Creating password_resets table...")
    query = """
    CREATE TABLE password_resets (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL,
        token VARCHAR(255) NOT NULL UNIQUE,
        is_used BOOLEAN NOT NULL DEFAULT FALSE,
        expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        used_at TIMESTAMP WITH TIME ZONE NULL,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """
    await conn.execute(text(query))
    
    # Create indexes
    indexes = [
        "CREATE INDEX idx_password_resets_user_id ON password_resets(user_id);",
        "CREATE INDEX idx_password_resets_token ON password_resets(token);",
        "CREATE INDEX idx_password_resets_expires_at ON password_resets(expires_at);"
    ]
    for index_query in indexes:
        await conn.execute(text(index_query))


async def create_api_usage_logs_table(conn):
    """Create api_usage_logs table"""
    if await table_exists(conn, 'api_usage_logs'):
        print("✅ Table 'api_usage_logs' already exists")
        return
    
    print("Creating api_usage_logs table...")
    query = """
    CREATE TABLE api_usage_logs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL,
        organisation_id UUID NOT NULL,
        endpoint VARCHAR(255) NOT NULL,
        method VARCHAR(10) NOT NULL,
        status_code VARCHAR(10),
        timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        duration_ms VARCHAR(50),
        query_params VARCHAR(1000),
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (organisation_id) REFERENCES organisation(id)
    );
    """
    await conn.execute(text(query))
    
    # Create indexes
    indexes = [
        "CREATE INDEX idx_api_usage_logs_user_id ON api_usage_logs(user_id);",
        "CREATE INDEX idx_api_usage_logs_organisation_id ON api_usage_logs(organisation_id);",
        "CREATE INDEX idx_api_usage_logs_endpoint ON api_usage_logs(endpoint);",
        "CREATE INDEX idx_api_usage_logs_timestamp ON api_usage_logs(timestamp);"
    ]
    for index_query in indexes:
        await conn.execute(text(index_query))


async def init_database():
    """Initialize database by creating all tables"""
    
    database_url = get_database_url()
    
    # Hide password in logs
    safe_url = database_url.split('@')[0].split(':')[:-1]
    safe_url = ':'.join(safe_url) + ':***@' + database_url.split('@')[1]
    print(f"Connecting to database: {safe_url}")
    
    # Create async engine
    engine = create_async_engine(database_url, echo=False)
    
    try:
        async with engine.begin() as conn:
            print("Creating database schema...")
            
            # Create tables in correct order (respecting foreign key constraints)
            await create_organisation_table(conn)
            await create_users_table(conn)
            await create_password_resets_table(conn)
            await create_api_usage_logs_table(conn)
            
            print("✅ Successfully created all database tables!")
            
            # List all created tables
            list_tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
            """
            result = await conn.execute(text(list_tables_query))
            tables = [row[0] for row in result.fetchall()]
            
            print("\n📋 Tables in database:")
            for table in tables:
                print(f"  - {table}")
            
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        raise
    finally:
        await engine.dispose()


async def main():
    """Main function"""
    print("🗄️  Database Initialization Script")
    print("=" * 40)
    
    try:
        await init_database()
        print("\n✅ Database initialization completed successfully!")
    except Exception as e:
        print(f"\n❌ Database initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())