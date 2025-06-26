#!/usr/bin/env python3
"""
Standalone database migration script to create password_resets table

This script creates the password_resets table needed for the forgot password functionality.
It's completely self-contained and can be copied to any project folder.

Usage:
    python create_password_reset_table.py

Environment Variables Required:
    DATABASE_URL - PostgreSQL connection string (e.g., postgresql://user:pass@host:port/db)
"""

import os
import sys

from sqlalchemy import create_engine, text


def get_database_url():
    """Get database URL from environment variables"""
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        # Try alternative environment variable names
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_user = os.getenv("DB_USER", os.getenv("DB_USERNAME", "postgres"))
        db_password = os.getenv("DB_PASSWORD", os.getenv("DB_PASS", "example"))
        db_name = os.getenv("DB_NAME", os.getenv("DATABASE_NAME", "organisation"))

        if db_user and db_password and db_name:
            database_url = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )
        else:
            print("❌ Error: Database configuration not found!")
            print(
                "Set DATABASE_URL environment variable or DB_HOST, DB_USER, DB_PASSWORD, DB_NAME"
            )
            print(
                "Example: export DATABASE_URL='postgresql://user:pass@localhost:5432/dbname'"
            )
            sys.exit(1)

    return database_url


def create_password_reset_table():
    """Create the password_resets table"""

    database_url = get_database_url()

    # Hide password in logs
    safe_url = database_url.split("@")[0].split(":")[:-1]
    safe_url = ":".join(safe_url) + ":***@" + database_url.split("@")[1]
    print(f"Connecting to database: {safe_url}")

    # Create sync engine
    engine = create_engine(database_url, echo=False)

    try:
        with engine.begin() as conn:
            # Check if table already exists
            check_table_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'password_resets'
            );
            """

            result = conn.execute(text(check_table_query))
            table_exists = result.scalar()

            if table_exists:
                print("✅ Table 'password_resets' already exists!")
                return

            print("Creating password_resets table...")

            # Create the password_resets table
            create_table_query = """
            CREATE TABLE password_resets (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID NOT NULL,
                token VARCHAR(255) NOT NULL UNIQUE,
                is_used BOOLEAN NOT NULL DEFAULT FALSE,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                used_at TIMESTAMP WITH TIME ZONE NULL
            );
            """

            conn.execute(text(create_table_query))

            # Create indexes for better performance
            print("Creating indexes...")

            create_indexes_queries = [
                "CREATE INDEX idx_password_resets_user_id ON password_resets(user_id);",
                "CREATE INDEX idx_password_resets_token ON password_resets(token);",
                "CREATE INDEX idx_password_resets_expires_at ON password_resets(expires_at);",
            ]

            for query in create_indexes_queries:
                conn.execute(text(query))

            # Add foreign key constraint if users table exists
            print("Checking for users table...")
            check_users_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'users'
            );
            """

            result = conn.execute(text(check_users_query))
            users_table_exists = result.scalar()

            if users_table_exists:
                print("Adding foreign key constraint to users table...")
                add_fk_query = """
                ALTER TABLE password_resets
                ADD CONSTRAINT fk_password_resets_user_id
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
                """
                conn.execute(text(add_fk_query))
            else:
                print(
                    "⚠️  Warning: users table not found. Foreign key constraint not added."
                )

            print("✅ Successfully created password_resets table with indexes!")

    except Exception as e:
        print(f"❌ Error creating table: {e}")
        raise
    finally:
        engine.dispose()


def main():
    """Main function"""
    print("🔧 Database Migration: Creating password_resets table")
    print("=" * 55)

    try:
        create_password_reset_table()
        print("✅ Migration completed successfully!")
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
