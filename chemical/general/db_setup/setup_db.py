"""Minimal script to create evaluation database tables."""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Database configuration
DB_NAME = "evaluation"
DB_USER = "postgres"  # Update with your username
DB_PASSWORD = "example"  # Update with your password
DB_HOST = "localhost"
DB_PORT = "5432"

# SQL to create tables
CREATE_TABLES_SQL = """
-- Test runs table (one row per test suite run)
CREATE TABLE IF NOT EXISTS test_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) UNIQUE NOT NULL,
    test_suite VARCHAR(50),
    total_tests INTEGER DEFAULT 0,
    passed_tests INTEGER DEFAULT 0,
    failed_tests INTEGER DEFAULT 0,
    model_id VARCHAR(255),
    model_api_base VARCHAR(255),
    model_temperature VARCHAR(10)
);

-- Test results table (one row per test)
CREATE TABLE IF NOT EXISTS test_results (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) REFERENCES test_runs(run_id),
    test_name VARCHAR(255),
    question TEXT,
    expected TEXT,
    actual TEXT,
    passed BOOLEAN,
    execution_time_ms INTEGER,

    -- Judge scores (NULL for exact match tests)
    overall_score DECIMAL(3,1),
    accuracy_score DECIMAL(3,1),
    relevance_score DECIMAL(3,1),
    completeness_score DECIMAL(3,1),
    hallucination_score DECIMAL(3,1),
    judge_assessment TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_test_results_run_id ON test_results(run_id);
CREATE INDEX IF NOT EXISTS idx_test_runs_suite ON test_runs(test_suite);
"""


def create_database():
    """Create the evaluation database if it doesn't exist."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()

    # Check if database exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"Created database '{DB_NAME}'")
    else:
        print(f"Database '{DB_NAME}' already exists")

    cur.close()
    conn.close()


def create_tables():
    """Create the tables in the evaluation database."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    cur = conn.cursor()
    cur.execute(CREATE_TABLES_SQL)
    conn.commit()

    print("Created tables 'test_runs' and 'test_results'")

    cur.close()
    conn.close()


if __name__ == "__main__":
    print("Setting up evaluation database...")
    create_database()
    create_tables()
    print("Done!")
