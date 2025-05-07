import json

import psycopg2


def ingest_assets(json_file, db_host, db_name, db_user, db_password, table_name):
    """
    Ingests asset data from a JSON file into a PostgreSQL database.

    Args:
        json_file (str): Path to the JSON file containing asset data.
        db_host (str): PostgreSQL database host.
        db_name (str): PostgreSQL database name.
        db_user (str): PostgreSQL database username.
        db_password (str): PostgreSQL database password.
        table_name (str): Name of the table to insert data into.
    """
    conn = psycopg2.connect(
        host=db_host, database="postgres", user=db_user, password=db_password
    )
    conn.autocommit = True  # Required to create a database
    cur = conn.cursor()

    # Check if the database exists, and create it if it doesn't
    db_exists_query = f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"
    cur.execute(db_exists_query)
    db_exists = cur.fetchone()
    if not db_exists:
        print(f"Database '{db_name}' does not exist. Creating it...")
        # Connect to the template1 database to create the new database
        conn.close()
        conn = psycopg2.connect(
            host=db_host, database="postgres", user=db_user, password=db_password
        )
        conn.autocommit = True  # Required to create a database
        cur = conn.cursor()
        create_db_query = f"CREATE DATABASE {db_name}"
        cur.execute(create_db_query)
        conn.commit()
        conn.close()

        # Reconnect to the newly created database
        conn = psycopg2.connect(
            host=db_host, database=db_name, user=db_user, password=db_password
        )
        cur = conn.cursor()
        print(f"Database '{db_name}' created successfully.")
    else:
        print(f"Database '{db_name}' already exists.")

    try:
        # Establish database connection
        conn = psycopg2.connect(
            host=db_host, database=db_name, user=db_user, password=db_password
        )
        cur = conn.cursor()

        # Create table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                name VARCHAR(255),
                tag VARCHAR(255),
                parent_id UUID,
                description TEXT,
                asset_type VARCHAR(255),
                type VARCHAR(255),
                unit VARCHAR(255) NULL,
                range NUMERIC[] NULL
            )
        """
        cur.execute(create_table_query)

        # Read data from JSON file
        with open(json_file, "r") as f:
            assets = json.load(f)

        # Insert data into the table
        for asset in assets:
            insert_query = f"""
                INSERT INTO {table_name} (id, name, tag, parent_id, description, asset_type, type, unit, range)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(
                insert_query,
                (
                    asset["id"],
                    asset["name"],
                    asset["tag"],
                    asset["parent_id"],
                    asset["description"],
                    asset["asset_type"],
                    asset["type"],
                    asset.get("unit", None),
                    asset.get("range", []),
                ),
            )

        # Commit changes and close connection
        conn.commit()
        cur.close()
        conn.close()

        print("Data ingestion complete!")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    DB_HOST = "localhost"
    DB_NAME = "chemical"
    DB_USER = "postgres"
    DB_PASSWORD = "example"
    TABLE_NAME = "assets"
    JSON_FILE = "raw/assets.json"

    # Call the ingestion function
    ingest_assets(JSON_FILE, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, TABLE_NAME)
