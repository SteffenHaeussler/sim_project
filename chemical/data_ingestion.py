import glob

import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def ingest_parquet_data(
    parquet_dir, db_host, db_name, db_user, db_password, table_name
):
    """
    Ingests data from Parquet files in a directory into a PostgreSQL database.

    Args:
        parquet_dir (str): Path to the directory containing Parquet files.
        db_host (str): PostgreSQL database host.
        db_name (str): PostgreSQL database name.
        db_user (str): PostgreSQL database username.
        db_password (str): PostgreSQL database password.
        table_name (str): Name of the table to insert data into.
        assets_table_name (str): Name of the assets table.
    """
    aggregations = ["min", "h", "d"]
    try:
        # Establish database connection
        conn = psycopg2.connect(
            host=db_host, database=db_name, user=db_user, password=db_password
        )
        cur = conn.cursor()

        # Create table if it doesn't exist
        for aggregation in aggregations:
            new_name = f"{table_name}_{aggregation}"
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {new_name} (
                pk_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                asset_id UUID references assets(id),
                timestamp TIMESTAMP,
                value FLOAT
                INDEX asset_timestamp_idx (asset_id, timestamp)
            )
        """
            cur.execute(create_table_query)
            conn.commit()
            conn.close()

    except Exception as e:
        print(f"Error: {e}")

    parquet_files = glob.glob(f"{parquet_dir}/*.parquet")

    if not parquet_files:
        print(f"No Parquet files found in {parquet_dir}")
    engine = create_engine("postgresql://postgres:example@localhost:5432/chemical")

    for file in parquet_files:
        print(file)
        # Read each Parquet file
        df = pd.read_parquet(file)

        _id = df["id"].unique()[0]
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        df.set_index("timestamp", inplace=True)
        df = df["value"]

        for aggregation in aggregations:
            new_name = f"{table_name}_{aggregation}"

            df_agg = df.resample(aggregation).mean().reset_index()
            df_agg["asset_id"] = _id

            df_agg.to_sql(new_name, engine, if_exists="append", index=False)

    print("All Parquet files ingested successfully.")


if __name__ == "__main__":
    DB_HOST = "localhost"
    DB_NAME = "chemical"
    DB_USER = "postgres"
    DB_PASSWORD = "example"
    TABLE_NAME = "data"
    DATA_DIR = "raw/data"

    # Call the ingestion function
    ingest_parquet_data(DATA_DIR, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, TABLE_NAME)
