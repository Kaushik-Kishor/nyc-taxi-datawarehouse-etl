import os
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from io import StringIO

# --- 1. Database Config ---
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB = os.getenv("PG_DB")

PG_SCHEMA = "staging"
TABLE_NAME = "yellow_tripdata_2023_01"

# --- 2. File Path ---
PROJECT_ROOT = "/path/to/project"  # Adjust this path as needed
PARQUET_PATH = os.path.join(
    PROJECT_ROOT, "data_raw", "nyc_taxi", "yellow_tripdata_2023-01.parquet"
)

def df_to_postgres_copy(df, table_name, schema, conn):
    # Convert DataFrame → CSV in memory
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()

    # Create table if it doesn't exist (SQLA reflection)
    columns = ", ".join([f'"{col}" TEXT' for col in df.columns])
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            {columns}
        );
        TRUNCATE TABLE {schema}.{table_name};
    """
    cursor.execute(create_sql)

    # COPY the CSV into the table
    cursor.copy_expert(
        f"""
        COPY {schema}.{table_name} FROM STDIN WITH CSV;
        """,
        buffer
    )

    conn.commit()
    cursor.close()

def main():
    print("Reading parquet file...")
    df = pd.read_parquet(PARQUET_PATH)
    print(f"Loaded {len(df):,} rows.")

    # PostgreSQL raw connection (not SQLAlchemy)
    print("Connecting to Postgres using psycopg2...")
    conn = psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )

    print("Inserting using COPY (fast)...")
    df_to_postgres_copy(df, TABLE_NAME, PG_SCHEMA, conn)

    conn.close()
    print(f"Done! Data loaded into {PG_SCHEMA}.{TABLE_NAME}")

if __name__ == "__main__":
    main()
