"""
NYC Taxi Data Pipeline (Idempotent Full Refresh)
------------------------------------------------
1️⃣ Download monthly Parquet files for 2024 (if missing)
2️⃣ Merge them into one CSV (if missing)
3️⃣ Verify raw table and load only if counts differ
4️⃣ Run Silver and Gold SQL transformations
"""

import os
import requests
import psycopg2
import logging
import csv
from pathlib import Path
from dotenv import load_dotenv
import pyarrow.parquet as pq
import pyarrow.csv as pc

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
YEAR = 2024
DATA_DIR = Path("data")
COMBINED_CSV_PATH = DATA_DIR / f"yellow_tripdata_{YEAR}.csv"

load_dotenv()
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
SQL_DIR = Path(os.getenv("SQL_DIR"))
DATA_PATH = COMBINED_CSV_PATH

# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)

# Helper: run SQL file

def run_sql_file(file_path: Path):
    """Execute a .sql file directly against PostgreSQL."""
    logging.info(f"Running SQL: {file_path.name}")
    try:
        with psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        ) as conn:
            with conn.cursor() as cur:
                sql_content = file_path.read_text()
                cur.execute(sql_content)
                conn.commit()
        logging.info(f"Done: {file_path.name}\n")
    except Exception as e:
        logging.error(f"Error running {file_path.name}: {e}", exc_info=True)
        raise


# Data Download and Merge

def download_parquet(url: str, dest_path: Path):
    """Download a Parquet file if it doesn't already exist."""
    if dest_path.exists():
        logging.info(f"Skipping download (exists): {dest_path.name}")
        return
    try:
        logging.info(f"Downloading: {url}")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"Downloaded: {dest_path.name}")
    except Exception as e:
        logging.error(f"Failed to download {url}: {e}", exc_info=True)

def merge_parquet_files_to_csv(output_csv_path: Path, year: int):
    """Merge monthly Parquet files into one CSV (skip if already exists)."""
    if output_csv_path.exists():
        logging.info(f"Combined CSV already exists: {output_csv_path.name}")
        return

    logging.info("Merging monthly Parquet files into one CSV...")
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_csv_path, "wb") as out:
        first = True
        for month in range(1, 13):
            month_str = f"{month:02}"
            parquet_path = DATA_DIR / f"yellow_tripdata_{year}-{month_str}.parquet"
            if not parquet_path.exists():
                logging.warning(f"Missing: {parquet_path.name}")
                continue
            table = pq.read_table(parquet_path)
            pc.write_csv(table, out, write_options=pc.WriteOptions(include_header=first))
            first = False
            logging.info(f"Appended: {parquet_path.name}")
    logging.info(f"Combined CSV saved: {output_csv_path.resolve()}")


# Database Utilities

def get_table_row_count(table_name: str) -> int:
    """Return the number of rows currently in the given table."""
    try:
        with psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                result = cur.fetchone()[0]
                return result
    except Exception as e:
        logging.warning(f"Could not count rows for {table_name}: {e}")
        return 0

def get_csv_row_count(csv_path: Path) -> int:
    """Efficiently count rows in a CSV (excluding header)."""
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            row_count = sum(1 for _ in f) - 1
        return max(row_count, 0)
    except Exception as e:
        logging.warning(f"Could not count rows in {csv_path}: {e}")
        return 0

def truncate_table(table_name: str):
    """Truncate the specified table."""
    logging.info(f"Truncating table {table_name}...")
    try:
        with psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_name};")
                conn.commit()
        logging.info(f"Table truncated: {table_name}")
    except Exception as e:
        logging.error(f"Error truncating {table_name}: {e}", exc_info=True)
        raise

def load_csv_to_postgres(csv_path: Path, table_name: str = "raw_taxi_data_2024"):
    """
    Load CSV data into PostgreSQL using COPY FROM STDIN.
    Idempotent — only loads if data differs.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    csv_count = get_csv_row_count(csv_path)
    db_count = get_table_row_count(table_name)

    logging.info(f"CSV rows: {csv_count:,} | DB rows: {db_count:,}")

    if csv_count == db_count and csv_count > 0:
        logging.info("Skipping load: data already up-to-date.")
        return

    logging.info("Loading CSV into PostgreSQL...")
    truncate_table(table_name)
    try:
        with psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        ) as conn:
            with conn.cursor() as cur:
                with open(csv_path, "r", encoding="utf-8") as f:
                    copy_sql = f"""
                        COPY {table_name}
                        FROM STDIN
                        WITH (FORMAT csv, HEADER true);
                    """
                    cur.copy_expert(copy_sql, f)
                    conn.commit()
        logging.info(f"CSV successfully loaded into {table_name}.\n")
    except Exception as e:
        logging.error(f"Error during CSV import: {e}", exc_info=True)
        raise


# Full Refresh Pipeline

def run_full_refresh():
    """Run the entire full-refresh pipeline with intelligent load skipping."""
    logging.info(" Starting PostgreSQL full refresh pipeline...")

    # Create raw table schema
    run_sql_file(SQL_DIR / "create_raw_table.sql")

    # Download & merge raw data
    for month in range(1, 13):
        month_str = f"{month:02}"
        fname = f"yellow_tripdata_{YEAR}-{month_str}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{fname}"
        dest = DATA_DIR / fname
        download_parquet(url, dest)
    merge_parquet_files_to_csv(COMBINED_CSV_PATH, YEAR)

    # Load CSV into raw table (if needed)
    load_csv_to_postgres(DATA_PATH, "raw_taxi_data_2024")

    # Transform → Silver
    run_sql_file(SQL_DIR / "transform_silver.sql")

    # Aggregate → Gold
    run_sql_file(SQL_DIR / "aggregate_gold.sql")

    logging.info("FULL REFRESH COMPLETED SUCCESSFULLY.\n")

# Entry Point

if __name__ == "__main__":
    try:
        run_full_refresh()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        exit(1)
