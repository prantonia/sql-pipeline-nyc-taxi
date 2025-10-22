"""
pipeline_helpers.py (updated)

Contains helper utilities for:
- downloading parquet files
- merging parquet -> csv (existing)
- DB SQL runner
- COUNT helpers
- metadata table operations for incremental loads
- in-memory parquet -> CSV conversion and COPY into Postgres (no on-disk CSV)
"""

from pathlib import Path
import os
import logging
from dotenv import load_dotenv
import requests
import pyarrow.parquet as pq
import pyarrow.csv as pc
import psycopg2
import io
from typing import Optional, Tuple


# CONFIG

load_dotenv()
YEAR = int(os.getenv("YEAR", "2024"))
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
COMBINED_CSV_PATH = DATA_DIR / f"yellow_tripdata_{YEAR}.csv"
SQL_DIR = Path(os.getenv("SQL_DIR", "sql"))

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE")

RAW_TABLE = os.getenv("RAW_TABLE", "raw_taxi_data_2024")
SILVER_TABLE = os.getenv("SILVER_TABLE", "silver_taxi_data_2024")
GOLD_TABLE = os.getenv("GOLD_TABLE", "gold_taxi_summary_2024")

# Pipeline metadata table to track incremental month loads
PIPELINE_METADATA = os.getenv("PIPELINE_METADATA", "pipeline_metadata")

# Whether to delete monthly parquet after successful load to save disk
DELETE_PARQUET_AFTER_LOAD = os.getenv("DELETE_PARQUET_AFTER_LOAD", "false").lower() in ("1", "true", "yes")

# -------------------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# DB helper
# -------------------------------------------------------------------------
def get_db_conn():
    """
    Return a new psycopg2 connection using env vars.
    Use 'with get_db_conn() as conn:' to auto-close.
    """
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE
    )

# -------------------------------------------------------------------------
# SQL runner
# -------------------------------------------------------------------------
def run_sql_file(file_path: Path) -> None:
    """
    Execute SQL file against the DB. Raises FileNotFoundError if missing.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    logger.info("Running SQL: %s", file_path.name)
    sql_text = file_path.read_text()
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)
            conn.commit()
        logger.info("Completed SQL: %s", file_path.name)
    except Exception as exc:
        logger.error("Error executing SQL %s: %s", file_path.name, exc, exc_info=True)
        raise

# -------------------------------------------------------------------------
# Parquet download + merge (existing)
# -------------------------------------------------------------------------
def download_parquet(url: str, dest_path: Path, timeout: int = 60) -> bool:
    """
    Download Parquet file to dest_path if missing.
    Returns True if present (downloaded or already exists), False on failure.
    """
    if dest_path.exists():
        logger.debug("Parquet already exists: %s", dest_path)
        return True
    try:
        logger.info("Downloading %s", url)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=8192):
                    fh.write(chunk)
        logger.info("Downloaded %s", dest_path.name)
        return True
    except Exception as exc:
        logger.error("Download failed %s: %s", url, exc, exc_info=True)
        return False

def merge_parquet_files_to_csv(output_csv_path: Path, year: int = YEAR) -> None:
    """
    Merge monthly Parquet files into a single CSV (idempotent).
    Skips if output CSV already exists.
    """
    if output_csv_path.exists():
        logger.info("Combined CSV already exists: %s", output_csv_path.name)
        return

    logger.info("Merging Parquet files into CSV: %s", output_csv_path)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(output_csv_path, "wb") as out:
            first = True
            for month in range(1, 13):
                parquet_path = DATA_DIR / f"yellow_tripdata_{year}-{month:02}.parquet"
                if not parquet_path.exists():
                    logger.warning("Missing parquet: %s - skipping", parquet_path.name)
                    continue
                try:
                    table = pq.read_table(parquet_path)
                    pc.write_csv(table, out, write_options=pc.WriteOptions(include_header=first))
                    first = False
                    logger.info("Appended %s", parquet_path.name)
                except Exception as exc:
                    logger.error("❌ Failed to convert %s: %s", parquet_path.name, exc, exc_info=True)
                    continue
        logger.info("Combined CSV written: %s", output_csv_path.resolve())
    except Exception as exc:
        logger.error("Error merging parquet files: %s", exc, exc_info=True)
        raise

# -------------------------------------------------------------------------
# CSV / DB counting helpers
# -------------------------------------------------------------------------
def get_csv_row_count(csv_path: Path) -> int:
    """
    Count rows in CSV excluding header. Returns 0 if missing or error.
    """
    if not csv_path.exists():
        return 0
    try:
        with open(csv_path, "r", encoding="utf-8") as fh:
            total = sum(1 for _ in fh)
            return max(total - 1, 0)
    except Exception as exc:
        logger.warning("Could not count CSV rows %s: %s", csv_path, exc, exc_info=True)
        return 0

def get_table_row_count(table_name: str) -> int:
    """
    Return COUNT(*) from table_name or 0 on error.
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                return int(cur.fetchone()[0] or 0)
    except Exception as exc:
        logger.warning("Could not count rows for %s: %s", table_name, exc, exc_info=True)
        return 0

def get_raw_stats() -> Tuple[int, Optional[str]]:
    """
    Return (row_count, max_pickup_ts) from RAW table.
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*), MAX(tpep_pickup_datetime) FROM {RAW_TABLE};")
                count, max_ts = cur.fetchone()
                return int(count or 0), max_ts
    except Exception as exc:
        logger.warning("Could not fetch raw stats: %s", exc, exc_info=True)
        return 0, None

def get_silver_max_pickup() -> Optional[str]:
    """
    Return MAX(pickup_datetime) from silver table or None.
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(pickup_datetime) FROM {SILVER_TABLE};")
                return cur.fetchone()[0]
    except Exception as exc:
        logger.warning("Could not fetch silver max pickup: %s", exc, exc_info=True)
        return None

# -------------------------------------------------------------------------
# Metadata table for incremental loads
# -------------------------------------------------------------------------
def ensure_pipeline_metadata_table():
    """
    Create pipeline_metadata table if not exists.
    Columns:
      - pipeline_name TEXT PRIMARY KEY
      - last_loaded_month TEXT (format YYYY-MM)
      - last_loaded_at TIMESTAMP
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {PIPELINE_METADATA} (
        pipeline_name TEXT PRIMARY KEY,
        last_loaded_month TEXT,
        last_loaded_at TIMESTAMP
    );
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        logger.info("Ensured pipeline metadata table exists.")
    except Exception as exc:
        logger.error("Could not ensure pipeline metadata table: %s", exc, exc_info=True)
        raise

def get_last_loaded_month(pipeline_name: str = "nyc_taxi_2024") -> Optional[str]:
    """
    Return last_loaded_month (YYYY-MM) for pipeline_name, or None if not present.
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT last_loaded_month FROM {PIPELINE_METADATA} WHERE pipeline_name = %s;", (pipeline_name,))
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as exc:
        logger.warning("Could not read pipeline metadata: %s", exc, exc_info=True)
        return None

def update_last_loaded_month(month: str, pipeline_name: str = "nyc_taxi_2024"):
    """
    Upsert last_loaded_month for pipeline_name.
    """
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {PIPELINE_METADATA} (pipeline_name, last_loaded_month, last_loaded_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (pipeline_name) DO UPDATE
                    SET last_loaded_month = EXCLUDED.last_loaded_month,
                        last_loaded_at = EXCLUDED.last_loaded_at;
                    """,
                    (pipeline_name, month)
                )
            conn.commit()
        logger.info("pipeline metadata updated: %s -> %s", pipeline_name, month)
    except Exception as exc:
        logger.error("Could not update pipeline metadata: %s", exc, exc_info=True)
        raise


# In-memory Parquet -> CSV -> COPY loader (no on-disk CSV)

def load_parquet_month_to_raw_inmemory(parquet_path: Path, table_name: str = RAW_TABLE) -> None:
    """
    Convert a Parquet file to CSV in-memory and COPY into Postgres via copy_expert.
    - parquet_path: local parquet file path
    - table_name: destination raw table (assumes schema exists)
    This function does NOT write a CSV to disk.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    logger.info("Streaming Parquet -> CSV (in-memory) for %s", parquet_path.name)

    try:
        # Read parquet into Arrow Table
        table = pq.read_table(parquet_path)

        # Write to in-memory bytes buffer as CSV
        bio = io.BytesIO()
        pc.write_csv(table, bio, write_options=pc.WriteOptions(include_header=True))
        bio.seek(0)

        # Wrap as text stream for psycopg2 (copy_expert expects a file-like text object)
        text_stream = io.TextIOWrapper(bio, encoding="utf-8")
        # Important: seek to start of text_stream as well
        text_stream.seek(0)

        # Perform COPY FROM STDIN
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                # If you want to append, remove TRUNCATE. For incremental we append.
                # Here we simply COPY into raw table (append)
                copy_sql = f"""
                    COPY {table_name}
                    FROM STDIN
                    WITH (FORMAT csv, HEADER true);
                """
                cur.copy_expert(copy_sql, text_stream)
            conn.commit()
        logger.info("✅ In-memory load complete for %s", parquet_path.name)
    except Exception as exc:
        logger.error("❌ In-memory load failed for %s: %s", parquet_path.name, exc, exc_info=True)
        raise
    finally:
        try:
            text_stream.detach()
        except Exception:
            pass


# Convenience: compute month string helper

def month_to_filename(year: int, month: int) -> str:
    """Return the standardized parquet file name for a year/month."""
    return f"yellow_tripdata_{year}-{month:02}.parquet"

