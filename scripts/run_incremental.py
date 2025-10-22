"""
run_incremental.py

Incremental loader: loads exactly one next-month parquet file per execution.
Behavior:
- Ensures pipeline_metadata table exists
- Reads last_loaded_month from pipeline_metadata (format "YYYY-MM")
- Determines next month to load (if none, starts at 1 -> "YYYY-01")
- Downloads missing Parquet for that month
- Converts Parquet -> CSV in memory and appends to RAW table
- Runs transform_silver.sql and aggregate_gold.sql (both expected to be idempotent/upsert)
- Updates pipeline_metadata to the newly loaded month
- Optionally deletes parquet after successful load
"""

import logging
from typing import Optional
from pathlib import Path
from datetime import datetime
from pipeline_helpers import (
    DATA_DIR, YEAR, SQL_DIR,
    download_parquet, month_to_filename,
    ensure_pipeline_metadata_table, get_last_loaded_month, update_last_loaded_month,
    load_parquet_month_to_raw_inmemory, run_sql_file,
)

logger = logging.getLogger(__name__)

def next_month_after(month_str: str) -> Optional[str]:
    """
    Given 'YYYY-MM', return the next month string 'YYYY-MM', or None if beyond December of YEAR.
    If month_str is None, returns f"{YEAR}-01".
    """
    if month_str is None:
        return f"{YEAR}-01"
    try:
        dt = datetime.strptime(month_str, "%Y-%m")
    except Exception as exc:
        logger.error("Invalid month_str in metadata: %s", month_str, exc_info=True)
        return None
    # compute next month
    year = dt.year
    month = dt.month + 1
    if month > 12:
        logger.info("All months for year %s have been loaded (metadata shows %s).", YEAR, month_str)
        return None
    return f"{year}-{month:02}"

def run_incremental_one_month(delete_parquet_after: bool = False):
    """
    Execute incremental load for exactly one month:
      - find next month using pipeline_metadata
      - download the parquet file if missing
      - stream parquet -> CSV in-memory and COPY into raw table (append)
      - run transform_silver.sql and aggregate_gold.sql
      - update pipeline_metadata
    """
    logger.info("Starting incremental load (one month)")

    ensure_pipeline_metadata_table()

    last_loaded = get_last_loaded_month()
    logger.info("Last loaded month from metadata: %s", last_loaded)

    next_month = next_month_after(last_loaded)
    if not next_month:
        logger.info("No next month to load (either metadata invalid or all months loaded). Exiting.")
        return

    # parse next_month into integer month
    year_s, month_s = next_month.split("-")
    month_int = int(month_s)
    filename = month_to_filename(YEAR, month_int)
    parquet_path = DATA_DIR / filename
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

    # download parquet if missing
    ok = download_parquet(url, parquet_path)
    if not ok:
        logger.error("Failed to download parquet for %s; aborting incremental run.", next_month)
        return

    # load parquet -> raw in memory (append)
    try:
        load_parquet_month_to_raw_inmemory(parquet_path, table_name="raw_taxi_data_2024")
    except Exception as exc:
        logger.error("Error loading month %s into raw: %s", next_month, exc, exc_info=True)
        raise

    # run silver transform and gold aggregation (they should handle incremental upsert logic)
    try:
        logger.info("Running transform_silver.sql (incremental step)")
        run_sql_file(SQL_DIR / "transform_silver.sql")
    except Exception as exc:
        logger.error("Transform to silver failed for %s: %s", next_month, exc, exc_info=True)
        raise

    try:
        logger.info("Running aggregate_gold.sql (incremental step)")
        run_sql_file(SQL_DIR / "aggregate_gold.sql")
    except Exception as exc:
        logger.error("Aggregate to gold failed for %s: %s", next_month, exc, exc_info=True)
        raise

    # update metadata
    try:
        update_last_loaded_month(next_month)
    except Exception as exc:
        logger.error("Failed to update pipeline metadata after loading %s: %s", next_month, exc, exc_info=True)
        raise

    # optionally delete parquet to save disk
    if delete_parquet_after:
        try:
            parquet_path.unlink(missing_ok=True)
            logger.info("Deleted parquet for %s after successful load.", next_month)
        except Exception as exc:
            logger.warning("Could not delete parquet %s: %s", parquet_path, exc, exc_info=True)

    logger.info("Incremental load complete for %s", next_month)


if __name__ == "__main__":
    try:
        # set True if you want to remove parquet after load
        run_incremental_one_month(delete_parquet_after=False)
    except Exception as exc:
        logger.error("Incremental pipeline failed: %s", exc, exc_info=True)
        raise
