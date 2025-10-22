# NYC Taxi Data Pipeline (SQL-Based, Python + PostgreSQL)

A **modular, SQL-driven data pipeline** that ingests, transforms, and aggregates **NYC Yellow Taxi trip data (2024)** using Python and PostgreSQL.  
The pipeline supports both **full-refresh** and **incremental** data loads, and is designed to be **idempotent**, **metadata-driven**, and **extensible**.

---

## Table of Contents

1. [Project Overview](#-project-overview)  
2. [Folder Structure](#-folder-structure)  
3. [Environment Setup](#-environment-setup)  
4. [Design Rationale](#-design-rationale)  
5. [Pipeline Flow](#-pipeline-flow)  
6. [Full Refresh Process](#-full-refresh-process)  
7. [Incremental Load Process](#-incremental-load-process)  
8. [Metadata Management](#-metadata-management)  
9. [SQL Layers Explained](#-sql-layers-explained)  
10. [Example Analytical Queries](#-example-analytical-queries)  
11. [Key Learnings & Next Steps](#-key-learnings--next-steps)

---

## Project Overview

This project demonstrates how to build a **SQL-based data pipeline** that automates:
- Downloading public NYC Taxi trip data (Parquet format)
- Loading and transforming it into PostgreSQL
- Producing monthly metrics for analytics dashboards

The design mirrors modern **data warehouse architecture**:  
**Raw → Silver → Gold**, orchestrated with **Python**, powered by **SQL transformations**.

---

## Folder Structure

```bash
sql_pipeline/
├── data/                              # Temporary local data storage (ignored in .gitignore)
│   ├── yellow_tripdata_2024-01.parquet
│   └── yellow_tripdata_2024.csv
│
├── sql/                               # SQL transformation scripts (core of pipeline logic)
│   ├── create_raw_table.sql           # Raw table DDL schema
│   ├── transform_silver.sql           # Cleansing, filtering, standardization logic
│   └── aggregate_gold.sql             # Aggregation and business metrics
│
├── docs/                              # Documentation deliverables
│   ├── project_documentation.md       # Full project explanation
│   └── data_dictionary.md             # Raw table field descriptions
│
├── scripts/                           # Executable scripts for pipeline automation
│   ├── full_refresh.py                # Full-year load (Raw → Silver → Gold)
│   ├── run_incremental.py             # Incremental monthly load with metadata tracking
│   └── pipeline_helpers.py            # Shared utilities (download, merge, DB ops, metadata)
│
├── requirements.txt                   # Python dependency list
├── .env                               # Environment config (Postgres credentials, paths)
├── .gitignore                         # Ignore data, env, caches, etc.
├── README.md                          # GitHub overview (walkthrough & usage)
└── A_flowchart_diagram_in_the_image_illustrates_a_SQL.png  # Architecture diagram

```

---

## Environment Setup

### Create Project Folder
```bash
mkdir sql_pipeline && cd sql_pipeline
```

### Create Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate        # (Linux/Mac)
# OR
.venv\Scripts\activate           # (Windows)
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

**requirements.txt**
```
psycopg2
python-dotenv
pyarrow
requests
pandas
logging
```

---

### Configure `.env`

Create a `.env` file at project root:

```ini
# PostgreSQL connection
PG_USER=postgres
PG_PASSWORD=yourpassword
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=nyc_taxi

# File and SQL paths
DATA_DIR=data
SQL_DIR=sql

# Table names
RAW_TABLE=raw_taxi_data_2024
SILVER_TABLE=silver_taxi_data_2024
GOLD_TABLE=gold_taxi_summary_2024
PIPELINE_METADATA=pipeline_metadata
```

> **Tip:** Add `.env` to `.gitignore` to avoid committing credentials.

---

## Design Rationale

The pipeline was designed around the following principles:

| Principle | Description |
|------------|--------------|
| **SQL-Driven** | All transformations happen inside PostgreSQL using SQL scripts. |
| **Python-Orchestrated** | Python handles extraction, file merging, and SQL execution. |
| **Idempotent** | Safe to rerun — no duplicates or re-loads. |
| **Incremental** | Only loads new monthly data after initial setup. |
| **Metadata-Aware** | Keeps track of the last loaded month dynamically. |

---

## Pipeline Flow

![Pipeline Diagram](A_flowchart_diagram_in_the_image_illustrates_a_SQL.png)

1. **Extract:** Download monthly Parquet files from NYC TLC open data.  
2. **Load:** Stream CSVs or Parquet data into PostgreSQL (`raw_taxi_data_2024`).  
3. **Transform:** Run SQL scripts for Silver (clean) and Gold (aggregate) layers.  
4. **Store:** Persist results in database for analytics dashboards.  
5. **Track:** Update metadata table to manage monthly progress.

---

## Full Refresh Process

**Script:** `full_refresh.py`  
Used for initial or complete rebuild of the pipeline.

### Steps
1. Run `create_raw_table.sql` to ensure schema exists.  
2. Download all Parquet files (Jan–Dec 2024).  
3. Merge them into one CSV using `pyarrow`.  
4. Load the CSV into PostgreSQL using `COPY FROM STDIN`.  
5. Execute transformations:
   - `transform_silver.sql`
   - `aggregate_gold.sql`

### Run Command
```bash
python full_refresh.py
```

### Expected Log Output
```
2025-10-22 10:15 | INFO | Starting PostgreSQL full refresh pipeline...
2025-10-22 10:16 | INFO | Downloading yellow_tripdata_2024-01.parquet...
...
2025-10-22 10:25 | INFO | FULL REFRESH COMPLETED SUCCESSFULLY.
```

---

## Incremental Load Process

**Script:** `run_incremental.py`  
Used for adding new data month-by-month.

### Steps
1. Check `pipeline_metadata` table for last loaded month.  
2. Compute the next month automatically.  
3. Download that specific month’s Parquet file.  
4. Load Parquet directly into Postgres (in-memory streaming).  
5. Run Silver and Gold SQL transformations.  
6. Update metadata table with new month.

### Run Command
```bash
python run_incremental.py
```

### Example Log
```
Starting incremental load (one month)
Last loaded month: 2024-03
Next month to load: 2024-04
In-memory load complete for yellow_tripdata_2024-04.parquet
Incremental load complete for 2024-04
```

---

## Metadata Management

**Table:** `pipeline_metadata`

| Column | Type | Description |
|---------|------|-------------|
| `pipeline_name` | TEXT (PK) | Unique identifier (e.g., “nyc_taxi_2024”) |
| `last_loaded_month` | TEXT | The most recent month successfully loaded |
| `last_loaded_at` | TIMESTAMP | When the last load occurred |

This table ensures **dynamic, resumable loading** — no hardcoded months.

---

## SQL Layers Explained

### Raw Layer
- Defined by `create_raw_table.sql`  
- Holds unmodified taxi trip records as loaded from TLC.

### Silver Layer
- Defined by `transform_silver.sql`  
- Cleans nulls, filters invalid trips, and standardizes timestamps.

Example snippet:
```sql
CREATE TABLE IF NOT EXISTS silver_taxi_data_2024 AS
SELECT *
FROM raw_taxi_data_2024
WHERE fare_amount > 0
  AND trip_distance > 0
  AND passenger_count > 0;
```

### Gold Layer
- Defined by `aggregate_gold.sql`  
- Aggregates business metrics for reporting.

```sql
CREATE TABLE gold_taxi_summary_2024 AS
SELECT
  DATE_TRUNC('month', pickup_datetime) AS month,
  COUNT(*) AS total_trips,
  ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
  ROUND(AVG(tip_amount)::numeric, 2) AS avg_tip,
  SUM(total_amount) AS total_revenue
FROM silver_taxi_data_2024
GROUP BY 1
ORDER BY 1;
```

---

## Example Analytical Queries

### Q1 — Monthly Trip and Revenue Trends
```sql
SELECT month, total_trips, total_revenue
FROM gold_taxi_summary_2024
ORDER BY month;
```

### Q2 — Average Fare and Tip Analysis
```sql
SELECT month, avg_fare, avg_tip, (avg_tip / avg_fare) * 100 AS tip_pct
FROM gold_taxi_summary_2024;
```

### Q3 — Average Passengers per Trip
```sql
SELECT DATE_TRUNC('month', pickup_datetime) AS month,
       AVG(passenger_count) AS avg_passengers
FROM silver_taxi_data_2024
GROUP BY 1;
```

---

## Key Learnings & Next Steps

**Learnings**
- How to build a production-style SQL pipeline with both full and incremental logic.  
- The importance of metadata in ensuring dynamic, resumable ingestion.  
- How SQL layers (Raw → Silver → Gold) simplify debugging and analytics.  

**Next Steps**
- Integrate with **Airflow** for automated scheduling.  
- Store raw files in **MinIO or AWS S3**.  
- Add **data quality validation** using dbt or Great Expectations.  
- Build dashboards (Looker Studio / Metabase) powered by the Gold table.

---
