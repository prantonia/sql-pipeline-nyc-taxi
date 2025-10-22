# Project Documentation: SQL-Based NYC Taxi Data Pipeline

## Design Rationale

### Objective
The goal of this project is to design a **scalable, SQL-driven data pipeline** for the **NYC Yellow Taxi 2024 dataset** — capable of:
- Automatically ingesting raw trip data each month,  
- Cleaning and transforming it using SQL,  
- Maintaining monthly aggregates for analytics,  
- Running efficiently in both **full-refresh** and **incremental** modes.

This setup mirrors real-world data warehouse design patterns where SQL is the central transformation layer and Python orchestrates the pipeline logic.

---

### Architecture Overview

| Layer | Description | Key Script/File |
|-------|--------------|----------------|
| **Raw (Bronze)** | Raw ingestion of Parquet/CSV data into PostgreSQL. | `create_raw_table.sql`, `full_refresh.py` |
| **Silver (Clean)** | Cleans and standardizes records for analysis. | `transform_silver.sql` |
| **Gold (Aggregate)** | Summarizes metrics for dashboards. | `aggregate_gold.sql` |
| **Metadata** | Tracks last successful load month. | `pipeline_helpers.py` |

---

### Why SQL-Based?
- **Declarative & transparent:** Transformations are readable and version-controlled.  
- **Database-optimized:** PostgreSQL handles aggregation and joins efficiently.  
- **Maintainable:** Analysts and engineers can collaborate using familiar SQL.  
- **Portable:** Works with any relational backend (Postgres, Snowflake, BigQuery).

---

### Technology Stack

| Component | Tool/Library | Purpose |
|------------|---------------|----------|
| **Storage** | NYC TLC Parquet files | Raw source data |
| **ETL Orchestration** | Python (`full_refresh.py`, `run_incremental.py`) | Orchestration |
| **Database** | PostgreSQL | Data warehouse engine |
| **Transformations** | SQL Scripts | Data cleaning + aggregation |
| **Metadata Tracking** | PostgreSQL Table (`pipeline_metadata`) | Incremental load tracking |
| **Visualization** | Looker Studio / Metabase | Analytics dashboard |

---

## Full vs Incremental Load Logic

Both pipelines share the same data flow (Raw → Silver → Gold) but differ in *how* they load and refresh data.

---

### Full Refresh Mode (`full_refresh.py`)

**Use case:** Initial load or complete rebuild.

**Steps:**
1. **Download all 12 Parquet files** for the year (2024).  
2. **Merge them** into a single combined CSV file.  
3. **Create the raw table** if it doesn’t exist (`create_raw_table.sql`).  
4. **Compare row counts** between CSV and database to detect changes.  
5. **Truncate and reload** if the database is outdated.  
6. **Run SQL transformations** for Silver and Gold layers.

**Key property:**  
> *Idempotent* — it can be safely re-run without duplicating data.

---

### Incremental Load Mode (`run_incremental.py`)

**Use case:** Monthly updates after initial full refresh.

**Logic Flow:**
1. **Read the last loaded month** from the metadata table (`pipeline_metadata`).  
2. **Determine the next month** to load (e.g., if last = “2024-03”, next = “2024-04”).  
3. **Download only that Parquet file** from the NYC TLC repository.  
4. **Stream it directly into Postgres** (no intermediate CSV) using in-memory COPY.  
5. **Run transformations:**  
   - `transform_silver.sql` updates Silver layer.  
   - `aggregate_gold.sql` updates aggregated summaries.  
6. **Update metadata** to mark the month as successfully loaded.

**Key property:**  
> *Efficient and append-only* — avoids reprocessing old data.

---

## Metadata Management for Dynamic Loading

To prevent hardcoding months or reloading existing data, the pipeline uses a **metadata tracking table** in PostgreSQL.

### Table: `pipeline_metadata`
| Column | Type | Description |
|---------|------|-------------|
| `pipeline_name` | TEXT (PK) | Unique pipeline identifier (e.g., “nyc_taxi_2024”) |
| `last_loaded_month` | TEXT (YYYY-MM) | The most recent month successfully loaded |
| `last_loaded_at` | TIMESTAMP | Timestamp of the last load |

### Behavior:
- On startup, the pipeline ensures the table exists (`ensure_pipeline_metadata_table()`).  
- Before each load, it fetches the current `last_loaded_month`.  
- The script computes the next month dynamically.  
- After a successful load, the record is **upserted** (insert or update).

This mechanism makes the pipeline **self-aware** — it knows where it left off and automatically resumes from the right month.

---

## Example Analytical Queries

### Q1: Total trips and revenue by month
```sql
SELECT
  TO_CHAR(month, 'YYYY-MM') AS month,
  total_trips,
  total_revenue
FROM gold_taxi_summary_2024
ORDER BY month;
```

### Q2: Average fare vs. tip performance
```sql
SELECT
  TO_CHAR(month, 'YYYY-MM') AS month,
  ROUND(avg_fare, 2) AS avg_fare_usd,
  ROUND(avg_tip, 2) AS avg_tip_usd,
  ROUND((avg_tip / avg_fare) * 100, 2) AS tip_pct
FROM gold_taxi_summary_2024
ORDER BY month;
```

### Q3: Passenger trends (from Silver layer)
```sql
SELECT
  DATE_TRUNC('month', pickup_datetime) AS month,
  AVG(passenger_count) AS avg_passengers_per_trip
FROM silver_taxi_data_2024
GROUP BY 1
ORDER BY 1;
```

### Q4: Trip duration distribution
```sql
SELECT
  DATE_TRUNC('month', tpep_pickup_datetime) AS month,
  ROUND(AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60), 2) AS avg_duration_min
FROM raw_taxi_data_2024
GROUP BY 1
ORDER BY 1;
```

### Q5: Most active pickup zones (if location data is joined)
```sql
SELECT
  pu_location_id,
  COUNT(*) AS trip_count
FROM silver_taxi_data_2024
GROUP BY pu_location_id
ORDER BY trip_count DESC
LIMIT 10;
```

---

## Key Benefits

| Feature | Advantage |
|----------|------------|
| **SQL-based transformations** | Transparent, version-controlled, and optimized |
| **Incremental metadata tracking** | Efficient monthly updates |
| **Idempotent design** | Safe to re-run anytime |
| **In-memory ingestion** | Faster loads without temporary CSVs |
| **Modular architecture** | Easy to extend (e.g., add dbt or Airflow) |

---

