-- Gold Layer Aggregation for NYC Taxi 2024 Data

-- Purpose:
-- Aggregates cleaned (Silver-layer) data to produce monthly
-- business metrics: total trips, average fare, tips, totals, and revenue.


-- Drop the Gold table if it already exists (idempotent)
DROP TABLE IF EXISTS gold_taxi_summary_2024;

-- Create a new monthly summary table from the Silver layer
CREATE TABLE gold_taxi_summary_2024 AS
SELECT
    -- Truncate pickup_datetime to the month level (first day of month)
    DATE_TRUNC('month', pickup_datetime)::DATE AS month,

    -- Count total trips within each month
    COUNT(*) AS total_trips,

    -- Average fare amount per trip (rounded to 2 decimals)
    ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,

    -- Average tip amount per trip (rounded to 2 decimals)
    ROUND(AVG(tip_amount)::numeric, 2) AS avg_tip,

    -- Average total charged per trip (fare + extras + tip)
    ROUND(AVG(total_amount)::numeric, 2) AS avg_total,

    -- Total revenue for the month (rounded to 2 decimals)
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue

FROM silver_taxi_data_2024
GROUP BY 1
ORDER BY 1;
