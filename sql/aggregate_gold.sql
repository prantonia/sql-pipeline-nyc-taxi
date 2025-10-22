DROP TABLE IF EXISTS gold_taxi_summary_2024;

CREATE TABLE gold_taxi_summary_2024 AS
SELECT
    DATE_TRUNC('month', pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
    ROUND(AVG(tip_amount)::numeric, 2) AS avg_tip,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_total,
    SUM(total_amount) AS total_revenue
FROM silver_taxi_data_2024
GROUP BY 1
ORDER BY 1;
