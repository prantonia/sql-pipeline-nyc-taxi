DROP TABLE IF EXISTS silver_taxi_data_2024;

CREATE TABLE silver_taxi_data_2024 AS
SELECT
    vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
FROM raw_taxi_data_2024
WHERE
    vendor_id IS NOT NULL
    AND passenger_count > 0
    AND trip_distance > 0
    AND fare_amount >= 0
    AND total_amount >= 0
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL;
