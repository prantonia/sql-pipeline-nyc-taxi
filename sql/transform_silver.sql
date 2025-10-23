-- Silver Layer Transformation for NYC Taxi 2024 Data

-- Purpose:
-- This query cleans and standardizes data from the raw_taxi_data_2024 table.
-- It removes invalid, incomplete, or out-of-year records and
-- creates a clean, analytics-ready "Silver" table for downstream use.



-- Drop existing Silver table (if it exists)
-- This ensures the transformation is idempotent (safe to re-run)
DROP TABLE IF EXISTS silver_taxi_data_2024;


-- Create the new Silver table with cleaned data
CREATE TABLE silver_taxi_data_2024 AS
SELECT
    -- Keep only the most relevant fields for analytics
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
    -- Filter out invalid or missing values to ensure data quality
    vendor_id IS NOT NULL                 
    AND passenger_count > 0              
    AND trip_distance > 0                 
    AND fare_amount >= 0                  
    AND total_amount >= 0                 
    AND tpep_pickup_datetime IS NOT NULL  
    AND tpep_dropoff_datetime IS NOT NULL 

    -- Restrict data to only trips that occurred in 2024
    -- Some monthly TLC files may include data from late 2023 or early 2025
    AND EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024;
