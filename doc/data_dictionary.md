# Data Dictionary — `raw_taxi_data_2024`

| **Column Name** | **Data Type** | **Description** |
|------------------|---------------|------------------|
| `vendor_id` | TEXT | Code indicating the provider associated with the trip record (e.g., 1 = Creative Mobile, 2 = VeriFone). |
| `tpep_pickup_datetime` | TIMESTAMP | Date and time when the meter was engaged (start of the trip). |
| `tpep_dropoff_datetime` | TIMESTAMP | Date and time when the meter was disengaged (end of the trip). |
| `passenger_count` | INTEGER | Number of passengers in the vehicle. Reported by the driver. |
| `trip_distance` | FLOAT | Distance of the trip in miles, as measured by the taximeter. |
| `ratecode_id` | INTEGER | Final rate code in effect at the end of the trip (1 = Standard rate, 2 = JFK, 3 = Newark, etc.). |
| `store_and_fwd_flag` | TEXT | Indicates whether the trip record was held in vehicle memory before sending to the vendor (Y = store and forward trip, N = not store and forward). |
| `pu_location_id` | INTEGER | TLC Taxi Zone ID where the passenger was picked up. |
| `do_location_id` | INTEGER | TLC Taxi Zone ID where the passenger was dropped off. |
| `payment_type` | INTEGER | Numeric code signifying payment method (1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute, 5 = Unknown, 6 = Voided trip). |
| `fare_amount` | FLOAT | Fare amount in USD charged by the meter (excluding extra fees). |
| `extra` | FLOAT | Miscellaneous extras and surcharges (e.g., rush hour or overnight fees). |
| `mta_tax` | FLOAT | MTA tax applicable (usually $0.50 per trip). |
| `tip_amount` | FLOAT | Tip amount in USD — automatically populated for credit card payments. |
| `tolls_amount` | FLOAT | Total amount of all tolls paid during the trip. |
| `improvement_surcharge` | FLOAT | A fixed $0.30 fee per trip to fund improvements in taxi services. |
| `total_amount` | FLOAT | Total charged amount to the passenger (fare + extras + tolls + tips + surcharges). |
| `congestion_surcharge` | FLOAT | Congestion fee applied for trips within Manhattan south of 96th Street. |
| `airport_fee` | FLOAT | Additional fee for trips originating/ending at an airport (e.g., JFK or LaGuardia). |

---