-- TEST ONLY: plant one bad row per validation rule in MySQL staging.
-- Run AFTER a full DAG run (staging ids 1-57000), then in the Airflow UI
-- Clear "validate_data" with Downstream. Expected: 7 invalid rows,
-- 1 calculated total, quality gate passes with a WARNING alert.
-- Undo: trigger the full DAG (the CSV reload removes these rows).

-- Copy rows 1-8 to new ids 57001-57008
INSERT INTO stg_flight_prices (airline, source_code, source_name, destination_code, destination_name, departure_datetime, arrival_datetime, duration_hrs, stopovers, aircraft_type, travel_class, booking_source, base_fare_bdt, tax_surcharge_bdt, total_fare_bdt, seasonality, days_before_departure)
SELECT airline, source_code, source_name, destination_code, destination_name, departure_datetime, arrival_datetime, duration_hrs, stopovers, aircraft_type, travel_class, booking_source, base_fare_bdt, tax_surcharge_bdt, total_fare_bdt, seasonality, days_before_departure
FROM stg_flight_prices WHERE id <= 8;

-- Break one thing in each copy
UPDATE stg_flight_prices SET airline = '   ' WHERE id = 57001;                       -- missing value
UPDATE stg_flight_prices SET base_fare_bdt = -500 WHERE id = 57002;                  -- negative fare
UPDATE stg_flight_prices SET source_name = 'Dhaka Airprt' WHERE id = 57003;          -- invalid city name
UPDATE stg_flight_prices SET destination_code = 'XYZ' WHERE id = 57004;              -- unknown airport code
UPDATE stg_flight_prices SET destination_code = source_code, destination_name = source_name WHERE id = 57005;  -- same city
UPDATE stg_flight_prices SET travel_class = 'Premium' WHERE id = 57006;              -- bad category
UPDATE stg_flight_prices SET arrival_datetime = departure_datetime - INTERVAL 1 HOUR WHERE id = 57007;  -- bad times
UPDATE stg_flight_prices SET total_fare_bdt = NULL WHERE id = 57008;                 -- total to calculate

SELECT id, airline, source_code, destination_code, travel_class, base_fare_bdt, total_fare_bdt
FROM stg_flight_prices WHERE id > 57000;