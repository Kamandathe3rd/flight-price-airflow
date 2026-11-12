-- KPI cross-check: the same 4 KPIs computed in plain SQL (MySQL), straight
-- from the validated table. The numbers must match the compute_kpis task log.
-- Run: Get-Content sql\kpi_crosscheck.sql | docker compose exec -T mysql sh -c 'mysql -t -ustaging_user -p$MYSQL_PASSWORD flight_staging'

-- KPI 1: Average Fare by Airline (top 5)
SELECT airline, ROUND(AVG(total_fare_bdt), 2) AS avg_total_fare_bdt
FROM flight_prices_validated
WHERE is_valid = 1
GROUP BY airline
ORDER BY avg_total_fare_bdt DESC
LIMIT 5;

-- KPI 2: Seasonal Fare Variation (peak = Eid, Hajj, Winter Holidays)
SELECT CASE WHEN seasonality IN ('Eid', 'Hajj', 'Winter Holidays') THEN 'Peak' ELSE 'Non-peak' END AS season_type,
       COUNT(*) AS booking_count,
       ROUND(AVG(total_fare_bdt), 2) AS avg_total_fare_bdt
FROM flight_prices_validated
WHERE is_valid = 1
GROUP BY season_type
ORDER BY season_type DESC;

-- KPI 3: Booking Count by Airline (top 5)
SELECT airline, COUNT(*) AS booking_count
FROM flight_prices_validated
WHERE is_valid = 1
GROUP BY airline
ORDER BY booking_count DESC, airline
LIMIT 5;

-- KPI 4: Most Popular Routes (top 5)
SELECT CONCAT(source_code, '-', destination_code) AS route, COUNT(*) AS booking_count
FROM flight_prices_validated
WHERE is_valid = 1
GROUP BY route
ORDER BY booking_count DESC, route
LIMIT 5;