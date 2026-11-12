-- Phase 2: MySQL table for validated (and corrected) flight rows
-- Same 17 data columns as staging, plus:
--   staging_id          link back to stg_flight_prices.id (traceability)
--   total_fare_original the CSV total before correction (audit trail)
--   is_valid            1 = passed every rule, 0 = has at least one issue
--   dq_issues           problems found, e.g. "missing:airline;"
--   dq_corrections      fixes applied, e.g. "total_fare_corrected;"

CREATE TABLE IF NOT EXISTS flight_prices_validated (
    staging_id             INT PRIMARY KEY,
    airline                VARCHAR(100),
    source_code            VARCHAR(10),
    source_name            VARCHAR(150),
    destination_code       VARCHAR(10),
    destination_name       VARCHAR(150),
    departure_datetime     DATETIME,
    arrival_datetime       DATETIME,
    duration_hrs           DECIMAL(6,2),
    stopovers              VARCHAR(20),
    aircraft_type          VARCHAR(50),
    travel_class           VARCHAR(20),
    booking_source         VARCHAR(30),
    base_fare_bdt          DECIMAL(12,2),
    tax_surcharge_bdt      DECIMAL(12,2),
    total_fare_bdt         DECIMAL(12,2),
    seasonality            VARCHAR(30),
    days_before_departure  INT,
    total_fare_original    DECIMAL(12,2),
    is_valid               TINYINT(1) NOT NULL,
    dq_issues              VARCHAR(500) NOT NULL DEFAULT '',
    dq_corrections         VARCHAR(200) NOT NULL DEFAULT '',
    validated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;