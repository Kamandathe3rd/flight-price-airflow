-- Phase 1: MySQL staging table for the raw flight price CSV
-- One column per CSV column (17), plus an id and a load timestamp.
-- Staging is deliberately permissive (NULLs allowed) so bad rows still land
-- and the validation task can find and report them.

CREATE TABLE IF NOT EXISTS stg_flight_prices (
    id                     INT AUTO_INCREMENT PRIMARY KEY,
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
    loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;