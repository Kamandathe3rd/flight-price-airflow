-- Phase 4: PostgreSQL analytics tables (database flight_analytics)
-- fact_flight_prices: one row per VALID booking, corrected and enriched
-- kpi_*: the 4 KPI tables from Phase 3
-- Safe to run many times (CREATE ... IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS fact_flight_prices (
    staging_id             INTEGER PRIMARY KEY,   -- link back to MySQL staging
    airline                VARCHAR(100)  NOT NULL,
    source_code            VARCHAR(10)   NOT NULL,
    source_name            VARCHAR(150)  NOT NULL,
    destination_code       VARCHAR(10)   NOT NULL,
    destination_name       VARCHAR(150)  NOT NULL,
    route                  VARCHAR(25)   NOT NULL,
    departure_datetime     TIMESTAMP     NOT NULL,
    arrival_datetime       TIMESTAMP     NOT NULL,
    departure_month        CHAR(7)       NOT NULL,  -- 'YYYY-MM'
    duration_hrs           NUMERIC(6,2)  NOT NULL,
    stopovers              VARCHAR(20)   NOT NULL,
    aircraft_type          VARCHAR(50)   NOT NULL,
    travel_class           VARCHAR(20)   NOT NULL,
    booking_source         VARCHAR(30)   NOT NULL,
    base_fare_bdt          NUMERIC(12,2) NOT NULL CHECK (base_fare_bdt >= 0),
    tax_surcharge_bdt      NUMERIC(12,2) NOT NULL CHECK (tax_surcharge_bdt >= 0),
    total_fare_bdt         NUMERIC(12,2) NOT NULL CHECK (total_fare_bdt >= 0),
    total_fare_original    NUMERIC(12,2),
    seasonality            VARCHAR(30)   NOT NULL,
    season_type            VARCHAR(10)   NOT NULL CHECK (season_type IN ('Peak', 'Non-peak')),
    days_before_departure  INTEGER       NOT NULL,
    dq_corrections         VARCHAR(200)  NOT NULL DEFAULT '',
    loaded_at              TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for the most common analysis filters
CREATE INDEX IF NOT EXISTS idx_fact_airline     ON fact_flight_prices (airline);
CREATE INDEX IF NOT EXISTS idx_fact_route       ON fact_flight_prices (route);
CREATE INDEX IF NOT EXISTS idx_fact_season_type ON fact_flight_prices (season_type);

CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    airline                VARCHAR(100)  PRIMARY KEY,
    avg_total_fare_bdt     NUMERIC(12,2) NOT NULL,
    avg_base_fare_bdt      NUMERIC(12,2) NOT NULL,
    avg_tax_surcharge_bdt  NUMERIC(12,2) NOT NULL,
    fare_rank              INTEGER       NOT NULL,
    computed_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kpi_seasonal_fare_variation (
    season_type            VARCHAR(10)   NOT NULL,
    seasonality            VARCHAR(30)   NOT NULL,   -- 'All' = whole season type
    booking_count          INTEGER       NOT NULL,
    avg_total_fare_bdt     NUMERIC(12,2) NOT NULL,
    pct_vs_non_peak        NUMERIC(6,2)  NOT NULL,
    computed_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season_type, seasonality)
);

CREATE TABLE IF NOT EXISTS kpi_bookings_by_airline (
    airline                VARCHAR(100)  PRIMARY KEY,
    booking_count          INTEGER       NOT NULL,
    share_pct              NUMERIC(5,2)  NOT NULL,
    booking_rank           INTEGER       NOT NULL,
    computed_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    route                  VARCHAR(25)   PRIMARY KEY,
    route_rank             INTEGER       NOT NULL,
    source_code            VARCHAR(10)   NOT NULL,
    destination_code       VARCHAR(10)   NOT NULL,
    booking_count          INTEGER       NOT NULL,
    avg_total_fare_bdt     NUMERIC(12,2) NOT NULL,
    computed_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);