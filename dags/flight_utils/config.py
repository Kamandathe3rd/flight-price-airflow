"""Central settings for the flight price pipeline.

Every path, connection ID, table name and column name lives here ONCE.
Other files import from here, so a change is made in one place only.
"""

# Airflow connection IDs (defined in docker-compose.yaml as AIRFLOW_CONN_*)
MYSQL_CONN_ID = "mysql_staging"
POSTGRES_CONN_ID = "postgres_analytics"

# Paths inside the Airflow container (mounted from the project folders)
CSV_PATH = "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv"
SQL_DIR = "/opt/airflow/sql"

# Table names
STAGING_TABLE = "stg_flight_prices"

# CSV header -> MySQL column name (same order as the CSV)
COLUMN_MAP = {
    "Airline": "airline",
    "Source": "source_code",
    "Source Name": "source_name",
    "Destination": "destination_code",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "travel_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}

# Columns the assignment says must exist (checked in Phase 2)
REQUIRED_CSV_COLUMNS = [
    "Airline",
    "Source",
    "Destination",
    "Base Fare (BDT)",
    "Tax & Surcharge (BDT)",
    "Total Fare (BDT)",
]