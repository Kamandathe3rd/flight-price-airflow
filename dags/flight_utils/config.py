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


# ---------------------------------------------------------------------------
# Phase 2: validation rules and reference data
# ---------------------------------------------------------------------------

# Table that holds the validated (and corrected) rows
VALIDATED_TABLE = "flight_prices_validated"

# Reference list of valid airports: IATA code -> official airport name.
# A source/destination is valid only if its code is here AND its name matches.
VALID_AIRPORTS = {
    "BKK": "Suvarnabhumi Airport, Bangkok",
    "BZL": "Barisal Airport",
    "CCU": "Netaji Subhas Chandra Bose International Airport, Kolkata",
    "CGP": "Shah Amanat International Airport, Chittagong",
    "CXB": "Cox's Bazar Airport",
    "DAC": "Hazrat Shahjalal International Airport, Dhaka",
    "DEL": "Indira Gandhi International Airport, Delhi",
    "DOH": "Hamad International Airport, Doha",
    "DXB": "Dubai International Airport",
    "IST": "Istanbul Airport",
    "JED": "King Abdulaziz International Airport, Jeddah",
    "JFK": "John F. Kennedy International Airport, New York",
    "JSR": "Jessore Airport",
    "KUL": "Kuala Lumpur International Airport",
    "LHR": "London Heathrow Airport",
    "RJH": "Shah Makhdum Airport, Rajshahi",
    "SIN": "Singapore Changi Airport",
    "SPD": "Saidpur Airport",
    "YYZ": "Toronto Pearson International Airport",
    "ZYL": "Osmani International Airport, Sylhet",
}

# Allowed values for the categorical columns (anything else is flagged)
ALLOWED_VALUES = {
    "stopovers": ["Direct", "1 Stop", "2 Stops"],
    "travel_class": ["Economy", "Business", "First Class"],
    "booking_source": ["Direct Booking", "Online Website", "Travel Agency"],
    "seasonality": ["Regular", "Winter Holidays", "Hajj", "Eid"],
}

# Largest gap (BDT) allowed between Total Fare and Base Fare + Tax & Surcharge
TOTAL_FARE_TOLERANCE_BDT = 1.00

# Quality gate: alert if more than this share of rows is invalid
MAX_INVALID_SHARE = 0.05

# ---------------------------------------------------------------------------
# Phase 3: transformation and KPIs
# ---------------------------------------------------------------------------

# Seasons counted as "peak" for the seasonal fare KPI ("Regular" = non-peak)
PEAK_SEASONS = ["Eid", "Hajj", "Winter Holidays"]