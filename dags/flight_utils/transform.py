"""Phase 3 helpers: turn validated rows into an analysis-ready (enriched) table.

Functions:
    read_valid_rows: read only the rows that passed validation (is_valid = 1).
    enrich: add the columns the KPIs need (route, season_type, departure_month).
    build_kpis: read -> enrich -> compute every KPI, as plain lists (for XCom).

Note: the rule "Total Fare = Base Fare + Tax & Surcharge" is applied once,
in Phase 2 (validation.fix_total_fare), so it is not repeated here.
"""
import logging

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_utils.config import MYSQL_CONN_ID, PEAK_SEASONS, VALIDATED_TABLE
from flight_utils.ingestion import FARE_COLUMNS
from flight_utils.kpis import compute_all_kpis

log = logging.getLogger(__name__)


def read_valid_rows():
    """Read the rows that passed every validation rule.

    Returns:
        pandas.DataFrame: All columns of the validated table, valid rows only.
    """
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = pd.read_sql(
        f"SELECT * FROM {VALIDATED_TABLE} WHERE is_valid = 1",
        hook.get_sqlalchemy_engine(),
    )
    log.info("Read %d valid rows from %s", len(df), VALIDATED_TABLE)
    return df


def enrich(df):
    """Add the columns needed for the KPIs. The input is not changed.

    New columns:
        route            "<source>-<destination>", e.g. "DAC-DXB"
        season_type      "Peak" if seasonality is in PEAK_SEASONS, else "Non-peak"
        departure_month  "YYYY-MM" of the departure date

    Fare columns are converted from MySQL DECIMAL to float for calculations.

    Args:
        df (pandas.DataFrame): Valid rows (see ``read_valid_rows``).

    Returns:
        pandas.DataFrame: A copy of ``df`` with the new columns.
    """
    out = df.copy()
    out[FARE_COLUMNS] = out[FARE_COLUMNS].astype(float)
    out["route"] = out["source_code"] + "-" + out["destination_code"]
    out["season_type"] = out["seasonality"].isin(PEAK_SEASONS).map(
        {True: "Peak", False: "Non-peak"}
    )
    out["departure_month"] = pd.to_datetime(out["departure_datetime"]).dt.strftime("%Y-%m")
    log.info(
        "Enriched %d rows: %d routes, season types %s",
        len(out), out["route"].nunique(), out["season_type"].value_counts().to_dict(),
    )
    return out


def build_kpis():
    """Compute every KPI from the valid rows in MySQL.

    Returns:
        dict: ``{kpi_table_name: list of row dicts}``. Plain Python lists and
        dicts, so Airflow can pass the (small) KPI tables to the next task
        via XCom.
    """
    results = compute_all_kpis(enrich(read_valid_rows()))
    return {name: table.to_dict(orient="records") for name, table in results.items()}