"""Flight price pipeline: CSV -> MySQL staging -> validation -> KPIs -> PostgreSQL.

This DAG is the pipeline's table of contents: it lists the tasks and their
order. The real work lives in the ``flight_utils`` package.

Phase 1 (ingestion):
    create_staging_table -> load_csv_to_mysql -> verify_ingestion

Phase 2 (validation):
    validate_data -> quality_gate
    validate_data writes flight_prices_validated (is_valid, dq_issues,
    dq_corrections). quality_gate fails the run if more than 5% of rows
    are invalid, and logs a WARNING alert if any rows are invalid.

Phase 3 (transformation and KPIs):
    compute_kpis reads the valid rows, adds route / season_type /
    departure_month, and computes the 4 KPI tables: average fare by airline,
    seasonal fare variation (peak vs non-peak), bookings by airline and
    most popular routes. The tables are logged and passed on via XCom.

Alerts:
    Every task retries once (after 30 seconds), except quality_gate (a data
    problem does not fix itself). Retries, failures and the final DAG result
    are logged as ALERT blocks (see ``flight_utils/alerts.py``).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from flight_utils import alerts, ingestion, transform, validation

# Settings applied to every task in this DAG
DEFAULT_ARGS = {
    "owner": "david",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": alerts.task_failure_alert,
    "on_retry_callback": alerts.task_retry_alert,
}


@dag(
    dag_id="flight_price_pipeline",
    description="Bangladesh flight prices: CSV -> MySQL -> KPIs -> PostgreSQL",
    doc_md=__doc__,       # show this file's docstring in the UI (Docs)
    start_date=datetime(2026, 1, 1),
    schedule=None,        # run manually from the UI
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_success_callback=alerts.dag_success_alert,
    on_failure_callback=alerts.dag_failure_alert,
    tags=["flight_prices"],
)
def flight_price_pipeline():
    """Build the flight price pipeline DAG (Phases 1 and 3)."""

    # ----- Phase 1: ingestion ------------------------------------------------
    @task
    def create_staging_table():
        """Create the MySQL staging table if it does not exist yet."""
        ingestion.create_staging_table()

    @task
    def load_csv_to_mysql():
        """Load every CSV row into MySQL staging.

        Returns:
            int: Number of rows loaded (passed to the next task via XCom).
        """
        return ingestion.load_csv_to_mysql()

    @task
    def verify_ingestion(loaded_rows):
        """Check that MySQL holds exactly what the CSV holds.

        Args:
            loaded_rows (int): Row count returned by ``load_csv_to_mysql``.
        """
        ingestion.verify_ingestion(loaded_rows)

    # ----- Phase 2: validation -----------------------------------------------
    @task
    def validate_data():
        """Validate every staging row and write flight_prices_validated.

        Returns:
            dict: Validation summary (rows, valid/invalid counts, issue counts).
        """
        return validation.validate_staging()

    @task(retries=0)
    def quality_gate(summary):
        """Stop the pipeline if too many rows are invalid.

        Args:
            summary (dict): Validation summary from ``validate_data``.
        """
        validation.check_quality_gate(summary)

    # ----- Phase 3: transformation and KPIs ----------------------------------
    @task
    def compute_kpis():
        """Enrich the valid rows and compute the 4 KPI tables.

        Returns:
            dict: {kpi_table_name: list of row dicts} for Phase 4.
        """
        return transform.build_kpis()

    # ----- Order ---------------------------------------------------------------
    loaded_rows = load_csv_to_mysql()
    create_staging_table() >> loaded_rows
    ingestion_checked = verify_ingestion(loaded_rows)

    summary = validate_data()
    ingestion_checked >> summary
    gate = quality_gate(summary)

    kpi_tables = compute_kpis()
    gate >> kpi_tables


flight_price_pipeline()