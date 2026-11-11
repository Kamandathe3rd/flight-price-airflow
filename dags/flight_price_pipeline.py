"""Flight price pipeline: CSV -> MySQL staging -> validation -> KPIs -> PostgreSQL.

This DAG is the pipeline's table of contents: it lists the tasks and their
order. The real work lives in the ``flight_utils`` package.

Phase 1 (this version):
    create_staging_table -> load_csv_to_mysql -> verify_ingestion

Alerts:
    Every task retries once (after 30 seconds). Retries, failures and the
    final DAG result are logged as ALERT blocks (see ``flight_utils/alerts.py``).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from flight_utils import alerts, ingestion

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
    """Build the flight price pipeline DAG (Phase 1: ingestion)."""

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

    loaded_rows = load_csv_to_mysql()
    create_staging_table() >> loaded_rows
    verify_ingestion(loaded_rows)


flight_price_pipeline()