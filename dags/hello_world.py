"""Hello world DAG: a tiny practice pipeline to learn how DAGs work.

Tasks:
    say_hello and count_csv_rows run first (in parallel), then report
    prints the row count it receives from count_csv_rows via XCom.
"""
from datetime import datetime

from airflow.sdk import dag, task

from flight_utils.config import CSV_PATH


@dag(
    dag_id="hello_world",
    start_date=datetime(2026, 1, 1),
    schedule=None,        # only runs when you click Trigger
    catchup=False,
    tags=["practice"],
)
def hello_world():
    """Build the practice DAG: say_hello, count_csv_rows -> report."""

    @task
    def say_hello():
        """Print a greeting to prove tasks run."""
        print("Hello from Airflow!")

    @task
    def count_csv_rows():
        """Count the data rows in the CSV (header excluded).

        Returns:
            int: Number of data rows.
        """
        with open(CSV_PATH, encoding="utf-8") as f:
            rows = sum(1 for _ in f) - 1   # minus the header line
        print(f"The CSV has {rows} rows")
        return rows

    @task
    def report(rows):
        """Print the row count received from count_csv_rows.

        Args:
            rows (int): Row count passed in via XCom.
        """
        print(f"Ready for Phase 1: {rows} flights to load")

    say_hello() >> report(count_csv_rows())


hello_world()