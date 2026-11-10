"""Hello world DAG: a tiny practice pipeline to learn how DAGs work."""
from datetime import datetime

from airflow.sdk import dag, task

CSV_PATH = "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv"


@dag(
    dag_id="hello_world",
    start_date=datetime(2026, 1, 1),
    schedule=None,        # only runs when you click Trigger
    catchup=False,
    tags=["practice"],
)
def hello_world():

    @task
    def say_hello():
        print("Hello from Airflow!")

    @task
    def count_csv_rows():
        with open(CSV_PATH, encoding="utf-8") as f:
            rows = sum(1 for _ in f) - 1   # minus the header line
        print(f"The CSV has {rows} rows")
        return rows

    @task
    def report(rows):
        print(f"Ready for Phase 1: {rows} flights to load")

    say_hello() >> report(count_csv_rows())


hello_world()