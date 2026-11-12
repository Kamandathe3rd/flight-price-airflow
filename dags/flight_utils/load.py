"""Phase 4 helpers: load the enriched valid rows and the KPI tables into PostgreSQL.

Everything happens inside ONE PostgreSQL transaction:
    1. create the tables if needed (sql/03_postgres_analytics.sql)
    2. TRUNCATE the fact table and the KPI tables
    3. COPY all enriched valid rows into the fact table (bulk load, fast)
    4. INSERT the KPI rows (from the compute_kpis task)
    5. COMMIT, so the new data appears all at once

If any step fails, the transaction is rolled back and PostgreSQL keeps the
previous complete load (in PostgreSQL even TRUNCATE can be rolled back).

verify_postgres_load then checks that PostgreSQL matches MySQL.
"""
import io
import logging
import os
import time

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from flight_utils.config import (
    FACT_TABLE,
    MYSQL_CONN_ID,
    POSTGRES_CONN_ID,
    SQL_DIR,
    VALIDATED_TABLE,
)
from flight_utils.transform import enrich, read_valid_rows

log = logging.getLogger(__name__)

# Columns copied into the fact table (same names as 03_postgres_analytics.sql)
FACT_COLUMNS = [
    "staging_id", "airline", "source_code", "source_name", "destination_code",
    "destination_name", "route", "departure_datetime", "arrival_datetime",
    "departure_month", "duration_hrs", "stopovers", "aircraft_type",
    "travel_class", "booking_source", "base_fare_bdt", "tax_surcharge_bdt",
    "total_fare_bdt", "total_fare_original", "seasonality", "season_type",
    "days_before_departure", "dq_corrections",
]


def _to_csv_buffer(df):
    """Write the fact columns to an in-memory CSV file for COPY.

    Empty values are written as \\N, which COPY reads as NULL (so an empty
    text such as dq_corrections = "" stays an empty text, not NULL).

    Args:
        df (pandas.DataFrame): Enriched valid rows.

    Returns:
        io.StringIO: CSV data, positioned at the start.
    """
    buffer = io.StringIO()
    df[FACT_COLUMNS].to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)
    return buffer


def load_to_postgres(kpi_tables, fail_after_copy=False):
    """Load the fact rows and the KPI tables in one all-or-nothing transaction.

    Args:
        kpi_tables (dict): ``{kpi_table_name: list of row dicts}`` from the
            compute_kpis task.
        fail_after_copy (bool): TEST ONLY. Raise an error after the COPY and
            before COMMIT, to prove that a failed load is rolled back.

    Returns:
        dict: Rows loaded per table, plus ``seconds`` taken.

    Raises:
        RuntimeError: If ``fail_after_copy`` is True (test only).
        Exception: Any database error, after the transaction is rolled back.
    """
    start = time.perf_counter()
    df = enrich(read_valid_rows())
    with open(os.path.join(SQL_DIR, "03_postgres_analytics.sql"), encoding="utf-8") as f:
        create_sql = f.read()

    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            cur.execute(f"TRUNCATE {FACT_TABLE}, {', '.join(kpi_tables)}")

            cur.copy_expert(
                f"COPY {FACT_TABLE} ({', '.join(FACT_COLUMNS)}) "
                "FROM STDIN WITH (FORMAT csv, NULL '\\N')",
                _to_csv_buffer(df),
            )
            log.info("COPY sent %d rows to %s", len(df), FACT_TABLE)

            if fail_after_copy:
                raise RuntimeError("TEST: simulated failure after COPY, before COMMIT")

            for table, rows in kpi_tables.items():
                columns = list(rows[0])
                execute_values(
                    cur,
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s",
                    [tuple(row[c] for c in columns) for row in rows],
                )
                log.info("Inserted %d rows into %s", len(rows), table)
        conn.commit()
    except Exception:
        conn.rollback()
        log.error("Load FAILED: transaction rolled back, PostgreSQL keeps the previous load")
        raise
    finally:
        conn.close()

    result = {FACT_TABLE: len(df), **{t: len(r) for t, r in kpi_tables.items()}}
    result["seconds"] = round(time.perf_counter() - start, 2)
    log.info("Load committed: %s", result)
    return result


def verify_postgres_load(load_result):
    """Check that PostgreSQL holds exactly what MySQL says it should.

    Compares the valid rows in MySQL with the fact table in PostgreSQL
    (row count, smallest/largest staging_id, and the three fare totals),
    and checks every KPI table has the number of rows that was loaded.

    Args:
        load_result (dict): Return value of ``load_to_postgres``.

    Raises:
        ValueError: If anything differs. The message lists every difference.
    """
    totals_sql = (
        "SELECT COUNT(*), MIN(staging_id), MAX(staging_id), "
        "SUM(base_fare_bdt), SUM(tax_surcharge_bdt), SUM(total_fare_bdt) FROM {table}"
    )
    labels = ["rows", "min staging_id", "max staging_id",
              "sum base_fare_bdt", "sum tax_surcharge_bdt", "sum total_fare_bdt"]

    mysql = MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_first(
        totals_sql.format(table=VALIDATED_TABLE) + " WHERE is_valid = 1"
    )
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    postgres = pg_hook.get_first(totals_sql.format(table=FACT_TABLE))

    problems = []
    for label, my_value, pg_value in zip(labels, mysql, postgres):
        log.info("%s -> MySQL: %s, PostgreSQL: %s", label, my_value, pg_value)
        if my_value != pg_value:
            problems.append(f"{label}: MySQL {my_value}, PostgreSQL {pg_value}")

    for table, expected in load_result.items():
        if table in (FACT_TABLE, "seconds"):
            continue
        actual = pg_hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        log.info("%s -> expected %d rows, found %d", table, expected, actual)
        if actual != expected:
            problems.append(f"{table}: expected {expected} rows, found {actual}")

    if problems:
        raise ValueError("PostgreSQL load check FAILED:\n" + "\n".join(problems))
    log.info("PostgreSQL load check passed: PostgreSQL matches MySQL")