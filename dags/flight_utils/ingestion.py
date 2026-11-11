"""Phase 1 helpers: create the MySQL staging table, load the CSV, verify the load.

Functions:
    create_staging_table: run sql/01_mysql_staging.sql.
    read_csv_for_staging: read the CSV and rename columns to the MySQL names.
    load_csv_to_mysql: empty the staging table and batch-insert every row.
    verify_ingestion: compare CSV and MySQL (columns, row counts, fare totals).
"""
import logging
import os

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_utils.config import (
    COLUMN_MAP,
    CSV_PATH,
    MYSQL_CONN_ID,
    SQL_DIR,
    STAGING_TABLE,
)

log = logging.getLogger(__name__)

# Columns stored as DECIMAL(..., 2) in MySQL: round them the same way here
DECIMAL_COLUMNS = ["duration_hrs", "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]

# Fare columns whose totals must match between CSV and MySQL
FARE_COLUMNS = ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]

# Largest difference (in BDT) allowed between CSV and MySQL totals
SUM_TOLERANCE_BDT = 0.01


def create_staging_table():
    """Create the MySQL staging table by running sql/01_mysql_staging.sql.

    Safe to run many times, because the SQL uses CREATE TABLE IF NOT EXISTS.
    """
    with open(os.path.join(SQL_DIR, "01_mysql_staging.sql"), encoding="utf-8") as f:
        sql = f.read()
    MySqlHook(mysql_conn_id=MYSQL_CONN_ID).run(sql)
    log.info("Table %s is ready", STAGING_TABLE)


def read_csv_for_staging(path=CSV_PATH):
    """Read the CSV and rename its columns to the MySQL names.

    Args:
        path (str): Location of the CSV file. Defaults to ``CSV_PATH``.

    Returns:
        pandas.DataFrame: The 17 mapped columns in table order, with the
        DECIMAL columns rounded to 2 places (exactly as MySQL stores them).
    """
    df = pd.read_csv(path, encoding="utf-8")
    df = df.rename(columns=COLUMN_MAP)[list(COLUMN_MAP.values())]
    df[DECIMAL_COLUMNS] = df[DECIMAL_COLUMNS].round(2)
    return df


def load_csv_to_mysql():
    """Empty the staging table, then insert every CSV row in batches of 5,000.

    The table is emptied first (TRUNCATE), so running the task again never
    creates duplicate rows (the load is idempotent).

    Returns:
        int: Number of rows inserted.
    """
    df = read_csv_for_staging()
    # MySQL needs None (NULL) instead of pandas' NaN for empty cells
    rows = df.astype(object).where(df.notna(), None).values.tolist()

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(f"TRUNCATE TABLE {STAGING_TABLE}")   # full refresh = safe to re-run
    hook.insert_rows(
        table=STAGING_TABLE,
        rows=rows,
        target_fields=list(df.columns),
        commit_every=5000,
        executemany=True,
    )
    log.info("Loaded %d rows into %s", len(rows), STAGING_TABLE)
    return len(rows)


def verify_ingestion(loaded_rows):
    """Compare the CSV with the MySQL staging table.

    Checks that every mapped column exists, that the row counts match
    (CSV = rows loaded = rows in MySQL) and that the fare totals match
    within ``SUM_TOLERANCE_BDT``.

    Args:
        loaded_rows (int): Row count returned by ``load_csv_to_mysql``.

    Raises:
        ValueError: If any check fails. The message lists every problem found.
    """
    df = read_csv_for_staging()
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    problems = []

    # 1. Every mapped column exists in the table
    table_cols = {
        row[0]
        for row in hook.get_records(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            parameters=(STAGING_TABLE,),
        )
    }
    missing = set(df.columns) - table_cols
    if missing:
        problems.append(f"Columns missing in MySQL: {sorted(missing)}")

    # 2. Row counts: CSV = rows the load task inserted = rows in MySQL
    mysql_rows = hook.get_first(f"SELECT COUNT(*) FROM {STAGING_TABLE}")[0]
    log.info("Rows -> CSV: %d, loaded: %d, MySQL: %d", len(df), loaded_rows, mysql_rows)
    if not len(df) == loaded_rows == mysql_rows:
        problems.append(f"Row count mismatch: CSV {len(df)}, loaded {loaded_rows}, MySQL {mysql_rows}")

    # 3. Fare totals match (small tolerance for float rounding)
    for col in FARE_COLUMNS:
        csv_sum = round(float(df[col].sum()), 2)
        mysql_sum = float(hook.get_first(f"SELECT COALESCE(SUM({col}), 0) FROM {STAGING_TABLE}")[0])
        log.info("%s -> CSV: %s, MySQL: %s", col, f"{csv_sum:,.2f}", f"{mysql_sum:,.2f}")
        if abs(csv_sum - mysql_sum) > SUM_TOLERANCE_BDT:
            problems.append(f"{col} total mismatch: CSV {csv_sum:,.2f}, MySQL {mysql_sum:,.2f}")

    if problems:
        raise ValueError("Ingestion check FAILED:\n" + "\n".join(problems))
    log.info("Ingestion check passed: all rows and fare totals match")