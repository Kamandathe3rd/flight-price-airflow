"""Phase 2 helpers: data-quality rules for the flight price data.

Every rule is a small function that takes the DataFrame and returns
``{label: mask}``, where ``mask`` is True for the rows that break the rule.
``validate_dataframe`` runs all rules and adds three columns to each row:

    dq_issues       problems that make the row invalid, e.g. "missing:airline;"
    dq_corrections  fixes applied to the row, e.g. "total_fare_corrected;"
    is_valid        True when dq_issues is empty

Rows are flagged, never deleted, so nothing is lost and every decision is
visible in the data.

Database step:
    validate_staging reads stg_flight_prices, runs validate_dataframe and
    writes the result to flight_prices_validated (save_validated_rows).
"""
import logging
import os

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_utils.config import (
    ALLOWED_VALUES,
    COLUMN_MAP,
    MAX_INVALID_SHARE,
    MYSQL_CONN_ID,
    REQUIRED_CSV_COLUMNS,
    SQL_DIR,
    STAGING_TABLE,
    TOTAL_FARE_TOLERANCE_BDT,
    VALID_AIRPORTS,
    VALIDATED_TABLE,
)
from flight_utils.ingestion import FARE_COLUMNS

log = logging.getLogger(__name__)

# The assignment's required columns, using the MySQL names
REQUIRED_COLUMNS = [COLUMN_MAP[c] for c in REQUIRED_CSV_COLUMNS]

NUMERIC_COLUMNS = FARE_COLUMNS + ["duration_hrs", "days_before_departure"]
DATETIME_COLUMNS = ["departure_datetime", "arrival_datetime"]

# Columns written to the validated table (same order as 02_mysql_validated.sql)
VALIDATED_COLUMNS = (
    ["staging_id"]
    + list(COLUMN_MAP.values())
    + ["total_fare_original", "is_valid", "dq_issues", "dq_corrections"]
)

# Labels that describe fixes, not problems (they do not make a row invalid)
CORRECTION_LABELS = ["total_fare_calculated", "total_fare_corrected"]


def check_required_columns(df):
    """Stop the pipeline if any column required by the assignment is missing.

    Args:
        df (pandas.DataFrame): Rows read from the staging table.

    Raises:
        ValueError: If one or more required columns are not in ``df``.
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Required columns missing: {missing}")
    log.info("All %d required columns are present", len(REQUIRED_COLUMNS))


def find_missing_values(df):
    """Find empty cells: NULL/NaN, or text that is blank after trimming spaces.

    Args:
        df (pandas.DataFrame): Rows to check.

    Returns:
        dict: ``{"missing:<column>": mask}`` for every mapped column.
    """
    def is_blank(value):
        return isinstance(value, str) and value.strip() == ""

    return {
        f"missing:{col}": df[col].isna() | df[col].map(is_blank)
        for col in COLUMN_MAP.values()
    }


def coerce_types(df):
    """Convert numbers and dates to real types, in place, and find bad values.

    A value is a bad type if it was present but could not be converted
    (e.g. "abc" in a fare column), or if days_before_departure is not whole.

    Args:
        df (pandas.DataFrame): Rows to convert. Changed in place.

    Returns:
        dict: ``{"bad_type:<column>": mask}``.
    """
    flags = {}
    for col in NUMERIC_COLUMNS:
        converted = pd.to_numeric(df[col], errors="coerce")
        flags[f"bad_type:{col}"] = df[col].notna() & converted.isna()
        df[col] = converted
    for col in DATETIME_COLUMNS:
        converted = pd.to_datetime(df[col], errors="coerce")
        flags[f"bad_type:{col}"] = df[col].notna() & converted.isna()
        df[col] = converted
    days = df["days_before_departure"]
    flags["bad_type:days_before_departure"] |= days.notna() & (days % 1 != 0)
    return flags


def fix_total_fare(df):
    """Apply the rule Total Fare = Base Fare + Tax & Surcharge, in place.

    The CSV total is kept in ``total_fare_original``. Then:
    - total missing, parts present         -> calculate it
    - total differs by > tolerance (BDT)   -> replace it

    Args:
        df (pandas.DataFrame): Rows with numeric fare columns. Changed in place.

    Returns:
        dict: ``{"total_fare_calculated": mask, "total_fare_corrected": mask}``.
    """
    expected = (df["base_fare_bdt"] + df["tax_surcharge_bdt"]).round(2)
    has_parts = df["base_fare_bdt"].notna() & df["tax_surcharge_bdt"].notna()
    total = df["total_fare_bdt"]

    calculated = has_parts & total.isna()
    corrected = has_parts & total.notna() & (total - expected).abs().gt(TOTAL_FARE_TOLERANCE_BDT)

    df["total_fare_original"] = total
    df.loc[calculated | corrected, "total_fare_bdt"] = expected
    return {"total_fare_calculated": calculated, "total_fare_corrected": corrected}


def find_bad_categories(df):
    """Find categorical values that are present but not in ``ALLOWED_VALUES``.

    Args:
        df (pandas.DataFrame): Rows to check.

    Returns:
        dict: ``{"bad_value:<column>": mask}``.
    """
    return {
        f"bad_value:{col}": df[col].notna() & ~df[col].isin(allowed)
        for col, allowed in ALLOWED_VALUES.items()
    }


def find_negative_fares(df):
    """Find fares below zero (a ticket price or tax cannot be negative).

    Args:
        df (pandas.DataFrame): Rows with numeric fare columns.

    Returns:
        dict: ``{"negative_fare:<column>": mask}``.
    """
    return {f"negative_fare:{col}": df[col].lt(0) for col in FARE_COLUMNS}


def find_invalid_airports(df):
    """Find unknown airport codes, wrong airport names and same-city trips.

    Args:
        df (pandas.DataFrame): Rows to check.

    Returns:
        dict: masks labelled ``invalid_airport:<column>`` and
        ``same_source_destination``.
    """
    flags = {}
    for side in ["source", "destination"]:
        code, name = df[f"{side}_code"], df[f"{side}_name"]
        expected_name = code.map(VALID_AIRPORTS)
        flags[f"invalid_airport:{side}_code"] = code.notna() & expected_name.isna()
        flags[f"invalid_airport:{side}_name"] = (
            expected_name.notna() & name.notna() & (name != expected_name)
        )
    flags["same_source_destination"] = (
        df["source_code"].notna() & (df["source_code"] == df["destination_code"])
    )
    return flags


def find_bad_times(df):
    """Find flights that arrive at or before their departure time.

    Args:
        df (pandas.DataFrame): Rows with datetime columns.

    Returns:
        dict: ``{"bad_times": mask}``.
    """
    return {"bad_times": df["arrival_datetime"] <= df["departure_datetime"]}


def _add_label(labels, mask, label):
    """Append ``label;`` to the text of every row where ``mask`` is True.

    Args:
        labels (pandas.Series): Current label text per row.
        mask (pandas.Series): True for the rows that get the label.
        label (str): The label to add.

    Returns:
        pandas.Series: Updated label text.
    """
    return labels.where(~mask.fillna(False).astype(bool), labels + label + ";")


def validate_dataframe(df):
    """Run every rule, apply corrections and label each row.

    Args:
        df (pandas.DataFrame): Rows read from the staging table.

    Returns:
        tuple: ``(validated_df, summary)`` where ``summary`` is a dict with
        ``rows``, ``valid_rows``, ``invalid_rows``, ``invalid_share`` and
        ``counts`` (number of rows per label, only labels that occurred).

    Raises:
        ValueError: If a required column is missing (see
        ``check_required_columns``).
    """
    check_required_columns(df)
    df = df.copy()

    missing = find_missing_values(df)      # on the raw values
    bad_types = coerce_types(df)           # converts df in place
    fixes = fix_total_fare(df)             # fills / corrects totals
    # A total we could calculate is no longer missing
    missing["missing:total_fare_bdt"] &= ~fixes["total_fare_calculated"]

    issue_flags = {**missing, **bad_types}
    for rule in (find_bad_categories, find_negative_fares, find_invalid_airports, find_bad_times):
        issue_flags.update(rule(df))

    issues = pd.Series("", index=df.index)
    for label, mask in issue_flags.items():
        issues = _add_label(issues, mask, label)
    corrections = pd.Series("", index=df.index)
    for label, mask in fixes.items():
        corrections = _add_label(corrections, mask, label)

    df["dq_issues"] = issues
    df["dq_corrections"] = corrections
    df["is_valid"] = issues.eq("")

    counts = {
        label: int(mask.fillna(False).sum())
        for label, mask in {**issue_flags, **fixes}.items()
        if mask.fillna(False).any()
    }
    summary = {
        "rows": len(df),
        "valid_rows": int(df["is_valid"].sum()),
        "invalid_rows": int((~df["is_valid"]).sum()),
        "invalid_share": round(float((~df["is_valid"]).mean()), 4) if len(df) else 0.0,
        "counts": counts,
    }
    log.info("Validation summary: %s", summary)
    return df, summary



# ---------------------------------------------------------------------------
# Database step: staging table -> validated table
# ---------------------------------------------------------------------------

def save_validated_rows(df):
    """Replace the contents of the validated table with ``df``.

    Creates the table if needed (sql/02_mysql_validated.sql), empties it,
    then inserts every row in batches of 5,000 (safe to re-run).

    Args:
        df (pandas.DataFrame): Output of ``validate_dataframe``.

    Returns:
        int: Number of rows written.
    """
    out = df.rename(columns={"id": "staging_id"})[VALIDATED_COLUMNS].copy()
    out["is_valid"] = out["is_valid"].astype(int)
    # MySQL needs None (NULL) instead of pandas' NaN/NaT for empty cells
    rows = out.astype(object).where(out.notna(), None).values.tolist()

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    with open(os.path.join(SQL_DIR, "02_mysql_validated.sql"), encoding="utf-8") as f:
        hook.run(f.read())
    hook.run(f"TRUNCATE TABLE {VALIDATED_TABLE}")
    hook.insert_rows(
        table=VALIDATED_TABLE,
        rows=rows,
        target_fields=VALIDATED_COLUMNS,
        commit_every=5000,
        executemany=True,
    )
    log.info("Wrote %d rows to %s", len(rows), VALIDATED_TABLE)
    return len(rows)


def validate_staging():
    """Validate every staging row and save the result in MySQL.

    Returns:
        dict: The validation summary (see ``validate_dataframe``).
    """
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = pd.read_sql(f"SELECT * FROM {STAGING_TABLE}", hook.get_sqlalchemy_engine())
    validated, summary = validate_dataframe(df)
    save_validated_rows(validated)
    return summary



def check_quality_gate(summary):
    """Decide whether the data is good enough for the KPI phase.

    - No invalid rows                   -> pass quietly
    - Some, but at most MAX_INVALID_SHARE -> pass with a WARNING alert
    - More than MAX_INVALID_SHARE       -> fail (stops the pipeline)

    Args:
        summary (dict): Output of ``validate_dataframe`` / ``validate_staging``.

    Raises:
        ValueError: If the share of invalid rows is above ``MAX_INVALID_SHARE``.
    """
    share = summary["invalid_share"]
    issues = {k: v for k, v in summary["counts"].items() if k not in CORRECTION_LABELS}
    if share > MAX_INVALID_SHARE:
        raise ValueError(
            f"Quality gate FAILED: {summary['invalid_rows']} of {summary['rows']} rows "
            f"invalid ({share:.2%} > {MAX_INVALID_SHARE:.0%}). Issues: {issues}"
        )
    if summary["invalid_rows"]:
        log.warning(
            "⚠️ ALERT: DATA QUALITY: %d invalid rows (%.2f%%) will be excluded from KPIs. Issues: %s",
            summary["invalid_rows"], share * 100, issues,
        )
    log.info(
        "Quality gate passed: %d of %d rows valid", summary["valid_rows"], summary["rows"]
    )