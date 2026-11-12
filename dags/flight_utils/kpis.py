"""Phase 3: KPI calculations.

Each KPI is a pure function: enriched DataFrame in, small KPI table out.
There is no database access here, so every KPI can be tested with any
DataFrame. ``compute_all_kpis`` runs them all.

KPI table name -> function:
    kpi_avg_fare_by_airline      avg_fare_by_airline
    kpi_seasonal_fare_variation  seasonal_fare_variation
    kpi_bookings_by_airline      booking_count_by_airline
    kpi_popular_routes           popular_routes
"""
import logging

import pandas as pd

log = logging.getLogger(__name__)


def avg_fare_by_airline(df):
    """Average Fare by Airline: mean fares per airline, most expensive first.

    Args:
        df (pandas.DataFrame): Enriched valid rows (see ``transform.enrich``).

    Returns:
        pandas.DataFrame: airline, avg_total_fare_bdt, avg_base_fare_bdt,
        avg_tax_surcharge_bdt, fare_rank (1 = highest average total fare;
        ties share a rank).
    """
    out = (
        df.groupby("airline")
        .agg(
            avg_total_fare_bdt=("total_fare_bdt", "mean"),
            avg_base_fare_bdt=("base_fare_bdt", "mean"),
            avg_tax_surcharge_bdt=("tax_surcharge_bdt", "mean"),
        )
        .round(2)
        .sort_values("avg_total_fare_bdt", ascending=False)
        .reset_index()
    )
    out["fare_rank"] = out["avg_total_fare_bdt"].rank(method="min", ascending=False).astype(int)
    return out


def seasonal_fare_variation(df):
    """Seasonal Fare Variation: average total fare in peak vs non-peak seasons.

    Gives one row per season type (seasonality = "All") and one per season.
    Each row is compared with the non-peak average:
    pct_vs_non_peak = (avg_total_fare_bdt / non-peak average - 1) * 100

    Args:
        df (pandas.DataFrame): Enriched valid rows (needs season_type).

    Returns:
        pandas.DataFrame: season_type, seasonality, booking_count,
        avg_total_fare_bdt, pct_vs_non_peak. Peak rows first.

    Raises:
        ValueError: If there are no non-peak rows to compare with.
    """
    by_type = (
        df.groupby("season_type")
        .agg(booking_count=("total_fare_bdt", "size"), avg_total_fare_bdt=("total_fare_bdt", "mean"))
        .reset_index()
    )
    by_type["seasonality"] = "All"

    by_season = (
        df.groupby(["season_type", "seasonality"])
        .agg(booking_count=("total_fare_bdt", "size"), avg_total_fare_bdt=("total_fare_bdt", "mean"))
        .reset_index()
    )

    non_peak = by_type.loc[by_type["season_type"] == "Non-peak", "avg_total_fare_bdt"]
    if non_peak.empty:
        raise ValueError("No non-peak bookings to compare with: check PEAK_SEASONS in config.py")
    non_peak_avg = non_peak.iloc[0]
    out = pd.concat([by_type, by_season], ignore_index=True)
    out["pct_vs_non_peak"] = (out["avg_total_fare_bdt"] / non_peak_avg - 1) * 100
    out["is_all"] = out["seasonality"] == "All"
    out = out.sort_values(
        ["season_type", "is_all", "avg_total_fare_bdt"], ascending=[False, False, False]
    )
    columns = ["season_type", "seasonality", "booking_count", "avg_total_fare_bdt", "pct_vs_non_peak"]
    return out[columns].round(2).reset_index(drop=True)


def booking_count_by_airline(df):
    """Booking Count by Airline: number of bookings per airline, most first.

    Args:
        df (pandas.DataFrame): Enriched valid rows.

    Returns:
        pandas.DataFrame: airline, booking_count, share_pct (of all
        bookings), booking_rank (1 = most bookings; ties share a rank).
    """
    out = df.groupby("airline").size().reset_index(name="booking_count")
    out["share_pct"] = (out["booking_count"] / out["booking_count"].sum() * 100).round(2)
    out = out.sort_values(["booking_count", "airline"], ascending=[False, True]).reset_index(drop=True)
    out["booking_rank"] = out["booking_count"].rank(method="min", ascending=False).astype(int)
    return out


def popular_routes(df):
    """Most Popular Routes: source-destination pairs ranked by booking count.

    Ties share the same rank (e.g. two routes with 408 bookings are both 4th).

    Args:
        df (pandas.DataFrame): Enriched valid rows (needs route).

    Returns:
        pandas.DataFrame: route_rank, route, source_code, destination_code,
        booking_count, avg_total_fare_bdt. All routes, most popular first.
    """
    out = (
        df.groupby(["route", "source_code", "destination_code"])
        .agg(booking_count=("total_fare_bdt", "size"), avg_total_fare_bdt=("total_fare_bdt", "mean"))
        .round(2)
        .reset_index()
        .sort_values(["booking_count", "route"], ascending=[False, True])
        .reset_index(drop=True)
    )
    out.insert(0, "route_rank", out["booking_count"].rank(method="min", ascending=False).astype(int))
    return out


# KPI table name -> function (Phase 4 uses the names as PostgreSQL tables)
KPI_FUNCTIONS = {
    "kpi_avg_fare_by_airline": avg_fare_by_airline,
    "kpi_seasonal_fare_variation": seasonal_fare_variation,
    "kpi_bookings_by_airline": booking_count_by_airline,
    "kpi_popular_routes": popular_routes,
}


def compute_all_kpis(df):
    """Run every KPI function on the enriched rows.

    Args:
        df (pandas.DataFrame): Enriched valid rows.

    Returns:
        dict: ``{kpi_table_name: KPI DataFrame}``.
    """
    results = {name: func(df) for name, func in KPI_FUNCTIONS.items()}
    for name, table in results.items():
        log.info("%s: %d rows\n%s", name, len(table), table.head(10).to_string(index=False))
    return results