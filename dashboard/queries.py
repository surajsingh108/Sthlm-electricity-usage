"""
Database query functions for the Streamlit dashboard.

All SQL lives here — no query strings in app.py. Each function returns a
pandas DataFrame (or dict for single-row lookups). Results are cached for
5 minutes via st.cache_data so rapid widget interactions don't hammer the DB.

Connection parameters are read from the environment (same .env as the pipeline).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure the repo root is importable when Streamlit launches from any cwd
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.db import get_conn


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _read_sql(query: str, params: tuple = ()) -> pd.DataFrame:
    """Open a connection, run query, close connection, return DataFrame."""
    conn = get_conn()
    try:
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_latest_signal(zone: str) -> dict:
    """
    Return the most recent row from features_hourly for the given zone.

    Parameters
    ----------
    zone : str
        Bidding zone, e.g. 'SE3'.

    Returns
    -------
    dict
        Keys: hour, price_eur_mwh, price_level, greenness_score,
              appliance_signal, windspeed_ms.
        Values default to sensible fallbacks when no data is present.
    """
    sql = """
        SELECT hour, price_eur_mwh, price_level, greenness_score,
               appliance_signal, windspeed_ms
        FROM features_hourly
        WHERE zone = %s
        ORDER BY hour DESC
        LIMIT 1
    """
    df = _read_sql(sql, (zone,))
    if df.empty:
        return {
            "hour": None,
            "price_eur_mwh": None,
            "price_level": "medium",
            "greenness_score": None,
            "appliance_signal": "wait",
            "windspeed_ms": None,
        }
    row = df.iloc[0]
    return {
        "hour": row["hour"],
        "price_eur_mwh": float(row["price_eur_mwh"]) if row["price_eur_mwh"] is not None else None,
        "price_level": row["price_level"] or "medium",
        "greenness_score": float(row["greenness_score"]) if row["greenness_score"] is not None else None,
        "appliance_signal": row["appliance_signal"] or "wait",
        "windspeed_ms": float(row["windspeed_ms"]) if row["windspeed_ms"] is not None else None,
    }


@st.cache_data(ttl=300)
def get_price_history(zone: str, hours: int = 48) -> pd.DataFrame:
    """
    Return the last N hours of price and rolling-average data for a zone.

    Parameters
    ----------
    zone : str
        Bidding zone.
    hours : int
        Number of most-recent hours to return.

    Returns
    -------
    pd.DataFrame
        Columns: hour, price_eur_mwh, rolling_avg_6h, rolling_avg_24h.
        Ordered by hour ASC.
    """
    sql = """
        SELECT hour, price_eur_mwh, rolling_avg_6h, rolling_avg_24h
        FROM features_hourly
        WHERE zone = %s
        ORDER BY hour DESC
        LIMIT %s
    """
    df = _read_sql(sql, (zone, hours))
    if df.empty:
        return df
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    return df.sort_values("hour").reset_index(drop=True)


@st.cache_data(ttl=300)
def get_greenness_history(hours: int = 48) -> pd.DataFrame:
    """
    Return the last N hours of greenness and generation mix data (SE3 only).

    Parameters
    ----------
    hours : int
        Number of most-recent hours to return.

    Returns
    -------
    pd.DataFrame
        Columns: hour, greenness_score, wind_mw, hydro_mw, nuclear_mw, solar_mw.
        Ordered by hour ASC.
    """
    sql = """
        SELECT hour, greenness_score, wind_mw, hydro_mw, nuclear_mw, solar_mw
        FROM features_hourly
        WHERE zone = 'SE3'
          AND greenness_score IS NOT NULL
        ORDER BY hour DESC
        LIMIT %s
    """
    df = _read_sql(sql, (hours,))
    if df.empty:
        return df
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    return df.sort_values("hour").reset_index(drop=True)


@st.cache_data(ttl=300)
def get_best_hours(zone: str) -> pd.DataFrame:
    """
    Return all 24 best-hour rows for a zone, ordered by hour_of_day.

    Parameters
    ----------
    zone : str
        Bidding zone.

    Returns
    -------
    pd.DataFrame
        Columns: hour_of_day, avg_price, avg_greenness, combined_score.
        24 rows ordered by hour_of_day ASC.
    """
    sql = """
        SELECT hour_of_day, avg_price, avg_greenness, combined_score
        FROM features_best_hours
        WHERE zone = %s
        ORDER BY hour_of_day ASC
    """
    return _read_sql(sql, (zone,))


@st.cache_data(ttl=300)
def get_correlations(zone: str) -> pd.DataFrame:
    """
    Return Pearson correlation rows for a zone.

    Parameters
    ----------
    zone : str
        Bidding zone.

    Returns
    -------
    pd.DataFrame
        Columns: metric_a, metric_b, pearson_r.
    """
    sql = """
        SELECT metric_a, metric_b, pearson_r
        FROM features_correlation
        WHERE zone = %s
        ORDER BY metric_a, metric_b
    """
    return _read_sql(sql, (zone,))


@st.cache_data(ttl=300)
def get_price_by_zone_now() -> pd.DataFrame:
    """
    Return the most recent price for all four bidding zones.

    Returns
    -------
    pd.DataFrame
        Columns: zone, price_eur_mwh. Four rows.
    """
    sql = """
        SELECT DISTINCT ON (zone) zone, price_eur_mwh
        FROM features_hourly
        WHERE price_eur_mwh IS NOT NULL
        ORDER BY zone, hour DESC
    """
    return _read_sql(sql)
