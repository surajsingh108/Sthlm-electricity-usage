"""
Phase 4 — Feature engineering for the Sweden energy pipeline.

Reads from raw_prices, raw_weather, and raw_generation; writes derived
features into three tables:

  features_hourly      — one row per zone per hour (rolling avgs, signals)
  features_correlation — Pearson r for key metric pairs per zone
  features_best_hours  — best hours-of-day to run appliances per zone

All rolling calculations are done in pandas (not SQL). Reads use a SQLAlchemy
engine (required by pandas). Upserts use psycopg2 execute_values — same
pattern as the rest of the pipeline.

Generation data (greenness, wind_mw, hydro_mw, nuclear_mw, solar_mw) is
available for SE3 only — those columns are NULL for SE1/SE2/SE4.

PSR codes used for greenness (low-carbon generation):
  B11 Hydro Run-of-river   B12 Hydro Reservoir
  B16 Solar                B18 Wind Offshore
  B19 Wind Onshore         B20 Nuclear
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from scipy.stats import pearsonr

from pipeline.db import get_conn

logger = logging.getLogger(__name__)

ZONES = ["SE1", "SE2", "SE3", "SE4"]

# PSR codes considered low-carbon
_LOW_CARBON_CODES = {"B11", "B12", "B16", "B18", "B19", "B20"}

# Individual type → column mapping (used when building features_hourly)
_PSR_COLS = {
    "B18": "wind_offshore_mw",
    "B19": "wind_onshore_mw",
    "B11": "hydro_ror_mw",
    "B12": "hydro_res_mw",
    "B20": "nuclear_mw",
    "B16": "solar_mw",
}

# Correlation pairs to compute
_CORR_PAIRS = [
    ("windspeed_ms", "price_eur_mwh"),
    ("windspeed_ms", "greenness_score"),
    ("temperature_c", "price_eur_mwh"),
    ("radiation_wm2", "greenness_score"),
    ("greenness_score", "price_eur_mwh"),
]


# ---------------------------------------------------------------------------
# Pure helper functions (no DB I/O)
# ---------------------------------------------------------------------------

def classify_price_level(price: float | None, avg_24h: float | None) -> str | None:
    """
    Classify a price relative to its 24-hour rolling average.

    Parameters
    ----------
    price : float | None
        Spot price in EUR/MWh.
    avg_24h : float | None
        24-hour rolling average price.

    Returns
    -------
    str | None
        'low', 'medium', or 'high'. Returns None if either input is None/NaN.
    """
    if price is None or avg_24h is None:
        return None
    try:
        p, a = float(price), float(avg_24h)
    except (TypeError, ValueError):
        return None
    if pd.isna(p) or pd.isna(a) or a == 0:
        return None
    if p < a * 0.85:
        return "low"
    if p > a * 1.15:
        return "high"
    return "medium"


def compute_greenness_score(
    hour: datetime,
    conn,
    document_type: str = "A75",
) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None]:
    """
    Compute greenness and generation breakdown for one hour from raw_generation.

    Parameters
    ----------
    hour : datetime
        UTC-truncated hour to query.
    conn : psycopg2 connection
        Open database connection.
    document_type : str
        ENTSO-E document type; default 'A75' (actual generation).

    Returns
    -------
    tuple of (greenness_score, low_carbon_mw, total_gen_mw,
              wind_mw, hydro_mw, nuclear_mw, solar_mw)
        All float or None. greenness_score is 0–100.
    """
    sql = """
        SELECT psr_type, quantity_mw
        FROM raw_generation
        WHERE gen_time = %s AND document_type = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (hour, document_type))
        rows = cur.fetchall()

    if not rows:
        return None, None, None, None, None, None, None

    by_type: dict[str, float] = {psr: qty for psr, qty in rows if qty is not None}
    total = sum(by_type.values())
    if total <= 0:
        return None, None, None, None, None, None, None

    low_carbon = sum(v for k, v in by_type.items() if k in _LOW_CARBON_CODES)
    greenness = round(low_carbon / total * 100, 1)

    wind_mw = (by_type.get("B18", 0) or 0) + (by_type.get("B19", 0) or 0)
    hydro_mw = (by_type.get("B11", 0) or 0) + (by_type.get("B12", 0) or 0)
    nuclear_mw = by_type.get("B20")
    solar_mw = by_type.get("B16")

    return greenness, low_carbon, total, wind_mw, hydro_mw, nuclear_mw, solar_mw


def compute_appliance_signal(
    price_level: str | None,
    greenness_score: float | None,
) -> str:
    """
    Derive a user-facing appliance scheduling signal.

    When greenness_score is None (non-SE3 zones), the signal is based on
    price_level alone: 'low' → 'run_now', 'high' → 'avoid', else 'wait'.

    Parameters
    ----------
    price_level : str | None
        'low', 'medium', or 'high'.
    greenness_score : float | None
        0–100 scale; None for zones without generation data.

    Returns
    -------
    str
        'run_now', 'wait', or 'avoid'.
    """
    if price_level == "high":
        return "avoid"
    if greenness_score is not None:
        if price_level == "low" and greenness_score >= 80:
            return "run_now"
        if greenness_score < 50:
            return "avoid"
    else:
        # No greenness data — price-only decision
        if price_level == "low":
            return "run_now"
    return "wait"


# ---------------------------------------------------------------------------
# Rolling averages
# ---------------------------------------------------------------------------

def compute_rolling_averages(zone: str, conn) -> pd.DataFrame:
    """
    Compute 6h and 24h rolling price averages for a zone.

    Parameters
    ----------
    zone : str
        Bidding zone, e.g. 'SE3'.
    conn : psycopg2 connection
        Open database connection.

    Returns
    -------
    pd.DataFrame
        Columns: hour, zone, price_eur_mwh, rolling_avg_6h, rolling_avg_24h.
        Sorted ascending by hour.
    """
    sql = """
        SELECT price_time AS hour, price_eur_mwh
        FROM raw_prices
        WHERE zone = %s
        ORDER BY price_time
    """
    df = pd.read_sql(sql, conn, params=(zone,))
    if df.empty:
        logger.warning("No price rows found for zone %s", zone)
        return df

    df["rolling_avg_6h"] = (
        df["price_eur_mwh"].rolling(window=6, min_periods=1).mean().round(4)
    )
    df["rolling_avg_24h"] = (
        df["price_eur_mwh"].rolling(window=24, min_periods=1).mean().round(4)
    )
    df["zone"] = zone
    return df


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def _upsert_features_hourly(rows: list[dict], conn) -> int:
    """
    Upsert a list of feature-row dicts into features_hourly via execute_values.

    Each dict must include all column keys. Returns number of rows upserted.
    """
    if not rows:
        return 0

    from psycopg2.extras import execute_values

    _COLS = (
        "hour", "zone", "price_eur_mwh",
        "rolling_avg_6h", "rolling_avg_24h", "price_level",
        "temperature_c", "windspeed_ms", "radiation_wm2",
        "greenness_score", "low_carbon_mw", "total_gen_mw",
        "wind_mw", "hydro_mw", "nuclear_mw", "solar_mw",
        "appliance_signal", "computed_at",
    )
    tuples = [tuple(r[c] for c in _COLS) for r in rows]

    upsert_sql = """
        INSERT INTO features_hourly (
            hour, zone, price_eur_mwh,
            rolling_avg_6h, rolling_avg_24h, price_level,
            temperature_c, windspeed_ms, radiation_wm2,
            greenness_score, low_carbon_mw, total_gen_mw,
            wind_mw, hydro_mw, nuclear_mw, solar_mw,
            appliance_signal, computed_at
        ) VALUES %s
        ON CONFLICT (hour, zone) DO UPDATE SET
            price_eur_mwh    = EXCLUDED.price_eur_mwh,
            rolling_avg_6h   = EXCLUDED.rolling_avg_6h,
            rolling_avg_24h  = EXCLUDED.rolling_avg_24h,
            price_level      = EXCLUDED.price_level,
            temperature_c    = EXCLUDED.temperature_c,
            windspeed_ms     = EXCLUDED.windspeed_ms,
            radiation_wm2    = EXCLUDED.radiation_wm2,
            greenness_score  = EXCLUDED.greenness_score,
            low_carbon_mw    = EXCLUDED.low_carbon_mw,
            total_gen_mw     = EXCLUDED.total_gen_mw,
            wind_mw          = EXCLUDED.wind_mw,
            hydro_mw         = EXCLUDED.hydro_mw,
            nuclear_mw       = EXCLUDED.nuclear_mw,
            solar_mw         = EXCLUDED.solar_mw,
            appliance_signal = EXCLUDED.appliance_signal,
            computed_at      = EXCLUDED.computed_at
    """
    with conn.cursor() as cur:
        execute_values(cur, upsert_sql, tuples)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Correlation
# ---------------------------------------------------------------------------

def compute_correlations(zone: str, conn) -> None:
    """
    Compute Pearson r for five metric pairs from features_hourly and upsert
    results into features_correlation.

    Parameters
    ----------
    zone : str
        Bidding zone, e.g. 'SE3'.
    conn : psycopg2 connection
        Open database connection.

    Notes
    -----
    Expected direction: windspeed_ms vs price_eur_mwh should be negative in
    SE1/SE2 (more wind in Norrland → lower prices).
    """
    sql = """
        SELECT price_eur_mwh, windspeed_ms, temperature_c,
               radiation_wm2, greenness_score
        FROM features_hourly
        WHERE zone = %s
    """
    df = pd.read_sql(sql, conn, params=(zone,))
    if df.empty:
        logger.warning("No features_hourly rows for zone %s — skipping correlations", zone)
        return

    now = datetime.now(timezone.utc)
    upsert_sql = """
        INSERT INTO features_correlation
            (zone, metric_a, metric_b, pearson_r, sample_size, computed_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (zone, metric_a, metric_b) DO UPDATE SET
            pearson_r   = EXCLUDED.pearson_r,
            sample_size = EXCLUDED.sample_size,
            computed_at = EXCLUDED.computed_at
    """
    with conn.cursor() as cur:
        for col_a, col_b in _CORR_PAIRS:
            sub = df[[col_a, col_b]].dropna()
            if len(sub) < 3:
                logger.debug("Insufficient data for %s vs %s in %s", col_a, col_b, zone)
                continue
            r, _ = pearsonr(sub[col_a], sub[col_b])
            cur.execute(upsert_sql, (zone, col_a, col_b, round(float(r), 4), len(sub), now))
    conn.commit()
    logger.info("Correlations upserted for zone %s", zone)


# ---------------------------------------------------------------------------
# Best hours
# ---------------------------------------------------------------------------

def compute_best_hours(zone: str, conn) -> None:
    """
    Compute the best hours-of-day (0–23 UTC) to run appliances for a zone.

    combined_score = (1 - norm_price) + norm_greenness
    where norm() scales each averaged metric to 0–1 across the 24 hours.
    Higher combined_score → better time to run appliances.

    Parameters
    ----------
    zone : str
        Bidding zone, e.g. 'SE3'.
    conn : psycopg2 connection
        Open database connection.
    """
    sql = """
        SELECT EXTRACT(HOUR FROM hour AT TIME ZONE 'UTC') AS hour_of_day,
               price_eur_mwh, greenness_score
        FROM features_hourly
        WHERE zone = %s
    """
    df = pd.read_sql(sql, conn, params=(zone,))
    if df.empty:
        logger.warning("No features_hourly rows for zone %s — skipping best_hours", zone)
        return

    df["hour_of_day"] = df["hour_of_day"].astype(int)
    grouped = (
        df.groupby("hour_of_day")
        .agg(avg_price=("price_eur_mwh", "mean"), avg_greenness=("greenness_score", "mean"))
        .reset_index()
    )

    # Normalise both metrics to 0–1 across the 24 hours
    p_min, p_max = grouped["avg_price"].min(), grouped["avg_price"].max()
    g_min, g_max = grouped["avg_greenness"].min(), grouped["avg_greenness"].max()

    def _norm(series: pd.Series, lo: float, hi: float) -> pd.Series:
        rng = hi - lo
        if rng == 0:
            return pd.Series(0.5, index=series.index)
        return (series - lo) / rng

    norm_price = _norm(grouped["avg_price"], p_min, p_max)
    norm_green = _norm(grouped["avg_greenness"], g_min, g_max)
    grouped["combined_score"] = ((1 - norm_price) + norm_green).round(4)

    now = datetime.now(timezone.utc)
    upsert_sql = """
        INSERT INTO features_best_hours
            (zone, hour_of_day, avg_price, avg_greenness, combined_score, computed_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (zone, hour_of_day) DO UPDATE SET
            avg_price      = EXCLUDED.avg_price,
            avg_greenness  = EXCLUDED.avg_greenness,
            combined_score = EXCLUDED.combined_score,
            computed_at    = EXCLUDED.computed_at
    """
    with conn.cursor() as cur:
        for _, row in grouped.iterrows():
            avg_green = None if pd.isna(row["avg_greenness"]) else round(float(row["avg_greenness"]), 4)
            cur.execute(upsert_sql, (
                zone,
                int(row["hour_of_day"]),
                round(float(row["avg_price"]), 4) if not pd.isna(row["avg_price"]) else None,
                avg_green,
                float(row["combined_score"]),
                now,
            ))
    conn.commit()
    logger.info("Best hours upserted for zone %s", zone)


# ---------------------------------------------------------------------------
# Master function
# ---------------------------------------------------------------------------

def run_analysis(conn=None) -> dict:
    """
    Master analysis function — called after all parse steps in run_pipeline.py.

    For each zone: computes rolling averages, price levels, greenness (SE3
    only), appliance signals, then writes into features_hourly. Afterwards
    computes correlations and best-hours summaries.

    Parameters
    ----------
    conn : psycopg2 connection | None
        If None, opens and closes its own connection.

    Returns
    -------
    dict
        {'rows': total rows upserted into features_hourly}
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    # Ensure feature tables exist
    _ensure_feature_tables(conn)

    now = datetime.now(timezone.utc)
    total_rows = 0

    try:
        for zone in ZONES:
            logger.info("Analysing zone %s …", zone)

            # --- rolling averages ------------------------------------------------
            prices_df = compute_rolling_averages(zone, conn)
            if prices_df.empty:
                logger.warning("Zone %s has no price data — skipping", zone)
                continue

            # --- weather join (Stockholm openmeteo for all zones) ----------------
            weather_sql = """
                SELECT forecast_time AS hour,
                       temperature_c, windspeed_ms, radiation_wm2
                FROM raw_weather
                WHERE city = 'stockholm' AND source = 'openmeteo'
                ORDER BY forecast_time
            """
            weather_df = pd.read_sql(weather_sql, conn)

            merged = prices_df.merge(weather_df, on="hour", how="left")

            # --- build feature rows ----------------------------------------------
            rows: list[dict] = []
            for _, r in merged.iterrows():
                price = r.get("price_eur_mwh")
                avg24 = r.get("rolling_avg_24h")
                p_level = classify_price_level(price, avg24)

                # Generation features — SE3 only
                g_score = hydro_mw = wind_mw = nuclear_mw = solar_mw = None
                low_carbon_mw = total_gen_mw = None
                if zone == "SE3":
                    g_score, low_carbon_mw, total_gen_mw, wind_mw, hydro_mw, nuclear_mw, solar_mw = (
                        compute_greenness_score(r["hour"], conn)
                    )

                signal = compute_appliance_signal(p_level, g_score)

                rows.append({
                    "hour": r["hour"],
                    "zone": zone,
                    "price_eur_mwh": float(price) if price is not None and not pd.isna(price) else None,
                    "rolling_avg_6h": float(r["rolling_avg_6h"]) if not pd.isna(r["rolling_avg_6h"]) else None,
                    "rolling_avg_24h": float(avg24) if avg24 is not None and not pd.isna(avg24) else None,
                    "price_level": p_level,
                    "temperature_c": float(r["temperature_c"]) if not pd.isna(r.get("temperature_c", float("nan"))) else None,
                    "windspeed_ms": float(r["windspeed_ms"]) if not pd.isna(r.get("windspeed_ms", float("nan"))) else None,
                    "radiation_wm2": float(r["radiation_wm2"]) if not pd.isna(r.get("radiation_wm2", float("nan"))) else None,
                    "greenness_score": g_score,
                    "low_carbon_mw": low_carbon_mw,
                    "total_gen_mw": total_gen_mw,
                    "wind_mw": wind_mw,
                    "hydro_mw": hydro_mw,
                    "nuclear_mw": nuclear_mw,
                    "solar_mw": solar_mw,
                    "appliance_signal": signal,
                    "computed_at": now,
                })

            n = _upsert_features_hourly(rows, conn)
            total_rows += n
            logger.info("Zone %s: %d rows upserted into features_hourly", zone, n)

        # --- correlations and best hours ----------------------------------------
        for zone in ZONES:
            compute_correlations(zone, conn)
            compute_best_hours(zone, conn)

    finally:
        if close_conn:
            conn.close()

    logger.info("Analysis complete: %d rows in features_hourly", total_rows)
    return {"rows": total_rows}


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

def _ensure_feature_tables(conn) -> None:
    """Create feature tables if they do not exist (reads features.sql)."""
    from pathlib import Path

    sql_path = Path(__file__).parent.parent / "db" / "features.sql"
    if not sql_path.exists():
        logger.warning("features.sql not found at %s — tables may be missing", sql_path)
        return
    sql = sql_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    result = run_analysis()
    print(f"[analyse] {result['rows']} rows upserted into features_hourly")
