"""
Parse raw weather JSON files into the raw_weather table.

Handles two sources:
  openmeteo_*.json  — Open-Meteo API response; hourly block with lists.
  metno_*.json      — MET Norway Locationforecast 2.0; properties.timeseries list.

City is extracted from the filename (second underscore-delimited segment).
All timestamps are stored as UTC. Rows are upserted so re-runs are idempotent.

MET Norway does not provide shortwave_radiation; that column is stored as NULL
for met_norway rows. The hourly_energy view joins only on openmeteo rows, so
this is intentional.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import execute_values

from pipeline.db import get_conn

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

_CITY_NORM: dict[str, str] = {
    "Stockholm": "stockholm",
    "Malmo": "malmo",
    "Sundsvall": "sundsvall",
    "Lulea": "lulea",
}


def _city_from_path(path: Path, prefix: str) -> str:
    """Extract and normalise city name from filename, e.g. openmeteo_Stockholm_… → 'stockholm'."""
    stem = path.stem  # e.g. 'openmeteo_Stockholm_20260409_1308'
    city_raw = stem[len(prefix):]  # 'Stockholm_20260409_1308'
    city_raw = city_raw.split("_")[0]  # 'Stockholm'
    return _CITY_NORM.get(city_raw, city_raw.lower())


def _parse_ts(ts_str: str) -> datetime:
    """Parse an ISO-8601 timestamp string to a UTC-aware datetime."""
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)


def _rows_openmeteo(path: Path) -> list[tuple]:
    """Extract upsert rows from an Open-Meteo JSON file."""
    city = _city_from_path(path, "openmeteo_")
    now = datetime.now(timezone.utc)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [None] * len(times))
    winds = hourly.get("windspeed_10m", [None] * len(times))
    rads = hourly.get("shortwave_radiation", [None] * len(times))
    rows = []
    for t, temp, wind, rad in zip(times, temps, winds, rads):
        forecast_time = _parse_ts(t if t.endswith("Z") else t + ":00Z" if "T" in t else t)
        rows.append(("openmeteo", city, forecast_time, now, temp, wind, rad))
    return rows


def _rows_metno(path: Path) -> list[tuple]:
    """Extract upsert rows from a MET Norway JSON file."""
    city = _city_from_path(path, "metno_")
    now = datetime.now(timezone.utc)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    timeseries = data.get("properties", {}).get("timeseries", [])
    rows = []
    for entry in timeseries:
        t = entry.get("time")
        if not t:
            continue
        details = entry.get("data", {}).get("instant", {}).get("details", {})
        forecast_time = _parse_ts(t)
        rows.append((
            "met_norway",
            city,
            forecast_time,
            now,
            details.get("air_temperature"),
            details.get("wind_speed"),
            None,  # MET Norway does not provide shortwave_radiation
        ))
    return rows


def parse(conn=None) -> dict[str, int]:
    """
    Parse all weather JSON files and upsert into raw_weather.

    Parameters
    ----------
    conn : psycopg2 connection, optional
        If provided, uses this connection (useful for testing). Otherwise opens
        a new connection.

    Returns
    -------
    dict[str, int]
        {'files': N, 'rows': M} — number of files processed and rows upserted.
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    sql = """
        INSERT INTO raw_weather (source, city, forecast_time, ingested_at,
                                 temperature_c, windspeed_ms, radiation_wm2)
        VALUES %s
        ON CONFLICT (source, city, forecast_time)
        DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            windspeed_ms  = EXCLUDED.windspeed_ms,
            radiation_wm2 = EXCLUDED.radiation_wm2,
            ingested_at   = EXCLUDED.ingested_at
    """

    total_rows = 0
    files_processed = 0

    try:
        with conn.cursor() as cur:
            for prefix, extractor in [
                ("openmeteo_", _rows_openmeteo),
                ("metno_", _rows_metno),
            ]:
                for path in sorted(RAW_DIR.glob(f"{prefix}*.json")):
                    try:
                        rows = extractor(path)
                        if rows:
                            execute_values(cur, sql, rows)
                            total_rows += len(rows)
                        files_processed += 1
                        logger.info("Parsed %s → %d rows", path.name, len(rows))
                    except Exception as exc:
                        logger.warning("Skipping %s: %s", path.name, exc)
            conn.commit()
    finally:
        if close_conn:
            conn.close()

    logger.info("parse_weather complete: %d files, %d rows upserted", files_processed, total_rows)
    return {"files": files_processed, "rows": total_rows}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = parse()
    print(f"[parse_weather] {result['files']} files → {result['rows']} rows upserted")
