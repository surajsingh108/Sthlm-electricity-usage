"""
Parse raw ENTSO-E generation JSON files into the raw_generation table.

Handles both document types:
  generation_actual_generation_SE3_*.json   → A75 (actual per production type)
  generation_wind_solar_forecast_SE3_*.json → A69 (wind + solar forecast)

Each TimeSeries entry has one psrType (B-code). PT15M points are summed to
hourly totals before insert. Upsert on (document_type, psr_type, gen_time)
keeps re-runs idempotent.

PSR types observed in SE3 actual generation (A75):
  B04  Fossil Gas
  B12  Hydro Water Reservoir
  B14  Fossil Hard Coal
  B16  Solar
  B19  Wind Onshore
  B20  Nuclear

PSR types in wind/solar forecast (A69):
  B16  Solar
  B19  Wind Onshore
"""
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psycopg2.extras import execute_values

from pipeline.db import get_conn

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

_RESOLUTION_RE = re.compile(r"PT(\d+)M")


def _parse_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)


def _resolution_minutes(res_str: str) -> int:
    m = _RESOLUTION_RE.match(res_str or "")
    return int(m.group(1)) if m else 60


def _ensure_list(val) -> list:
    if val is None:
        return []
    return val if isinstance(val, list) else [val]


def _extract_hourly_generation(data: dict) -> dict[tuple[str, str, datetime], float]:
    """
    Extract (document_type, psr_type, hour_utc) → quantity_mw_sum.

    Returns a flat dict ready for upsert. Each TimeSeries has exactly one
    psrType; PT15M quantities within an hour are summed to MW·h equivalent.
    """
    doc_type = data.get("document_type", "UNKNOWN")
    raw = data.get("raw", {})
    ts_list = _ensure_list(raw.get("TimeSeries"))

    hourly: dict[tuple[str, str, datetime], float] = defaultdict(float)

    for ts_entry in ts_list:
        psr_type = ts_entry.get("MktPSRType", {}).get("psrType", "UNKNOWN")
        periods = _ensure_list(ts_entry.get("Period"))
        for period in periods:
            interval = period.get("timeInterval", {})
            start = interval.get("start")
            if not start:
                continue
            t0 = _parse_ts(start)
            res_min = _resolution_minutes(period.get("resolution", "PT60M"))
            points = _ensure_list(period.get("Point"))
            for point in points:
                try:
                    pos = int(point.get("position", 1))
                    qty = float(point.get("quantity", 0))
                except (TypeError, ValueError):
                    continue
                ts = t0 + timedelta(minutes=(pos - 1) * res_min)
                hour = ts.replace(minute=0, second=0, microsecond=0)
                hourly[(doc_type, psr_type, hour)] += qty

    return hourly


def _rows_from_file(path: Path, now: datetime) -> list[tuple]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    hourly = _extract_hourly_generation(data)
    rows = []
    for (doc_type, psr_type, hour), qty in hourly.items():
        rows.append((doc_type, psr_type, hour, now, round(qty, 3)))
    return rows


def parse(conn=None) -> dict[str, int]:
    """
    Parse all generation_*.json files and upsert into raw_generation.

    Returns
    -------
    dict[str, int]
        {'files': N, 'rows': M}
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    sql = """
        INSERT INTO raw_generation (document_type, psr_type, gen_time, ingested_at, quantity_mw)
        VALUES %s
        ON CONFLICT (document_type, psr_type, gen_time)
        DO UPDATE SET
            quantity_mw = EXCLUDED.quantity_mw,
            ingested_at = EXCLUDED.ingested_at
    """

    total_rows = 0
    files_processed = 0
    now = datetime.now(timezone.utc)

    try:
        with conn.cursor() as cur:
            for path in sorted(RAW_DIR.glob("generation_*.json")):
                try:
                    rows = _rows_from_file(path, now)
                    if rows:
                        execute_values(cur, sql, rows)
                        total_rows += len(rows)
                    files_processed += 1
                    logger.info("Parsed %s → %d hourly rows", path.name, len(rows))
                except Exception as exc:
                    logger.warning("Skipping %s: %s", path.name, exc)
            conn.commit()
    finally:
        if close_conn:
            conn.close()

    logger.info("parse_carbon complete: %d files, %d rows upserted", files_processed, total_rows)
    return {"files": files_processed, "rows": total_rows}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = parse()
    print(f"[parse_carbon] {result['files']} files → {result['rows']} rows upserted")
