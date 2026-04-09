"""
Parse raw ENTSO-E price JSON files into the raw_prices table.

The ingest step saves ENTSO-E XML responses as nested dicts inside JSON
(keyed as 'raw'). This module navigates that structure, resolves PT15M
timestamps from period start + position offset, aggregates to hourly average,
and upserts into raw_prices.

Zone is read from the 'zone' field in the JSON (set by ingest_prices.py).
All timestamps are UTC. Upsert is idempotent.
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
    """Parse ISO 8601 duration string to minutes. e.g. 'PT15M' → 15."""
    m = _RESOLUTION_RE.match(res_str or "")
    return int(m.group(1)) if m else 60


def _ensure_list(val) -> list:
    if val is None:
        return []
    return val if isinstance(val, list) else [val]


def _extract_hourly_prices(data: dict) -> dict[tuple[str, datetime], list[float]]:
    """
    Extract (zone, hour_utc) → [price_eur_mwh, ...] from one price JSON.

    Returns a dict mapping (zone, truncated_hour) to a list of price values
    (one per 15-min point falling within that hour) ready for averaging.
    """
    zone = data.get("zone", "UNKNOWN")
    raw = data.get("raw", {})
    ts_list = _ensure_list(raw.get("TimeSeries"))

    hourly: dict[tuple[str, datetime], list[float]] = defaultdict(list)

    for ts_entry in ts_list:
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
                    price = float(point.get("price.amount", 0))
                except (TypeError, ValueError):
                    continue
                ts = t0 + timedelta(minutes=(pos - 1) * res_min)
                hour = ts.replace(minute=0, second=0, microsecond=0)
                hourly[(zone, hour)].append(price)

    return hourly


def _rows_from_file(path: Path, now: datetime) -> list[tuple]:
    """Return upsert rows for raw_prices from one JSON file."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    hourly = _extract_hourly_prices(data)
    rows = []
    for (zone, hour), prices in hourly.items():
        avg_price = sum(prices) / len(prices)
        rows.append((zone, hour, now, round(avg_price, 4)))
    return rows


def parse(conn=None) -> dict[str, int]:
    """
    Parse all prices_SE*.json files and upsert into raw_prices.

    Returns
    -------
    dict[str, int]
        {'files': N, 'rows': M}
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    sql = """
        INSERT INTO raw_prices (zone, price_time, ingested_at, price_eur_mwh)
        VALUES %s
        ON CONFLICT (zone, price_time)
        DO UPDATE SET
            price_eur_mwh = EXCLUDED.price_eur_mwh,
            ingested_at   = EXCLUDED.ingested_at
    """

    total_rows = 0
    files_processed = 0
    now = datetime.now(timezone.utc)

    try:
        with conn.cursor() as cur:
            for path in sorted(RAW_DIR.glob("prices_SE*.json")):
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

    logger.info("parse_prices complete: %d files, %d rows upserted", files_processed, total_rows)
    return {"files": files_processed, "rows": total_rows}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = parse()
    print(f"[parse_prices] {result['files']} files → {result['rows']} rows upserted")
