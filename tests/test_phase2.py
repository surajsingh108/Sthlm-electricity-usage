"""
Phase 2 integration tests.

Assumes:
  - Docker Postgres is running (`docker-compose up -d`)
  - `python pipeline/run_pipeline.py` has been executed at least once

Run with:
    pytest tests/test_phase2.py -v
"""
from __future__ import annotations

import pytest
import psycopg2

from pipeline.db import get_conn
from pipeline import parse_weather


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def conn():
    """Open one connection for the whole test module, close after."""
    c = get_conn()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Test 1 — Postgres is reachable
# ---------------------------------------------------------------------------

def test_postgres_reachable():
    c = get_conn()
    c.close()


# ---------------------------------------------------------------------------
# Test 2 — all 4 tables exist
# ---------------------------------------------------------------------------

def test_tables_exist(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = {row[0] for row in cur.fetchall()}
    for expected in ("raw_weather", "raw_prices", "raw_generation", "pipeline_runs"):
        assert expected in tables, f"Table '{expected}' not found in DB"


# ---------------------------------------------------------------------------
# Test 3 — hourly_energy view exists and returns rows
# ---------------------------------------------------------------------------

def test_hourly_energy_view_returns_rows(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hourly_energy")
        count = cur.fetchone()[0]
    assert count > 0, "hourly_energy view returned 0 rows"


# ---------------------------------------------------------------------------
# Test 4 — raw_weather has rows for all 4 cities
# ---------------------------------------------------------------------------

def test_weather_all_cities(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT city FROM raw_weather")
        cities = {row[0] for row in cur.fetchall()}
    for city in ("stockholm", "malmo", "sundsvall", "lulea"):
        assert city in cities, f"No weather rows for city '{city}'"


# ---------------------------------------------------------------------------
# Test 5 — raw_prices has rows for all 4 zones
# ---------------------------------------------------------------------------

def test_prices_all_zones(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT zone FROM raw_prices")
        zones = {row[0] for row in cur.fetchall()}
    for zone in ("SE1", "SE2", "SE3", "SE4"):
        assert zone in zones, f"No price rows for zone '{zone}'"


# ---------------------------------------------------------------------------
# Test 6 — raw_generation has at least 5 distinct psr_type values
# ---------------------------------------------------------------------------

def test_generation_psr_types(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT psr_type) FROM raw_generation")
        count = cur.fetchone()[0]
    assert count >= 5, f"Only {count} distinct psr_type values in raw_generation (expected ≥5)"


# ---------------------------------------------------------------------------
# Test 7 — re-running parse_weather does not increase row count (upsert works)
# ---------------------------------------------------------------------------

def test_weather_upsert_idempotent(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_weather")
        before = cur.fetchone()[0]

    parse_weather.parse(conn=conn)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_weather")
        after = cur.fetchone()[0]

    assert after == before, (
        f"Row count changed after re-parse: {before} → {after} (upsert not idempotent)"
    )


# ---------------------------------------------------------------------------
# Test 8 — greenness_score is between 0 and 100 for all rows
# ---------------------------------------------------------------------------

def test_greenness_score_range(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM hourly_energy
            WHERE greenness_score IS NOT NULL
              AND (greenness_score < 0 OR greenness_score > 100)
        """)
        out_of_range = cur.fetchone()[0]
    assert out_of_range == 0, f"{out_of_range} rows have greenness_score outside [0, 100]"


# ---------------------------------------------------------------------------
# Test 9 — pipeline_runs has at least 1 row with status = 'success' or 'partial'
# ---------------------------------------------------------------------------

def test_pipeline_runs_logged(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM pipeline_runs
            WHERE status IN ('success', 'partial')
        """)
        count = cur.fetchone()[0]
    assert count >= 1, "No successful/partial pipeline runs logged in pipeline_runs"
