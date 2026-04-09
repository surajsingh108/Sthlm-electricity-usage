"""
Phase 4 tests — feature engineering tables and derived metrics.

All tests require a live PostgreSQL connection and the full pipeline
(ingest + parse + analyse) to have run at least once.

Run with:
    pytest tests/test_phase4.py -v
"""
from __future__ import annotations

import pytest

from pipeline.db import get_conn


# ---------------------------------------------------------------------------
# Shared DB fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def conn():
    c = get_conn()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Ensure analyse has run
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def run_analysis_once():
    """Run the full analysis before any test in this module."""
    from pipeline.analyse import run_analysis
    run_analysis()


# ---------------------------------------------------------------------------
# Test 1 — features_hourly table exists and has rows
# ---------------------------------------------------------------------------

def test_features_hourly_exists_and_has_rows(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM features_hourly")
        count = cur.fetchone()[0]
    assert count > 0, "features_hourly is empty — run_analysis() produced no rows"


# ---------------------------------------------------------------------------
# Test 2 — all 4 zones present in features_hourly
# ---------------------------------------------------------------------------

def test_all_zones_present(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT zone FROM features_hourly ORDER BY zone")
        zones = {row[0] for row in cur.fetchall()}
    for expected in ("SE1", "SE2", "SE3", "SE4"):
        assert expected in zones, f"Zone {expected} missing from features_hourly"


# ---------------------------------------------------------------------------
# Test 3 — rolling_avg_6h is within the global price range
# ---------------------------------------------------------------------------

def test_rolling_avg_6h_within_price_range(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                MIN(price_eur_mwh),
                MAX(price_eur_mwh),
                MIN(rolling_avg_6h),
                MAX(rolling_avg_6h)
            FROM features_hourly
            WHERE price_eur_mwh IS NOT NULL
              AND rolling_avg_6h IS NOT NULL
        """)
        row = cur.fetchone()
    assert row is not None, "No rows with both price and rolling_avg_6h"
    p_min, p_max, r_min, r_max = row
    assert float(r_min) >= float(p_min) - 0.01, (
        f"rolling_avg_6h min ({r_min}) is below price min ({p_min})"
    )
    assert float(r_max) <= float(p_max) + 0.01, (
        f"rolling_avg_6h max ({r_max}) exceeds price max ({p_max})"
    )


# ---------------------------------------------------------------------------
# Test 4 — price_level values are only 'low', 'medium', 'high'; no nulls
# ---------------------------------------------------------------------------

def test_price_level_values_valid(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT price_level
            FROM features_hourly
            WHERE price_level IS NOT NULL
        """)
        values = {row[0] for row in cur.fetchall()}
        cur.execute("SELECT COUNT(*) FROM features_hourly WHERE price_level IS NULL")
        null_count = cur.fetchone()[0]

    allowed = {"low", "medium", "high"}
    unexpected = values - allowed
    assert not unexpected, f"Unexpected price_level values: {unexpected}"
    assert null_count == 0, f"{null_count} rows have NULL price_level"


# ---------------------------------------------------------------------------
# Test 5 — greenness_score is between 0 and 100 wherever not NULL
# ---------------------------------------------------------------------------

def test_greenness_score_in_range(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM features_hourly
            WHERE greenness_score IS NOT NULL
              AND (greenness_score < 0 OR greenness_score > 100)
        """)
        bad_count = cur.fetchone()[0]
    assert bad_count == 0, f"{bad_count} rows have greenness_score outside [0, 100]"


# ---------------------------------------------------------------------------
# Test 6 — appliance_signal values are only 'run_now', 'wait', 'avoid'
# ---------------------------------------------------------------------------

def test_appliance_signal_values_valid(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT appliance_signal FROM features_hourly")
        values = {row[0] for row in cur.fetchall()}
    allowed = {"run_now", "wait", "avoid"}
    unexpected = values - allowed
    assert not unexpected, f"Unexpected appliance_signal values: {unexpected}"


# ---------------------------------------------------------------------------
# Test 7 — features_correlation has rows for all 5 pairs for at least one zone
# ---------------------------------------------------------------------------

def test_correlations_populated(conn):
    expected_pairs = {
        ("windspeed_ms", "price_eur_mwh"),
        ("windspeed_ms", "greenness_score"),
        ("temperature_c", "price_eur_mwh"),
        ("radiation_wm2", "greenness_score"),
        ("greenness_score", "price_eur_mwh"),
    }
    with conn.cursor() as cur:
        cur.execute("""
            SELECT zone, metric_a, metric_b
            FROM features_correlation
        """)
        rows = cur.fetchall()

    # Group by zone
    by_zone: dict[str, set] = {}
    for zone, a, b in rows:
        by_zone.setdefault(zone, set()).add((a, b))

    assert by_zone, "features_correlation is empty"
    # At least one zone should have all 5 pairs
    zones_with_all = [z for z, pairs in by_zone.items() if expected_pairs <= pairs]
    assert zones_with_all, (
        f"No zone has all 5 correlation pairs. Found: "
        + ", ".join(f"{z}: {pairs}" for z, pairs in by_zone.items())
    )


# ---------------------------------------------------------------------------
# Test 8 — features_best_hours has exactly 24 rows per zone
# ---------------------------------------------------------------------------

def test_best_hours_24_rows_per_zone(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT zone, COUNT(*) AS n
            FROM features_best_hours
            GROUP BY zone
            ORDER BY zone
        """)
        rows = cur.fetchall()

    assert rows, "features_best_hours is empty"
    for zone, n in rows:
        assert n == 24, f"Zone {zone} has {n} rows in features_best_hours (expected 24)"


# ---------------------------------------------------------------------------
# Test 9 — combined_score in features_best_hours is always between 0 and 2
# ---------------------------------------------------------------------------

def test_combined_score_in_range(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM features_best_hours
            WHERE combined_score < -0.0001 OR combined_score > 2.0001
        """)
        bad_count = cur.fetchone()[0]
    assert bad_count == 0, f"{bad_count} rows have combined_score outside [0, 2]"
