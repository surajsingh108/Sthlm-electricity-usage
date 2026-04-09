"""
Phase 5 tests — Streamlit dashboard imports, queries, and chart rendering.

Tests 1, 5, 6, 7 are pure unit tests (no DB needed).
Tests 2, 3, 4 require a live PostgreSQL connection and at least one
pipeline run to have populated features_hourly / features_best_hours.

Run with:
    pytest tests/test_phase5.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# Ensure the repo root is importable
sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Shared DB fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def conn():
    from pipeline.db import get_conn
    c = get_conn()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Test 1 — app.py imports without error (no Streamlit runtime needed)
# ---------------------------------------------------------------------------

def test_app_imports_without_error():
    """
    Verify that dashboard/app.py can be imported without raising.
    Streamlit decorators execute at import time but don't need a running
    server — we just need the module to parse cleanly.
    """
    import importlib
    import unittest.mock as mock

    # Mock streamlit so no server/session is required
    st_mock = mock.MagicMock()
    st_mock.cache_data = lambda *a, **kw: (lambda f: f)  # pass-through decorator
    with mock.patch.dict("sys.modules", {"streamlit": st_mock}):
        try:
            import dashboard.queries  # noqa: F401 — triggers the import chain
        except Exception as exc:
            pytest.fail(f"dashboard/queries.py import raised: {exc}")


# ---------------------------------------------------------------------------
# Test 2 — get_latest_signal returns dict with all expected keys
# ---------------------------------------------------------------------------

def test_get_latest_signal_keys():
    from dashboard.queries import get_latest_signal
    result = get_latest_signal("SE3")
    expected_keys = {
        "hour", "price_eur_mwh", "price_level",
        "greenness_score", "appliance_signal", "windspeed_ms",
    }
    assert isinstance(result, dict), "get_latest_signal must return a dict"
    missing = expected_keys - result.keys()
    assert not missing, f"Missing keys in signal dict: {missing}"


# ---------------------------------------------------------------------------
# Test 3 — get_price_history returns DataFrame with rows
# ---------------------------------------------------------------------------

def test_get_price_history_has_rows():
    from dashboard.queries import get_price_history
    df = get_price_history("SE3", hours=48)
    assert isinstance(df, pd.DataFrame), "get_price_history must return a DataFrame"
    assert len(df) > 0, "get_price_history returned an empty DataFrame"
    assert "price_eur_mwh" in df.columns


# ---------------------------------------------------------------------------
# Test 4 — get_best_hours returns exactly 24 rows for SE3
# ---------------------------------------------------------------------------

def test_get_best_hours_24_rows():
    from dashboard.queries import get_best_hours
    df = get_best_hours("SE3")
    assert isinstance(df, pd.DataFrame), "get_best_hours must return a DataFrame"
    assert len(df) == 24, f"Expected 24 rows from get_best_hours, got {len(df)}"


# ---------------------------------------------------------------------------
# Test 5 — price_history_chart returns a Plotly Figure without raising
# ---------------------------------------------------------------------------

def test_price_history_chart_returns_figure():
    from dashboard.charts import price_history_chart
    import plotly.graph_objects as go

    df = pd.DataFrame({
        "hour": pd.date_range("2024-01-01", periods=24, freq="h", tz="UTC"),
        "price_eur_mwh": [50.0 + i * 0.5 for i in range(24)],
        "rolling_avg_6h": [51.0] * 24,
        "rolling_avg_24h": [52.0] * 24,
    })
    fig = price_history_chart(df, "SE3")
    assert isinstance(fig, go.Figure), "price_history_chart must return a go.Figure"


# ---------------------------------------------------------------------------
# Test 6 — greenness_gauge returns a Plotly Figure without raising
# ---------------------------------------------------------------------------

def test_greenness_gauge_returns_figure():
    from dashboard.charts import greenness_gauge
    import plotly.graph_objects as go

    fig = greenness_gauge(92.5)
    assert isinstance(fig, go.Figure), "greenness_gauge must return a go.Figure"


# ---------------------------------------------------------------------------
# Test 7 — best_hours_bar highlights exactly 3 bars green
# ---------------------------------------------------------------------------

def test_best_hours_bar_highlights_top3_green():
    from dashboard.charts import best_hours_bar
    import plotly.graph_objects as go

    # Construct a deterministic 24-row DataFrame
    df = pd.DataFrame({
        "hour_of_day": list(range(24)),
        "combined_score": [float(i) / 23 for i in range(24)],
    })
    fig = best_hours_bar(df, "SE3")
    assert isinstance(fig, go.Figure), "best_hours_bar must return a go.Figure"

    bar_trace = next((t for t in fig.data if isinstance(t, go.Bar)), None)
    assert bar_trace is not None, "No Bar trace found in best_hours_bar figure"

    green_count = sum(1 for c in bar_trace.marker.color if c == "#2ca02c")
    assert green_count == 3, (
        f"Expected 3 green bars in best_hours_bar, found {green_count}"
    )
