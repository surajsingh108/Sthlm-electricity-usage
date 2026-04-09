"""
Sweden Energy Intelligence Dashboard — Streamlit entry point.

Run with:
    streamlit run dashboard/app.py

All data queries are in dashboard/queries.py.
All chart definitions are in dashboard/charts.py.
No SQL or chart-building code lives here.
"""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Ensure the repo root is on sys.path when launched from any working directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard import charts, queries

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Sweden Energy Dashboard",
    page_icon="⚡",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("⚡ Sweden Energy")
    zone = st.selectbox("Bidding zone", ["SE1", "SE2", "SE3", "SE4"], index=2)
    hours = st.slider("History (hours)", min_value=12, max_value=168, value=48, step=12)
    st.markdown("---")
    st.caption("Data updates every hour via APScheduler.")
    st.markdown("[View source on GitHub](https://github.com/YOUR_USERNAME/sweden-energy-pipeline)")

# ---------------------------------------------------------------------------
# Row 1 — current state metric cards
# ---------------------------------------------------------------------------
signal = queries.get_latest_signal(zone)

col1, col2, col3, col4 = st.columns(4)

_price = signal["price_eur_mwh"]
col1.metric(
    "Current price",
    f"{_price:.1f} EUR/MWh" if _price is not None else "—",
)

_level = (signal["price_level"] or "—").upper()
col2.metric("Price level", _level)

_green = signal["greenness_score"]
col3.metric(
    "Grid greenness (SE3)",
    f"{_green:.1f}%" if _green is not None else "—",
)

_wind = signal["windspeed_ms"]
col4.metric(
    "Wind speed · Stockholm",
    f"{_wind:.1f} m/s" if _wind is not None else "—",
)

# ---------------------------------------------------------------------------
# Row 2 — appliance signal banner (full width)
# ---------------------------------------------------------------------------
_signal_config: dict[str, tuple[str, str, str]] = {
    "run_now": (
        "RUN YOUR APPLIANCES NOW",
        "#27ae60",
        "Price is low and the grid is clean.",
    ),
    "wait": (
        "WAIT A FEW HOURS",
        "#e67e22",
        "Conditions are average right now.",
    ),
    "avoid": (
        "AVOID IF POSSIBLE",
        "#c0392b",
        "Price is high or the grid is carbon-heavy.",
    ),
}

_label, _colour, _explanation = _signal_config.get(
    signal["appliance_signal"], _signal_config["wait"]
)

st.markdown(
    f"""
    <div style="
        background:{_colour};
        padding:18px 24px;
        border-radius:8px;
        text-align:center;
        margin:8px 0 16px;
    ">
      <h2 style="color:white;margin:0;font-size:1.6rem">{_label}</h2>
      <p style="color:rgba(255,255,255,0.9);margin:6px 0 0;font-size:1rem">
        {_explanation}
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Row 3 — price history + greenness gauge
# ---------------------------------------------------------------------------
col_left, col_right = st.columns([2, 1])

with col_left:
    price_df = queries.get_price_history(zone, hours)
    st.plotly_chart(
        charts.price_history_chart(price_df, zone),
        use_container_width=True,
    )

with col_right:
    st.plotly_chart(
        charts.greenness_gauge(signal["greenness_score"] or 0),
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Row 4 — generation mix + zone price comparison
# ---------------------------------------------------------------------------
col_left, col_right = st.columns([2, 1])

with col_left:
    green_df = queries.get_greenness_history(hours)
    st.plotly_chart(
        charts.generation_mix_chart(green_df),
        use_container_width=True,
    )

with col_right:
    zone_df = queries.get_price_by_zone_now()
    st.plotly_chart(
        charts.zone_price_comparison(zone_df),
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Row 5 — best hours + correlations
# ---------------------------------------------------------------------------
col_left, col_right = st.columns(2)

with col_left:
    best_df = queries.get_best_hours(zone)
    st.plotly_chart(
        charts.best_hours_bar(best_df, zone),
        use_container_width=True,
    )

with col_right:
    corr_df = queries.get_correlations(zone)
    st.plotly_chart(
        charts.correlation_heatmap(corr_df, zone),
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(
    "Sources: Open-Meteo · MET Norway · ENTSO-E · "
    "Data refreshes hourly. All times UTC."
)
