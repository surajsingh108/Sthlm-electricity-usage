"""
Plotly chart-building functions for the Sweden Energy dashboard.

Each function accepts a pandas DataFrame (or scalar) and returns a
plotly.graph_objects.Figure. No Streamlit calls here — rendering is
handled by app.py.

Design conventions:
- template="plotly_white" on all figures
- Zone colour palette: SE1=blue, SE2=green, SE3=purple, SE4=orange
- Titles and axis labels use sentence case
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Zone palette
# ---------------------------------------------------------------------------
_ZONE_COLOUR: dict[str, str] = {
    "SE1": "#1f77b4",   # blue
    "SE2": "#2ca02c",   # green
    "SE3": "#9467bd",   # purple
    "SE4": "#ff7f0e",   # orange
}

_TEMPLATE = "plotly_white"


# ---------------------------------------------------------------------------
# 1. Price history line chart
# ---------------------------------------------------------------------------

def price_history_chart(df: pd.DataFrame, zone: str) -> go.Figure:
    """
    Line chart showing price_eur_mwh, rolling_avg_6h, and rolling_avg_24h.

    A semi-transparent band marks the ±15 % low/high threshold around the
    most recent rolling_avg_24h value.

    Parameters
    ----------
    df : pd.DataFrame
        Columns: hour, price_eur_mwh, rolling_avg_6h, rolling_avg_24h.
    zone : str
        Used for the price line colour and chart title.

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()

    if df.empty:
        fig.update_layout(title=f"Price history — {zone}", template=_TEMPLATE)
        return fig

    zone_colour = _ZONE_COLOUR.get(zone, "#636efa")

    # ±15 % band around the most recent 24h average
    if df["rolling_avg_24h"].notna().any():
        last_avg24 = float(df["rolling_avg_24h"].dropna().iloc[-1])
        low_band = last_avg24 * 0.85
        high_band = last_avg24 * 1.15

        fig.add_trace(go.Scatter(
            x=list(df["hour"]) + list(df["hour"])[::-1],
            y=[high_band] * len(df) + [low_band] * len(df),
            fill="toself",
            fillcolor="rgba(200,200,200,0.25)",
            line=dict(color="rgba(0,0,0,0)"),
            name="±15% band",
            hoverinfo="skip",
        ))

    fig.add_trace(go.Scatter(
        x=df["hour"], y=df["rolling_avg_24h"],
        mode="lines", name="24h avg",
        line=dict(color="gray", width=1.5, dash="dot"),
    ))
    fig.add_trace(go.Scatter(
        x=df["hour"], y=df["rolling_avg_6h"],
        mode="lines", name="6h avg",
        line=dict(color="darkgray", width=1.5, dash="dash"),
    ))
    fig.add_trace(go.Scatter(
        x=df["hour"], y=df["price_eur_mwh"],
        mode="lines", name=f"Price ({zone})",
        line=dict(color=zone_colour, width=2),
    ))

    fig.update_layout(
        title=f"Spot price history — {zone}",
        xaxis_title="Hour (UTC)",
        yaxis_title="EUR / MWh",
        template=_TEMPLATE,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=60, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# 2. Greenness gauge
# ---------------------------------------------------------------------------

def greenness_gauge(score: float) -> go.Figure:
    """
    Indicator gauge showing the current grid greenness score (0–100).

    Colour bands: green 80–100, amber 50–80, red 0–50.

    Parameters
    ----------
    score : float
        Current greenness_score value.

    Returns
    -------
    go.Figure
    """
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        title={"text": "Grid greenness (SE3)", "font": {"size": 16}},
        number={"suffix": "%", "font": {"size": 28}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "darkgray"},
            "bar": {"color": "#2ca02c" if score >= 80 else ("#ff7f0e" if score >= 50 else "#d62728")},
            "bgcolor": "white",
            "borderwidth": 1,
            "bordercolor": "lightgray",
            "steps": [
                {"range": [0, 50],  "color": "#ffe0e0"},
                {"range": [50, 80], "color": "#fff3cd"},
                {"range": [80, 100],"color": "#d4edda"},
            ],
            "threshold": {
                "line": {"color": "black", "width": 2},
                "thickness": 0.75,
                "value": score,
            },
        },
    ))
    fig.update_layout(
        template=_TEMPLATE,
        margin=dict(t=60, b=20, l=30, r=30),
        height=260,
    )
    return fig


# ---------------------------------------------------------------------------
# 3. Generation mix stacked area
# ---------------------------------------------------------------------------

def generation_mix_chart(df: pd.DataFrame) -> go.Figure:
    """
    Stacked area chart of the SE3 generation mix over time.

    Parameters
    ----------
    df : pd.DataFrame
        Columns: hour, wind_mw, hydro_mw, nuclear_mw, solar_mw.

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()

    if df.empty:
        fig.update_layout(title="Generation mix — SE3", template=_TEMPLATE)
        return fig

    for col, name, colour in [
        ("nuclear_mw", "Nuclear",  "#9467bd"),
        ("hydro_mw",   "Hydro",    "#17becf"),
        ("wind_mw",    "Wind",     "#1f77b4"),
        ("solar_mw",   "Solar",    "#ffbb33"),
    ]:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df["hour"], y=df[col].fillna(0),
                mode="lines", name=name,
                stackgroup="one",
                line=dict(width=0.5, color=colour),
                fillcolor=colour,
            ))

    fig.update_layout(
        title="Generation mix — SE3",
        xaxis_title="Hour (UTC)",
        yaxis_title="MW",
        template=_TEMPLATE,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=60, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# 4. Best hours bar
# ---------------------------------------------------------------------------

def best_hours_bar(df: pd.DataFrame, zone: str) -> go.Figure:
    """
    Bar chart of combined_score by hour_of_day.

    Top 3 bars are highlighted green, bottom 3 red, rest gray.

    Parameters
    ----------
    df : pd.DataFrame
        Columns: hour_of_day (0–23), combined_score.
    zone : str
        Used in the chart title.

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()

    if df.empty:
        fig.update_layout(title=f"Best hours to run appliances — {zone}", template=_TEMPLATE)
        return fig

    df = df.sort_values("hour_of_day").reset_index(drop=True)
    scores = df["combined_score"].tolist()
    sorted_scores = sorted(enumerate(scores), key=lambda x: x[1])
    bottom3_idx = {i for i, _ in sorted_scores[:3]}
    top3_idx    = {i for i, _ in sorted_scores[-3:]}

    colours = []
    for i in range(len(df)):
        if i in top3_idx:
            colours.append("#2ca02c")     # green
        elif i in bottom3_idx:
            colours.append("#d62728")     # red
        else:
            colours.append("#aec7e8")     # light blue-gray

    fig.add_trace(go.Bar(
        x=df["hour_of_day"],
        y=df["combined_score"],
        marker_color=colours,
        name="Combined score",
        hovertemplate="Hour %{x}:00 UTC<br>Score: %{y:.3f}<extra></extra>",
    ))

    fig.update_layout(
        title=f"Best hours to run appliances — {zone}",
        xaxis=dict(title="Hour of day (UTC)", tickmode="linear", dtick=2),
        yaxis_title="Combined score (higher = better)",
        template=_TEMPLATE,
        showlegend=False,
        margin=dict(t=60, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# 5. Correlation horizontal bar
# ---------------------------------------------------------------------------

def correlation_heatmap(df: pd.DataFrame, zone: str) -> go.Figure:
    """
    Horizontal bar chart of Pearson r values for key metric pairs.

    Positive bars are coral, negative bars are steel blue.
    A vertical reference line is drawn at x = 0.

    Parameters
    ----------
    df : pd.DataFrame
        Columns: metric_a, metric_b, pearson_r.
    zone : str
        Used in the chart title.

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()

    if df.empty:
        fig.update_layout(title=f"Variable correlations — {zone}", template=_TEMPLATE)
        return fig

    _label_map = {
        "windspeed_ms":    "Wind speed",
        "price_eur_mwh":   "Price",
        "greenness_score": "Greenness",
        "temperature_c":   "Temperature",
        "radiation_wm2":   "Solar radiation",
    }
    labels = [
        f"{_label_map.get(r['metric_a'], r['metric_a'])} vs "
        f"{_label_map.get(r['metric_b'], r['metric_b'])}"
        for _, r in df.iterrows()
    ]
    r_vals = df["pearson_r"].tolist()
    colours = ["#d62728" if v >= 0 else "#1f77b4" for v in r_vals]

    fig.add_trace(go.Bar(
        x=r_vals,
        y=labels,
        orientation="h",
        marker_color=colours,
        hovertemplate="%{y}<br>r = %{x:.3f}<extra></extra>",
    ))

    fig.add_vline(x=0, line_width=1, line_color="black")

    fig.update_layout(
        title=f"Variable correlations — {zone}",
        xaxis=dict(title="Pearson r", range=[-1, 1], zeroline=False),
        yaxis_title=None,
        template=_TEMPLATE,
        margin=dict(t=60, b=40, l=20),
    )
    return fig


# ---------------------------------------------------------------------------
# 6. Zone price comparison
# ---------------------------------------------------------------------------

def zone_price_comparison(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart comparing current prices across all four zones.

    The cheapest zone bar is highlighted green.

    Parameters
    ----------
    df : pd.DataFrame
        Columns: zone, price_eur_mwh.

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()

    if df.empty:
        fig.update_layout(title="Current price by zone", template=_TEMPLATE)
        return fig

    df = df.sort_values("price_eur_mwh", ascending=True).reset_index(drop=True)
    min_idx = int(df["price_eur_mwh"].idxmin())

    colours = [
        "#2ca02c" if i == min_idx else _ZONE_COLOUR.get(z, "#aec7e8")
        for i, z in enumerate(df["zone"])
    ]

    fig.add_trace(go.Bar(
        x=df["price_eur_mwh"],
        y=df["zone"],
        orientation="h",
        marker_color=colours,
        hovertemplate="%{y}: %{x:.1f} EUR/MWh<extra></extra>",
    ))

    fig.update_layout(
        title="Current price by zone",
        xaxis_title="EUR / MWh",
        yaxis_title=None,
        template=_TEMPLATE,
        showlegend=False,
        margin=dict(t=60, b=40),
        height=220,
    )
    return fig
