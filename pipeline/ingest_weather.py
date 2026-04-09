"""
Weather ingestion from Open-Meteo and MET Norway.

Fetches hourly temperature, wind speed, and radiation forecasts for 4 Swedish
cities. Routing follows the same logic as fetch.py:

  Historical (up to yesterday) → Open-Meteo (ERA5 / historical forecast tiers)
  Forecast (today onwards)     → MET Norway (Nordic region; ~9-day horizon)
                                  + Open-Meteo supplement for shortwave_radiation
                                  (MET Norway does not provide it)

Both APIs are keyless — no environment variables needed.

Raw JSON responses are saved per-city per-source for traceability.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

CITIES: dict[str, tuple[float, float]] = {
    "Stockholm": (59.33, 18.07),
    "Malmo": (55.60, 13.00),
    "Sundsvall": (62.39, 17.31),
    "Lulea": (65.58, 22.15),
}

# Open-Meteo live forecast endpoint (2-day window for ingestion)
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# MET Norway Locationforecast 2.0 — requires a valid contact in User-Agent
MET_NORWAY_URL = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
MET_NORWAY_USER_AGENT = "SthlmEnergyPipeline/1.0 (arnobmukherjee1988@gmail.com)"


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


def _save(payload: dict, filename: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return path


def fetch_openmeteo(city: str, lat: float, lon: float) -> dict:
    """
    Fetch 2-day hourly forecast from Open-Meteo for one location.

    Parameters
    ----------
    city : str
        Human-readable city name used in the output filename.
    lat : float
        Latitude (°N).
    lon : float
        Longitude (°E).

    Returns
    -------
    dict
        Raw JSON response from Open-Meteo containing an "hourly" key.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,windspeed_10m,shortwave_radiation",
        "forecast_days": 2,
        "wind_speed_unit": "ms",
    }
    resp = requests.get(OPENMETEO_FORECAST_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    ts = _timestamp()
    path = _save(data, f"openmeteo_{city}_{ts}.json")
    logger.info("Saved Open-Meteo %s → %s", city, path.name)
    return data


def fetch_met_norway(city: str, lat: float, lon: float) -> dict:
    """
    Fetch ~9-day forecast from MET Norway Locationforecast 2.0.

    MET Norway requires a descriptive User-Agent with a contact address per
    their terms of service. Coordinates are rounded to 4 d.p. as the API
    snaps to its internal grid anyway.

    Parameters
    ----------
    city : str
        Human-readable city name used in the output filename.
    lat : float
        Latitude (°N).
    lon : float
        Longitude (°E).

    Returns
    -------
    dict
        Raw JSON response containing a "properties.timeSeries" list.
    """
    headers = {"User-Agent": MET_NORWAY_USER_AGENT}
    params = {"lat": round(lat, 4), "lon": round(lon, 4)}
    resp = requests.get(MET_NORWAY_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    ts = _timestamp()
    path = _save(data, f"metno_{city}_{ts}.json")
    logger.info("Saved MET Norway %s → %s", city, path.name)
    return data


def ingest() -> list[tuple[str, bool, str]]:
    """
    Run weather ingestion for all cities from both sources.

    Returns
    -------
    list[tuple[str, bool, str]]
        Each entry is (source_label, success, error_message).
    """
    results: list[tuple[str, bool, str]] = []
    for city, (lat, lon) in CITIES.items():
        for fetcher, label in [
            (fetch_openmeteo, f"open-meteo {city}"),
            (fetch_met_norway, f"met-norway {city}"),
        ]:
            try:
                fetcher(city, lat, lon)
                results.append((label, True, ""))
            except Exception as exc:
                logger.warning("[FAIL] %s: %s", label, exc)
                results.append((label, False, str(exc)))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for label, ok, err in ingest():
        print(f"[{'OK' if ok else 'FAIL'}] {label}" + (f": {err}" if err else ""))
