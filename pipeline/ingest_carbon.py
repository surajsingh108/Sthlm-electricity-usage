"""
Generation mix ingestion from ENTSO-E Transparency Platform.

Replaces the Electricity Maps carbon-intensity feed with first-party generation
data, giving us full control over the greenness derivation in Phase 4.

Two document types are fetched per Swedish bidding zone (SE1–SE4):

  A75  Actual generation per production type (hourly, realised)
  A69  Wind and solar forecast (day-ahead)

Production type codes relevant to the greenness score:

  B01  Biomass
  B09  Geothermal
  B10  Hydro Pumped Storage
  B11  Hydro Run-of-river
  B12  Hydro Water Reservoir
  B16  Solar
  B18  Wind Offshore
  B19  Wind Onshore
  B20  Nuclear

Phase 4 will derive the score as:

  green_sources = [B11, B12, B16, B18, B19, B20]        # pure renewables
  low_carbon    = green_sources + [B01, B09, B10, B20]   # + nuclear + bio
  greenness     = sum(low_carbon) / sum(all_sources) * 100

Sweden runs ~96 % low-carbon on average, but the score varies hour-by-hour
as wind fluctuates — exactly the signal needed for the dashboard.

Requires ENTSOE_API_KEY from .env (same key used by ingest_prices.py).
"""
from __future__ import annotations

import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

ENTSOE_URL = "https://web-api.tp.entsoe.eu/api"

# SE3 (central Sweden) is the only zone with complete A75/A69 data.
# SE1/SE2/SE4 return "No matching data" from ENTSO-E for these document types.
ZONE = "SE3"
ZONE_CODE = "10Y1001A1001A46L"

# doc_type → (process_type, label)
# A75/A16: Actual generation per production type (realised)
# A69/A01: Wind and solar generation forecast (day-ahead)
DOCUMENT_TYPES: dict[str, tuple[str, str]] = {
    "A75": ("A16", "actual_generation"),
    "A69": ("A01", "wind_solar_forecast"),
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


def _save(payload: dict, filename: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return path


def _time_range_utc(doc_type: str) -> tuple[str, str]:
    """
    Return (period_start, period_end) in YYYYMMDDHHmm UTC.

    A75 (actual): yesterday midnight → end of today  (captures last full day)
    A69 (forecast): start of today → end of tomorrow
    """
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    fmt = "%Y%m%d%H%M"
    if doc_type == "A75":
        return (now - timedelta(days=1)).strftime(fmt), (now + timedelta(days=1)).strftime(fmt)
    else:  # A69
        return now.strftime(fmt), (now + timedelta(days=2)).strftime(fmt)


def _xml_to_dict(xml_text: str, zone: str, doc_type: str, label: str) -> dict:
    """Parse ENTSO-E XML into a serialisable dict, preserving all time-series data."""
    try:
        root = ET.fromstring(xml_text)

        def strip_ns(tag: str) -> str:
            return tag.split("}")[-1] if "}" in tag else tag

        def elem_to_dict(elem: ET.Element) -> dict | str:
            children = list(elem)
            if not children:
                return elem.text or ""
            d: dict = {}
            for child in children:
                key = strip_ns(child.tag)
                val = elem_to_dict(child)
                if key in d:
                    if not isinstance(d[key], list):
                        d[key] = [d[key]]
                    d[key].append(val)
                else:
                    d[key] = val
            return d

        return {
            "zone": zone,
            "document_type": doc_type,
            "label": label,
            "raw": elem_to_dict(root),
        }
    except ET.ParseError:
        return {"zone": zone, "document_type": doc_type, "label": label, "raw": xml_text}


def fetch_generation(doc_type: str, api_key: str) -> dict:
    """
    Fetch one ENTSO-E generation document for SE3.

    Parameters
    ----------
    doc_type : str
        ENTSO-E document type: "A75" (actual) or "A69" (forecast).
    api_key : str
        ENTSO-E API security token.

    Returns
    -------
    dict
        Parsed response dict containing keys: zone, document_type, label, raw.
    """
    process_type, label = DOCUMENT_TYPES[doc_type]
    period_start, period_end = _time_range_utc(doc_type)
    params = {
        "securityToken": api_key,
        "documentType": doc_type,
        "processType": process_type,
        "in_Domain": ZONE_CODE,
        "out_Domain": ZONE_CODE,
        "periodStart": period_start,
        "periodEnd": period_end,
    }
    resp = requests.get(ENTSOE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = _xml_to_dict(resp.text, ZONE, doc_type, label)
    ts = _timestamp()
    filename = f"generation_{label}_{ZONE}_{ts}.json"
    path = _save(data, filename)
    logger.info("Saved ENTSO-E %s %s → %s", label, ZONE, path.name)
    return data


def ingest() -> list[tuple[str, bool, str]]:
    """
    Fetch A75 (actual generation) and A69 (wind/solar forecast) for SE3.

    Returns
    -------
    list[tuple[str, bool, str]]
        Each entry is (source_label, success, error_message).
    """
    api_key = os.environ.get("ENTSOE_API_KEY", "")
    if not api_key:
        msg = "ENTSOE_API_KEY not set"
        logger.warning("[FAIL] entsoe generation: %s", msg)
        return [("entsoe generation", False, msg)]

    results: list[tuple[str, bool, str]] = []
    for doc_type, (_, label) in DOCUMENT_TYPES.items():
        source_label = f"entsoe {label} {ZONE}"
        try:
            fetch_generation(doc_type, api_key)
            results.append((source_label, True, ""))
        except Exception as exc:
            logger.warning("[FAIL] %s: %s", source_label, exc)
            results.append((source_label, False, str(exc)))

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for label, ok, err in ingest():
        print(f"[{'OK' if ok else 'FAIL'}] {label}" + (f": {err}" if err else ""))
