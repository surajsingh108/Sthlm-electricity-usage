"""
Electricity price ingestion from ENTSO-E Transparency Platform.

Fetches Day-Ahead prices (documentType A44) for the four Swedish bidding zones
(SE1–SE4) covering today and tomorrow in UTC. Falls back to a Nord Pool public
CSV if the ENTSO-E API is unavailable or the key is missing.

ENTSO-E API key is read from the ENTSOE_API_KEY environment variable.
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

# EIC codes for Swedish bidding zones
BIDDING_ZONES: dict[str, str] = {
    "SE1": "10YSE-1-------K",
    "SE2": "10Y1001A1001A45N",
    "SE3": "10Y1001A1001A46L",
    "SE4": "10Y1001A1001A47J",
}

NORDPOOL_FALLBACK_URL = (
    "https://www.nordpoolgroup.com/en/Market-data1/Dayahead/Area-Prices/SE/Hourly/"
)


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


def _save(payload: dict, filename: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return path


def _time_range_utc() -> tuple[str, str]:
    """Return (period_start, period_end) in YYYYMMDDHHmm UTC covering today+tomorrow."""
    now_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = now_utc + timedelta(days=2)
    fmt = "%Y%m%d%H%M"
    return now_utc.strftime(fmt), tomorrow.strftime(fmt)


def _xml_to_dict(xml_text: str, zone: str) -> dict:
    """Parse ENTSO-E XML response into a serialisable dict."""
    try:
        root = ET.fromstring(xml_text)
        ns = {"ns": root.tag.split("}")[0].lstrip("{")} if "}" in root.tag else {}

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

        return {"zone": zone, "raw": elem_to_dict(root)}
    except ET.ParseError:
        return {"zone": zone, "raw": xml_text}


def fetch_entsoe(zone: str, zone_code: str, api_key: str) -> dict:
    """
    Fetch Day-Ahead prices for one Swedish bidding zone from ENTSO-E.

    Parameters
    ----------
    zone : str
        Short zone label, e.g. "SE1".
    zone_code : str
        EIC code for the bidding zone.
    api_key : str
        ENTSO-E API security token.

    Returns
    -------
    dict
        Parsed response dict saved to disk.
    """
    period_start, period_end = _time_range_utc()
    params = {
        "securityToken": api_key,
        "documentType": "A44",
        "in_Domain": zone_code,
        "out_Domain": zone_code,
        "periodStart": period_start,
        "periodEnd": period_end,
    }
    resp = requests.get(ENTSOE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = _xml_to_dict(resp.text, zone)
    ts = _timestamp()
    path = _save(data, f"prices_{zone}_{ts}.json")
    logger.info("Saved ENTSO-E %s → %s", zone, path.name)
    return data


def fetch_nordpool_fallback() -> dict:
    """
    Fetch Nord Pool Day-Ahead price page as a fallback (HTML, not structured data).

    Returns
    -------
    dict
        Minimal dict with the raw HTML content, saved to disk.
    """
    resp = requests.get(NORDPOOL_FALLBACK_URL, timeout=30)
    resp.raise_for_status()
    data = {"source": "nordpool_fallback", "url": NORDPOOL_FALLBACK_URL, "html": resp.text}
    ts = _timestamp()
    path = _save(data, f"prices_nordpool_fallback_{ts}.json")
    logger.info("Saved Nord Pool fallback → %s", path.name)
    return data


def ingest() -> list[tuple[str, bool, str]]:
    """
    Run price ingestion for all SE bidding zones.

    Attempts ENTSO-E first; falls back to Nord Pool CSV page on failure.

    Returns
    -------
    list[tuple[str, bool, str]]
        Each entry is (source_label, success, error_message).
    """
    api_key = os.environ.get("ENTSOE_API_KEY", "")
    results: list[tuple[str, bool, str]] = []

    if not api_key:
        logger.warning("ENTSOE_API_KEY not set — skipping ENTSO-E, trying Nord Pool fallback")
        try:
            fetch_nordpool_fallback()
            results.append(("nordpool fallback", True, ""))
        except Exception as exc:
            logger.warning("[FAIL] nordpool fallback: %s", exc)
            results.append(("nordpool fallback", False, str(exc)))
        return results

    any_entsoe_ok = False
    for zone, zone_code in BIDDING_ZONES.items():
        label = f"entsoe {zone}"
        try:
            fetch_entsoe(zone, zone_code, api_key)
            results.append((label, True, ""))
            any_entsoe_ok = True
        except Exception as exc:
            logger.warning("[FAIL] %s: %s", label, exc)
            results.append((label, False, str(exc)))

    if not any_entsoe_ok:
        logger.warning("All ENTSO-E zones failed — trying Nord Pool fallback")
        try:
            fetch_nordpool_fallback()
            results.append(("nordpool fallback", True, ""))
        except Exception as exc:
            logger.warning("[FAIL] nordpool fallback: %s", exc)
            results.append(("nordpool fallback", False, str(exc)))

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for label, ok, err in ingest():
        print(f"[{'OK' if ok else 'FAIL'}] {label}" + (f": {err}" if err else ""))
