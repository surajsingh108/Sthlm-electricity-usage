"""
Phase 1 integration tests.

These tests assume that `python pipeline/run_ingestion.py` has already been
executed so that data/raw/ is populated. They validate file presence, JSON
validity, expected response shapes, log correctness, and absence of leaked
API keys.

Run with:
    pytest tests/test_phase1.py -v
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

# Regex that matches common API key patterns (long alphanumeric tokens)
_API_KEY_PATTERN = re.compile(r"[A-Za-z0-9_\-]{32,}")


# ---------------------------------------------------------------------------
# Test 1 — raw directory exists
# ---------------------------------------------------------------------------

def test_raw_dir_exists() -> None:
    assert RAW_DIR.is_dir(), f"data/raw/ directory not found at {RAW_DIR}"


# ---------------------------------------------------------------------------
# Test 2 — at least one file per source type
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("prefix", ["openmeteo_", "metno_", "prices_", "generation_"])
def test_source_files_exist(prefix: str) -> None:
    matches = list(RAW_DIR.glob(f"{prefix}*.json"))
    assert matches, f"No files matching '{prefix}*.json' found in {RAW_DIR}"


# ---------------------------------------------------------------------------
# Test 3 — every JSON file is valid JSON
# ---------------------------------------------------------------------------

def test_all_json_files_valid() -> None:
    json_files = list(RAW_DIR.glob("*.json"))
    assert json_files, "No JSON files found in data/raw/"
    for path in json_files:
        try:
            with open(path, encoding="utf-8") as f:
                json.load(f)
        except json.JSONDecodeError as exc:
            pytest.fail(f"{path.name} is not valid JSON: {exc}")


# ---------------------------------------------------------------------------
# Test 4 — Open-Meteo response contains "hourly"
# ---------------------------------------------------------------------------

def test_openmeteo_has_hourly() -> None:
    files = list(RAW_DIR.glob("openmeteo_*.json"))
    assert files, "No openmeteo_*.json files found"
    for path in files:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        assert "hourly" in data, f"{path.name} missing 'hourly' key"


# ---------------------------------------------------------------------------
# Test 5 — MET Norway response contains "properties" with "timeseries"
# ---------------------------------------------------------------------------

def test_metno_has_timeseries() -> None:
    files = list(RAW_DIR.glob("metno_*.json"))
    assert files, "No metno_*.json files found"
    for path in files:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        assert "properties" in data, f"{path.name} missing 'properties' key"
        assert "timeseries" in data["properties"], f"{path.name} missing 'properties.timeseries' key"


# ---------------------------------------------------------------------------
# Test 6 — ENTSO-E generation files contain zone, document_type, and raw data
# ---------------------------------------------------------------------------

def test_generation_has_required_keys() -> None:
    files = list(RAW_DIR.glob("generation_*.json"))
    assert files, "No generation_*.json files found"
    for path in files:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for key in ("zone", "document_type", "raw"):
            assert key in data, f"{path.name} missing '{key}' key"


# ---------------------------------------------------------------------------
# Test 7 — run_log.txt exists and last line contains "OK"
# ---------------------------------------------------------------------------

def test_run_log_exists_and_ok() -> None:
    log_path = RAW_DIR / "run_log.txt"
    assert log_path.is_file(), "run_log.txt not found"
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert lines, "run_log.txt is empty"
    assert "OK" in lines[-1], f"Last log line does not contain 'OK': {lines[-1]!r}"


# ---------------------------------------------------------------------------
# Test 8 — no raw file contains a bare API key pattern in a sensitive field
# ---------------------------------------------------------------------------

def test_no_api_keys_leaked() -> None:
    """
    Checks that no raw JSON file stores a value that looks like an API key
    under field names associated with credentials.
    """
    sensitive_fields = re.compile(
        r'"(api_key|apikey|token|secret|password|auth[_-]?token)"'
        r'\s*:\s*"([A-Za-z0-9_\-]{20,})"',
        re.IGNORECASE,
    )
    json_files = list(RAW_DIR.glob("*.json"))
    for path in json_files:
        content = path.read_text(encoding="utf-8")
        match = sensitive_fields.search(content)
        if match:
            pytest.fail(
                f"{path.name} appears to contain a credential in field "
                f"'{match.group(1)}': value starts with "
                f"'{match.group(2)[:6]}...'"
            )
