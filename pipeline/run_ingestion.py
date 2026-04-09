"""
Master ingestion runner for the Sweden energy pipeline.

Calls weather, price, and carbon ingestion modules in sequence.
A single source failure does not crash the run — all results are collected
and a summary is printed and appended to data/raw/run_log.txt.

Usage
-----
    python pipeline/run_ingestion.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
LOG_FILE = RAW_DIR / "run_log.txt"


def _write_log(total: int, ok_count: int) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    line = f"{ts} | {ok_count}/{total} OK\n"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)


def run() -> None:
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
    from pipeline import ingest_weather, ingest_prices, ingest_carbon  # deferred import

    all_results: list[tuple[str, bool, str]] = []

    sources = [
        ("weather", ingest_weather.ingest),
        ("prices", ingest_prices.ingest),
        ("carbon", ingest_carbon.ingest),
    ]

    for group_name, ingest_fn in sources:
        try:
            results = ingest_fn()
            all_results.extend(results)
        except Exception as exc:
            logger.error("Unexpected failure in %s ingestion: %s", group_name, exc)
            all_results.append((group_name, False, str(exc)))

    total = len(all_results)
    ok_count = sum(1 for _, ok, _ in all_results if ok)

    print()
    for label, ok, err in all_results:
        tag = "OK" if ok else "FAIL"
        suffix = f": {err}" if err else ""
        print(f"[{tag}] {label}{suffix}")

    print()
    print(f"Ingestion complete: {ok_count}/{total} sources OK")

    _write_log(total, ok_count)

    if ok_count < total:
        sys.exit(1)


if __name__ == "__main__":
    run()
