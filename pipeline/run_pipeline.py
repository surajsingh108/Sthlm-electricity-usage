"""
Master pipeline runner — Phase 4 entry point.

Executes in order:
  1. Ingest   — fetch live data from all sources (Phase 1 modules)
  2. Parse    — read raw JSON files and upsert into PostgreSQL (Phase 2 modules)
  3. Analyse  — compute derived features (Phase 4 module)

`run_full_pipeline()` is the importable core used by runner.py (Phase 3).
Running this file directly prints a human-readable summary and exits with
code 1 on any partial failure.

Usage
-----
    python pipeline/run_pipeline.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Ensure the repo root is on the path when run as a script
sys.path.insert(0, str(Path(__file__).parent.parent))


def run_full_pipeline() -> dict:
    """
    Execute ingest + parse in sequence and return a result summary.

    All failures are caught per-source; no exception propagates to the caller.

    Returns
    -------
    dict with keys:
        status       : 'success' | 'partial' | 'failed'
        sources_ok   : int
        sources_total: int
        rows_upserted: int
        notes        : str | None
    """
    from pipeline import ingest_weather, ingest_prices, ingest_carbon
    from pipeline import parse_weather, parse_prices, parse_carbon
    from pipeline import analyse

    # ------------------------------------------------------------------
    # Phase 1 — ingest
    # ------------------------------------------------------------------
    ingest_results: list[tuple[str, bool, str]] = []
    for group, fn in [
        ("weather", ingest_weather.ingest),
        ("prices", ingest_prices.ingest),
        ("carbon", ingest_carbon.ingest),
    ]:
        try:
            ingest_results.extend(fn())
        except Exception as exc:
            logger.error("Ingest group '%s' failed: %s", group, exc)
            ingest_results.append((group, False, str(exc)))

    sources_ok = sum(1 for _, ok, _ in ingest_results if ok)
    sources_total = len(ingest_results)

    # ------------------------------------------------------------------
    # Phase 2 — parse into DB
    # ------------------------------------------------------------------
    total_rows = 0
    parse_errors: list[str] = []
    for name, fn in [
        ("parse_weather", parse_weather.parse),
        ("parse_prices", parse_prices.parse),
        ("parse_carbon", parse_carbon.parse),
    ]:
        try:
            result = fn()
            total_rows += result.get("rows", 0)
            logger.info("[%s] %d rows upserted", name, result.get("rows", 0))
        except Exception as exc:
            logger.error("[%s] failed: %s", name, exc)
            parse_errors.append(f"{name}: {exc}")

    # ------------------------------------------------------------------
    # Phase 4 — feature engineering
    # ------------------------------------------------------------------
    try:
        analysis_result = analyse.run_analysis()
        total_rows += analysis_result.get("rows", 0)
        logger.info("[run_analysis] %d rows upserted into features_hourly", analysis_result.get("rows", 0))
    except Exception as exc:
        logger.error("[run_analysis] failed: %s", exc)
        parse_errors.append(f"run_analysis: {exc}")

    # ------------------------------------------------------------------
    # Determine status
    # ------------------------------------------------------------------
    notes_parts = []
    if parse_errors:
        notes_parts.append("parse errors: " + "; ".join(parse_errors))
    notes_parts.append(f"{total_rows} rows upserted")

    if sources_ok == 0 and sources_total > 0:
        status = "failed"
    elif sources_ok < sources_total or parse_errors:
        status = "partial"
    else:
        status = "success"

    return {
        "status": status,
        "sources_ok": sources_ok,
        "sources_total": sources_total,
        "rows_upserted": total_rows,
        "notes": " | ".join(notes_parts),
        "ingest_results": ingest_results,
    }


def run() -> None:
    """CLI entry point — runs the pipeline and prints a summary."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    result = run_full_pipeline()

    print()
    for label, ok, err in result.get("ingest_results", []):
        print(f"[{'OK' if ok else 'FAIL'}] {label}" + (f": {err}" if err else ""))

    print()
    print(
        f"[DONE] {result['sources_ok']}/{result['sources_total']} sources OK"
        f" | {result['rows_upserted']} rows upserted"
    )

    if result["status"] != "success":
        sys.exit(1)


if __name__ == "__main__":
    run()
