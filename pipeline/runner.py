"""
Pipeline job function with retry logic and health logging.

This module is the single callable that the scheduler invokes. It wraps
`run_full_pipeline()` with up to MAX_RETRIES attempts and writes every
outcome to the pipeline_runs table via `write_run_log()`.

Retry behaviour
---------------
- Waits RETRY_DELAY_SECONDS between attempts.
- A partial result (some sources OK) is written immediately and returned —
  only a hard exception triggers a retry.
- If all retries are exhausted, status 'failed' is written.

Usage
-----
    # Manual trigger (no scheduler needed):
    python pipeline/runner.py
"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure repo root is importable when run as a script
sys.path.insert(0, str(Path(__file__).parent.parent))

# File logger — scheduler.py configures its own handler; this one is for
# direct runner.py invocations and is additive (does not replace root logger).
_log_dir = Path(__file__).parent.parent / "logs"
_log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_log_dir / "pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30


def write_run_log(result: dict) -> None:
    """
    Insert one row into pipeline_runs.

    Parameters
    ----------
    result : dict
        Must contain keys: status, sources_ok, sources_total, notes.
        Additional keys are ignored.
    """
    from pipeline.db import get_conn

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (run_at, status, sources_ok, sources_total, notes)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    datetime.now(timezone.utc),
                    result.get("status", "failed"),
                    result.get("sources_ok", 0),
                    result.get("sources_total", 0),
                    result.get("notes"),
                ),
            )
        conn.commit()
        conn.close()
        logger.info(
            "Logged run: status=%s %d/%d sources OK",
            result.get("status"),
            result.get("sources_ok", 0),
            result.get("sources_total", 0),
        )
    except Exception as exc:
        logger.warning("write_run_log: DB write failed: %s", exc)


def run_with_retry() -> dict:
    """
    Run the full pipeline with up to MAX_RETRIES attempts.

    A result dict is written to pipeline_runs after each terminal outcome
    (success, partial, or final failure). Consecutive-failure alerting is
    checked after every successful DB write.

    Returns
    -------
    dict
        The result dict from the last attempt (or a 'failed' sentinel).
    """
    from pipeline.run_pipeline import run_full_pipeline
    from pipeline.alerts import check_consecutive_failures

    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info("Run attempt %d/%d starting", attempt, MAX_RETRIES)
        try:
            result = run_full_pipeline()

            # Partial results are logged and returned immediately — no retry.
            write_run_log(result)
            check_consecutive_failures()
            logger.info(
                "Run complete: status=%s  %d/%d sources OK  %d rows upserted",
                result["status"],
                result["sources_ok"],
                result["sources_total"],
                result.get("rows_upserted", 0),
            )
            return result

        except Exception as exc:
            last_error = exc
            logger.warning("Attempt %d/%d raised: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                logger.info("Retrying in %ds…", RETRY_DELAY_SECONDS)
                time.sleep(RETRY_DELAY_SECONDS)

    # All retries exhausted
    failure_result = {
        "status": "failed",
        "sources_ok": 0,
        "sources_total": 0,
        "rows_upserted": 0,
        "notes": str(last_error),
    }
    write_run_log(failure_result)
    logger.error("All %d attempts failed. Last error: %s", MAX_RETRIES, last_error)
    return failure_result


if __name__ == "__main__":
    print("Running pipeline manually…")
    result = run_with_retry()
    print(
        f"\n[DONE] status={result['status']}"
        f"  {result['sources_ok']}/{result['sources_total']} sources OK"
        f"  {result.get('rows_upserted', 0)} rows upserted"
    )
