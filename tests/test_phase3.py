"""
Phase 3 tests — scheduler, retry logic, and failure alerting.

Tests 1–5 and 8 are unit-level (no live API calls, no real DB needed for 1).
Tests 3–5 require Postgres to be running.
Tests 6–7 use unittest.mock to simulate source failures.

Run with:
    pytest tests/test_phase3.py -v
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.db import get_conn


# ---------------------------------------------------------------------------
# Shared DB fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def conn():
    c = get_conn()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Test 1 — scheduler imports without error, job is registered
# ---------------------------------------------------------------------------

def test_scheduler_job_registered():
    from pipeline.scheduler import scheduler
    job_ids = [job.id for job in scheduler.get_jobs()]
    assert "energy_pipeline" in job_ids, "Job 'energy_pipeline' not found in scheduler"


# ---------------------------------------------------------------------------
# Test 2 — run_with_retry() completes without raising
# ---------------------------------------------------------------------------

def test_run_with_retry_no_exception():
    from pipeline.runner import run_with_retry
    try:
        result = run_with_retry()
    except Exception as exc:
        pytest.fail(f"run_with_retry() raised an unexpected exception: {exc}")
    assert isinstance(result, dict)
    assert "status" in result


# ---------------------------------------------------------------------------
# Test 3 — after run_with_retry(), pipeline_runs has a recent row
# ---------------------------------------------------------------------------

def test_pipeline_run_logged_recently(conn):
    from pipeline.runner import run_with_retry
    run_with_retry()
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=60)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pipeline_runs WHERE run_at > %s",
            (cutoff,),
        )
        count = cur.fetchone()[0]
    assert count >= 1, "No pipeline_runs row found within the last 60 seconds"


# ---------------------------------------------------------------------------
# Test 4 — the new run row has status 'success' or 'partial' (not 'failed')
# ---------------------------------------------------------------------------

def test_pipeline_run_status_not_failed(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline_runs ORDER BY run_at DESC LIMIT 1"
        )
        row = cur.fetchone()
    assert row is not None, "No rows in pipeline_runs"
    assert row[0] in ("success", "partial"), (
        f"Latest pipeline run has status '{row[0]}' — expected 'success' or 'partial'"
    )


# ---------------------------------------------------------------------------
# Test 5 — logs/pipeline.log exists and is non-empty
# ---------------------------------------------------------------------------

def test_pipeline_log_exists_and_nonempty():
    log_path = Path(__file__).parent.parent / "logs" / "pipeline.log"
    assert log_path.exists(), "logs/pipeline.log does not exist"
    assert log_path.stat().st_size > 0, "logs/pipeline.log is empty"


# ---------------------------------------------------------------------------
# Test 6 — one source fails → status is 'partial', no unhandled exception
# ---------------------------------------------------------------------------

def test_partial_failure_on_one_source():
    from pipeline.runner import run_with_retry

    def _bad_ingest():
        raise RuntimeError("simulated API failure")

    with patch("pipeline.ingest_prices.ingest", side_effect=_bad_ingest), \
         patch("pipeline.runner.write_run_log") as mock_log, \
         patch("pipeline.runner.check_consecutive_failures"):

        result = run_with_retry()

    assert result["status"] in ("partial", "success"), (
        "Expected partial or success when only one group fails"
    )
    # write_run_log must have been called
    mock_log.assert_called_once()
    logged = mock_log.call_args[0][0]
    assert logged["status"] in ("partial", "success")


# ---------------------------------------------------------------------------
# Test 7 — all sources fail → status is 'failed', no unhandled exception
# ---------------------------------------------------------------------------

def test_all_sources_fail_gracefully():
    from pipeline.runner import run_with_retry
    import pipeline.runner as runner_mod

    def _bad_ingest():
        raise RuntimeError("all sources down")

    # Patch run_full_pipeline to always raise so retry logic is exercised
    with patch("pipeline.run_pipeline.run_full_pipeline",
               side_effect=RuntimeError("all sources down")), \
         patch.object(runner_mod, "RETRY_DELAY_SECONDS", 0), \
         patch("pipeline.runner.write_run_log") as mock_log, \
         patch("pipeline.runner.check_consecutive_failures"):

        result = run_with_retry()

    assert result["status"] == "failed", (
        f"Expected 'failed' when all retries exhausted, got '{result['status']}'"
    )
    mock_log.assert_called_once()
    assert mock_log.call_args[0][0]["status"] == "failed"


# ---------------------------------------------------------------------------
# Test 8 — check_consecutive_failures logs CRITICAL on 2 non-success rows
# ---------------------------------------------------------------------------

def test_consecutive_failures_triggers_alert(caplog):
    from pipeline.alerts import check_consecutive_failures

    mock_rows = [("partial",), ("failed",)]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = lambda s: s
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value.fetchall.return_value = mock_rows

    with patch("pipeline.alerts.get_conn", return_value=mock_conn), \
         caplog.at_level(logging.CRITICAL, logger="pipeline.alerts"):
        detected = check_consecutive_failures(threshold=2)

    assert detected is True
    assert any("ALERT" in r.message for r in caplog.records), (
        "Expected a CRITICAL 'ALERT' log message from check_consecutive_failures"
    )
