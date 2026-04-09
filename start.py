"""
One-command launcher for the Sweden Energy Pipeline.

Usage
-----
    python start.py

What it does (in order):
  1. docker-compose up -d      — starts PostgreSQL
  2. waits for the DB to accept connections (up to 60 s)
  3. python pipeline/runner.py — first pipeline run (ingest + parse + analyse)
  4. python pipeline/scheduler.py — hourly scheduler in a background subprocess
  5. streamlit run dashboard/app.py — dashboard in the foreground

Press Ctrl+C to stop both the dashboard and the scheduler cleanly.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))


def _start_postgres() -> None:
    print("Starting PostgreSQL …")
    result = subprocess.run(
        ["docker-compose", "up", "-d"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr.strip())
        sys.exit(1)
    print("docker-compose: OK")


def _wait_for_db(timeout: int = 60) -> None:
    print("Waiting for database to be ready …", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            from pipeline.db import get_conn
            conn = get_conn()
            conn.close()
            print(" ready.")
            return
        except Exception:
            print(".", end="", flush=True)
            time.sleep(2)
    print()
    print(f"ERROR: database not ready after {timeout}s. Check docker-compose logs.")
    sys.exit(1)


def _run_pipeline() -> None:
    print("Running first pipeline pass (ingest + parse + analyse) …")
    from pipeline.runner import run_with_retry
    result = run_with_retry()
    rows = result.get("rows_upserted", 0)
    print(f"Pipeline: {result['status']} — {rows} rows upserted")


def _start_scheduler() -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "pipeline" / "scheduler.py")],
        cwd=ROOT,
    )
    print(f"Scheduler started (PID {proc.pid}) — pipeline runs every hour.")
    return proc


def _start_dashboard() -> None:
    print("\nDashboard starting → http://localhost:8501")
    print("Press Ctrl+C to stop.\n")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(ROOT / "dashboard" / "app.py")],
        cwd=ROOT,
    )


def main() -> None:
    _start_postgres()
    _wait_for_db()
    _run_pipeline()
    scheduler = _start_scheduler()
    try:
        _start_dashboard()
    finally:
        print("\nStopping scheduler …")
        scheduler.terminate()
        scheduler.wait()
        print("Done.")


if __name__ == "__main__":
    main()
