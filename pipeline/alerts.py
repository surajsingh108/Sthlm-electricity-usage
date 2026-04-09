"""
Failure detection and alerting.

`check_consecutive_failures()` queries the most recent N rows from
pipeline_runs. If all of them are non-success it logs a CRITICAL message.

This is intentionally kept simple — no external dependencies, no SMTP.
To add email alerting later, extend the block marked with # EXTEND HERE.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def check_consecutive_failures(threshold: int = 2) -> bool:
    """
    Warn if the last `threshold` pipeline runs all have non-success status.

    Parameters
    ----------
    threshold : int
        Number of recent runs to inspect. Default 2.

    Returns
    -------
    bool
        True if a consecutive-failure condition was detected, False otherwise.
    """
    from pipeline.db import get_conn  # deferred to avoid import cycle

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status FROM pipeline_runs
                ORDER BY run_at DESC
                LIMIT %s
                """,
                (threshold,),
            )
            rows = cur.fetchall()
        conn.close()
    except Exception as exc:
        logger.warning("check_consecutive_failures: DB query failed: %s", exc)
        return False

    if len(rows) < threshold:
        return False  # not enough history yet

    if all(row[0] != "success" for row in rows):
        logger.critical(
            "ALERT: %d consecutive pipeline failures detected. "
            "Check logs/pipeline.log for details.",
            threshold,
        )
        # EXTEND HERE: send email via smtplib, post to Slack webhook, etc.
        return True

    return False
