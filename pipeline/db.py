"""
Database connection helper.

Reads connection parameters from environment variables (loaded from .env).
All callers should use `get_conn()` and close the connection when done.
"""
from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_conn() -> psycopg2.extensions.connection:
    """
    Open and return a psycopg2 connection using environment config.

    Returns
    -------
    psycopg2.extensions.connection
        Open database connection. Caller is responsible for closing.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "sweden_energy"),
        user=os.getenv("DB_USER", "pipeline"),
        password=os.getenv("DB_PASSWORD", "pipeline"),
    )
