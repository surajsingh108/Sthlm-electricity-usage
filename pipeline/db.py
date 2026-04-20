"""
Database connection helpers.

Reads connection parameters from environment variables (loaded from .env).

- get_conn()   — psycopg2 connection for pipeline write operations
- get_engine() — SQLAlchemy engine for pd.read_sql() in the dashboard
"""
from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

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


def get_engine() -> Engine:
    """
    Return a SQLAlchemy engine using environment config.

    Used by dashboard/queries.py for pd.read_sql() calls.

    Returns
    -------
    sqlalchemy.engine.Engine
    """
    user = os.getenv("DB_USER", "pipeline")
    password = os.getenv("DB_PASSWORD", "pipeline")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "sweden_energy")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )
