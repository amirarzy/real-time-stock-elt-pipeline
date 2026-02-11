from __future__ import annotations

import psycopg2
from psycopg2.extensions import connection
import logging


CREATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS market_data (
    symbol TEXT NOT NULL,
    datetime TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (symbol, datetime)
);
"""


def connect_db(*, host: str, port: int, dbname: str, user: str, password: str) -> connection:
    """Create a new Postgres connection."""
    if not password:
        raise RuntimeError("DB_PASSWORD is empty. Set it in .env")
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def ensure_schema(conn: connection, logger: logging.Logger) -> None:
    """Ensure required tables exist."""
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_DDL)
    conn.commit()
    logger.info("DB schema ensured.")
