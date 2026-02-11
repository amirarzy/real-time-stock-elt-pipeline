from typing import List, Tuple
import logging
from psycopg2.extensions import connection


Row = Tuple[str, object, float, float, float, float, int]


INSERT_QUERY = """
INSERT INTO market_data
(symbol, datetime, open, high, low, close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, datetime) DO NOTHING
"""


def insert_rows(conn: connection, rows: List[Row], logger: logging.Logger) -> None:
    """
    Insert market data rows into database.
    """
    if not rows:
        logger.info("No rows to insert.")
        return

    with conn.cursor() as cur:
        cur.executemany(INSERT_QUERY, rows)

    conn.commit()
    logger.info(f"Inserted {len(rows)} rows.")
