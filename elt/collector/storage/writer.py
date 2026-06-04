from __future__ import annotations

from typing import List, Tuple, Literal
import logging
from psycopg2.extensions import connection

Row = Tuple[str, object, float, float, float, float, int]
ConflictMode = Literal["do_nothing", "do_update"]

INSERT_TEMPLATE = """
INSERT INTO {table}
(symbol, datetime, open, high, low, close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, datetime) {on_conflict}
"""


def insert_rows(
    conn: connection,
    rows: List[Row],
    logger: logging.Logger,
    *,
    table: str = "market_data",
    conflict: ConflictMode = "do_nothing",
) -> None:
    if not rows:
        logger.info("No rows to insert.")
        return

    if conflict == "do_nothing":
        on_conflict = "DO NOTHING"
    elif conflict == "do_update":
        on_conflict = """DO UPDATE SET
            open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
    else:
        raise ValueError(f"Unknown conflict mode: {conflict}")

    query = INSERT_TEMPLATE.format(table=table, on_conflict=on_conflict)

    with conn.cursor() as cur:
        cur.executemany(query, rows)

    conn.commit()
    logger.info(f"Upserted {len(rows)} rows into {table} (conflict={conflict}).")