from __future__ import annotations

import logging

from elt.collector.config import load_settings
from elt.collector.market_session import MarketSession, MarketSessionConfig
from elt.collector.collectors.yahoo import YahooConfig, YahooMinuteCollector
from elt.collector.storage.db import connect_db, ensure_schema
from elt.collector.storage.writer import insert_rows


def run_once(logger: logging.Logger) -> None:
    """
    One execution unit for Airflow: check session -> fetch -> store -> exit.
    """
    s = load_settings()

    market = MarketSession(
        MarketSessionConfig(
            tz_name="America/New_York",
            market_open=s.market_open,
            market_close=s.market_close,
            grace_period_minutes=s.grace_period_minutes,
        )
    )

    # Airflow will call this every minute; if closed, we just skip quickly.
    if not (market.is_market_open_now() or market.is_in_grace_period()):
        logger.info("Market closed (not in grace period) -> skip.")
        return

    yahoo = YahooMinuteCollector(YahooConfig(symbols=s.symbols))

    conn = connect_db(
        host=s.db_host,
        port=s.db_port,
        dbname=s.db_name,
        user=s.db_user,
        password=s.db_password,
    )
    try:
        ensure_schema(conn, logger)
        rows = yahoo.fetch_rows()
        insert_rows(conn, rows, logger)
    finally:
        conn.close()
