import time
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

import pytz
import yfinance as yf
import pandas_market_calendars as mcal
import psycopg2

# Load environment variables from .env (project root)
load_dotenv()

# ---------------- CONFIG ----------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "market_data"),
    "user": os.getenv("DB_USER", "amir"),
    "password": os.getenv("DB_PASSWORD", ""),
}

LOG_PATH = os.getenv("LOG_PATH", "/opt/market_data/logs/collector.log")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL,MSFT,TSLA,NVDA").split(",") if s.strip()]

NY_TZ = pytz.timezone("America/New_York")
UTC = timezone.utc

MARKET_OPEN = datetime.strptime("09:30", "%H:%M").time()
MARKET_CLOSE = datetime.strptime("16:00", "%H:%M").time()

GRACE_PERIOD_MINUTES = 60
# ----------------------------------------


def log(msg: str):
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def get_db():
    if not DB_CONFIG.get("password"):
        raise RuntimeError("DB_PASSWORD is empty. Set it in .env")
    return psycopg2.connect(**DB_CONFIG)


def ensure_table(conn):
    ddl = """
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
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def market_calendar():
    return mcal.get_calendar("NYSE")


def is_trading_day(date_ny):
    cal = market_calendar()
    sched = cal.schedule(start_date=date_ny, end_date=date_ny)
    return not sched.empty


def now_ny():
    return datetime.now(UTC).astimezone(NY_TZ)


def is_market_open_now():
    ny = now_ny()
    if not is_trading_day(ny.date()):
        return False
    return MARKET_OPEN <= ny.time() < MARKET_CLOSE


def is_in_grace_period():
    ny = now_ny()
    if not is_trading_day(ny.date()):
        return False

    close_naive = datetime.combine(ny.date(), MARKET_CLOSE)
    close_dt = NY_TZ.localize(close_naive)

    return close_dt <= ny < close_dt + timedelta(minutes=GRACE_PERIOD_MINUTES)


def final_minute_fully_collected(conn):
    ny = now_ny()
    if not is_trading_day(ny.date()):
        return False

    final_minute_naive = datetime.combine(ny.date(), MARKET_CLOSE) - timedelta(minutes=1)
    final_minute_local = NY_TZ.localize(final_minute_naive)

    start_utc = final_minute_local.astimezone(UTC)
    end_utc = start_utc + timedelta(minutes=1)

    q = """
    SELECT DISTINCT symbol
    FROM market_data
    WHERE datetime >= %s AND datetime < %s
    """

    with conn.cursor() as cur:
        cur.execute(q, (start_utc, end_utc))
        symbols = {r[0] for r in cur.fetchall()}

    return set(SYMBOLS).issubset(symbols)


def fetch_and_store(conn):
    data = yf.download(
        tickers=SYMBOLS,
        period="1d",
        interval="1m",
        group_by="ticker",
        auto_adjust=False,
        prepost=False,
        progress=False,
        threads=True,
    )

    rows = []
    for sym in SYMBOLS:
        if sym not in data:
            continue
        df = data[sym].dropna()
        for ts, r in df.iterrows():
            dt = ts.to_pydatetime().astimezone(UTC)
            rows.append(
                (
                    sym,
                    dt,
                    float(r["Open"]),
                    float(r["High"]),
                    float(r["Low"]),
                    float(r["Close"]),
                    int(r["Volume"]),
                )
            )

    if not rows:
        log("No rows fetched (possibly market closed or API lag).")
        return

    insert_q = """
    INSERT INTO market_data
    (symbol, datetime, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, datetime) DO NOTHING
    """

    with conn.cursor() as cur:
        cur.executemany(insert_q, rows)
    conn.commit()

    log(f"Inserted {len(rows)} rows")


def sleep_to_next_minute():
    now = datetime.now(UTC)
    time.sleep(max(1, 60 - now.second))


def main():
    log("Collector started")

    while True:
        conn = None
        try:
            conn = get_db()
            ensure_table(conn)

            if is_market_open_now():
                log("Market open → fetching")
                fetch_and_store(conn)

            elif is_in_grace_period():
                if final_minute_fully_collected(conn):
                    log("Final minute collected → sleeping till next trading day")
                    time.sleep(60 * 60 * 6)
                else:
                    log("Grace period → checking missing final minute")
                    fetch_and_store(conn)

            else:
                log("Market closed → sleeping")
                time.sleep(60 * 30)

            conn.close()
            sleep_to_next_minute()

        except Exception as e:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            log(f"ERROR: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
