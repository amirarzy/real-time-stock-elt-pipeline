import time
import sqlite3
from datetime import datetime, timezone
import yfinance as yf

DB_PATH = "data/market.db"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA"]
INTERVAL = "1m"
PERIOD = "7d"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT,
            datetime TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (symbol, datetime)
        )
    """)
    conn.commit()
    conn.close()

def fetch_and_store():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for symbol in SYMBOLS:
        df = yf.download(
            symbol,
            interval=INTERVAL,
            period=PERIOD,
            progress=False
        )

        if df.empty:
            print(f"[{symbol}] No data")
            continue

        for ts, row in df.iterrows():
            dt = ts.tz_convert(timezone.utc).isoformat()

            cur.execute("""
                INSERT OR IGNORE INTO market_data
                (symbol, datetime, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                dt,
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                int(row["Volume"])
            ))

        print(f"[{symbol}] stored {len(df)} rows")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    fetch_and_store()
