import sqlite3
import pandas as pd
import pandas_market_calendars as mcal
from pathlib import Path

# ======================
# تنظیمات
# ======================
DB_PATH = "/opt/market_data/data/market.db"
OUT_PATH = "/opt/market_data/data/missing_nasdaq_minutes.csv"

SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA"]
MARKET = "NASDAQ"

# ======================
# اتصال به دیتابیس
# ======================
conn = sqlite3.connect(DB_PATH)

df = pd.read_sql(
    """
    SELECT symbol, datetime
    FROM market_data
    WHERE symbol IN ('AAPL','MSFT','TSLA','NVDA')
    """,
    conn,
    parse_dates=["datetime"]
)

conn.close()

if df.empty:
    raise RuntimeError("Database returned no data")

df["datetime"] = df["datetime"].dt.tz_convert("UTC")

# ======================
# تقویم رسمی NASDAQ
# ======================
cal = mcal.get_calendar(MARKET)

start = df["datetime"].min().date()
end = df["datetime"].max().date()

schedule = cal.schedule(start_date=start, end_date=end)

# ======================
# ساخت دقیقه‌های مجاز بازار
# ======================
market_minutes = []

for _, row in schedule.iterrows():
    minutes = pd.date_range(
        start=row["market_open"],
        end=row["market_close"] - pd.Timedelta(minutes=1),
        freq="1min",
        tz="UTC"
    )
    market_minutes.append(minutes)

market_minutes = pd.DatetimeIndex(
    pd.concat([pd.Series(m) for m in market_minutes])
)

# ======================
# پیدا کردن missing ها
# ======================
missing_rows = []

for symbol in SYMBOLS:
    actual = (
        df[df["symbol"] == symbol]
        .set_index("datetime")
        .index
    )

    missing_idx = market_minutes.difference(actual)

    if len(missing_idx) == 0:
        continue

    missing_df = pd.DataFrame({
        "symbol": symbol,
        "datetime": missing_idx
    })

    missing_rows.append(missing_df)

# ======================
# خروجی CSV
# ======================
if not missing_rows:
    print("✅ No missing market minutes found.")
else:
    result = pd.concat(missing_rows, ignore_index=True)

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUT_PATH, index=False)

    print("✅ DONE")
    print(f"Saved to: {OUT_PATH}")
    print(f"Missing rows: {len(result)}")
