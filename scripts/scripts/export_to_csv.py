import sqlite3
import pandas as pd
from pathlib import Path

# مسیرها
DB_PATH = Path("data/market.db")
OUT_PATH = Path("data/market_data_full.csv")

# اتصال به دیتابیس
conn = sqlite3.connect(DB_PATH)

# خواندن کل جدول
df = pd.read_sql(
    """
    SELECT *
    FROM market_data
    ORDER BY symbol, datetime
    """,
    conn
)

conn.close()

# ذخیره به CSV
df.to_csv(OUT_PATH, index=False)

print("✅ DONE")
print(f"Rows exported: {len(df)}")
print(f"Saved to: {OUT_PATH.resolve()}")
