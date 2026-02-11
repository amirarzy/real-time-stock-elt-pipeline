# =====================================================
# CONFIG
# =====================================================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market_data",
    "user": "amir",
    "password": "@A82473054n"
}

TABLE_NAME = "market_data"
SYMBOL = "AAPL"

MARKET_START = "09:30"
MARKET_END   = "16:00"

MAX_GAP = 2              # forward fill فقط برای gapهای کوتاه (دقیقه)
ACF_LAGS = 800            # برای دیتای 1 دقیقه‌ای
SEASONAL_PERIOD = 390     # یک روز معاملاتی (US market)

# =====================================================
# IMPORTS
# =====================================================
import psycopg2
import pandas as pd
import matplotlib

# برای محیط headless (سرور)
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.seasonal import STL

# =====================================================
# LOAD RAW DATA (READ-ONLY)
# =====================================================
print("Connecting to PostgreSQL...")
conn = psycopg2.connect(**DB_CONFIG)

query = f"""
SELECT datetime, close
FROM {TABLE_NAME}
WHERE symbol = %s
ORDER BY datetime
"""

df = pd.read_sql(query, conn, params=(SYMBOL,))
conn.close()

print(f"Loaded {len(df)} rows for {SYMBOL}")

# =====================================================
# BASIC PREPROCESSING
# =====================================================
df["datetime"] = pd.to_datetime(df["datetime"])
df = df.set_index("datetime")

# فقط ساعات کاری (فرض: دیتابیس فقط روزهای کاری دارد)
df = df.between_time(MARKET_START, MARKET_END)

# =====================================================
# HANDLE MISSING TIMESTAMPS (IN MEMORY ONLY)
# =====================================================
# ساخت index دقیقه‌ای پیوسته
full_index = pd.date_range(
    start=df.index.min(),
    end=df.index.max(),
    freq="1min"
)

df = df.reindex(full_index)

# forward fill محدود فقط برای gapهای کوتاه
df["close_filled"] = df["close"].ffill(limit=MAX_GAP)

# حذف gapهای بلند (شب / تعطیلی / قطع طولانی)
df = df.dropna(subset=["close_filled"])

print(f"After continuity handling: {len(df)} rows remain")

# =====================================================
# ACF – MAIN SEASONALITY TEST
# =====================================================
plt.figure(figsize=(12, 4))
plot_acf(
    df["close_filled"],
    lags=ACF_LAGS,
    alpha=0.05
)
plt.title(f"ACF – {SYMBOL} (1-minute, trading hours)")
plt.xlabel("Lag (minutes)")
plt.ylabel("Autocorrelation")
plt.tight_layout()

acf_path = f"acf_{SYMBOL}.png"
plt.savefig(acf_path, dpi=150)
plt.close()

print(f"ACF plot saved to {acf_path}")

# =====================================================
# STL DECOMPOSITION – VISUAL CONFIRMATION
# =====================================================
stl = STL(
    df["close_filled"],
    period=SEASONAL_PERIOD,
    robust=True
)

result = stl.fit()
fig = result.plot()
fig.set_size_inches(12, 8)
plt.tight_layout()

stl_path = f"stl_{SYMBOL}.png"
fig.savefig(stl_path, dpi=150)
plt.close(fig)

print(f"STL plot saved to {stl_path}")

print("✅ Seasonality check completed successfully")
