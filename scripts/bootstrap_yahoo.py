import yfinance as yf
import pandas as pd

SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA"]
INTERVAL = "1m"
PERIOD = "7d"

def fetch_symbol(symbol: str) -> pd.DataFrame:
    print(f"Fetching {symbol} ...")

    df = yf.download(
        tickers=symbol,
        interval=INTERVAL,
        period=PERIOD,
        auto_adjust=False,
        progress=False,
        threads=False
    )

    if df.empty:
        print(f"⚠️ No data for {symbol}")
        return df

    df = df.reset_index()

    df.columns = [
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume"
    ]

    df["symbol"] = symbol
    df["source"] = "yahoo"

    print(
        f"{symbol} | rows={len(df)} | "
        f"{df['datetime'].min()} → {df['datetime'].max()}"
    )

    return df


def main():
    all_data = []

    for symbol in SYMBOLS:
        df = fetch_symbol(symbol)
        if not df.empty:
            all_data.append(df)

    if not all_data:
        print("❌ No data fetched")
        return

    final_df = pd.concat(all_data, ignore_index=True)

    print("\n=== SUMMARY ===")
    print("Total rows:", len(final_df))
    print(final_df.groupby("symbol").size())

    # فعلاً فقط نمایش
    return final_df


if __name__ == "__main__":
    main()
