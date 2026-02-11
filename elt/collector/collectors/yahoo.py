from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from typing import List, Tuple
import yfinance as yf

UTC = timezone.utc

Row = Tuple[str, object, float, float, float, float, int]


@dataclass(frozen=True)
class YahooConfig:
    symbols: list[str]


class YahooMinuteCollector:
    """
    Fetches 1-minute OHLCV bars from Yahoo Finance for a list of symbols.
    This class does NOT know anything about databases.
    """

    def __init__(self, cfg: YahooConfig):
        self.cfg = cfg

    def fetch_rows(self) -> List[Row]:
        data = yf.download(
            tickers=self.cfg.symbols,
            period="1d",
            interval="1m",
            group_by="ticker",
            auto_adjust=False,
            prepost=False,
            progress=False,
            threads=True,
        )

        rows: List[Row] = []

        for sym in self.cfg.symbols:
            if sym not in data:
                continue

            df = data[sym].dropna()
            for ts, r in df.iterrows():
                # Convert pandas timestamp to UTC datetime
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

        return rows
