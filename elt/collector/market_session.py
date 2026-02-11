from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import pytz
import pandas_market_calendars as mcal


UTC = timezone.utc


@dataclass(frozen=True)
class MarketSessionConfig:
    tz_name: str
    market_open: str
    market_close: str
    grace_period_minutes: int


class MarketSession:
    """
    Encapsulates market calendar and session logic.
    """

    def __init__(self, cfg: MarketSessionConfig):
        self.cfg = cfg
        self.tz = pytz.timezone(cfg.tz_name)
        self._cal = mcal.get_calendar("NYSE")

        self._open_time = datetime.strptime(cfg.market_open, "%H:%M").time()
        self._close_time = datetime.strptime(cfg.market_close, "%H:%M").time()

    def now_local(self) -> datetime:
        return datetime.now(UTC).astimezone(self.tz)

    def is_trading_day(self, date_local) -> bool:
        sched = self._cal.schedule(start_date=date_local, end_date=date_local)
        return not sched.empty

    def is_market_open_now(self) -> bool:
        now = self.now_local()
        if not self.is_trading_day(now.date()):
            return False
        return self._open_time <= now.time() < self._close_time

    def is_in_grace_period(self) -> bool:
        now = self.now_local()
        if not self.is_trading_day(now.date()):
            return False

        close_naive = datetime.combine(now.date(), self._close_time)
        close_dt = self.tz.localize(close_naive)

        return close_dt <= now < close_dt + timedelta(minutes=self.cfg.grace_period_minutes)

    def final_minute_window_utc(self):
        now = self.now_local()
        if not self.is_trading_day(now.date()):
            return None

        final_minute_naive = datetime.combine(now.date(), self._close_time) - timedelta(minutes=1)
        final_minute_local = self.tz.localize(final_minute_naive)

        start_utc = final_minute_local.astimezone(UTC)
        end_utc = start_utc + timedelta(minutes=1)
        return start_utc, end_utc
