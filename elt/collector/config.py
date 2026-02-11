import os
from dataclasses import dataclass
from datetime import datetime, timezone
import pytz
from dotenv import load_dotenv

# Load environment variables from .env (project root)
load_dotenv()

UTC = timezone.utc
NY_TZ = pytz.timezone("America/New_York")


@dataclass(frozen=True)
class Settings:
    # Database
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # Logging
    log_path: str

    # Symbols
    symbols: list[str]

    # Market session
    market_open: str  # "09:30"
    market_close: str  # "16:00"
    grace_period_minutes: int


def load_settings() -> Settings:
    symbols_raw = os.getenv("SYMBOLS", "AAPL,MSFT,TSLA,NVDA")
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]

    return Settings(
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.getenv("DB_NAME", "market_data"),
        db_user=os.getenv("DB_USER", "amir"),
        db_password=os.getenv("DB_PASSWORD", ""),
        log_path=os.getenv("LOG_PATH", "/opt/market_data/logs/collector.log"),
        symbols=symbols,
        market_open=os.getenv("MARKET_OPEN", "09:30"),
        market_close=os.getenv("MARKET_CLOSE", "16:00"),
        grace_period_minutes=int(os.getenv("GRACE_PERIOD_MINUTES", "60")),
    )
