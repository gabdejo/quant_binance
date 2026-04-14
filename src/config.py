import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    # Database
    db_url: str = field(
        default_factory=lambda: os.environ["DATABASE_URL"]
    )

    # Binance
    symbol: str = field(
        default_factory=lambda: os.getenv("SYMBOL", "BTCUSDT")
    )

    # Dollar bar calibration
    target_bars_per_day: int = field(
        default_factory=lambda: int(os.getenv("TARGET_BARS_PER_DAY", "75"))
    )

    # Historical ingestion
    binance_vision_base_url: str = (
        "https://data.binance.vision/data/spot/daily/aggTrades"
    )


config = Config()
