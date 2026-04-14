"""Historical ingestion from Binance Vision daily aggTrade ZIPs.

ZIPs are streamed in-memory (no temp files). Each ZIP contains one CSV
with columns: agg_trade_id, price, qty, first_trade_id, last_trade_id,
transact_time, is_buyer_maker.
"""
import io
import zipfile
import csv
import logging
from datetime import date, timedelta
from typing import Iterator

import requests

from src.config import config

logger = logging.getLogger(__name__)


def iter_dates(start: date, end: date) -> Iterator[date]:
    """Yield each date from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def zip_url(symbol: str, day: date) -> str:
    """Build the Binance Vision URL for a given symbol and date."""
    filename = f"{symbol}-aggTrades-{day.isoformat()}.zip"
    return f"{config.binance_vision_base_url}/{symbol}/{filename}"


def stream_trades(symbol: str, day: date) -> Iterator[dict]:
    """Download and stream aggTrades for *symbol* on *day*.

    Yields dicts with keys: price, qty, timestamp (ms).
    Skips days where data is unavailable (404).
    """
    url = zip_url(symbol, day)
    logger.debug("Fetching %s", url)

    response = requests.get(url, stream=True, timeout=60)
    if response.status_code == 404:
        logger.warning("No data for %s on %s (404)", symbol, day)
        return
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                # agg_trade_id, price, qty, first_trade_id,
                # last_trade_id, transact_time, is_buyer_maker
                yield {
                    "price": row[1],
                    "qty": row[2],
                    "timestamp": int(row[5]),
                }
