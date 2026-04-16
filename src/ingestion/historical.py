"""Historical ingestion from Binance Vision daily aggTrade ZIPs.

ZIPs are optionally cached on disk (cache_dir) to avoid re-downloading on
repeated runs. Each ZIP contains one CSV with columns: agg_trade_id, price,
qty, first_trade_id, last_trade_id, transact_time, is_buyer_maker.
"""
import io
import zipfile
import csv
import logging
from datetime import date, timedelta
from pathlib import Path
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


def fetch_zip(symbol: str, day: date, cache_dir: Path | None = None) -> bytes | None:
    """Return the raw ZIP bytes for *symbol* on *day*.

    If *cache_dir* is given, the ZIP is saved there on first download and read
    from disk on subsequent calls — no network round-trip needed.

    Returns None if the day has no data (404).
    """
    if cache_dir is not None:
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"{symbol}-aggTrades-{day.isoformat()}.zip"
        if cache_file.exists():
            logger.debug("Cache hit: %s", cache_file)
            return cache_file.read_bytes()

    url = zip_url(symbol, day)
    logger.debug("Fetching %s", url)
    response = requests.get(url, timeout=60)
    if response.status_code == 404:
        logger.warning("No data for %s on %s (404)", symbol, day)
        return None
    response.raise_for_status()

    if cache_dir is not None:
        cache_file.write_bytes(response.content)
        logger.debug("Cached → %s", cache_file)

    return response.content


def stream_trades(
    symbol: str,
    day: date,
    cache_dir: Path | None = None,
) -> Iterator[dict]:
    """Download (or load from cache) and stream aggTrades for *symbol* on *day*.

    Yields dicts with keys: price, qty, timestamp (ms).
    Skips days where data is unavailable (404).

    Parameters
    ----------
    cache_dir:
        Optional directory for caching raw ZIPs. Pass the same path on every
        call and each ZIP is downloaded only once.
    """
    data = fetch_zip(symbol, day, cache_dir=cache_dir)
    if data is None:
        return

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                # agg_trade_id, price, qty, first_trade_id,
                # last_trade_id, transact_time, is_buyer_maker
                yield {
                    "price": row[1],
                    "qty": row[2],
                    "timestamp": int(row[5]) // 1000,  # Binance Vision uses µs; normalise to ms
                }
