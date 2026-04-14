"""Backfill dollar bars from Binance Vision historical ZIPs.

Usage:
    python scripts/backfill.py --symbol BTCUSDT --start 2024-01-01 --end 2024-12-31
"""
import argparse
import logging
from datetime import date

from src.config import config
from src.ingestion.historical import iter_dates, stream_trades
from src.processing.bars import AccumulatorState, process_trade
from src.processing.threshold import calibrate
from src.storage.db import (
    get_conn,
    save_bar,
    save_accumulator_state,
    load_accumulator_state,
    get_mean_daily_dollar_volume,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=config.symbol)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    args = parser.parse_args()

    symbol: str = args.symbol
    start: date = args.start
    end: date = args.end

    with get_conn() as conn:
        mean_dv = get_mean_daily_dollar_volume(conn, symbol)
        if mean_dv > 0:
            threshold = calibrate(mean_dv, config.target_bars_per_day)
            logger.info("Calibrated threshold: $%.2f", threshold)
        else:
            # Fallback for cold start — will be recalibrated after first day
            threshold = 10_000_000.0
            logger.warning("No existing data; using cold-start threshold: $%.2f", threshold)

        persisted = load_accumulator_state(conn, symbol)
        if persisted:
            state = AccumulatorState.from_dict(persisted)
            state.threshold = threshold
            logger.info("Resumed accumulator state from DB")
        else:
            state = AccumulatorState(symbol=symbol, threshold=threshold)

    bars_total = 0
    for day in iter_dates(start, end):
        logger.info("Processing %s %s", symbol, day)
        day_bars = 0

        with get_conn() as conn:
            for trade in stream_trades(symbol, day):
                bar = process_trade(state, trade)
                if bar:
                    save_bar(conn, bar)
                    day_bars += 1

            save_accumulator_state(conn, symbol, state.to_dict())

        bars_total += day_bars
        logger.info("  %d bars emitted", day_bars)

    logger.info("Backfill complete. Total bars: %d", bars_total)


if __name__ == "__main__":
    main()
