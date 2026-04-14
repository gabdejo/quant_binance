"""Run the live WebSocket aggTrade stream and emit dollar bars in real time.

Usage:
    python scripts/stream.py --symbol BTCUSDT
"""
import argparse
import logging

from src.config import config
from src.ingestion.live import stream
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

_state: AccumulatorState | None = None


def handle_trade(trade: dict) -> None:
    assert _state is not None
    bar = process_trade(_state, trade)
    if bar:
        with get_conn() as conn:
            save_bar(conn, bar)
            save_accumulator_state(conn, _state.symbol, _state.to_dict())
        logger.info("Bar closed: %s", bar)


def main():
    global _state

    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=config.symbol)
    args = parser.parse_args()
    symbol: str = args.symbol

    with get_conn() as conn:
        mean_dv = get_mean_daily_dollar_volume(conn, symbol)
        threshold = calibrate(mean_dv, config.target_bars_per_day) if mean_dv > 0 else 10_000_000.0
        logger.info("Threshold: $%.2f", threshold)

        persisted = load_accumulator_state(conn, symbol)
        if persisted:
            _state = AccumulatorState.from_dict(persisted)
            _state.threshold = threshold
            logger.info("Resumed accumulator state from DB")
        else:
            _state = AccumulatorState(symbol=symbol, threshold=threshold)

    logger.info("Starting live stream for %s", symbol)
    stream(symbol, handle_trade)


if __name__ == "__main__":
    main()
