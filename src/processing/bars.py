"""Dollar bar accumulator.

Consumes a stream of aggTrade dicts and emits completed dollar bars.
State is serialisable so it can be persisted to DB for a seamless
historical → live handoff.
"""
from dataclasses import dataclass, field, asdict
from typing import Iterator


@dataclass
class AccumulatorState:
    symbol: str
    threshold: float          # dollar value that closes a bar

    # running totals
    dollar_volume: float = 0.0
    volume: float = 0.0
    trade_count: int = 0

    # OHLC bookkeeping
    open: float | None = None
    high: float = float("-inf")
    low: float = float("inf")
    close: float | None = None

    # timestamps
    open_time: str | None = None   # ISO string
    close_time: str | None = None  # ISO string

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "AccumulatorState":
        return cls(**d)


def process_trade(state: AccumulatorState, trade: dict) -> dict | None:
    """Feed one aggTrade into the accumulator.

    Parameters
    ----------
    state:
        Mutable accumulator state; updated in-place.
    trade:
        Dict with keys: price (str/float), qty (str/float), timestamp (ms int).

    Returns
    -------
    Completed bar dict if the threshold was crossed, else None.
    """
    price = float(trade["price"])
    qty = float(trade["qty"])
    ts_ms = int(trade["timestamp"])
    dollar_value = price * qty

    # Open the bar on first trade
    if state.open is None:
        state.open = price
        state.open_time = _ms_to_iso(ts_ms)

    state.high = max(state.high, price)
    state.low = min(state.low, price)
    state.close = price
    state.close_time = _ms_to_iso(ts_ms)
    state.volume += qty
    state.dollar_volume += dollar_value
    state.trade_count += 1

    if state.dollar_volume >= state.threshold:
        bar = _emit_bar(state)
        _reset(state)
        return bar

    return None


def _emit_bar(state: AccumulatorState) -> dict:
    return {
        "symbol": state.symbol,
        "open_time": state.open_time,
        "close_time": state.close_time,
        "open": state.open,
        "high": state.high,
        "low": state.low,
        "close": state.close,
        "volume": state.volume,
        "dollar_volume": state.dollar_volume,
        "trade_count": state.trade_count,
    }


def _reset(state: AccumulatorState) -> None:
    state.dollar_volume = 0.0
    state.volume = 0.0
    state.trade_count = 0
    state.open = None
    state.high = float("-inf")
    state.low = float("inf")
    state.close = None
    state.open_time = None
    state.close_time = None


def _ms_to_iso(ts_ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
