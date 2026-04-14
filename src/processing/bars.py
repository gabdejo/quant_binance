"""Dollar bar accumulator.

Consumes a stream of aggTrade dicts and emits completed dollar bars.
State is serialisable so it can be persisted to DB for a seamless
historical → live handoff.

Microstructure features computed per bar
-----------------------------------------
ofi            Order Flow Imbalance = (buy_vol - sell_vol) / total_vol  ∈ [-1, 1]
kyle_lambda    Price impact per unit signed flow (Kyle 1985): OLS slope ΔP ~ ΔV_signed
realized_vol   sqrt( Σ log(p_i / p_{i-1})² ) over all trades in the bar
duration_s     Bar fill time in seconds (close_time_ms - open_time_ms)

VPIN (rolling order-flow toxicity) is NOT computed here — it is a rolling mean of
|buy_vol - sell_vol| / total_vol over N bars, best expressed as a SQL window
function over stored bar rows. See storage/db.py::get_vpin.
"""
import math
from dataclasses import dataclass, field, asdict


@dataclass
class AccumulatorState:
    symbol: str
    threshold: float          # dollar value that closes a bar

    # --- persists across bar resets (tick-rule continuity) ---
    last_price: float | None = None
    last_side: str | None = None    # 'buy' | 'sell'

    # --- resets on bar close ---
    dollar_volume: float = 0.0
    volume: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    trade_count: int = 0

    # OHLC
    open: float | None = None
    high: float = field(default_factory=lambda: float("-inf"))
    low: float = field(default_factory=lambda: float("inf"))
    close: float | None = None

    # timestamps — ISO strings for output, ms ints for duration arithmetic
    open_time: str | None = None
    open_time_ms: int | None = None
    close_time: str | None = None
    close_time_ms: int | None = None

    # Kyle's lambda: OLS slope of ΔP_i ~ ΔV_signed_i
    kyle_num: float = 0.0    # Σ ΔP_i * V_signed_i
    kyle_den: float = 0.0    # Σ V_signed_i²

    # Realized volatility
    sum_sq_log_ret: float = 0.0    # Σ (log p_i / p_{i-1})²

    def to_dict(self) -> dict:
        d = asdict(self)
        # float('inf') / float('-inf') are not JSON-serialisable
        d["high"] = None if math.isinf(d.get("high") or 0) else d["high"]
        d["low"] = None if math.isinf(d.get("low") or 0) else d["low"]
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "AccumulatorState":
        d = dict(d)
        if d.get("high") is None:
            d["high"] = float("-inf")
        if d.get("low") is None:
            d["low"] = float("inf")
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

    # --- tick rule: classify each trade as buyer- or seller-initiated ---
    if state.last_price is None or price > state.last_price:
        side = "buy"
    elif price < state.last_price:
        side = "sell"
    else:
        side = state.last_side or "buy"   # unchanged price → carry last side

    signed_vol = qty if side == "buy" else -qty

    # --- log return + Kyle's lambda accumulators (need a prior price) ---
    if state.last_price is not None and state.last_price > 0:
        log_ret = math.log(price / state.last_price)
        dp = price - state.last_price
        state.sum_sq_log_ret += log_ret ** 2
        state.kyle_num += dp * signed_vol
        state.kyle_den += signed_vol ** 2

    state.last_price = price
    state.last_side = side

    # --- open the bar on the first trade ---
    if state.open is None:
        state.open = price
        state.open_time = _ms_to_iso(ts_ms)
        state.open_time_ms = ts_ms

    # --- update OHLCV accumulators ---
    state.high = max(state.high, price)
    state.low = min(state.low, price)
    state.close = price
    state.close_time = _ms_to_iso(ts_ms)
    state.close_time_ms = ts_ms
    state.volume += qty
    state.dollar_volume += dollar_value
    state.trade_count += 1

    if side == "buy":
        state.buy_volume += qty
    else:
        state.sell_volume += qty

    if state.dollar_volume >= state.threshold:
        bar = _emit_bar(state)
        _reset(state)
        return bar

    return None


def _emit_bar(state: AccumulatorState) -> dict:
    total_vol = state.buy_volume + state.sell_volume

    ofi = (
        (state.buy_volume - state.sell_volume) / total_vol
        if total_vol > 0 else None
    )
    kyle_lambda = state.kyle_num / state.kyle_den if state.kyle_den > 0 else None
    realized_vol = math.sqrt(state.sum_sq_log_ret) if state.trade_count > 1 else None
    duration_s = (
        (state.close_time_ms - state.open_time_ms) / 1000.0
        if state.open_time_ms is not None and state.close_time_ms is not None
        else None
    )

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
        "buy_volume": state.buy_volume,
        "sell_volume": state.sell_volume,
        "trade_count": state.trade_count,
        "ofi": ofi,
        "kyle_lambda": kyle_lambda,
        "realized_vol": realized_vol,
        "duration_s": duration_s,
    }


def _reset(state: AccumulatorState) -> None:
    # last_price and last_side intentionally NOT reset — tick-rule continuity across bars
    state.dollar_volume = 0.0
    state.volume = 0.0
    state.buy_volume = 0.0
    state.sell_volume = 0.0
    state.trade_count = 0
    state.open = None
    state.high = float("-inf")
    state.low = float("inf")
    state.close = None
    state.open_time = None
    state.open_time_ms = None
    state.close_time = None
    state.close_time_ms = None
    state.kyle_num = 0.0
    state.kyle_den = 0.0
    state.sum_sq_log_ret = 0.0


def _ms_to_iso(ts_ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
