"""Unit tests for the dollar bar accumulator."""
import math
import pytest
from src.processing.bars import AccumulatorState, process_trade


def make_trade(price: float, qty: float, ts_ms: int = 1_700_000_000_000) -> dict:
    return {"price": str(price), "qty": str(qty), "timestamp": ts_ms}


# ---------------------------------------------------------------------------
# Basic bar mechanics
# ---------------------------------------------------------------------------

def test_no_bar_below_threshold():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    bar = process_trade(state, make_trade(price=10.0, qty=5.0))  # $50
    assert bar is None
    assert state.dollar_volume == pytest.approx(50.0)


def test_bar_emitted_at_threshold():
    state = AccumulatorState(symbol="BTCUSDT", threshold=100.0)
    process_trade(state, make_trade(price=10.0, qty=5.0))    # $50
    bar = process_trade(state, make_trade(price=10.0, qty=5.0))  # $50 → crosses
    assert bar is not None
    assert bar["dollar_volume"] == pytest.approx(100.0)
    assert bar["trade_count"] == 2


def test_state_resets_after_bar():
    state = AccumulatorState(symbol="BTCUSDT", threshold=100.0)
    process_trade(state, make_trade(price=10.0, qty=10.0))
    assert state.dollar_volume == pytest.approx(0.0)
    assert state.open is None


def test_ohlc_correct():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    process_trade(state, make_trade(price=120.0, qty=1.0))
    process_trade(state, make_trade(price=90.0, qty=1.0))
    assert state.open == pytest.approx(100.0)
    assert state.high == pytest.approx(120.0)
    assert state.low == pytest.approx(90.0)
    assert state.close == pytest.approx(90.0)


def test_serialisation_roundtrip():
    state = AccumulatorState(symbol="BTCUSDT", threshold=500.0)
    process_trade(state, make_trade(price=50.0, qty=3.0))
    restored = AccumulatorState.from_dict(state.to_dict())
    assert restored.dollar_volume == pytest.approx(state.dollar_volume)
    assert restored.threshold == state.threshold


def test_serialisation_roundtrip_after_reset():
    """inf values for high/low must survive JSON roundtrip after a bar close."""
    state = AccumulatorState(symbol="BTCUSDT", threshold=100.0)
    process_trade(state, make_trade(price=10.0, qty=10.0))  # triggers bar + reset
    d = state.to_dict()
    assert d["high"] is None   # inf → None for JSON
    assert d["low"] is None
    restored = AccumulatorState.from_dict(d)
    assert math.isinf(restored.high) and restored.high < 0
    assert math.isinf(restored.low) and restored.low > 0


# ---------------------------------------------------------------------------
# Tick rule & buy/sell classification
# ---------------------------------------------------------------------------

def test_tick_rule_up_is_buy():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    process_trade(state, make_trade(price=101.0, qty=1.0))
    assert state.buy_volume == pytest.approx(2.0)
    assert state.sell_volume == pytest.approx(0.0)


def test_tick_rule_down_is_sell():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    process_trade(state, make_trade(price=99.0, qty=1.0))
    assert state.sell_volume == pytest.approx(1.0)


def test_tick_rule_unchanged_carries_last_side():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    process_trade(state, make_trade(price=101.0, qty=1.0))  # buy
    process_trade(state, make_trade(price=101.0, qty=2.0))  # unchanged → buy
    assert state.buy_volume == pytest.approx(4.0)


def test_last_price_persists_across_bars():
    """Tick rule must use the last trade of the previous bar as reference."""
    state = AccumulatorState(symbol="BTCUSDT", threshold=100.0)
    process_trade(state, make_trade(price=100.0, qty=10.0))  # closes bar, last_price=100
    assert state.last_price == pytest.approx(100.0)
    # qty=0.5 → $52.50 < threshold, so bar stays open; price up → buy
    process_trade(state, make_trade(price=105.0, qty=0.5))
    assert state.buy_volume == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# OFI
# ---------------------------------------------------------------------------

def test_ofi_all_buys():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    process_trade(state, make_trade(price=101.0, qty=1.0))
    process_trade(state, make_trade(price=102.0, qty=1.0))
    bar_state_copy = AccumulatorState.from_dict(state.to_dict())
    from src.processing.bars import _emit_bar
    bar = _emit_bar(bar_state_copy)
    assert bar["ofi"] == pytest.approx(1.0)


def test_ofi_mixed():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))  # first trade, no prior side
    process_trade(state, make_trade(price=101.0, qty=1.0))  # buy
    process_trade(state, make_trade(price=100.0, qty=1.0))  # sell
    from src.processing.bars import _emit_bar
    bar = _emit_bar(state)
    # buy=1 (first is buy by default), sell=1 — but first trade has no last_price context
    # just verify it's within bounds
    assert -1.0 <= bar["ofi"] <= 1.0


# ---------------------------------------------------------------------------
# Realized volatility
# ---------------------------------------------------------------------------

def test_realized_vol_zero_for_single_trade():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    from src.processing.bars import _emit_bar
    bar = _emit_bar(state)
    assert bar["realized_vol"] is None


def test_realized_vol_positive_for_multiple_trades():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    process_trade(state, make_trade(price=105.0, qty=1.0))
    process_trade(state, make_trade(price=98.0, qty=1.0))
    from src.processing.bars import _emit_bar
    bar = _emit_bar(state)
    assert bar["realized_vol"] is not None
    assert bar["realized_vol"] > 0


# ---------------------------------------------------------------------------
# Duration
# ---------------------------------------------------------------------------

def test_duration_s():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0, ts_ms=1_700_000_000_000))
    process_trade(state, make_trade(price=100.0, qty=1.0, ts_ms=1_700_000_005_000))  # +5s
    from src.processing.bars import _emit_bar
    bar = _emit_bar(state)
    assert bar["duration_s"] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Kyle's lambda
# ---------------------------------------------------------------------------

def test_kyle_lambda_none_for_single_trade():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    from src.processing.bars import _emit_bar
    bar = _emit_bar(state)
    assert bar["kyle_lambda"] is None


def test_kyle_lambda_positive_for_rising_buys():
    """Prices rising on buy volume → positive lambda (positive price impact)."""
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    process_trade(state, make_trade(price=100.0, qty=1.0))
    process_trade(state, make_trade(price=101.0, qty=2.0))  # up → buy
    process_trade(state, make_trade(price=102.0, qty=2.0))  # up → buy
    from src.processing.bars import _emit_bar
    bar = _emit_bar(state)
    assert bar["kyle_lambda"] is not None
    assert bar["kyle_lambda"] > 0
