"""Unit tests for the dollar bar accumulator."""
import pytest
from src.processing.bars import AccumulatorState, process_trade


def make_trade(price: float, qty: float, ts_ms: int = 1_700_000_000_000) -> dict:
    return {"price": str(price), "qty": str(qty), "timestamp": ts_ms}


def test_no_bar_below_threshold():
    state = AccumulatorState(symbol="BTCUSDT", threshold=1_000.0)
    bar = process_trade(state, make_trade(price=10.0, qty=5.0))  # $50
    assert bar is None
    assert state.dollar_volume == pytest.approx(50.0)


def test_bar_emitted_at_threshold():
    state = AccumulatorState(symbol="BTCUSDT", threshold=100.0)
    process_trade(state, make_trade(price=10.0, qty=5.0))   # $50
    bar = process_trade(state, make_trade(price=10.0, qty=5.0))  # $50 → crosses
    assert bar is not None
    assert bar["dollar_volume"] == pytest.approx(100.0)
    assert bar["trade_count"] == 2


def test_state_resets_after_bar():
    state = AccumulatorState(symbol="BTCUSDT", threshold=100.0)
    process_trade(state, make_trade(price=10.0, qty=10.0))  # $100 → bar
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
