"""Unit tests for threshold calibration."""
import pytest
from src.processing.threshold import calibrate


def test_basic_calibration():
    # $1B daily volume / 100 bars = $10M per bar
    assert calibrate(1_000_000_000, 100) == pytest.approx(10_000_000.0)


def test_zero_volume_raises():
    with pytest.raises(ValueError):
        calibrate(0.0, 75)


def test_negative_volume_raises():
    with pytest.raises(ValueError):
        calibrate(-500.0, 75)


def test_zero_bars_raises():
    with pytest.raises(ValueError):
        calibrate(1_000_000.0, 0)
