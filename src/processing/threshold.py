"""Dynamic dollar-bar threshold calibration.

Threshold = mean_daily_dollar_volume / target_bars_per_day

A threshold calibrated this way targets ~50-100 bars/day for a liquid
pair like BTCUSDT and remains meaningful as price levels change over time.
"""


def calibrate(mean_daily_dollar_volume: float, target_bars_per_day: int) -> float:
    """Return the dollar threshold that yields *target_bars_per_day* bars/day.

    Parameters
    ----------
    mean_daily_dollar_volume:
        Rolling mean daily dollar volume (e.g. 30-day window from DB).
    target_bars_per_day:
        Desired number of bars per trading day.

    Raises
    ------
    ValueError
        If mean_daily_dollar_volume is zero or negative.
    """
    if mean_daily_dollar_volume <= 0:
        raise ValueError(
            f"mean_daily_dollar_volume must be positive, got {mean_daily_dollar_volume}"
        )
    if target_bars_per_day <= 0:
        raise ValueError(
            f"target_bars_per_day must be positive, got {target_bars_per_day}"
        )
    return mean_daily_dollar_volume / target_bars_per_day
