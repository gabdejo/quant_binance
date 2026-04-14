"""Download one day of aggTrades and print the first N dollar bars.

No database required — useful for a quick sanity check.

Usage:
    python scripts/sample.py                          # defaults
    python scripts/sample.py --symbol ETHUSDT --date 2025-03-01 --n 20
"""
import argparse
from datetime import date, timedelta

from src.ingestion.historical import stream_trades
from src.processing.bars import AccumulatorState, process_trade
from src.processing.threshold import calibrate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=3),   # 3-day lag is safe for Binance Vision
        help="YYYY-MM-DD (default: 3 days ago)",
    )
    parser.add_argument("--n", type=int, default=10, help="Number of bars to print")
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Dollar threshold per bar. Omit to auto-calibrate from the day's volume.",
    )
    args = parser.parse_args()

    print(f"Fetching {args.symbol} aggTrades for {args.date} …")

    # --- pass 1: scan the day once to get total dollar volume for calibration ---
    if args.threshold is None:
        total_dv = sum(
            float(t["price"]) * float(t["qty"])
            for t in stream_trades(args.symbol, args.date)
        )
        threshold = calibrate(total_dv, target_bars_per_day=75)
        print(f"Daily dollar volume : ${total_dv:,.0f}")
        print(f"Auto threshold      : ${threshold:,.0f}  (75 bars/day target)\n")
    else:
        threshold = args.threshold
        print(f"Threshold           : ${threshold:,.0f}\n")

    # --- pass 2: process trades and collect first N bars ---
    state = AccumulatorState(symbol=args.symbol, threshold=threshold)
    bars = []

    for trade in stream_trades(args.symbol, args.date):
        bar = process_trade(state, trade)
        if bar:
            bars.append(bar)
            if len(bars) >= args.n:
                break

    # --- print table ---
    if not bars:
        print("No bars produced — try a lower --threshold.")
        return

    cols = ["open_time", "open", "high", "low", "close",
            "dollar_volume", "ofi", "kyle_lambda", "realized_vol", "duration_s"]
    widths = [26, 10, 10, 10, 10, 16, 7, 12, 13, 11]

    header = "  ".join(c.ljust(w) for c, w in zip(cols, widths))
    print(header)
    print("-" * len(header))

    for b in bars:
        def fmt(key, w):
            v = b.get(key)
            if v is None:
                return "None".ljust(w)
            if isinstance(v, float):
                if key in ("ofi",):
                    return f"{v:+.4f}".ljust(w)
                if key in ("kyle_lambda",):
                    return f"{v:.6f}".ljust(w)
                if key in ("realized_vol",):
                    return f"{v:.6f}".ljust(w)
                if key in ("duration_s",):
                    return f"{v:.1f}s".ljust(w)
                return f"{v:,.2f}".ljust(w)
            return str(v).ljust(w)

        row = "  ".join(fmt(c, w) for c, w in zip(cols, widths))
        print(row)

    print(f"\n{len(bars)} bars shown.")


if __name__ == "__main__":
    main()
