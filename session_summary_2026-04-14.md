# Session Summary — 2026-04-14
## Project: quant_binance (laptop)

---

## Issues Fixed

### 1. `OSError: [Errno 22] Invalid argument` in `_ms_to_iso`
**File:** `src/processing/bars.py` — `_ms_to_iso()` function  
**Cause:** `datetime.fromtimestamp()` on Windows delegates to the C runtime, which raises `EINVAL` for certain timestamp values.  
**Fix:** Replaced `datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)` with arithmetic using `timedelta`:
```python
datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ts_ms)
```

### 2. `OverflowError: date value out of range` in `_ms_to_iso`
**File:** `src/ingestion/historical.py` — `stream_trades()` function  
**Root cause:** Binance Vision aggTrades CSV files recently switched from **millisecond** to **microsecond** precision in the `transact_time` column (column index 5). A raw timestamp like `1775865600171123` (µs) passed as milliseconds overflowed Python's `timedelta`.  
**Fix:** Integer-divide by 1000 at ingestion time to normalise to ms:
```python
"timestamp": int(row[5]) // 1000,  # Binance Vision uses µs; normalise to ms
```
All downstream code (`bars.py`, `AccumulatorState`, etc.) already expected milliseconds and required no changes.

---

## Verified Working
`python scripts/sample.py --n 3` runs successfully, producing dollar bars with correct UTC timestamps (e.g. `2026-04-11T00:00:00.171000+00:00`).

---

## Repo State at Session End
- Branch: `main`
- Modified files (uncommitted):
  - `src/processing/bars.py` — `_ms_to_iso` fix
  - `src/ingestion/historical.py` — microsecond→millisecond normalisation
- Last commits: `d2bbf49` (microstructural features + sample.py), `b691b27` (initial commit)
