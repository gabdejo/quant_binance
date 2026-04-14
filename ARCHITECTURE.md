# Architecture

## Goal
Research pipeline for Binance spot tick data. Builds dollar bars from `aggTrade` events,
stores them in TimescaleDB, and supports both historical backfill and live streaming with
a seamless handoff between the two.

## Stack
| Layer | Choice | Reason |
|---|---|---|
| Data source | Binance Vision ZIPs + WebSocket | Same `aggTrade` schema for both historical and live |
| Bar type | Dollar bars | More stationary than time bars; López de Prado backing |
| Storage | TimescaleDB (PostgreSQL) | Hypertable compression, time-partitioning, plain SQL |
| Language | Python 3.11+ | dataclasses, `match`, fast iteration |

## Folder structure
```
quant_binance/
├── src/
│   ├── config.py               # Env-based config (DATABASE_URL, SYMBOL, ...)
│   ├── ingestion/
│   │   ├── historical.py       # Stream Binance Vision ZIPs in-memory
│   │   └── live.py             # WebSocket aggTrade listener
│   ├── processing/
│   │   ├── bars.py             # Dollar bar accumulator (serialisable state)
│   │   └── threshold.py        # Dynamic threshold calibration
│   └── storage/
│       ├── db.py               # psycopg2 helpers (save_bar, accumulator state)
│       └── schema.sql          # CREATE TABLE + hypertable + compression policy
├── scripts/
│   ├── backfill.py             # CLI: ingest historical range
│   └── stream.py               # CLI: live WebSocket ingestion
├── tests/
│   ├── test_bars.py
│   └── test_threshold.py
├── notebooks/                  # Research notebooks
├── .env.example
├── pyproject.toml
└── requirements.txt
```

## Key design decisions

### Dollar bars over time/volume bars
Dollar bars sample by traded dollar value rather than elapsed time or share count.
This produces approximately i.i.d. returns and reduces heteroscedasticity — see
López de Prado, *Advances in Financial Machine Learning*, Chapter 2.

### Dynamic threshold calibration
```
threshold = mean_daily_dollar_volume / target_bars_per_day
```
Recalibrated from a 30-day rolling window stored in `dollar_bars`. Targets ~75 bars/day
for BTCUSDT by default. Adjust `TARGET_BARS_PER_DAY` in `.env`.

### Seamless historical → live handoff
The accumulator state (`AccumulatorState`) is serialised to JSON and upserted into
`accumulator_state` after every processed day (backfill) or every completed bar (live).
The live script loads this state on startup so there is no seam gap between the two modes.

### In-memory ZIP streaming
Historical ZIPs are fetched with `requests` and decompressed with `zipfile` entirely in
memory. No temp files are written to disk.

## Database schema

### `dollar_bars`
| Column | Type | Notes |
|---|---|---|
| symbol | TEXT | e.g. `BTCUSDT` |
| open_time | TIMESTAMPTZ | Hypertable partition key |
| close_time | TIMESTAMPTZ | |
| open/high/low/close | NUMERIC | |
| volume | NUMERIC | Base asset volume |
| dollar_volume | NUMERIC | Quote asset volume |
| trade_count | INTEGER | aggTrades in this bar |

Hypertable partitioned by day. Compressed after 7 days, segmented by `symbol`.

### `accumulator_state`
| Column | Type | Notes |
|---|---|---|
| symbol | TEXT PRIMARY KEY | |
| state | JSONB | Serialised `AccumulatorState` |
| updated_at | TIMESTAMPTZ | |

## Usage

```bash
# Apply schema
psql $DATABASE_URL -f src/storage/schema.sql

# Backfill
python scripts/backfill.py --symbol BTCUSDT --start 2024-01-01 --end 2024-12-31

# Live stream
python scripts/stream.py --symbol BTCUSDT

# Tests
pytest
```
