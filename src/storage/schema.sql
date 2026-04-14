-- Dollar bars hypertable
CREATE TABLE IF NOT EXISTS dollar_bars (
    symbol        TEXT        NOT NULL,
    open_time     TIMESTAMPTZ NOT NULL,
    close_time    TIMESTAMPTZ NOT NULL,

    -- OHLCV
    open          NUMERIC     NOT NULL,
    high          NUMERIC     NOT NULL,
    low           NUMERIC     NOT NULL,
    close         NUMERIC     NOT NULL,
    volume        NUMERIC     NOT NULL,
    dollar_volume NUMERIC     NOT NULL,
    buy_volume    NUMERIC     NOT NULL,
    sell_volume   NUMERIC     NOT NULL,
    trade_count   INTEGER     NOT NULL,

    -- Microstructure features
    -- Order Flow Imbalance: (buy_vol - sell_vol) / total_vol  ∈ [-1, 1]
    ofi           NUMERIC,
    -- Kyle's lambda: OLS slope of ΔP ~ ΔV_signed (price impact per unit signed flow)
    kyle_lambda   NUMERIC,
    -- Realized volatility: sqrt( Σ log(p_i/p_{i-1})² ) over trades in bar
    realized_vol  NUMERIC,
    -- Bar fill duration in seconds
    duration_s    NUMERIC,

    PRIMARY KEY (symbol, open_time)
);

SELECT create_hypertable(
    'dollar_bars', 'open_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

-- Compression policy (compress chunks older than 7 days)
ALTER TABLE dollar_bars SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol'
);
SELECT add_compression_policy('dollar_bars', INTERVAL '7 days', if_not_exists => TRUE);

-- Accumulator state (one row per symbol)
CREATE TABLE IF NOT EXISTS accumulator_state (
    symbol     TEXT        PRIMARY KEY,
    state      JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- VPIN view
-- VPIN (Volume-Synchronized Probability of Informed Trading, Easley et al. 2012)
-- Rolling mean of |buy_vol - sell_vol| / total_vol over the last N bars.
-- N=50 is a common default. Adjust the ROWS BETWEEN clause as needed.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vpin_50 AS
SELECT
    symbol,
    open_time,
    close_time,
    AVG(ABS(buy_volume - sell_volume) / NULLIF(buy_volume + sell_volume, 0))
        OVER (
            PARTITION BY symbol
            ORDER BY open_time
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS vpin
FROM dollar_bars;
