-- Dollar bars hypertable
CREATE TABLE IF NOT EXISTS dollar_bars (
    symbol        TEXT        NOT NULL,
    open_time     TIMESTAMPTZ NOT NULL,
    close_time    TIMESTAMPTZ NOT NULL,
    open          NUMERIC     NOT NULL,
    high          NUMERIC     NOT NULL,
    low           NUMERIC     NOT NULL,
    close         NUMERIC     NOT NULL,
    volume        NUMERIC     NOT NULL,
    dollar_volume NUMERIC     NOT NULL,
    trade_count   INTEGER     NOT NULL,
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
