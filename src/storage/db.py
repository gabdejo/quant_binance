"""TimescaleDB connection and helper queries."""
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from src.config import config


@contextmanager
def get_conn():
    conn = psycopg2.connect(config.db_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_bar(conn, bar: dict) -> None:
    """Insert a completed dollar bar row."""
    sql = """
        INSERT INTO dollar_bars (
            symbol, open_time, close_time,
            open, high, low, close,
            volume, dollar_volume, buy_volume, sell_volume, trade_count,
            ofi, kyle_lambda, realized_vol, duration_s
        ) VALUES (
            %(symbol)s, %(open_time)s, %(close_time)s,
            %(open)s, %(high)s, %(low)s, %(close)s,
            %(volume)s, %(dollar_volume)s, %(buy_volume)s, %(sell_volume)s, %(trade_count)s,
            %(ofi)s, %(kyle_lambda)s, %(realized_vol)s, %(duration_s)s
        )
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, bar)


def save_accumulator_state(conn, symbol: str, state: dict) -> None:
    """Upsert the current accumulator state so live→historical handoff is seamless."""
    sql = """
        INSERT INTO accumulator_state (symbol, state, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (symbol) DO UPDATE
            SET state = EXCLUDED.state,
                updated_at = EXCLUDED.updated_at;
    """
    import json
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, json.dumps(state)))


def load_accumulator_state(conn, symbol: str) -> dict | None:
    """Load persisted accumulator state, returns None if none exists."""
    import json
    with conn.cursor() as cur:
        cur.execute(
            "SELECT state FROM accumulator_state WHERE symbol = %s;",
            (symbol,),
        )
        row = cur.fetchone()
    return json.loads(row[0]) if row else None


def get_vpin(conn, symbol: str, window: int = 50, limit: int = 500) -> list[dict]:
    """Return the last *limit* bars with their rolling VPIN value.

    VPIN = rolling mean of |buy_vol - sell_vol| / total_vol over *window* bars.
    """
    sql = """
        SELECT
            open_time,
            close_time,
            AVG(ABS(buy_volume - sell_volume) / NULLIF(buy_volume + sell_volume, 0))
                OVER (
                    ORDER BY open_time
                    ROWS BETWEEN %s PRECEDING AND CURRENT ROW
                ) AS vpin
        FROM dollar_bars
        WHERE symbol = %s
        ORDER BY open_time DESC
        LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (window - 1, symbol, limit))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_mean_daily_dollar_volume(conn, symbol: str, lookback_days: int = 30) -> float:
    """Compute mean daily dollar volume over the last N days from stored bars."""
    sql = """
        SELECT AVG(daily_dv)
        FROM (
            SELECT DATE_TRUNC('day', open_time) AS day,
                   SUM(dollar_volume)           AS daily_dv
            FROM dollar_bars
            WHERE symbol = %s
              AND open_time >= NOW() - INTERVAL '%s days'
            GROUP BY 1
        ) sub;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, lookback_days))
        row = cur.fetchone()
    return float(row[0]) if row and row[0] else 0.0
