"""Live ingestion via Binance WebSocket aggTrade stream."""
import json
import logging
from typing import Callable

import websocket

logger = logging.getLogger(__name__)

BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"


def on_trade_factory(callback: Callable[[dict], None]) -> Callable:
    """Return a websocket on_message handler that normalises the payload
    and forwards it to *callback* as {price, qty, timestamp}."""

    def on_message(ws, message):
        data = json.loads(message)
        # aggTrade event keys: p=price, q=qty, T=trade time ms
        trade = {
            "price": data["p"],
            "qty": data["q"],
            "timestamp": data["T"],
        }
        callback(trade)

    return on_message


def stream(symbol: str, callback: Callable[[dict], None]) -> None:
    """Open a blocking WebSocket stream for *symbol* aggTrades.

    Each trade is passed to *callback* as {price, qty, timestamp}.
    Reconnects automatically on connection errors.
    """
    stream_name = f"{symbol.lower()}@aggTrade"
    url = f"{BINANCE_WS_BASE}/{stream_name}"

    def on_error(ws, error):
        logger.error("WebSocket error: %s", error)

    def on_close(ws, close_status_code, close_msg):
        logger.warning("WebSocket closed (%s): %s", close_status_code, close_msg)

    def on_open(ws):
        logger.info("WebSocket connected: %s", url)

    ws = websocket.WebSocketApp(
        url,
        on_message=on_trade_factory(callback),
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    ws.run_forever(reconnect=5)
