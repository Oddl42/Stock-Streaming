#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:43:14 2026

@author: twi
"""

"""
Test-Daten Generatoren für verschiedene Szenarien.
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone


def generate_aggregate_message(
    symbol: str = "AAPL",
    event_type: str = "A",
    base_price: float = 185.0,
    volume: int = 5000,
    timestamp_ms: int = None,
) -> dict:
    """Generiert eine einzelne Aggregate-Nachricht."""
    if timestamp_ms is None:
        timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    noise = np.random.randn() * 0.5
    open_p = base_price + noise
    close_p = base_price + noise + np.random.randn() * 0.3
    high_p = max(open_p, close_p) + abs(np.random.randn() * 0.2)
    low_p = min(open_p, close_p) - abs(np.random.randn() * 0.2)

    duration = 1000 if event_type == "A" else 60000

    return {
        "ev": event_type,
        "sym": symbol,
        "v": volume,
        "av": volume * 1000,
        "op": round(base_price, 2),
        "vw": round((open_p + close_p) / 2, 4),
        "o": round(open_p, 2),
        "c": round(close_p, 2),
        "h": round(high_p, 2),
        "l": round(low_p, 2),
        "a": round(base_price + 0.1, 4),
        "z": 100,
        "s": timestamp_ms - duration,
        "e": timestamp_ms,
        "n": np.random.randint(10, 500),
        "otc": False,
    }


def generate_batch_messages(
    symbols: list[str] = None,
    event_type: str = "A",
    count_per_symbol: int = 10,
) -> list[dict]:
    """Generiert einen Batch von Nachrichten für mehrere Symbole."""
    if symbols is None:
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

    messages = []
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    prices = {"AAPL": 185, "MSFT": 378, "GOOGL": 141, "AMZN": 178, "NVDA": 875}

    for symbol in symbols:
        base = prices.get(symbol, 100.0)
        for i in range(count_per_symbol):
            ts = now_ms - (count_per_symbol - i) * 1000
            msg = generate_aggregate_message(
                symbol=symbol,
                event_type=event_type,
                base_price=base,
                timestamp_ms=ts,
            )
            messages.append(msg)

    return messages


def generate_kafka_message_bytes(
    symbol: str = "AAPL",
    event_type: str = "A",
) -> tuple[bytes, bytes]:
    """Generiert Key/Value Bytes für Kafka."""
    msg = generate_aggregate_message(symbol=symbol, event_type=event_type)
    key = symbol.encode("utf-8")
    value = json.dumps(msg).encode("utf-8")
    return key, value


def generate_ohlcv_dataframe(
    symbol: str = "AAPL",
    points: int = 100,
    freq: str = "1s",
    base_price: float = 185.0,
) -> pd.DataFrame:
    """Generiert einen OHLCV DataFrame für Chart-Tests."""
    np.random.seed(42)
    end_time = datetime.now(timezone.utc)
    delta = timedelta(seconds=1) if freq == "1s" else timedelta(minutes=1)
    times = [end_time - delta * i for i in range(points, 0, -1)]

    noise = np.cumsum(np.random.randn(points) * 0.5)
    prices = base_price + noise

    return pd.DataFrame({
        "time": times,
        "symbol": [symbol] * points,
        "open": np.round(prices + np.random.randn(points) * 0.2, 2),
        "high": np.round(prices + abs(np.random.randn(points) * 0.5), 2),
        "low": np.round(prices - abs(np.random.randn(points) * 0.5), 2),
        "close": np.round(prices, 2),
        "volume": np.random.randint(1000, 100000, points),
        "vwap": np.round(prices + np.random.randn(points) * 0.1, 4),
        "num_trades": np.random.randint(10, 500, points),
    })
    