#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:42:02 2026

@author: twi
"""

"""
Globale pytest Fixtures und Konfiguration.

Stellt gemeinsam genutzte Fixtures bereit für:
- Test-Daten
- Mocks (Kafka, WebSocket, DB)
- Temporäre Verzeichnisse
- Environment-Setup
"""

import os
import sys
import pytest
import asyncio
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Projektverzeichnis zum Python-Path hinzufügen
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Environment für Tests setzen
os.environ.setdefault("MASSIVE_API_KEY", "test_api_key_12345")
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_NAME", "stock_streaming_test")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "testpassword")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("SPARK_MASTER", "local[2]")


# ============================================================
# Event Loop für asyncio Tests
# ============================================================

@pytest.fixture(scope="session")
def event_loop():
    """Globaler Event Loop für alle async Tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ============================================================
# Sample Data Fixtures
# ============================================================

@pytest.fixture
def sample_ohlcv_df():
    """Erstellt einen Sample OHLCV DataFrame."""
    np.random.seed(42)
    n = 100
    now = datetime.now(timezone.utc)
    times = [now - timedelta(seconds=i) for i in range(n, 0, -1)]

    base_price = 185.0
    noise = np.cumsum(np.random.randn(n) * 0.5)
    prices = base_price + noise

    return pd.DataFrame({
        "time": times,
        "symbol": ["AAPL"] * n,
        "open": prices + np.random.randn(n) * 0.2,
        "high": prices + abs(np.random.randn(n) * 0.5),
        "low": prices - abs(np.random.randn(n) * 0.5),
        "close": prices,
        "volume": np.random.randint(1000, 100000, n),
        "vwap": prices + np.random.randn(n) * 0.1,
        "num_trades": np.random.randint(10, 500, n),
    })


@pytest.fixture
def sample_empty_df():
    """Leerer DataFrame mit korrektem Schema."""
    return pd.DataFrame(columns=[
        "time", "symbol", "open", "high", "low",
        "close", "volume", "vwap", "num_trades",
    ])


@pytest.fixture
def sample_ticker_df():
    """Sample Ticker DataFrame."""
    return pd.DataFrame({
        "Symbol": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"],
        "Name": [
            "Apple Inc.", "Microsoft Corp.", "Alphabet Inc.",
            "Amazon.com Inc.", "NVIDIA Corp.",
        ],
        "Sector": ["Technology"] * 5,
        "Industry": [
            "Consumer Electronics", "Software", "Internet Content",
            "Internet Retail", "Semiconductors",
        ],
        "MarketCap": [3.0e12, 2.8e12, 1.9e12, 1.8e12, 1.7e12],
    })


@pytest.fixture
def sample_ws_second_message():
    """Sample WebSocket Sekunden-Aggregation Nachricht."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return [
        {
            "ev": "A",
            "sym": "AAPL",
            "v": 5000,
            "av": 50000000,
            "op": 185.50,
            "vw": 186.10,
            "o": 186.00,
            "c": 186.25,
            "h": 186.50,
            "l": 185.80,
            "a": 186.05,
            "z": 120,
            "s": now_ms - 1000,
            "e": now_ms,
            "n": 45,
            "otc": False,
        },
        {
            "ev": "A",
            "sym": "MSFT",
            "v": 3000,
            "av": 30000000,
            "op": 378.00,
            "vw": 379.50,
            "o": 379.00,
            "c": 379.25,
            "h": 379.80,
            "l": 378.50,
            "a": 379.10,
            "z": 80,
            "s": now_ms - 1000,
            "e": now_ms,
            "n": 30,
            "otc": False,
        },
    ]


@pytest.fixture
def sample_ws_minute_message():
    """Sample WebSocket Minuten-Aggregation Nachricht."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return [
        {
            "ev": "AM",
            "sym": "AAPL",
            "v": 150000,
            "av": 50000000,
            "op": 185.50,
            "vw": 186.10,
            "o": 185.80,
            "c": 186.40,
            "h": 186.75,
            "l": 185.60,
            "a": 186.05,
            "z": 120,
            "s": now_ms - 60000,
            "e": now_ms,
            "n": 1200,
        },
    ]


@pytest.fixture
def sample_ws_status_message():
    """Sample Status-Nachricht."""
    return [{"ev": "status", "status": "connected", "message": "Connected Successfully"}]


@pytest.fixture
def sample_ws_auth_success():
    """Sample Auth-Success Nachricht."""
    return [{"ev": "status", "status": "auth_success", "message": "authenticated"}]


@pytest.fixture
def sample_ws_invalid_messages():
    """Sample ungültige Nachrichten."""
    return [
        # Fehlende Pflichtfelder
        {"ev": "A", "sym": "AAPL"},
        # Negative Preise
        {"ev": "A", "sym": "MSFT", "o": -1.0, "h": 5.0, "l": 1.0,
         "c": 3.0, "v": 100, "s": 1000, "e": 2000},
        # High < Low
        {"ev": "A", "sym": "GOOGL", "o": 5.0, "h": 2.0, "l": 8.0,
         "c": 4.0, "v": 100, "s": 1000, "e": 2000},
        # Kein Symbol
        {"ev": "A", "sym": "", "o": 5.0, "h": 8.0, "l": 3.0,
         "c": 6.0, "v": 100, "s": 1000, "e": 2000},
    ]


# ============================================================
# Mock Fixtures
# ============================================================

@pytest.fixture
def mock_kafka_producer():
    """Mock für confluent_kafka.Producer."""
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.poll = MagicMock(return_value=0)
    producer.flush = MagicMock(return_value=0)
    producer.__len__ = MagicMock(return_value=0)
    return producer


@pytest.fixture
def mock_websocket():
    """Mock für websockets.WebSocketClientProtocol."""
    ws = AsyncMock()
    ws.open = True
    ws.close = AsyncMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    return ws


@pytest.fixture
def mock_db_connection():
    """Mock für psycopg2 Connection."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.execute = MagicMock()
    cursor.fetchall = MagicMock(return_value=[])
    cursor.fetchone = MagicMock(return_value=None)
    return conn, cursor


# ============================================================
# Temporäre Verzeichnisse
# ============================================================

@pytest.fixture
def tmp_checkpoint_dir(tmp_path):
    """Temporäres Checkpoint-Verzeichnis für Spark."""
    checkpoint_dir = tmp_path / "spark-checkpoints"
    checkpoint_dir.mkdir()
    return str(checkpoint_dir)


@pytest.fixture
def tmp_csv_file(tmp_path, sample_ticker_df):
    """Temporäre CSV-Datei mit Ticker-Daten."""
    csv_path = tmp_path / "sp500_tickers.csv"
    sample_ticker_df.to_csv(csv_path, index=False)
    return str(csv_path)
