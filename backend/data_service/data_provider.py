#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:00:49 2026

@author: twi
"""

"""Stellt Daten für die GUI bereit (aus TimescaleDB oder Cache)."""

import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from config.settings import settings

import logging

logger = logging.getLogger(__name__)


class DataProvider:
    """Holt Streaming-Daten aus TimescaleDB für die GUI."""

    def __init__(self):
        self._conn_str = settings.db_url

    def _get_connection(self):
        return psycopg2.connect(self._conn_str)

    def get_latest_data(
        self,
        symbol: str,
        stream_type: str = "second",
        limit: int = 500,
    ) -> pd.DataFrame:
        """
        Holt die neuesten Datenpunkte für einen Ticker.

        Args:
            symbol: Ticker-Symbol (z.B. "AAPL")
            stream_type: "second" oder "minute"
            limit: Maximale Anzahl Datenpunkte
        """
        table = (
            "stock_agg_second" if stream_type == "second"
            else "stock_agg_minute"
        )

        query = f"""
            SELECT time, symbol, open, high, low, close, volume, vwap, num_trades
            FROM {table}
            WHERE symbol = %s
            ORDER BY time DESC
            LIMIT %s
        """

        try:
            conn = self._get_connection()
            df = pd.read_sql(query, conn, params=(symbol, limit))
            conn.close()

            if not df.empty:
                df = df.sort_values("time").reset_index(drop=True)
                df["time"] = pd.to_datetime(df["time"])
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return self._empty_dataframe()

    def get_latest_price_info(self, symbols: list[str]) -> pd.DataFrame:
        """Holt den letzten Preis für mehrere Ticker (für Tabelle)."""
        if not symbols:
            return pd.DataFrame()

        placeholders = ",".join(["%s"] * len(symbols))
        query = f"""
            SELECT DISTINCT ON (symbol)
                symbol, open, close, volume
            FROM stock_agg_second
            WHERE symbol IN ({placeholders})
            ORDER BY symbol, time DESC
        """

        try:
            conn = self._get_connection()
            df = pd.read_sql(query, conn, params=tuple(symbols))
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Error fetching price info: {e}")
            return pd.DataFrame()

    @staticmethod
    def _empty_dataframe() -> pd.DataFrame:
        """Leerer DataFrame mit korrektem Schema."""
        return pd.DataFrame(columns=[
            "time", "symbol", "open", "high", "low",
            "close", "volume", "vwap", "num_trades"
        ])

    @staticmethod
    def generate_demo_data(symbol: str = "AAPL", points: int = 100) -> pd.DataFrame:
        """Generiert Demo-Daten für die Entwicklung (ohne DB)."""
        import numpy as np
    
        # ✅ FIX: Dynamischer Seed basierend auf Symbol + aktuelle Sekunde
        #    → Jeder Ticker sieht anders aus
        #    → Bei jedem Aufruf ändern sich die Daten leicht (Live-Effekt)
        seed = hash(symbol) % 2**31 + int(datetime.now().timestamp()) % 1000
        np.random.seed(seed)
    
        now = datetime.now()
        times = [now - timedelta(seconds=i) for i in range(points, 0, -1)]
    
        # ✅ FIX: Unterschiedlicher Basispreis je Ticker
        SYMBOL_PRICES = {
            "AAPL": 185.0, "MSFT": 420.0, "GOOGL": 175.0,
            "AMZN": 195.0, "NVDA": 880.0, "META": 510.0,
            "TSLA": 175.0, "JPM": 205.0, "V": 285.0,
        }
        base_price = SYMBOL_PRICES.get(symbol, 100.0 + hash(symbol) % 200)
    
        noise = np.cumsum(np.random.randn(points) * 0.5)
        prices = base_price + noise
    
        data = {
            "time": times,
            "symbol": [symbol] * points,
            "open": prices + np.random.randn(points) * 0.2,
            "high": prices + abs(np.random.randn(points) * 0.5),
            "low": prices - abs(np.random.randn(points) * 0.5),
            "close": prices,
            "volume": np.random.randint(1000, 100000, points),
            "vwap": prices + np.random.randn(points) * 0.1,
            "num_trades": np.random.randint(10, 500, points),
        }
        return pd.DataFrame(data)


# Singleton
data_provider = DataProvider()
