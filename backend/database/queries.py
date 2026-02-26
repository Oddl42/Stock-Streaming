#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 14:09:34 2026

@author: twi
"""

"""
Zentralisierte Datenbank-Queries.

Enthält alle SQL-Abfragen, die von verschiedenen Teilen
der Applikation benötigt werden:
- GUI Data Provider
- Ticker Lookups
- Statistiken
- Admin-Operationen
"""

import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional

from backend.database.connection import db_manager

logger = logging.getLogger(__name__)


class StockQueries:
    """Queries für Stock-Aggregationsdaten."""

    @staticmethod
    def get_latest_ohlcv(
        symbol: str,
        table: str = "stock_agg_second",
        limit: int = 500,
    ) -> pd.DataFrame:
        """
        Holt die neuesten OHLCV-Daten für einen Ticker.

        Args:
            symbol: Ticker-Symbol
            table: "stock_agg_second" oder "stock_agg_minute"
            limit: Max Datenpunkte

        Returns:
            DataFrame mit OHLCV-Daten, aufsteigend nach Zeit sortiert
        """
        query = f"""
            SELECT time, symbol, open, high, low, close, volume,
                   vwap, num_trades
            FROM {table}
            WHERE symbol = %(symbol)s
            ORDER BY time DESC
            LIMIT %(limit)s
        """

        try:
            with db_manager.get_raw_connection() as conn:
                df = pd.read_sql(
                    query, conn,
                    params={"symbol": symbol, "limit": limit},
                )

            if not df.empty:
                df = df.sort_values("time").reset_index(drop=True)
                df["time"] = pd.to_datetime(df["time"], utc=True)

            return df

        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_latest_prices(symbols: list[str]) -> pd.DataFrame:
        """
        Holt den letzten Preis für mehrere Ticker.
        Verwendet DISTINCT ON für PostgreSQL-Performance.
        """
        if not symbols:
            return pd.DataFrame()

        query = """
            SELECT DISTINCT ON (symbol)
                symbol, open, close, high, low, volume, time
            FROM stock_agg_second
            WHERE symbol = ANY(%(symbols)s)
            ORDER BY symbol, time DESC
        """

        try:
            with db_manager.get_raw_connection() as conn:
                return pd.read_sql(
                    query, conn, params={"symbols": symbols}
                )
        except Exception as e:
            logger.error(f"Error fetching latest prices: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_symbol_stats(hours: int = 1) -> pd.DataFrame:
        """Statistiken pro Symbol der letzten X Stunden."""
        query = """
            SELECT
                symbol,
                count(*) as row_count,
                min(time) as first_seen,
                max(time) as last_seen,
                avg(close) as avg_close,
                min(low) as min_low,
                max(high) as max_high,
                sum(volume) as total_volume
            FROM stock_agg_second
            WHERE time > NOW() - make_interval(hours => %(hours)s)
            GROUP BY symbol
            ORDER BY row_count DESC
        """

        try:
            with db_manager.get_raw_connection() as conn:
                return pd.read_sql(query, conn, params={"hours": hours})
        except Exception as e:
            logger.error(f"Error fetching symbol stats: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_ingest_rate(minutes: int = 5) -> pd.DataFrame:
        """Ingest-Rate (Rows pro Minute)."""
        query = """
            SELECT
                time_bucket('1 minute', time) AS bucket,
                count(*) as rows_per_minute,
                count(DISTINCT symbol) as unique_symbols
            FROM stock_agg_second
            WHERE time > NOW() - make_interval(mins => %(minutes)s)
            GROUP BY bucket
            ORDER BY bucket DESC
        """

        try:
            with db_manager.get_raw_connection() as conn:
                return pd.read_sql(query, conn, params={"minutes": minutes})
        except Exception as e:
            logger.error(f"Error fetching ingest rate: {e}")
            return pd.DataFrame()


class TickerQueries:
    """Queries für Ticker-Verwaltung."""

    @staticmethod
    def get_all_tickers() -> pd.DataFrame:
        """Holt alle Ticker aus der sp500_tickers Tabelle."""
        query = """
            SELECT symbol, name, sector, industry, market_cap
            FROM sp500_tickers
            ORDER BY market_cap DESC NULLS LAST
        """
        try:
            with db_manager.get_raw_connection() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Error fetching tickers: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_top_n_tickers(n: int = 10) -> pd.DataFrame:
        """Holt die Top N Ticker nach Marktkapitalisierung."""
        query = """
            SELECT symbol, name, sector, industry, market_cap
            FROM sp500_tickers
            ORDER BY market_cap DESC NULLS LAST
            LIMIT %(n)s
        """
        try:
            with db_manager.get_raw_connection() as conn:
                return pd.read_sql(query, conn, params={"n": n})
        except Exception as e:
            logger.error(f"Error fetching top {n} tickers: {e}")
            return pd.DataFrame()

    @staticmethod
    def upsert_ticker(
        symbol: str,
        name: str = None,
        sector: str = None,
        industry: str = None,
        market_cap: float = None,
    ):
        """Fügt einen Ticker ein oder aktualisiert ihn."""
        query = """
            INSERT INTO sp500_tickers (symbol, name, sector, industry, market_cap)
            VALUES (%(symbol)s, %(name)s, %(sector)s, %(industry)s, %(market_cap)s)
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                market_cap = EXCLUDED.market_cap
        """
        try:
            with db_manager.get_raw_cursor() as cur:
                cur.execute(query, {
                    "symbol": symbol,
                    "name": name,
                    "sector": sector,
                    "industry": industry,
                    "market_cap": market_cap,
                })
        except Exception as e:
            logger.error(f"Error upserting ticker {symbol}: {e}")
            raise

    @staticmethod
    def bulk_upsert_tickers(df: pd.DataFrame):
        """Massenimport von Tickern aus einem DataFrame."""
        if df.empty:
            return

        # Spalten normalisieren
        col_map = {
            "Symbol": "symbol",
            "Name": "name",
            "Sector": "sector",
            "Industry": "industry",
            "MarketCap": "market_cap",
        }
        df = df.rename(columns=col_map)

        query = """
            INSERT INTO sp500_tickers (symbol, name, sector, industry, market_cap)
            VALUES (%(symbol)s, %(name)s, %(sector)s, %(industry)s, %(market_cap)s)
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                market_cap = EXCLUDED.market_cap
        """

        try:
            with db_manager.get_raw_cursor() as cur:
                records = df.to_dict("records")
                for record in records:
                    cur.execute(query, record)

            logger.info(f"Bulk upserted {len(df)} tickers.")
        except Exception as e:
            logger.error(f"Error bulk upserting tickers: {e}")
            raise


class AdminQueries:
    """Administrative Queries."""

    @staticmethod
    def get_table_sizes() -> pd.DataFrame:
        """Gibt die Größen aller relevanten Tabellen zurück."""
        query = """
            SELECT
                hypertable_name AS table_name,
                pg_size_pretty(hypertable_size(
                    format('%I.%I', hypertable_schema, hypertable_name)::regclass
                )) AS total_size,
                num_chunks AS chunks
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = 'public'
            ORDER BY hypertable_size(
                format('%I.%I', hypertable_schema, hypertable_name)::regclass
            ) DESC
        """
        try:
            with db_manager.get_raw_connection() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Error fetching table sizes: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_retention_policies() -> pd.DataFrame:
        """Gibt die aktiven Retention Policies zurück."""
        query = """
            SELECT
                hypertable_name,
                schedule_interval,
                config
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_retention'
        """
        try:
            with db_manager.get_raw_connection() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Error fetching retention policies: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_dead_letter_summary() -> dict:
        """Zusammenfassung der Dead Letter Queue."""
        query = """
            SELECT
                count(*) as total,
                count(*) FILTER (WHERE rejected_at > NOW() - INTERVAL '1 hour') as last_hour,
                count(DISTINCT rejection_reason) as unique_reasons,
                count(DISTINCT symbol) as unique_symbols
            FROM dead_letter_queue
        """
        try:
            with db_manager.get_raw_cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return {
                    "total": row[0],
                    "last_hour": row[1],
                    "unique_reasons": row[2],
                    "unique_symbols": row[3],
                }
        except Exception as e:
            logger.error(f"Error fetching DLQ summary: {e}")
            return {"total": 0, "last_hour": 0, "unique_reasons": 0, "unique_symbols": 0}


# Convenience-Instanzen
stock_queries = StockQueries()
ticker_queries = TickerQueries()
admin_queries = AdminQueries()
