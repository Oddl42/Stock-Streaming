#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:50:30 2026

@author: twi
"""

"""
Integration Tests: Datenbank-Operationen.

Benötigt laufende TimescaleDB-Instanz (Docker Compose).
"""

import pytest
import psycopg2
import pandas as pd
from datetime import datetime, timedelta, timezone

from config.settings import settings


def get_test_db_url():
    """Gibt die Test-DB URL zurück."""
    return (
        f"host={settings.DB_HOST} port={settings.DB_PORT} "
        f"dbname={settings.DB_NAME} user={settings.DB_USER} "
        f"password={settings.DB_PASSWORD}"
    )


@pytest.fixture(scope="module")
def db_conn():
    """Erstellt eine DB-Verbindung für Tests."""
    try:
        conn = psycopg2.connect(get_test_db_url())
        conn.autocommit = True
        yield conn
        conn.close()
    except psycopg2.OperationalError:
        pytest.skip("TimescaleDB not available")


@pytest.mark.integration
@pytest.mark.database
class TestDatabaseOperations:

    def test_connection(self, db_conn):
        """Testet DB-Verbindung."""
        cur = db_conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        assert result[0] == 1
        cur.close()

    def test_timescaledb_extension(self, db_conn):
        """Testet ob TimescaleDB-Extension geladen ist."""
        cur = db_conn.cursor()
        cur.execute(
            "SELECT extname FROM pg_extension WHERE extname = 'timescaledb'"
        )
        result = cur.fetchone()
        assert result is not None
        assert result[0] == "timescaledb"
        cur.close()

    def test_hypertable_exists(self, db_conn):
        """Testet ob Hypertables existieren."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT hypertable_name
            FROM timescaledb_information.hypertables
            WHERE hypertable_name IN ('stock_agg_second', 'stock_agg_minute')
        """)
        results = cur.fetchall()
        tables = [r[0] for r in results]

        assert "stock_agg_second" in tables
        assert "stock_agg_minute" in tables
        cur.close()

    def test_insert_and_query(self, db_conn):
        """Testet Einfügen und Abfragen von Daten."""
        cur = db_conn.cursor()

        now = datetime.now(timezone.utc)
        cur.execute("""
            INSERT INTO stock_agg_second (time, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, symbol) DO UPDATE SET close = EXCLUDED.close
        """, (now, "TEST", 100.0, 101.0, 99.0, 100.5, 1000))

        cur.execute("""
            SELECT symbol, close FROM stock_agg_second
            WHERE symbol = 'TEST' AND time = %s
        """, (now,))

        result = cur.fetchone()
        assert result is not None
        assert result[0] == "TEST"
        assert result[1] == 100.5

        # Cleanup
        cur.execute("DELETE FROM stock_agg_second WHERE symbol = 'TEST'")
        cur.close()

    def test_retention_policy_exists(self, db_conn):
        """Testet ob Retention-Policies konfiguriert sind."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT hypertable_name, schedule_interval
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_retention'
        """)
        results = cur.fetchall()

        tables_with_retention = [r[0] for r in results]
        assert len(tables_with_retention) > 0
        cur.close()
