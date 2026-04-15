#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 14:10:39 2026

@author: twi
"""
"""
Datenbank-Initialisierung – Stock Streaming Platform.

Führt alle notwendigen Setup-Schritte aus:
1. TimescaleDB Extension aktivieren
2. Tabellen erstellen (ORM Models)
3. Hypertables erstellen
4. Indizes erstellen
5. Retention Policies konfigurieren
6. Compression Policies konfigurieren
7. Continuous Aggregates erstellen (optional)
8. sp500_tickers Tabelle erstellen

Verwendung:
    python scripts/init_db.py
    python scripts/init_db.py --drop-existing    # Tabellen vorher löschen
    python scripts/init_db.py --skip-hypertables # Nur Tabellen, keine Hypertables
"""

import argparse
import logging
import sys
from pathlib import Path

# Projektverzeichnis zum Path hinzufügen
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from config.db_config import db_config
from backend.database.models import Base
from backend.database.connection import db_manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Initialize Stock Platform Database")
    parser.add_argument(
        "--drop-existing", action="store_true",
        help="Drop existing tables before creating",
    )
    parser.add_argument(
        "--skip-hypertables", action="store_true",
        help="Skip hypertable creation (only create regular tables)",
    )
    parser.add_argument(
        "--skip-continuous-agg", action="store_true",
        help="Skip continuous aggregate creation",
    )
    return parser.parse_args()


def create_extension(conn):
    """Aktiviert die TimescaleDB Extension."""
    logger.info("Step 1: Enabling TimescaleDB extension...")
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
    cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';")
    version = cur.fetchone()
    logger.info(f"  TimescaleDB version: {version[0] if version else 'NOT FOUND'}")
    cur.close()


def drop_tables(conn):
    """Löscht alle bestehenden Tabellen."""
    logger.warning("Dropping existing tables...")
    cur = conn.cursor()
    tables = [
        "stock_agg_second", "stock_agg_minute",
        "dead_letter_queue", "sp500_tickers",
    ]
    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        logger.info(f"  Dropped: {table}")
    cur.close()


def create_tables(engine):
    """Erstellt alle Tabellen aus den ORM Models."""
    logger.info("Step 2: Creating tables from ORM models...")
    Base.metadata.create_all(engine)
    logger.info("  Tables created ✅")


def create_hypertables(conn):
    """Konvertiert Tabellen in TimescaleDB Hypertables."""
    logger.info("Step 3: Converting tables to hypertables...")
    cur = conn.cursor()

    for table in ["stock_agg_second", "stock_agg_minute"]:
        try:
            cur.execute(f"""
                SELECT create_hypertable(
                    '{table}', 'time',
                    if_not_exists => TRUE,
                    migrate_data => TRUE,
                    chunk_time_interval => INTERVAL '{db_config.chunk_time_interval}'
                );
            """)
            logger.info(f"  Hypertable created: {table}")
        except Exception as e:
            logger.warning(f"  Hypertable {table}: {e}")

    cur.close()


def create_retention_policies(conn):
    """Konfiguriert Retention Policies."""
    logger.info("Step 4: Setting up retention policies...")
    cur = conn.cursor()

    for table in ["stock_agg_second", "stock_agg_minute"]:
        try:
            cur.execute(f"""
                SELECT add_retention_policy(
                    '{table}',
                    INTERVAL '{db_config.retention_days} days',
                    if_not_exists => TRUE
                );
            """)
            logger.info(
                f"  Retention policy: {table} → {db_config.retention_days} days"
            )
        except Exception as e:
            logger.warning(f"  Retention {table}: {e}")

    cur.close()


def create_compression_policies(conn):
    """Konfiguriert Compression Policies."""
    logger.info("Step 5: Setting up compression policies...")
    cur = conn.cursor()

    for table in ["stock_agg_second", "stock_agg_minute"]:
        try:
            cur.execute(f"""
                ALTER TABLE {table} SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'symbol',
                    timescaledb.compress_orderby = 'time DESC'
                );
            """)
            cur.execute(f"""
                SELECT add_compression_policy(
                    '{table}',
                    INTERVAL '{db_config.compression_after_days} day',
                    if_not_exists => TRUE
                );
            """)
            logger.info(
                f"  Compression policy: {table} → after {db_config.compression_after_days} day(s)"
            )
        except Exception as e:
            logger.warning(f"  Compression {table}: {e}")

    cur.close()


def create_continuous_aggregates(conn):
    """Erstellt Continuous Aggregates für Dashboard-Performance."""
    logger.info("Step 6: Creating continuous aggregates...")
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS stock_5min_agg
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('5 minutes', time) AS bucket,
                symbol,
                FIRST(open, time)   AS open,
                MAX(high)           AS high,
                MIN(low)            AS low,
                LAST(close, time)   AS close,
                SUM(volume)         AS volume,
                AVG(vwap)           AS avg_vwap,
                SUM(num_trades)     AS total_trades,
                COUNT(*)            AS tick_count
            FROM stock_agg_second
            GROUP BY bucket, symbol
            WITH NO DATA;
        """)
        logger.info("  Continuous aggregate: stock_5min_agg ✅")

        cur.execute("""
            SELECT add_continuous_aggregate_policy(
                'stock_5min_agg',
                start_offset    => INTERVAL '1 hour',
                end_offset      => INTERVAL '5 minutes',
                schedule_interval => INTERVAL '5 minutes',
                if_not_exists   => TRUE
            );
        """)
        logger.info("  Refresh policy for stock_5min_agg ✅")
    except Exception as e:
        logger.warning(f"  Continuous aggregate: {e}")

    cur.close()


def verify_setup(conn):
    """Verifiziert das gesamte Setup."""
    logger.info("Step 7: Verifying setup...")
    cur = conn.cursor()

    # Tabellen prüfen
    cur.execute("""
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename;
    """)
    tables = [row[0] for row in cur.fetchall()]
    logger.info(f"  Tables: {', '.join(tables)}")

    # Hypertables prüfen
    cur.execute("""
        SELECT hypertable_name FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'public';
    """)
    hypertables = [row[0] for row in cur.fetchall()]
    logger.info(f"  Hypertables: {', '.join(hypertables) if hypertables else 'NONE'}")

    # Jobs prüfen
    cur.execute("""
        SELECT proc_name, hypertable_name, schedule_interval
        FROM timescaledb_information.jobs
        WHERE hypertable_schema = 'public' OR hypertable_name IS NULL
        ORDER BY proc_name;
    """)
    jobs = cur.fetchall()
    for job in jobs:
        logger.info(f"  Job: {job[0]} → {job[1]} (interval: {job[2]})")

    cur.close()


def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info("  Stock Streaming Platform - Database Init")
    logger.info("=" * 60)
    logger.info(f"  Host:      {db_config.host}:{db_config.port}")
    logger.info(f"  Database:  {db_config.name}")
    logger.info(f"  Retention: {db_config.retention_days} days")
    logger.info("=" * 60)

    # Initialisiere Connection Manager
    db_manager.initialize()

    # Raw Connection für DDL
    conn = psycopg2.connect(db_config.psycopg2_dsn)
    conn.autocommit = True

    try:
        # 1. Extension
        create_extension(conn)

        # Optional: Drop existing
        if args.drop_existing:
            drop_tables(conn)

        # 2. Tabellen (ORM)
        create_tables(db_manager.engine)

        # 3. Hypertables
        if not args.skip_hypertables:
            create_hypertables(conn)
            create_retention_policies(conn)
            create_compression_policies(conn)

        # 6. Continuous Aggregates
        if not args.skip_continuous_agg and not args.skip_hypertables:
            create_continuous_aggregates(conn)

        # 7. Verify
        verify_setup(conn)

        logger.info("")
        logger.info("=" * 60)
        logger.info("  ✅ Database initialization complete!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        conn.close()
        db_manager.close()


if __name__ == "__main__":
    main()
