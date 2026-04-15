#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 21:48:23 2026

@author: twi
"""

"""TimescaleDB Hypertable Setup – Aktualisiert mit ML-Spalten."""

import psycopg2
import logging
from config.settings import settings

logger = logging.getLogger(__name__)


def setup_timescaledb():
    """Erstellt Hypertables, Indizes und Retention-Policies."""
    conn = psycopg2.connect(settings.db_url)
    conn.autocommit = True
    cur = conn.cursor()

    # TimescaleDB Extension
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

    # ============================================================
    # Sekunden-Aggregation
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_agg_second (
            time                TIMESTAMPTZ     NOT NULL,
            symbol              TEXT            NOT NULL,
            open                DOUBLE PRECISION,
            high                DOUBLE PRECISION,
            low                 DOUBLE PRECISION,
            close               DOUBLE PRECISION,
            volume              BIGINT,
            vwap                DOUBLE PRECISION,
            accumulated_volume  BIGINT,
            official_open       DOUBLE PRECISION,
            avg_trade_size      INTEGER,
            num_trades          INTEGER,
            tick_start          TIMESTAMPTZ,
            tick_end            TIMESTAMPTZ,
            is_otc              BOOLEAN         DEFAULT FALSE,
            -- Derived columns (ML Features)
            price_range         DOUBLE PRECISION,
            price_change        DOUBLE PRECISION,
            price_change_pct    DOUBLE PRECISION,
            is_bullish          BOOLEAN,
            body_size           DOUBLE PRECISION,
            upper_shadow        DOUBLE PRECISION,
            lower_shadow        DOUBLE PRECISION,
            -- Constraint für Upsert
            UNIQUE (time, symbol)
        );
    """)

    # ============================================================
    # Minuten-Aggregation
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_agg_minute (
            time                TIMESTAMPTZ     NOT NULL,
            symbol              TEXT            NOT NULL,
            open                DOUBLE PRECISION,
            high                DOUBLE PRECISION,
            low                 DOUBLE PRECISION,
            close               DOUBLE PRECISION,
            volume              BIGINT,
            vwap                DOUBLE PRECISION,
            accumulated_volume  BIGINT,
            official_open       DOUBLE PRECISION,
            avg_trade_size      INTEGER,
            num_trades          INTEGER,
            tick_start          TIMESTAMPTZ,
            tick_end            TIMESTAMPTZ,
            is_otc              BOOLEAN         DEFAULT FALSE,
            -- Derived columns (ML Features)
            price_range         DOUBLE PRECISION,
            price_change        DOUBLE PRECISION,
            price_change_pct    DOUBLE PRECISION,
            is_bullish          BOOLEAN,
            body_size           DOUBLE PRECISION,
            upper_shadow        DOUBLE PRECISION,
            lower_shadow        DOUBLE PRECISION,
            -- Constraint
            UNIQUE (time, symbol)
        );
    """)

    # ============================================================
    # Dead Letter Queue
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
            id                  BIGSERIAL PRIMARY KEY,
            time                TIMESTAMPTZ,
            symbol              TEXT,
            open                DOUBLE PRECISION,
            high                DOUBLE PRECISION,
            low                 DOUBLE PRECISION,
            close               DOUBLE PRECISION,
            volume              BIGINT,
            vwap                DOUBLE PRECISION,
            rejection_reason    TEXT,
            rejected_at         TIMESTAMPTZ     DEFAULT NOW(),
            batch_id            INTEGER
        );
    """)

    # ============================================================
    # Hypertables erstellen
    # ============================================================
    for table in ["stock_agg_second", "stock_agg_minute"]:
        cur.execute(f"""
            SELECT create_hypertable(
                '{table}', 'time',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
        """)

        # Composite Index: Symbol + Time (für schnelle Abfragen)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table}_symbol_time
            ON {table} (symbol, time DESC);
        """)

        # Index auf Symbol allein (für JOINs)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table}_symbol
            ON {table} (symbol);
        """)

        # 2-Day Retention Policy (Dev Phase)
        cur.execute(f"""
            SELECT add_retention_policy(
                '{table}',
                INTERVAL '2 days',
                if_not_exists => TRUE
            );
        """)

        logger.info(f"Hypertable '{table}' created with 2-day retention.")

    # ============================================================
    # Continuous Aggregates (Optional – für Dashboard Performance)
    # ============================================================
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

    # Refresh-Policy für den Continuous Aggregate
    cur.execute("""
        SELECT add_continuous_aggregate_policy(
            'stock_5min_agg',
            start_offset    => INTERVAL '1 hour',
            end_offset      => INTERVAL '5 minutes',
            schedule_interval => INTERVAL '5 minutes',
            if_not_exists   => TRUE
        );
    """)

    logger.info("Continuous aggregate 'stock_5min_agg' created.")

    # ============================================================
    # Compression (optional, spart Speicher)
    # ============================================================
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
                    INTERVAL '1 day',
                    if_not_exists => TRUE
                );
            """)
            logger.info(f"Compression enabled for '{table}' (after 1 day).")
        except Exception as e:
            logger.warning(f"Compression setup failed for {table}: {e}")

    cur.close()
    conn.close()

    logger.info("=" * 50)
    logger.info("  ✅ TimescaleDB Setup Complete!")
    logger.info("=" * 50)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    setup_timescaledb()
