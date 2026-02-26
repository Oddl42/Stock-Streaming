#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 14:11:44 2026

@author: twi
"""

"""
Lädt S&P 500 Ticker aus CSV in die TimescaleDB.

Verwendung:
    python scripts/seed_tickers.py
    python scripts/seed_tickers.py --csv data/sp500_tickers.csv
    python scripts/seed_tickers.py --clear   # Bestehende Daten löschen
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from config.settings import settings
from backend.database.connection import db_manager
from backend.database.queries import ticker_queries
from backend.data_service.ticker_loader import TickerLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Seed S&P 500 Tickers into DB")
    parser.add_argument(
        "--csv", type=str, default=settings.SP500_CSV_PATH,
        help="Pfad zur CSV-Datei",
    )
    parser.add_argument(
        "--clear", action="store_true",
        help="Bestehende Ticker vor dem Import löschen",
    )
    return parser.parse_args()


def clear_tickers():
    """Löscht alle bestehenden Ticker aus der DB."""
    logger.warning("Clearing existing tickers...")
    with db_manager.get_raw_cursor() as cur:
        cur.execute("DELETE FROM sp500_tickers;")
    logger.info("  All tickers cleared.")


def seed_from_csv(csv_path: str):
    """Importiert Ticker aus CSV."""
    logger.info(f"Loading tickers from: {csv_path}")

    loader = TickerLoader(csv_path=csv_path)
    df = loader.all_tickers

    if df.empty:
        logger.warning("No tickers found in CSV! Using demo data.")
        df = loader._create_demo_data()

    logger.info(f"  Found {len(df)} tickers in CSV")

    # Spalten-Preview
    logger.info(f"  Columns: {list(df.columns)}")
    logger.info(f"  Sample: {df.head(3).to_string()}")

    # In DB einfügen
    logger.info("Upserting tickers into database...")
    ticker_queries.bulk_upsert_tickers(df)

    logger.info(f"  ✅ {len(df)} tickers seeded into sp500_tickers")


def verify_seed():
    """Verifiziert den Import."""
    logger.info("Verifying seed...")
    with db_manager.get_raw_cursor() as cur:
        cur.execute("SELECT count(*) FROM sp500_tickers;")
        count = cur.fetchone()[0]

        cur.execute("""
            SELECT symbol, name, market_cap
            FROM sp500_tickers
            ORDER BY market_cap DESC NULLS LAST
            LIMIT 5;
        """)
        top5 = cur.fetchall()

    logger.info(f"  Total tickers in DB: {count}")
    logger.info("  Top 5 by Market Cap:")
    for row in top5:
        cap = f"${row[2]/1e12:.2f}T" if row[2] else "N/A"
        logger.info(f"    {row[0]:6s} | {row[1]:30s} | {cap}")


def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info("  Stock Streaming Platform - Seed Tickers")
    logger.info("=" * 60)

    # DB Connection
    db_manager.initialize()

    try:
        if args.clear:
            clear_tickers()

        seed_from_csv(args.csv)
        verify_seed()

        logger.info("")
        logger.info("  ✅ Ticker seeding complete!")

    except Exception as e:
        logger.error(f"Seeding failed: {e}")
        raise
    finally:
        db_manager.close()


if __name__ == "__main__":
    main()
