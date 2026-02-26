#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:35:21 2026

@author: twi
"""

"""
CLI Entrypoint für den WebSocket Producer.

Kann standalone oder als Docker Container gestartet werden.

Verwendung:
    # Lokales Ausführen
    python -m backend.websocket_producer.entrypoint --stream second
    python -m backend.websocket_producer.entrypoint --stream minute
    python -m backend.websocket_producer.entrypoint --stream both

    # Mit spezifischen Tickern
    python -m backend.websocket_producer.entrypoint --stream second --tickers AAPL,MSFT,GOOGL

    # Mit Top 10
    python -m backend.websocket_producer.entrypoint --stream second --top10

    # Alle S&P 500
    python -m backend.websocket_producer.entrypoint --stream second --all

    # Docker
    docker run stock-platform/ws-producer:latest --stream second --all
"""

import argparse
import asyncio
import logging
import signal
import sys

from backend.websocket_producer.stream_manager import StreamManager
from backend.websocket_producer.ws_client import StreamType
from backend.data_service.ticker_loader import ticker_loader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-45s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stock Platform - WebSocket Producer"
    )
    parser.add_argument(
        "--stream",
        type=str,
        required=True,
        choices=["second", "minute", "both"],
        help="Stream type: 'second', 'minute', or 'both'",
    )

    # Ticker-Auswahl (mutually exclusive)
    ticker_group = parser.add_mutually_exclusive_group(required=True)
    ticker_group.add_argument(
        "--tickers",
        type=str,
        help="Kommaseparierte Ticker-Liste (z.B. AAPL,MSFT,GOOGL)",
    )
    ticker_group.add_argument(
        "--top10",
        action="store_true",
        help="Top 10 S&P 500 Ticker nach Marktkapitalisierung",
    )
    ticker_group.add_argument(
        "--all",
        action="store_true",
        help="Alle S&P 500 Ticker",
    )

    parser.add_argument(
        "--health-port",
        type=int,
        default=8092,
        help="Port für Health Check Server (default: 8092)",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=8093,
        help="Port für Prometheus Metrics (default: 8093)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log Level (default: INFO)",
    )

    return parser.parse_args()


async def run(args):
    """Hauptfunktion: Erstellt und startet den StreamManager."""

    # Ticker bestimmen
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    elif args.top10:
        tickers = ticker_loader.top10_symbols
    elif args.all:
        tickers = ticker_loader.all_symbols
    else:
        logger.error("No tickers specified!")
        return

    logger.info("=" * 60)
    logger.info("  Stock Platform - WebSocket Producer")
    logger.info("=" * 60)
    logger.info(f"  Stream Type:   {args.stream}")
    logger.info(f"  Tickers:       {len(tickers)}")
    logger.info(f"  Health Port:   {args.health_port}")
    logger.info(f"  Metrics Port:  {args.metrics_port}")
    logger.info("=" * 60)

    # StreamManager erstellen
    manager = StreamManager(
        health_port=args.health_port,
        metrics_port=args.metrics_port,
    )
    manager.set_tickers(tickers)

    # Graceful Shutdown
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Shutdown signal received...")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    # Streams starten
    try:
        await manager.initialize()

        if args.stream in ("second", "both"):
            await manager.start_stream(StreamType.SECOND)

        if args.stream in ("minute", "both"):
            await manager.start_stream(StreamType.MINUTE)

        # Warte auf Shutdown-Signal
        logger.info("WebSocket Producer running. Press Ctrl+C to stop.")
        await shutdown_event.wait()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        # Sauberes Herunterfahren
        logger.info("Initiating graceful shutdown...")
        await manager.stop_all()
        logger.info("WebSocket Producer shutdown complete. ✅")


def main():
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
