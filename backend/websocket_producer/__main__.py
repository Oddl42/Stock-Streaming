#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 20:19:20 2026

@author: twi
"""
"""
Entry Point für den WebSocket Producer.

Verwendung:
    python -m backend.websocket_producer
"""

import asyncio
import logging
import signal

from backend.websocket_producer.stream_manager import StreamManager
from backend.websocket_producer.ws_client import StreamType
from backend.data_service.ticker_loader import ticker_loader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def run():
    """Startet den WebSocket Producer."""
    logger.info("Starting Stock Platform WebSocket Producer...")

    manager = StreamManager()

    # 1. Initialisieren (Kafka, Health Check, Metrics)
    await manager.initialize()

    # 2. Ticker laden und setzen
    tickers = ticker_loader.all_symbols
    logger.info(f"Loaded {len(tickers)} tickers from CSV")
    manager.set_tickers(tickers)

    # 3. Graceful Shutdown vorbereiten
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    def shutdown_handler():
        logger.info("Shutdown signal received...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_handler)

    # 4. Second-Stream starten
    await manager.start_stream(StreamType.SECOND)
    logger.info("WebSocket Producer running ✅ Waiting for data...")

    # 5. Warten bis Shutdown-Signal
    await stop_event.wait()

    # 6. Sauber herunterfahren
    logger.info("Shutting down...")
    await manager.stop_all()
    logger.info("WebSocket Producer stopped.")


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")


if __name__ == "__main__":
    main()