#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:15:30 2026

@author: twi
"""

"""
Aktualisierte Stream Callbacks – Nutzt den StreamJobManager
für die Spark Streaming Jobs.

Hinweis: Backend-Imports sind mit try/except abgesichert,
da im Panel-GUI-Container weder pyspark noch aiohttp
installiert sind. Diese Komponenten laufen in separaten Pods.
"""

import logging

logger = logging.getLogger(__name__)

# --- Spark Streaming (benötigt pyspark) ---
try:
    from backend.spark_streaming.stream_job_manager import stream_job_manager
except ImportError as e:
    stream_job_manager = None
    logger.warning(f"stream_job_manager nicht verfügbar: {e}")

# --- WebSocket Producer (benötigt aiohttp) ---
try:
    from backend.websocket_producer.stream_manager import StreamManager, StreamType
    WS_PRODUCER_AVAILABLE = True
except ImportError as e:
    StreamManager = None
    StreamType = None
    WS_PRODUCER_AVAILABLE = False
    logger.warning(f"WebSocket Producer nicht verfügbar: {e}")

# --- Settings (sollte immer verfügbar sein) ---
from config.settings import settings


class StreamCallbackHandler:
    """
    Verbindet GUI-Events mit Backend:
    1. WebSocket Producer (Massive.com → Kafka)
    2. Spark Streaming Job (Kafka → TimescaleDB)

    Im Panel-GUI-Container laufen beide Backends in separaten Pods.
    Die GUI zeigt nur Daten aus TimescaleDB an.
    """

    def __init__(self):
        self._ws_stream_manager = None
        self._is_initialized = False

    def initialize(self):
        """Initialisiert den WebSocket Producer (falls verfügbar)."""
        if self._is_initialized:
            return

        if not WS_PRODUCER_AVAILABLE:
            logger.warning(
                "WebSocket Producer nicht verfügbar (aiohttp fehlt). "
                "Producer läuft vermutlich in separatem Pod."
            )
            self._is_initialized = True
            return

        kafka_config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "panel-gui-producer",
            "acks": "all",
        }
        self._ws_stream_manager = StreamManager(
            api_key=settings.MASSIVE_API_KEY,
            kafka_config=kafka_config,
        )
        self._is_initialized = True

    def on_stream_start(self, stream_type_str: str, tickers: list[str]):
        """
        Startet den gesamten Streaming-Stack:
        1. Spark Streaming Job (Kafka → DB)
        2. WebSocket Producer (Massive.com → Kafka)
        """
        if not self._is_initialized:
            self.initialize()

        stream_type = (
            "second" if stream_type_str in ("Sekunden", "second")
            else "minute"
        )

        # 1. Spark Streaming Job starten (nur wenn verfügbar)
        if stream_job_manager is not None:
            logger.info(f"Starting Spark {stream_type} streaming job...")
            spark_started = stream_job_manager.start_job(stream_type)
            if not spark_started:
                logger.error("Failed to start Spark streaming job!")
                raise RuntimeError("Spark job failed to start")
        else:
            logger.warning(
                "stream_job_manager nicht verfügbar (pyspark fehlt). "
                "Spark-Job wird übersprungen – läuft vermutlich in separatem Pod."
            )

        # 2. WebSocket Producer starten (nur wenn verfügbar)
        if WS_PRODUCER_AVAILABLE and self._ws_stream_manager is not None:
            logger.info(f"Starting WebSocket producer for {len(tickers)} tickers...")
            ws_type = (
                StreamType.SECOND if stream_type == "second"
                else StreamType.MINUTE
            )
            self._ws_stream_manager.set_tickers(tickers)

            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.ensure_future(
                        self._ws_stream_manager.start_stream(ws_type)
                    )
                else:
                    loop.run_until_complete(
                        self._ws_stream_manager.start_stream(ws_type)
                    )
            except Exception as e:
                logger.error(f"WebSocket producer failed: {e}")
                if stream_job_manager is not None:
                    stream_job_manager.stop_job(stream_type)
                raise
        else:
            logger.warning(
                "WebSocket Producer nicht verfügbar (aiohttp fehlt). "
                "Producer läuft vermutlich in separatem Pod."
            )

        logger.info(
            f"✅ Full streaming stack started: "
            f"{stream_type} with {len(tickers)} tickers"
        )

    def on_stream_stop(self, stream_type_str: str):
        """
        Stoppt den gesamten Stack:
        1. WebSocket Producer stoppen
        2. Spark Streaming Job stoppen
        """
        stream_type = (
            "second" if stream_type_str in ("Sekunden", "second")
            else "minute"
        )

        # 1. WebSocket Producer stoppen (nur wenn verfügbar)
        if WS_PRODUCER_AVAILABLE and self._ws_stream_manager is not None:
            ws_type = (
                StreamType.SECOND if stream_type == "second"
                else StreamType.MINUTE
            )
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.ensure_future(
                        self._ws_stream_manager.stop_stream(ws_type)
                    )
                else:
                    loop.run_until_complete(
                        self._ws_stream_manager.stop_stream(ws_type)
                    )
            except Exception as e:
                logger.error(f"Error stopping WebSocket producer: {e}")

        # 2. Spark Job stoppen (nur wenn verfügbar)
        if stream_job_manager is not None:
            stream_job_manager.stop_job(stream_type)
        else:
            logger.warning(
                "stream_job_manager nicht verfügbar – "
                "Spark-Job läuft in separatem Pod und muss dort gestoppt werden."
            )

        logger.info(f"🛑 Full streaming stack stopped: {stream_type}")

    def on_shutdown(self):
        """Cleanup."""
        if WS_PRODUCER_AVAILABLE and self._ws_stream_manager is not None:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(
                    self._ws_stream_manager.stop_all()
                )
            except Exception:
                pass

        if stream_job_manager is not None:
            stream_job_manager.stop_all()

        logger.info("Full shutdown complete.")

    def get_health(self) -> dict:
        """Health-Status des gesamten Stacks."""
        health = {"panel_gui": "running"}

        if stream_job_manager is not None:
            health["spark"] = stream_job_manager.health_check()
        else:
            health["spark"] = {"status": "external", "reason": "runs in separate pod"}

        if WS_PRODUCER_AVAILABLE:
            health["ws_producer"] = "available"
        else:
            health["ws_producer"] = {"status": "external", "reason": "runs in separate pod"}

        return health


# Singleton
stream_callback_handler = StreamCallbackHandler()
