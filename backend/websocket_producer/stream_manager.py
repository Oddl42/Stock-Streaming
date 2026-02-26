#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 21:49:11 2026

@author: twi
"""

"""Stream Manager - Clean Start/Stop Logic for WebSocket Streams."""

"""
Stream Manager – Zentrale Verwaltung der WebSocket Streams.

Orchestriert:
- Erstellen/Zerstören von WebSocket Clients
- Start/Stop von Sekunden- und Minuten-Streams
- Gemeinsamen Kafka Producer
- Health Check Server
- Metriken

Wird von der GUI über StreamCallbackHandler angesteuert.
"""

import asyncio
import logging
from typing import Optional

from backend.websocket_producer.ws_client import (
    MassiveWebSocketClient,
    StreamType,
)
from backend.websocket_producer.kafka_producer import StockKafkaProducer
from backend.websocket_producer.ticker_manager import TickerManager
from backend.websocket_producer.health_check import HealthCheckServer
from backend.websocket_producer.metrics import start_producer_metrics_server
from config.settings import settings

logger = logging.getLogger(__name__)


class StreamManager:
    """
    Zentrale Verwaltung aller WebSocket Streams.

    Features:
    - Gemeinsamer Kafka Producer für beide Streams
    - Getrennte WebSocket Clients (Second/Minute)
    - Saubere Start/Stop Logik
    - Health Check Server
    """

    def __init__(
        self,
        api_key: str = None,
        kafka_config: dict = None,
        health_port: int = 8092,
        metrics_port: int = 8093,
    ):
        self.api_key = api_key or settings.MASSIVE_API_KEY

        # Gemeinsamer Kafka Producer
        self._kafka_producer = StockKafkaProducer(
            bootstrap_servers=(
                kafka_config.get("bootstrap.servers")
                if kafka_config
                else settings.KAFKA_BOOTSTRAP_SERVERS
            ),
            client_id="ws-stream-manager",
            additional_config=kafka_config,
        )

        # Ticker Manager (gemeinsam)
        self._ticker_manager = TickerManager()

        # Health Check Server
        self._health_server = HealthCheckServer(port=health_port)

        # WebSocket Clients
        self._clients: dict[StreamType, MassiveWebSocketClient] = {}
        self._tasks: dict[StreamType, asyncio.Task] = {}

        # Ports
        self._health_port = health_port
        self._metrics_port = metrics_port
        self._is_initialized = False

    async def initialize(self):
        """Initialisiert gemeinsame Ressourcen."""
        if self._is_initialized:
            return

        # Kafka Producer
        self._kafka_producer.initialize()

        # Health Check Server
        await self._health_server.start()
        self._health_server.set_healthy(True)
        self._health_server.register_status_provider(
            "kafka", self._kafka_producer
        )
        self._health_server.register_status_provider(
            "tickers", self._ticker_manager
        )

        # Metrics Server
        start_producer_metrics_server(port=self._metrics_port)

        self._is_initialized = True
        logger.info("StreamManager initialized ✅")

    def set_tickers(self, tickers: list[str]):
        """Setzt die zu streamenden Ticker."""
        self._ticker_manager.set_tickers(tickers)
        logger.info(
            f"StreamManager: {self._ticker_manager.active_count} tickers set"
        )

    # =========================================
    # Stream Start/Stop
    # =========================================

    async def start_stream(self, stream_type: StreamType):
        """
        Startet einen WebSocket Stream.

        Args:
            stream_type: SECOND oder MINUTE
        """
        if not self._is_initialized:
            await self.initialize()

        if stream_type in self._tasks:
            logger.warning(f"Stream {stream_type.value} is already running!")
            return

        if not self._ticker_manager.active_tickers:
            logger.error("No tickers set! Cannot start stream.")
            return

        logger.info(f"Starting {stream_type.value} stream...")

        # Client erstellen
        client = MassiveWebSocketClient(
            api_key=self.api_key,
            stream_type=stream_type,
            kafka_producer=self._kafka_producer,
            ticker_manager=self._ticker_manager,
            health_server=self._health_server,
        )
        self._clients[stream_type] = client

        # Task starten
        task = asyncio.create_task(
            client.start(),
            name=f"ws-{stream_type.value}-stream",
        )
        self._tasks[stream_type] = task

        # Error-Handling für den Task
        task.add_done_callback(
            lambda t: self._on_task_done(stream_type, t)
        )

        logger.info(f"Stream {stream_type.value} started ✅")

    async def stop_stream(self, stream_type: StreamType):
        """
        Stoppt einen WebSocket Stream sauber.

        Args:
            stream_type: SECOND oder MINUTE
        """
        if stream_type not in self._tasks:
            logger.warning(f"Stream {stream_type.value} is not running!")
            return

        logger.info(f"Stopping {stream_type.value} stream...")

        # Client stoppen
        client = self._clients.get(stream_type)
        if client:
            await client.stop()

        # Task abbrechen falls noch aktiv
        task = self._tasks.get(stream_type)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=10)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # Aufräumen
        self._clients.pop(stream_type, None)
        self._tasks.pop(stream_type, None)

        # Kafka flushen
        self._kafka_producer.flush()

        logger.info(f"Stream {stream_type.value} stopped 🛑")

    async def stop_all(self):
        """Stoppt alle aktiven Streams."""
        logger.info("Stopping all streams...")

        for stream_type in list(self._tasks.keys()):
            await self.stop_stream(stream_type)

        # Kafka Producer schließen
        self._kafka_producer.close()

        # Health Server stoppen
        await self._health_server.stop()

        self._is_initialized = False
        logger.info("All streams stopped. StreamManager shut down. 🛑")

    # =========================================
    # Hilfsmethoden
    # =========================================

    def _on_task_done(self, stream_type: StreamType, task: asyncio.Task):
        """Callback wenn ein Stream-Task beendet wird."""
        try:
            exc = task.exception()
            if exc:
                logger.error(
                    f"Stream {stream_type.value} task failed: {exc}"
                )
        except asyncio.CancelledError:
            logger.info(f"Stream {stream_type.value} task cancelled.")

    # =========================================
    # Status & Monitoring
    # =========================================

    @property
    def is_second_stream_active(self) -> bool:
        return StreamType.SECOND in self._tasks

    @property
    def is_minute_stream_active(self) -> bool:
        return StreamType.MINUTE in self._tasks

    def get_status(self) -> dict:
        """Gesamtstatus aller Streams."""
        status = {
            "is_initialized": self._is_initialized,
            "active_streams": [
                st.value for st in self._tasks.keys()
            ],
            "tickers": self._ticker_manager.get_stats(),
            "kafka": self._kafka_producer.get_stats(),
            "streams": {},
        }

        for stream_type, client in self._clients.items():
            status["streams"][stream_type.value] = client.get_stats()

        return status
