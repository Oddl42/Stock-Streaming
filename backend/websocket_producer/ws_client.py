#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:33:02 2026

@author: twi
"""

"""
Massive.com WebSocket Client.

Kernkomponente des Producers:
- Verbindet sich mit der Massive.com WebSocket API
- Authentifiziert sich mit dem API Key
- Abonniert Ticker (Second oder Minute Aggregates)
- Empfängt und parst Nachrichten
- Leitet gültige Daten an den Kafka Producer weiter

Der Client ist vollständig asynchron (asyncio) und unterstützt:
- Automatische Reconnection mit Exponential Backoff
- Rate Limiting
- Graceful Shutdown
- Health Checks
- Prometheus Metriken
"""

import asyncio
import json
import time
import logging
from typing import Optional
from enum import Enum

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidStatusCode,
)

from config.settings import settings
from backend.websocket_producer.message_parser import MessageParser
from backend.websocket_producer.kafka_producer import StockKafkaProducer
from backend.websocket_producer.ticker_manager import TickerManager
from backend.websocket_producer.reconnect_handler import (
    ReconnectHandler,
    ReconnectConfig,
    ConnectionState,
)
from backend.websocket_producer.rate_limiter import WebSocketRateLimiter
from backend.websocket_producer.health_check import HealthCheckServer
from backend.websocket_producer.metrics import (
    WS_MESSAGES_RECEIVED,
    WS_AGGREGATES_PARSED,
    WS_AGGREGATES_INVALID,
    WS_RECONNECTIONS,
    WS_CONNECTION_STATE,
    WS_SUBSCRIBED_TICKERS,
    WS_MESSAGE_PROCESSING_TIME,
    WS_MESSAGE_SIZE,
    start_producer_metrics_server,
)

logger = logging.getLogger(__name__)


class StreamType(Enum):
    SECOND = "second"
    MINUTE = "minute"


class MassiveWebSocketClient:
    """
    Vollständiger WebSocket Client für die Massive.com API.

    Lifecycle:
    1. connect()     → WebSocket-Verbindung herstellen
    2. authenticate() → API-Key senden
    3. subscribe()   → Ticker abonnieren (in Batches)
    4. listen()      → Nachrichten empfangen und verarbeiten
    5. disconnect()  → Sauberes Schließen

    Bei Verbindungsabbruch: Automatische Reconnection.
    """

    # WebSocket URLs
    WS_BASE_URL = "wss://delayed.massive.com"
    WS_STOCKS_URL = f"{WS_BASE_URL}/stocks"

    # Timeouts
    CONNECT_TIMEOUT = 30        # Sekunden
    AUTH_TIMEOUT = 10            # Sekunden
    PING_INTERVAL = 20           # Sekunden
    PING_TIMEOUT = 10            # Sekunden
    CLOSE_TIMEOUT = 5            # Sekunden

    # Subscription Prefixes
    SUBSCRIPTION_PREFIX = {
        StreamType.SECOND: "A",
        StreamType.MINUTE: "AM",
    }

    def __init__(
        self,
        api_key: str = None,
        stream_type: StreamType = StreamType.SECOND,
        kafka_producer: StockKafkaProducer = None,
        ticker_manager: TickerManager = None,
        health_server: HealthCheckServer = None,
    ):
        """
        Args:
            api_key: Massive.com API Key
            stream_type: SECOND oder MINUTE
            kafka_producer: Kafka Producer Instance
            ticker_manager: Ticker Manager Instance
            health_server: Health Check Server Instance
        """
        self.api_key = api_key or settings.MASSIVE_API_KEY
        self.stream_type = stream_type
        self.kafka_producer = kafka_producer or StockKafkaProducer()
        self.ticker_manager = ticker_manager or TickerManager()
        self.health_server = health_server

        # Internes
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._parser = MessageParser()
        self._reconnect = ReconnectHandler(
            ReconnectConfig(
                initial_delay=1.0,
                max_delay=60.0,
                multiplier=2.0,
                jitter=0.5,
                max_retries=0,    # Unbegrenzt
                reset_after_success=120.0,
            )
        )
        self._rate_limiter = WebSocketRateLimiter()

        # State
        self._is_running = False
        self._stop_event = asyncio.Event()
        self._message_count = 0
        self._start_time: float = 0

        # Kafka Flush Interval
        self._kafka_poll_interval = 0.1  # 100ms

    @property
    def prefix(self) -> str:
        """Subscription-Prefix für den Stream-Typ."""
        return self.SUBSCRIPTION_PREFIX[self.stream_type]

    @property
    def is_connected(self) -> bool:
        """Ist der WebSocket verbunden?"""
        return (
            self._ws is not None
            and self._ws.open
        )

    # =========================================
    # Hauptmethoden
    # =========================================

    async def start(self, tickers: list[str] = None):
        """
        Startet den WebSocket Client.

        Dies ist die Hauptmethode, die den gesamten Lifecycle verwaltet.
        Läuft in einer Endlosschleife mit automatischer Reconnection.

        Args:
            tickers: Liste der zu streamenden Ticker
        """
        logger.info("=" * 60)
        logger.info(f"  Starting WebSocket Client ({self.stream_type.value})")
        logger.info("=" * 60)

        if tickers:
            self.ticker_manager.set_tickers(tickers)

        if not self.ticker_manager.active_tickers:
            logger.error("No tickers configured! Cannot start.")
            return

        # Kafka Producer initialisieren
        self.kafka_producer.initialize()

        # Health Server konfigurieren
        if self.health_server:
            self.health_server.register_status_provider(
                "websocket", self._reconnect
            )
            self.health_server.register_status_provider(
                "kafka", self.kafka_producer
            )
            self.health_server.register_status_provider(
                "parser", self._parser
            )
            self.health_server.register_status_provider(
                "tickers", self.ticker_manager
            )
            self.health_server.set_healthy(True)

        self._is_running = True
        self._stop_event.clear()
        self._start_time = time.time()

        # Hauptschleife mit Reconnection
        while self._is_running and not self._stop_event.is_set():
            try:
                await self._run_connection_cycle()
            except asyncio.CancelledError:
                logger.info("WebSocket client cancelled.")
                break
            except Exception as e:
                logger.error(f"Unexpected error in connection cycle: {e}")
                if not self._is_running:
                    break

                # Reconnection
                self._reconnect.on_disconnected(str(e))
                WS_RECONNECTIONS.labels(
                    stream_type=self.stream_type.value
                ).inc()

                should_retry = await self._reconnect.wait_before_retry()
                if not should_retry:
                    logger.error("Max retries reached. Stopping client.")
                    break

        # Cleanup
        await self._cleanup()
        logger.info("WebSocket client stopped.")

    async def stop(self):
        """Stoppt den Client sauber."""
        logger.info(f"Stopping WebSocket client ({self.stream_type.value})...")
        self._is_running = False
        self._stop_event.set()

        # WebSocket schließen
        if self._ws and self._ws.open:
            try:
                # Unsubscribe zuerst
                await self._unsubscribe()
                await self._ws.close(code=1000, reason="Client shutdown")
            except Exception as e:
                logger.warning(f"Error during WebSocket close: {e}")

        # Metriken
        WS_CONNECTION_STATE.labels(
            stream_type=self.stream_type.value
        ).set(0)

    # =========================================
    # Connection Cycle
    # =========================================

    async def _run_connection_cycle(self):
        """
        Ein vollständiger Verbindungszyklus:
        1. Connect
        2. Authenticate
        3. Subscribe
        4. Listen (Hauptschleife)
        """
        # Rate Limit für Verbindungen
        await self._rate_limiter.wait_for_connection()

        # Verbinden
        self._reconnect.state = ConnectionState.CONNECTING
        await self._connect()

        # Authentifizieren
        self._reconnect.state = ConnectionState.AUTHENTICATING
        await self._authenticate()

        # Abonnieren
        self._reconnect.state = ConnectionState.SUBSCRIBING
        await self._subscribe()

        # Health: Bereit
        if self.health_server:
            self.health_server.set_ready(True)

        # Nachrichten empfangen
        self._reconnect.on_streaming()
        WS_CONNECTION_STATE.labels(
            stream_type=self.stream_type.value
        ).set(1)

        await self._listen()

    async def _connect(self):
        """Stellt die WebSocket-Verbindung her."""
        logger.info(f"Connecting to {self.WS_STOCKS_URL}...")

        self._ws = await asyncio.wait_for(
            websockets.connect(
                self.WS_STOCKS_URL,
                ping_interval=self.PING_INTERVAL,
                ping_timeout=self.PING_TIMEOUT,
                close_timeout=self.CLOSE_TIMEOUT,
                max_size=2**20,         # 1MB max message
                compression=None,       # Keine Kompression (schneller)
                extra_headers={
                    "User-Agent": "StockPlatform-WSProducer/1.0",
                },
            ),
            timeout=self.CONNECT_TIMEOUT,
        )

        self._reconnect.on_connected()
        logger.info("WebSocket connected ✅")

    async def _authenticate(self):
        """Sendet den API-Key zur Authentifizierung."""
        logger.info("Authenticating...")

        auth_message = json.dumps({
            "action": "auth",
            "params": self.api_key,
        })

        await self._ws.send(auth_message)

        # Warte auf Auth-Antwort
        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self.AUTH_TIMEOUT,
            )

            result = self._parser.parse(response)

            for status in result.status_messages:
                if status.status in ("auth_success", "connected"):
                    self._reconnect.on_authenticated()
                    logger.info(f"Authentication successful ✅: {status.message}")
                    return
                elif status.status == "auth_failed":
                    logger.error(f"Authentication FAILED: {status.message}")
                    raise ConnectionError(
                        f"Authentication failed: {status.message}"
                    )

            # Fallback: Prüfe auf "connected" Status
            logger.info(f"Auth response: {response[:200]}")
            self._reconnect.on_authenticated()

        except asyncio.TimeoutError:
            raise ConnectionError("Authentication timeout")

    async def _subscribe(self):
        """Abonniert alle konfigurierten Ticker in Batches."""
        batches = self.ticker_manager.get_subscription_batches(
            prefix=self.prefix
        )

        if not batches:
            logger.warning("No subscription batches to send!")
            return

        logger.info(
            f"Subscribing to {self.ticker_manager.active_count} tickers "
            f"in {len(batches)} batch(es)..."
        )

        for i, batch_params in enumerate(batches, 1):
            # Rate Limit
            ticker_count = batch_params.count(",") + 1
            await self._rate_limiter.wait_for_subscription(ticker_count)

            subscribe_msg = json.dumps({
                "action": "subscribe",
                "params": batch_params,
            })

            await self._ws.send(subscribe_msg)
            logger.info(
                f"  Batch {i}/{len(batches)}: "
                f"Subscribed to {ticker_count} tickers"
            )

            # Kurze Pause zwischen Batches
            if i < len(batches):
                await asyncio.sleep(0.5)

        # Markiere als abonniert
        self.ticker_manager.mark_subscribed(
            self.ticker_manager.active_tickers
        )

        WS_SUBSCRIBED_TICKERS.labels(
            stream_type=self.stream_type.value
        ).set(self.ticker_manager.active_count)

        logger.info(
            f"Subscribed to {self.ticker_manager.active_count} tickers ✅"
        )

    async def _unsubscribe(self):
        """Meldet alle Ticker ab."""
        batches = self.ticker_manager.get_unsubscribe_params(
            prefix=self.prefix
        )

        for batch_params in batches:
            try:
                unsub_msg = json.dumps({
                    "action": "unsubscribe",
                    "params": batch_params,
                })
                await self._ws.send(unsub_msg)
            except Exception as e:
                logger.warning(f"Unsubscribe error: {e}")

        self.ticker_manager.mark_unsubscribed()
        WS_SUBSCRIBED_TICKERS.labels(
            stream_type=self.stream_type.value
        ).set(0)

        logger.info("Unsubscribed from all tickers.")

    # =========================================
    # Message Loop
    # =========================================

    async def _listen(self):
        """
        Hauptschleife: Empfängt und verarbeitet WebSocket-Nachrichten.

        Läuft bis:
        - stop() aufgerufen wird
        - Die Verbindung abbricht
        - Ein kritischer Fehler auftritt
        """
        logger.info("Listening for messages...")
        last_poll_time = time.time()

        async for raw_message in self._ws:
            # Stop-Signal prüfen
            if self._stop_event.is_set():
                break

            # Verarbeite Nachricht
            process_start = time.time()

            try:
                await self._process_message(raw_message)
            except Exception as e:
                logger.error(f"Message processing error: {e}")

            # Metriken: Verarbeitungszeit
            WS_MESSAGE_PROCESSING_TIME.labels(
                stream_type=self.stream_type.value
            ).observe(time.time() - process_start)

            # Kafka Poll (Delivery Reports)
            now = time.time()
            if now - last_poll_time >= self._kafka_poll_interval:
                self.kafka_producer.poll(0)
                last_poll_time = now

    async def _process_message(self, raw_message: str):
        """
        Verarbeitet eine einzelne WebSocket-Nachricht.

        1. Parse JSON
        2. Validiere Daten
        3. Produziere in Kafka
        """
        self._message_count += 1

        # Metriken: Message-Größe
        msg_size = len(raw_message.encode("utf-8"))
        WS_MESSAGE_SIZE.labels(
            stream_type=self.stream_type.value
        ).observe(msg_size)
        WS_MESSAGES_RECEIVED.labels(
            stream_type=self.stream_type.value
        ).inc()

        # Parsen
        result = self._parser.parse(raw_message)

        # Status-Nachrichten verarbeiten
        for status in result.status_messages:
            self._handle_status_message(status)

        # Fehler loggen
        if result.errors:
            for error in result.errors:
                logger.warning(f"Parse error: {error}")

        # Ungültige Daten zählen
        if result.invalid_count > 0:
            WS_AGGREGATES_INVALID.labels(
                stream_type=self.stream_type.value
            ).inc(result.invalid_count)

        # Gültige Aggregates an Kafka senden
        if result.aggregates:
            self.kafka_producer.produce_batch(result.aggregates)

            # Metriken pro Symbol
            for agg in result.aggregates:
                WS_AGGREGATES_PARSED.labels(
                    stream_type=self.stream_type.value,
                    symbol=agg.symbol,
                ).inc()

            # Periodisches Logging
            if self._message_count % 100 == 0:
                elapsed = time.time() - self._start_time
                rate = self._message_count / max(elapsed, 1)
                logger.info(
                    f"Progress: {self._message_count} messages processed "
                    f"({rate:.1f} msg/s), "
                    f"last batch: {len(result.aggregates)} aggregates"
                )

    def _handle_status_message(self, status):
        """Verarbeitet Status-Nachrichten."""
        logger.info(f"Status: [{status.status}] {status.message}")

        if status.status == "force_disconnect":
            logger.warning(
                "Server forced disconnect! Possible rate limit or maintenance."
            )
            self._is_running = False

    # =========================================
    # Cleanup
    # =========================================

    async def _cleanup(self):
        """Räumt alle Ressourcen auf."""
        logger.info("Cleaning up...")

        # WebSocket schließen
        if self._ws and self._ws.open:
            try:
                await self._ws.close()
            except Exception:
                pass

        # Kafka flushen
        self.kafka_producer.flush(timeout=15.0)

        # Health: Nicht mehr bereit
        if self.health_server:
            self.health_server.set_ready(False)

        WS_CONNECTION_STATE.labels(
            stream_type=self.stream_type.value
        ).set(0)

        # Statistiken
        self._log_final_stats()

    def _log_final_stats(self):
        """Loggt abschließende Statistiken."""
        elapsed = time.time() - self._start_time if self._start_time else 0
        parser_stats = self._parser.get_stats()
        kafka_stats = self.kafka_producer.get_stats()
        reconnect_stats = self._reconnect.get_stats()

        logger.info("=" * 60)
        logger.info(f"  WebSocket Client Stats ({self.stream_type.value})")
        logger.info("=" * 60)
        logger.info(f"  Runtime:          {elapsed:.0f}s")
        logger.info(f"  Messages:         {self._message_count}")
        logger.info(f"  Msg/s:            {self._message_count / max(elapsed, 1):.1f}")
        logger.info(f"  Parser Valid:     {parser_stats['total_valid']}")
        logger.info(f"  Parser Invalid:   {parser_stats['total_invalid']}")
        logger.info(f"  Validity Rate:    {parser_stats['validity_rate']:.1f}%")
        logger.info(f"  Kafka Produced:   {kafka_stats['total_produced']}")
        logger.info(f"  Kafka Delivered:  {kafka_stats['total_delivered']}")
        logger.info(f"  Kafka Errors:     {kafka_stats['total_errors']}")
        logger.info(f"  Reconnections:    {reconnect_stats['total_reconnects']}")
        logger.info("=" * 60)

    def get_stats(self) -> dict:
        """Gesamt-Statistiken."""
        return {
            "stream_type": self.stream_type.value,
            "is_running": self._is_running,
            "is_connected": self.is_connected,
            "message_count": self._message_count,
            "parser": self._parser.get_stats(),
            "kafka": self.kafka_producer.get_stats(),
            "reconnect": self._reconnect.get_stats(),
            "tickers": self.ticker_manager.get_stats(),
        }
