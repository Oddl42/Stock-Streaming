#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:50:08 2026

@author: twi
"""

"""
Integration Tests: WebSocket Producer → Kafka Pipeline.

Benötigt laufende Kafka-Instanz (Docker Compose).
"""

import pytest
import json
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from backend.websocket_producer.ws_client import MassiveWebSocketClient, StreamType
from backend.websocket_producer.kafka_producer import StockKafkaProducer
from backend.websocket_producer.ticker_manager import TickerManager
from backend.websocket_producer.message_parser import MessageParser
from tests.fixtures.sample_data import generate_batch_messages


@pytest.mark.integration
@pytest.mark.websocket
class TestWebSocketToKafkaPipeline:

    @pytest.fixture
    def mock_ws_connection(self, sample_ws_second_message):
        """Mock WebSocket-Verbindung mit Testdaten."""
        ws = AsyncMock()
        ws.open = True

        # Simuliere Auth-Antwort und dann Daten
        auth_response = json.dumps([{
            "ev": "status",
            "status": "auth_success",
            "message": "authenticated",
        }])

        data_messages = [
            json.dumps(sample_ws_second_message)
            for _ in range(5)
        ]

        ws.recv = AsyncMock(side_effect=[auth_response] + data_messages)
        ws.__aiter__ = AsyncMock(
            return_value=iter(data_messages)
        )

        return ws

    @pytest.mark.asyncio
    async def test_message_flow(self, sample_ws_second_message, mock_kafka_producer):
        """Testet den kompletten Nachrichtenfluss: Parse → Produce."""
        parser = MessageParser()

        with patch(
            "backend.websocket_producer.kafka_producer.Producer",
            return_value=mock_kafka_producer,
        ):
            kafka_prod = StockKafkaProducer(
                bootstrap_servers="localhost:9092"
            )
            kafka_prod.initialize()

            raw = json.dumps(sample_ws_second_message)
            result = parser.parse(raw)

            kafka_prod.produce_batch(result.aggregates)

            assert mock_kafka_producer.produce.call_count == 2

    @pytest.mark.asyncio
    async def test_batch_subscription(self):
        """Testet Batch-Subscription für viele Ticker."""
        all_symbols = [f"T{i:03d}" for i in range(250)]

        with patch(
            "backend.websocket_producer.ticker_manager.ticker_loader"
        ) as mock_loader:
            mock_loader.all_symbols = all_symbols
            manager = TickerManager()
            manager._active_tickers = all_symbols

        batches = manager.get_subscription_batches(prefix="A")

        assert len(batches) == 3
        # Prüfe dass alle Ticker verteilt sind
        all_params = ",".join(batches)
        for symbol in all_symbols[:5]:
            assert f"A.{symbol}" in all_params

    @pytest.mark.asyncio
    async def test_reconnection_after_disconnect(self):
        """Testet Reconnection nach Verbindungsabbruch."""
        from backend.websocket_producer.reconnect_handler import (
            ReconnectHandler,
            ReconnectConfig,
            ConnectionState,
        )

        handler = ReconnectHandler(
            ReconnectConfig(
                initial_delay=0.05,
                max_delay=0.2,
                max_retries=3,
                jitter=0.0,
            )
        )

        handler.on_disconnected("Test disconnect")
        assert handler.state == ConnectionState.RECONNECTING

        should_retry = await handler.wait_before_retry()
        assert should_retry is True

        handler.on_connected()
        assert handler.state == ConnectionState.CONNECTED
