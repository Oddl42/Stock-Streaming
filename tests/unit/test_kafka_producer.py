#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:45:59 2026

@author: twi
"""

"""
Unit Tests für den Kafka Producer.
"""

import pytest
from unittest.mock import MagicMock, patch, call

from backend.websocket_producer.kafka_producer import StockKafkaProducer
from backend.websocket_producer.message_parser import AggregateData


class TestStockKafkaProducer:

    @pytest.fixture(autouse=True)
    def setup(self, mock_kafka_producer):
        """Setup mit gemocktem confluent-kafka Producer."""
        with patch(
            "backend.websocket_producer.kafka_producer.Producer",
            return_value=mock_kafka_producer,
        ):
            self.producer = StockKafkaProducer(
                bootstrap_servers="localhost:9092",
                client_id="test-producer",
            )
            self.producer.initialize()
            self.mock_inner = mock_kafka_producer

    def _create_aggregate(self, symbol="AAPL", event_type="A") -> AggregateData:
        """Hilfsfunktion: Erstellt ein AggregateData Objekt."""
        return AggregateData(
            event_type=event_type,
            symbol=symbol,
            open=185.0,
            high=186.0,
            low=184.0,
            close=185.5,
            volume=5000,
            vwap=185.3,
            accumulated_volume=50000000,
            official_open=185.0,
            avg_trade_size=100,
            num_trades=45,
            tick_start_ms=1700000000000,
            tick_end_ms=1700000001000,
            is_otc=False,
            raw_data={"ev": event_type, "sym": symbol, "o": 185.0},
        )

    def test_produce_single_message(self):
        """Testet Produzieren einer einzelnen Nachricht."""
        agg = self._create_aggregate("AAPL", "A")
        self.producer.produce(agg)

        self.mock_inner.produce.assert_called_once()
        call_kwargs = self.mock_inner.produce.call_args
        assert call_kwargs.kwargs["key"] == b"AAPL"
        assert b"AAPL" in call_kwargs.kwargs["value"] or "AAPL" in str(call_kwargs)

    def test_produce_auto_topic_second(self):
        """Testet automatische Topic-Zuordnung für Sekunden."""
        agg = self._create_aggregate(event_type="A")
        self.producer.produce(agg)

        call_kwargs = self.mock_inner.produce.call_args
        assert "second" in call_kwargs.kwargs["topic"]

    def test_produce_auto_topic_minute(self):
        """Testet automatische Topic-Zuordnung für Minuten."""
        agg = self._create_aggregate(event_type="AM")
        self.producer.produce(agg)

        call_kwargs = self.mock_inner.produce.call_args
        assert "minute" in call_kwargs.kwargs["topic"]

    def test_produce_custom_topic(self):
        """Testet Produzieren mit benutzerdefiniertem Topic."""
        agg = self._create_aggregate()
        self.producer.produce(agg, topic="custom.topic")

        call_kwargs = self.mock_inner.produce.call_args
        assert call_kwargs.kwargs["topic"] == "custom.topic"

    def test_produce_batch(self):
        """Testet Batch-Produzieren."""
        aggregates = [
            self._create_aggregate("AAPL"),
            self._create_aggregate("MSFT"),
            self._create_aggregate("GOOGL"),
        ]
        self.producer.produce_batch(aggregates)

        assert self.mock_inner.produce.call_count == 3
        self.mock_inner.poll.assert_called()

    def test_flush(self):
        """Testet Flush."""
        self.producer.flush(timeout=5.0)
        self.mock_inner.flush.assert_called_once_with(5.0)

    def test_close(self):
        """Testet Close mit Flush."""
        self.producer.close()
        self.mock_inner.flush.assert_called_once()
        assert self.producer._is_initialized is False

    def test_queue_size(self):
        """Testet Queue-Größe Abfrage."""
        self.mock_inner.__len__.return_value = 42
        assert self.producer.queue_size == 42

    def test_stats(self):
        """Testet Statistik-Ausgabe."""
        stats = self.producer.get_stats()

        assert "is_initialized" in stats
        assert "total_produced" in stats
        assert "total_delivered" in stats
        assert "total_errors" in stats
        assert "queue_size" in stats
        assert "delivery_rate" in stats

    def test_buffer_error_retry(self):
        """Testet Retry bei BufferError."""
        self.mock_inner.produce.side_effect = [BufferError("Queue full"), None]

        agg = self._create_aggregate()
        self.producer.produce(agg)

        # Sollte poll aufrufen und nochmal versuchen
        self.mock_inner.poll.assert_called()
        assert self.mock_inner.produce.call_count == 2

    def test_double_initialize(self):
        """Testet dass doppelte Initialisierung ignoriert wird."""
        self.producer.initialize()
        # Sollte keinen Fehler werfen
        assert self.producer._is_initialized is True
