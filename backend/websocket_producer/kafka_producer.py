#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:31:11 2026

@author: twi
"""

"""
Kafka Producer Wrapper für Stock-Daten.

Kapselt die confluent-kafka Producer-Logik mit:
- Delivery Callbacks
- Error Handling
- Batching
- Metriken
"""

import json
import time
import logging
from typing import Optional, Callable
from confluent_kafka import Producer, KafkaError, KafkaException

from config.settings import settings
from backend.websocket_producer.message_parser import AggregateData
from backend.websocket_producer.metrics import (
    KAFKA_MESSAGES_PRODUCED,
    KAFKA_PRODUCE_ERRORS,
    KAFKA_PRODUCE_LATENCY,
    KAFKA_QUEUE_SIZE,
)

logger = logging.getLogger(__name__)


class StockKafkaProducer:
    """
    Kafka Producer für Stock-Aggregationsdaten.

    Features:
    - Asynchrones Producing mit Delivery-Callbacks
    - Automatisches Batching
    - Partitioning nach Symbol (Key)
    - Error Handling & Retry
    - Metriken
    """

    def __init__(
        self,
        bootstrap_servers: str = None,
        client_id: str = "ws-producer",
        additional_config: dict = None,
    ):
        """
        Args:
            bootstrap_servers: Kafka Broker Adressen
            client_id: Eindeutige Client-ID
            additional_config: Zusätzliche Kafka-Konfigurationen
        """
        self._config = {
            # Connection
            "bootstrap.servers": bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": client_id,

            # Reliability
            "acks": "all",                          # Warte auf alle ISR
            "retries": 5,                           # Max Retries
            "retry.backoff.ms": 500,                # Retry Backoff
            "delivery.timeout.ms": 30000,           # Max Delivery Time

            # Batching (Performance)
            "batch.num.messages": 1000,             # Max Messages pro Batch
            "batch.size": 65536,                    # 64KB Batch-Größe
            "linger.ms": 50,                        # 50ms warten für Batching
            "queue.buffering.max.messages": 100000, # Max Queue-Größe
            "queue.buffering.max.kbytes": 1048576,  # 1GB Queue

            # Compression
            "compression.type": "lz4",              # Schnelle Kompression

            # Idempotenz (exactly-once)
            "enable.idempotence": True,

            # Errors
            "error_cb": self._error_callback,
        }

        if additional_config:
            self._config.update(additional_config)

        self._producer: Optional[Producer] = None
        self._is_initialized = False
        self._total_produced = 0
        self._total_errors = 0
        self._total_delivered = 0

    def initialize(self):
        """Initialisiert den Kafka Producer."""
        if self._is_initialized:
            return

        try:
            self._producer = Producer(self._config)
            self._is_initialized = True
            logger.info(
                f"Kafka Producer initialized: "
                f"{self._config['bootstrap.servers']}"
            )
        except KafkaException as e:
            logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def produce(
        self,
        aggregate: AggregateData,
        topic: str = None,
        on_delivery: Callable = None,
    ):
        """
        Produziert eine Aggregate-Nachricht in Kafka.

        Args:
            aggregate: Geparste Aggregationsdaten
            topic: Ziel-Topic (oder automatisch aus Event-Typ)
            on_delivery: Optionaler Delivery-Callback
        """
        if not self._is_initialized:
            self.initialize()

        # Topic bestimmen
        if topic is None:
            topic = (
                settings.KAFKA_TOPIC_SECOND
                if aggregate.event_type == "A"
                else settings.KAFKA_TOPIC_MINUTE
            )

        # Key = Symbol (für Partitioning)
        key = aggregate.kafka_key.encode("utf-8")

        # Value = JSON der Rohdaten
        value = aggregate.to_kafka_value().encode("utf-8")

        # Produce
        start_time = time.time()
        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=on_delivery or self._delivery_callback,
                timestamp=aggregate.tick_start_ms,
            )
            self._total_produced += 1

            KAFKA_MESSAGES_PRODUCED.labels(topic=topic).inc()
            KAFKA_PRODUCE_LATENCY.labels(topic=topic).observe(
                time.time() - start_time
            )

        except BufferError:
            # Queue voll → poll und retry
            logger.warning("Kafka producer queue full, polling...")
            self._producer.poll(1.0)
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery or self._delivery_callback,
                    timestamp=aggregate.tick_start_ms,
                )
                self._total_produced += 1
            except Exception as e:
                self._total_errors += 1
                KAFKA_PRODUCE_ERRORS.labels(topic=topic).inc()
                logger.error(f"Kafka produce failed after retry: {e}")

        except KafkaException as e:
            self._total_errors += 1
            KAFKA_PRODUCE_ERRORS.labels(topic=topic).inc()
            logger.error(f"Kafka produce error: {e}")

    def produce_batch(
        self,
        aggregates: list[AggregateData],
        topic: str = None,
    ):
        """
        Produziert eine Liste von Aggregaten in Kafka.

        Args:
            aggregates: Liste der zu produzierenden Daten
            topic: Optionales gemeinsames Topic
        """
        for agg in aggregates:
            self.produce(agg, topic=topic)

        # Trigger delivery reports
        self._producer.poll(0)

    def poll(self, timeout: float = 0):
        """Pollt für Delivery-Reports."""
        if self._producer:
            return self._producer.poll(timeout)
        return 0

    def flush(self, timeout: float = 10.0):
        """Flusht alle ausstehenden Nachrichten."""
        if self._producer:
            remaining = self._producer.flush(timeout)
            if remaining > 0:
                logger.warning(
                    f"Kafka flush: {remaining} messages still in queue"
                )
            else:
                logger.debug("Kafka flush: all messages delivered")
            return remaining
        return 0

    def close(self):
        """Schließt den Producer."""
        if self._producer:
            logger.info("Closing Kafka Producer...")
            remaining = self.flush(timeout=30.0)
            if remaining > 0:
                logger.warning(
                    f"Kafka Producer closed with {remaining} "
                    f"undelivered messages!"
                )
            self._is_initialized = False
            logger.info("Kafka Producer closed.")

    def _delivery_callback(self, err, msg):
        """Callback nach erfolgreicher/fehlgeschlagener Delivery."""
        if err is not None:
            self._total_errors += 1
            KAFKA_PRODUCE_ERRORS.labels(topic=msg.topic()).inc()
            logger.error(
                f"Delivery failed for {msg.key().decode()}: {err}"
            )
        else:
            self._total_delivered += 1
            logger.debug(
                f"Delivered: {msg.key().decode()} → "
                f"{msg.topic()}[{msg.partition()}] @ {msg.offset()}"
            )

    def _error_callback(self, err):
        """Globaler Error-Callback."""
        logger.error(f"Kafka Producer error: {err}")

    @property
    def queue_size(self) -> int:
        """Aktuelle Queue-Größe."""
        if self._producer:
            size = len(self._producer)
            KAFKA_QUEUE_SIZE.set(size)
            return size
        return 0

    def get_stats(self) -> dict:
        """Producer-Statistiken."""
        return {
            "is_initialized": self._is_initialized,
            "total_produced": self._total_produced,
            "total_delivered": self._total_delivered,
            "total_errors": self._total_errors,
            "queue_size": self.queue_size,
            "delivery_rate": (
                self._total_delivered / max(self._total_produced, 1) * 100
            ),
        }
