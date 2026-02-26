#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 14:06:42 2026

@author: twi
"""

"""
Kafka-Konfiguration für Producer und Consumer.

Zentrale Stelle für alle Kafka-Einstellungen,
genutzt vom WebSocket Producer und Spark Streaming.
"""

import os
from dataclasses import dataclass, field
from config.settings import settings


@dataclass
class KafkaProducerConfig:
    """Konfiguration für den Kafka Producer (WebSocket → Kafka)."""

    bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS
    client_id: str = os.getenv("KAFKA_PRODUCER_CLIENT_ID", "ws-producer")

    # Reliability
    acks: str = "all"
    retries: int = 5
    retry_backoff_ms: int = 500
    delivery_timeout_ms: int = 30000
    enable_idempotence: bool = True

    # Batching
    batch_num_messages: int = 1000
    batch_size: int = 65536           # 64KB
    linger_ms: int = 50
    queue_buffering_max_messages: int = 100000
    queue_buffering_max_kbytes: int = 1048576  # 1GB

    # Compression
    compression_type: str = "lz4"

    def to_confluent_config(self) -> dict:
        """Erstellt ein confluent-kafka kompatibles Config-Dict."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "retries": self.retries,
            "retry.backoff.ms": self.retry_backoff_ms,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "enable.idempotence": self.enable_idempotence,
            "batch.num.messages": self.batch_num_messages,
            "batch.size": self.batch_size,
            "linger.ms": self.linger_ms,
            "queue.buffering.max.messages": self.queue_buffering_max_messages,
            "queue.buffering.max.kbytes": self.queue_buffering_max_kbytes,
            "compression.type": self.compression_type,
        }


@dataclass
class KafkaConsumerConfig:
    """Konfiguration für Kafka Consumer (Spark liest aus Kafka)."""

    bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS
    group_id_prefix: str = os.getenv(
        "KAFKA_CONSUMER_GROUP_PREFIX", "spark-stock-streaming"
    )

    # Consumer Settings
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = False  # Spark managt Offsets
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_records: int = 10000
    fetch_max_bytes: int = 52428800   # 50MB
    max_partition_fetch_bytes: int = 1048576  # 1MB

    def get_spark_kafka_options(self, topic: str, group_suffix: str = "") -> dict:
        """Erstellt Spark readStream Kafka-Optionen."""
        group_id = f"{self.group_id_prefix}-{group_suffix}" if group_suffix else self.group_id_prefix
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": self.auto_offset_reset,
            "failOnDataLoss": "false",
            "kafka.group.id": group_id,
            "kafka.session.timeout.ms": str(self.session_timeout_ms),
            "kafka.heartbeat.interval.ms": str(self.heartbeat_interval_ms),
            "kafka.max.poll.records": str(self.max_poll_records),
            "kafka.fetch.max.bytes": str(self.fetch_max_bytes),
            "kafka.max.partition.fetch.bytes": str(self.max_partition_fetch_bytes),
        }


@dataclass
class KafkaTopicConfig:
    """Konfiguration der Kafka Topics."""

    topic_second: str = settings.KAFKA_TOPIC_SECOND
    topic_minute: str = settings.KAFKA_TOPIC_MINUTE

    # Topic-Einstellungen
    second_partitions: int = int(os.getenv("KAFKA_SECOND_PARTITIONS", "8"))
    minute_partitions: int = int(os.getenv("KAFKA_MINUTE_PARTITIONS", "4"))
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    retention_ms: int = int(os.getenv("KAFKA_RETENTION_MS", "172800000"))  # 2 Tage

    def get_topic_for_event(self, event_type: str) -> str:
        """Gibt das Topic basierend auf dem Event-Typ zurück."""
        if event_type == "A":
            return self.topic_second
        elif event_type == "AM":
            return self.topic_minute
        else:
            raise ValueError(f"Unknown event type: {event_type}")


# Singletons
kafka_producer_config = KafkaProducerConfig()
kafka_consumer_config = KafkaConsumerConfig()
kafka_topic_config = KafkaTopicConfig()
