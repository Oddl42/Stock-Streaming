#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:30:45 2026

@author: twi
"""

"""
Prometheus Metriken für den WebSocket Producer.
"""

import logging
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    CollectorRegistry,
    start_http_server,
)

logger = logging.getLogger(__name__)

REGISTRY = CollectorRegistry()

# ============================================================
# Counters
# ============================================================
WS_MESSAGES_RECEIVED = Counter(
    "ws_producer_messages_received_total",
    "Gesamtzahl empfangener WebSocket-Nachrichten",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

WS_AGGREGATES_PARSED = Counter(
    "ws_producer_aggregates_parsed_total",
    "Gesamtzahl geparster Aggregate-Events",
    labelnames=["stream_type", "symbol"],
    registry=REGISTRY,
)

WS_AGGREGATES_INVALID = Counter(
    "ws_producer_aggregates_invalid_total",
    "Gesamtzahl ungültiger Aggregate-Events",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

KAFKA_MESSAGES_PRODUCED = Counter(
    "ws_producer_kafka_messages_produced_total",
    "Gesamtzahl produzierter Kafka-Nachrichten",
    labelnames=["topic"],
    registry=REGISTRY,
)

KAFKA_PRODUCE_ERRORS = Counter(
    "ws_producer_kafka_produce_errors_total",
    "Gesamtzahl fehlgeschlagener Kafka-Produces",
    labelnames=["topic"],
    registry=REGISTRY,
)

WS_RECONNECTIONS = Counter(
    "ws_producer_reconnections_total",
    "Gesamtzahl WebSocket-Wiederverbindungen",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

# ============================================================
# Gauges
# ============================================================
WS_CONNECTION_STATE = Gauge(
    "ws_producer_connection_state",
    "WebSocket-Verbindungszustand (1=connected, 0=disconnected)",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

WS_SUBSCRIBED_TICKERS = Gauge(
    "ws_producer_subscribed_tickers",
    "Anzahl aktuell abonnierter Ticker",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

KAFKA_QUEUE_SIZE = Gauge(
    "ws_producer_kafka_queue_size",
    "Aktuelle Kafka Producer Queue-Größe",
    registry=REGISTRY,
)

# ============================================================
# Histograms
# ============================================================
WS_MESSAGE_PROCESSING_TIME = Histogram(
    "ws_producer_message_processing_seconds",
    "Verarbeitungszeit pro WebSocket-Nachricht",
    labelnames=["stream_type"],
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1),
    registry=REGISTRY,
)

KAFKA_PRODUCE_LATENCY = Histogram(
    "ws_producer_kafka_produce_latency_seconds",
    "Kafka Produce-Latenz",
    labelnames=["topic"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
    registry=REGISTRY,
)

# ============================================================
# Summaries
# ============================================================
WS_MESSAGE_SIZE = Summary(
    "ws_producer_message_size_bytes",
    "Größe der WebSocket-Nachrichten in Bytes",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

# ============================================================
# Server
# ============================================================
_metrics_started = False


def start_producer_metrics_server(port: int = 8092):
    """Startet den Prometheus Metrics HTTP Server."""
    global _metrics_started
    if _metrics_started:
        return
    try:
        start_http_server(port, registry=REGISTRY)
        _metrics_started = True
        logger.info(f"Producer metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
