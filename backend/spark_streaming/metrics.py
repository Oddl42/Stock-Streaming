#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:16:33 2026

@author: twi
"""

"""
Prometheus Custom Metrics für Spark Streaming Jobs.

Exportiert Metriken über den Prometheus Client,
damit sie von Prometheus gescraped werden können.

Monitoring von Spark Streaming Jobs ist essenziell in Produktion:
Nutze Spark UI, Prometheus und Grafana für Real-Time-Insights.
"""

import logging
import threading
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    start_http_server,
    CollectorRegistry,
)

logger = logging.getLogger(__name__)

# Eigene Registry (um Konflikte mit Spark's Prometheus zu vermeiden)
REGISTRY = CollectorRegistry()


# ============================================================
# Counters
# ============================================================
RECORDS_PROCESSED = Counter(
    "spark_streaming_records_processed_total",
    "Gesamtzahl verarbeiteter Datensätze",
    labelnames=["stream_type", "symbol"],
    registry=REGISTRY,
)

RECORDS_WRITTEN = Counter(
    "spark_streaming_records_written_total",
    "Gesamtzahl in DB geschriebener Datensätze",
    labelnames=["stream_type", "table"],
    registry=REGISTRY,
)

RECORDS_REJECTED = Counter(
    "spark_streaming_records_rejected_total",
    "Gesamtzahl abgelehnter Datensätze",
    labelnames=["stream_type", "reason"],
    registry=REGISTRY,
)

BATCHES_PROCESSED = Counter(
    "spark_streaming_batches_processed_total",
    "Gesamtzahl verarbeiteter Micro-Batches",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

WRITE_ERRORS = Counter(
    "spark_streaming_write_errors_total",
    "Gesamtzahl fehlgeschlagener DB-Writes",
    labelnames=["stream_type", "table"],
    registry=REGISTRY,
)


# ============================================================
# Gauges
# ============================================================
ACTIVE_STREAMS = Gauge(
    "spark_streaming_active_streams",
    "Anzahl aktiver Streaming-Jobs",
    registry=REGISTRY,
)

CURRENT_BATCH_SIZE = Gauge(
    "spark_streaming_current_batch_size",
    "Aktuelle Micro-Batch Größe",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

SYMBOLS_STREAMING = Gauge(
    "spark_streaming_symbols_count",
    "Anzahl aktuell gestreamter Symbole",
    labelnames=["stream_type"],
    registry=REGISTRY,
)

KAFKA_LAG = Gauge(
    "spark_streaming_kafka_lag",
    "Kafka Consumer Lag",
    labelnames=["stream_type", "partition"],
    registry=REGISTRY,
)


# ============================================================
# Histograms
# ============================================================
BATCH_PROCESSING_TIME = Histogram(
    "spark_streaming_batch_processing_seconds",
    "Verarbeitungszeit pro Micro-Batch",
    labelnames=["stream_type"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
    registry=REGISTRY,
)

BATCH_WRITE_TIME = Histogram(
    "spark_streaming_batch_write_seconds",
    "DB-Schreibzeit pro Micro-Batch",
    labelnames=["stream_type", "table"],
    buckets=(0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0),
    registry=REGISTRY,
)


# ============================================================
# Summaries
# ============================================================
PRICE_DISTRIBUTION = Summary(
    "spark_streaming_close_price",
    "Verteilung der Close-Preise",
    labelnames=["stream_type"],
    registry=REGISTRY,
)


# ============================================================
# Metrics Server
# ============================================================
_metrics_server_started = False


def start_metrics_server(port: int = 8090):
    """
    Startet den Prometheus Metrics HTTP Server.

    Args:
        port: Port für den Metrics-Endpoint
    """
    global _metrics_server_started
    if _metrics_server_started:
        return

    try:
        start_http_server(port, registry=REGISTRY)
        _metrics_server_started = True
        logger.info(f"Prometheus metrics server started on port {port}")
        logger.info(f"Metrics endpoint: http://localhost:{port}/metrics")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")


class BatchMetricsCollector:
    """
    Sammelt Metriken für jeden Micro-Batch.
    Wird als Wrapper um die foreachBatch-Funktion verwendet.
    """

    def __init__(self, stream_type: str):
        self.stream_type = stream_type

    def track_batch(self, batch_size: int, symbols: list = None):
        """Tracked einen verarbeiteten Batch."""
        BATCHES_PROCESSED.labels(stream_type=self.stream_type).inc()
        CURRENT_BATCH_SIZE.labels(stream_type=self.stream_type).set(batch_size)

        if symbols:
            SYMBOLS_STREAMING.labels(
                stream_type=self.stream_type
            ).set(len(set(symbols)))

    def track_write(self, table: str, row_count: int, duration_seconds: float):
        """Tracked einen DB-Write."""
        RECORDS_WRITTEN.labels(
            stream_type=self.stream_type, table=table
        ).inc(row_count)
        BATCH_WRITE_TIME.labels(
            stream_type=self.stream_type, table=table
        ).observe(duration_seconds)

    def track_rejection(self, count: int, reason: str = "quality_check"):
        """Tracked abgelehnte Datensätze."""
        RECORDS_REJECTED.labels(
            stream_type=self.stream_type, reason=reason
        ).inc(count)

    def track_error(self, table: str):
        """Tracked einen Schreibfehler."""
        WRITE_ERRORS.labels(
            stream_type=self.stream_type, table=table
        ).inc()
