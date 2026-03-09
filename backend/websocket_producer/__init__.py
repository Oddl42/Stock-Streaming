#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 08:59:22 2026

@author: twi
"""

"""
WebSocket Producer Package.

Verbindet sich mit der Massive.com WebSocket API,
empfängt Stock-Aggregationsdaten und schreibt sie in Kafka.
"""


def __getattr__(name):
    """Lazy imports to avoid circular dependencies."""
    if name == "StreamManager":
        from backend.websocket_producer.stream_manager import StreamManager
        return StreamManager
    if name == "StreamType":
        from backend.websocket_producer.stream_manager import StreamType
        return StreamType
    if name == "MassiveWebSocketClient":
        from backend.websocket_producer.ws_client import MassiveWebSocketClient
        return MassiveWebSocketClient
    if name == "StockKafkaProducer":
        from backend.websocket_producer.kafka_producer import StockKafkaProducer
        return StockKafkaProducer
    if name == "TickerManager":
        from backend.websocket_producer.ticker_manager import TickerManager
        return TickerManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "StreamManager",
    "StreamType",
    "MassiveWebSocketClient",
    "StockKafkaProducer",
    "TickerManager",
]