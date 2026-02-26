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

from backend.websocket_producer.stream_manager import StreamManager, StreamType
from backend.websocket_producer.ws_client import MassiveWebSocketClient
from backend.websocket_producer.kafka_producer import StockKafkaProducer
from backend.websocket_producer.ticker_manager import TickerManager

__all__ = [
    "StreamManager",
    "StreamType",
    "MassiveWebSocketClient",
    "StockKafkaProducer",
    "TickerManager",
]
