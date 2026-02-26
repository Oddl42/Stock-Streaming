#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:13:37 2026

@author: twi
"""

"""
JSON Schemas für Massive.com WebSocket Daten.

Definiert die Struktur der eingehenden Kafka-Messages
für Sekunden- und Minuten-Aggregationen.

Referenz: 
  - https://massive.com/docs/websocket/stocks/aggregates-per-second
  - https://massive.com/docs/websocket/stocks/aggregates-per-minute
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
    TimestampType,
    BooleanType,
)


# ============================================================
# Massive.com WebSocket: Aggregates Per Second ("A" Event)
# ============================================================
AGGREGATE_SECOND_SCHEMA = StructType([
    StructField("ev", StringType(), True),        # Event Type: "A"
    StructField("sym", StringType(), False),       # Symbol (Ticker)
    StructField("v", LongType(), True),            # Tick Volume
    StructField("av", LongType(), True),           # Accumulated Volume (heute)
    StructField("op", DoubleType(), True),         # Today's Official Opening Price
    StructField("vw", DoubleType(), True),         # Volume Weighted Average Price (Tick)
    StructField("o", DoubleType(), True),          # Tick Open Price
    StructField("c", DoubleType(), True),          # Tick Close Price
    StructField("h", DoubleType(), True),          # Tick High Price
    StructField("l", DoubleType(), True),          # Tick Low Price
    StructField("a", DoubleType(), True),          # Today's VWAP
    StructField("z", IntegerType(), True),         # Tick Average Trade Size
    StructField("s", LongType(), True),            # Tick Start Timestamp (ms epoch)
    StructField("e", LongType(), True),            # Tick End Timestamp (ms epoch)
    StructField("n", IntegerType(), True),         # Number of Trades in Tick
    StructField("otc", BooleanType(), True),       # OTC Flag
])

# ============================================================
# Massive.com WebSocket: Aggregates Per Minute ("AM" Event)
# ============================================================
AGGREGATE_MINUTE_SCHEMA = StructType([
    StructField("ev", StringType(), True),        # Event Type: "AM"
    StructField("sym", StringType(), False),       # Symbol (Ticker)
    StructField("v", LongType(), True),            # Tick Volume
    StructField("av", LongType(), True),           # Accumulated Volume (heute)
    StructField("op", DoubleType(), True),         # Today's Official Opening Price
    StructField("vw", DoubleType(), True),         # Volume Weighted Average Price
    StructField("o", DoubleType(), True),          # Tick Open Price
    StructField("c", DoubleType(), True),          # Tick Close Price
    StructField("h", DoubleType(), True),          # Tick High Price
    StructField("l", DoubleType(), True),          # Tick Low Price
    StructField("a", DoubleType(), True),          # Today's VWAP
    StructField("z", IntegerType(), True),         # Tick Average Trade Size
    StructField("s", LongType(), True),            # Tick Start Timestamp (ms epoch)
    StructField("e", LongType(), True),            # Tick End Timestamp (ms epoch)
    StructField("n", IntegerType(), True),         # Number of Trades in Tick
    StructField("otc", BooleanType(), True),       # OTC Flag
])


# ============================================================
# Ziel-Schema für TimescaleDB
# ============================================================
TIMESCALEDB_SECOND_SCHEMA = StructType([
    StructField("time", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("accumulated_volume", LongType(), True),
    StructField("official_open", DoubleType(), True),
    StructField("avg_trade_size", IntegerType(), True),
    StructField("num_trades", IntegerType(), True),
    StructField("tick_start", TimestampType(), True),
    StructField("tick_end", TimestampType(), True),
    StructField("is_otc", BooleanType(), True),
])

TIMESCALEDB_MINUTE_SCHEMA = StructType([
    StructField("time", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("accumulated_volume", LongType(), True),
    StructField("official_open", DoubleType(), True),
    StructField("avg_trade_size", IntegerType(), True),
    StructField("num_trades", IntegerType(), True),
    StructField("tick_start", TimestampType(), True),
    StructField("tick_end", TimestampType(), True),
    StructField("is_otc", BooleanType(), True),
])


# ============================================================
# Kafka Message Wrapper Schema (Key + Value + Metadata)
# ============================================================
KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("key", StringType(), True),        # Ticker als Key
    StructField("value", StringType(), True),      # JSON Payload
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("timestampType", IntegerType(), True),
])
