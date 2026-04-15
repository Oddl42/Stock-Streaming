#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:14:06 2026

@author: twi
"""

"""
DataFrame Transformations für Streaming-Daten.

Transformiert rohe Kafka-Messages in das TimescaleDB-Zielformat.
Enthält Validierung, Anreicherung und Filterung.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, TimestampType

from backend.spark_streaming.schemas import (
    AGGREGATE_SECOND_SCHEMA,
    AGGREGATE_MINUTE_SCHEMA,
)

logger = logging.getLogger(__name__)


class StreamTransformer:
    """
    Transformiert rohe Kafka-Nachrichten in das Zielformat für TimescaleDB.
    """

    @staticmethod
    def parse_kafka_messages(
        kafka_df: DataFrame,
        schema,
    ) -> DataFrame:
        """
        Parst die JSON-Nachrichten aus Kafka.

        Args:
            kafka_df: Roher Kafka DataFrame (mit key, value, topic, etc.)
            schema: Das erwartete JSON Schema (Second oder Minute)

        Returns:
            DataFrame mit geparsten Spalten
        """
        return (
            kafka_df
            .select(
                # Kafka Metadata behalten
                F.col("key").cast("string").alias("kafka_key"),
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
                # JSON parsen
                F.from_json(
                    F.col("value").cast("string"),
                    schema,
                ).alias("data"),
            )
            .select(
                "kafka_key",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "data.*",
            )
        )

    @staticmethod
    def transform_to_target_schema(
        parsed_df: DataFrame,
        stream_type: str = "second",
    ) -> DataFrame:
        """
        Transformiert geparste Daten in das TimescaleDB-Zielformat.

        Args:
            parsed_df: Geparster DataFrame
            stream_type: "second" oder "minute"

        Returns:
            DataFrame im Ziel-Schema
        """
        return (
            parsed_df
            .select(
                # Primärer Timestamp: Tick Start in Sekunden
                (F.col("s") / 1000).cast(TimestampType()).alias("time"),

                # Symbol
                F.col("sym").alias("symbol"),

                # OHLCV
                F.col("o").cast(DoubleType()).alias("open"),
                F.col("h").cast(DoubleType()).alias("high"),
                F.col("l").cast(DoubleType()).alias("low"),
                F.col("c").cast(DoubleType()).alias("close"),
                F.col("v").cast(LongType()).alias("volume"),

                # Zusätzliche Felder
                F.col("vw").cast(DoubleType()).alias("vwap"),
                F.col("av").cast(LongType()).alias("accumulated_volume"),
                F.col("op").cast(DoubleType()).alias("official_open"),
                F.col("z").alias("avg_trade_size"),
                F.col("n").alias("num_trades"),

                # Tick Start/End als Timestamps
                (F.col("s") / 1000).cast(TimestampType()).alias("tick_start"),
                (F.col("e") / 1000).cast(TimestampType()).alias("tick_end"),

                # OTC Flag
                F.coalesce(F.col("otc"), F.lit(False)).alias("is_otc"),

                # Metadata für Debugging
                F.current_timestamp().alias("processed_at"),
                F.lit(stream_type).alias("stream_type"),
            )
        )

    @staticmethod
    def validate_data(df: DataFrame) -> DataFrame:
        """
        Validiert und filtert ungültige Datensätze.

        Regeln:
        - Symbol darf nicht NULL sein
        - Timestamp darf nicht NULL sein
        - Preise müssen > 0 sein
        - Volume muss >= 0 sein
        - High >= Low
        """
        return (
            df
            .filter(
                # Pflichtfelder
                F.col("symbol").isNotNull()
                & F.col("time").isNotNull()

                # Preis-Validierung
                & (F.col("open") > 0)
                & (F.col("high") > 0)
                & (F.col("low") > 0)
                & (F.col("close") > 0)

                # Logische Konsistenz
                & (F.col("high") >= F.col("low"))
                & (F.col("high") >= F.col("open"))
                & (F.col("high") >= F.col("close"))
                & (F.col("low") <= F.col("open"))
                & (F.col("low") <= F.col("close"))

                # Volume
                & (F.col("volume") >= 0)
            )
        )

    @staticmethod
    def deduplicate(
        df: DataFrame,
        watermark_column: str = "time",
        watermark_delay: str = "10 seconds",
    ) -> DataFrame:
        """
        Entfernt Duplikate basierend auf Symbol + Timestamp.
        Nutzt Watermarking für State-Management.

        Args:
            df: Input DataFrame
            watermark_column: Spalte für Watermark
            watermark_delay: Maximale Verzögerung

        Returns:
            Deduplizierter DataFrame
        """
        return (
            df
            .withWatermark(watermark_column, watermark_delay)
            .dropDuplicates(["symbol", "time"])
        )

    @staticmethod
    def add_derived_columns(df: DataFrame) -> DataFrame:
        """
        Fügt berechnete Spalten hinzu für spätere ML-Nutzung.

        Neue Spalten:
        - price_range: High - Low
        - price_change: Close - Open
        - price_change_pct: Prozentuale Veränderung
        - is_bullish: Close >= Open
        - body_size: |Close - Open|
        - upper_shadow: High - max(Open, Close)
        - lower_shadow: min(Open, Close) - Low
        """
        return (
            df
            # Preis-Range
            .withColumn(
                "price_range",
                F.col("high") - F.col("low"),
            )
            # Preis-Änderung
            .withColumn(
                "price_change",
                F.col("close") - F.col("open"),
            )
            # Prozentuale Änderung
            .withColumn(
                "price_change_pct",
                F.when(
                    F.col("open") > 0,
                    ((F.col("close") - F.col("open")) / F.col("open")) * 100,
                ).otherwise(0.0),
            )
            # Bullish/Bearish
            .withColumn(
                "is_bullish",
                F.col("close") >= F.col("open"),
            )
            # Kerzenkörper
            .withColumn(
                "body_size",
                F.abs(F.col("close") - F.col("open")),
            )
            # Oberer Schatten
            .withColumn(
                "upper_shadow",
                F.col("high") - F.greatest(F.col("open"), F.col("close")),
            )
            # Unterer Schatten
            .withColumn(
                "lower_shadow",
                F.least(F.col("open"), F.col("close")) - F.col("low"),
            )
        )

    @staticmethod
    def filter_otc(df: DataFrame, include_otc: bool = False) -> DataFrame:
        """Filtert OTC-Aktien heraus (optional)."""
        if not include_otc:
            return df.filter(F.col("is_otc") == False)  # noqa: E712
        return df

    @staticmethod
    def select_db_columns(df: DataFrame) -> DataFrame:
        """
        Wählt nur die Spalten aus, die in TimescaleDB geschrieben werden.
        Entfernt Metadata- und Processing-Spalten.
        """
        db_columns = [
            "time",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "accumulated_volume",
            "official_open",
            "avg_trade_size",
            "num_trades",
            "tick_start",
            "tick_end",
            "is_otc",
            # Derived columns für ML
            "price_range",
            "price_change",
            "price_change_pct",
            "is_bullish",
            "body_size",
            "upper_shadow",
            "lower_shadow",
        ]
        # Nur Spalten nehmen, die existieren
        available = [c for c in db_columns if c in df.columns]
        return df.select(*available)


# Convenience-Instanz
transformer = StreamTransformer()
