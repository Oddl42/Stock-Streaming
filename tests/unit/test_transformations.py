#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:48:22 2026

@author: twi
"""

"""
Unit Tests für Spark DataFrame Transformations.

Verwendet eine lokale SparkSession für Tests.
"""

import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, BooleanType

from backend.spark_streaming.transformations import StreamTransformer
from backend.spark_streaming.schemas import AGGREGATE_SECOND_SCHEMA


@pytest.fixture(scope="module")
def spark():
    """Erstellt eine lokale SparkSession für Tests."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-transformations")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_kafka_df(spark):
    """Erstellt einen Sample Kafka-ähnlichen DataFrame."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    data = [
        ('AAPL', f'{{"ev":"A","sym":"AAPL","o":185.0,"h":186.0,"l":184.0,"c":185.5,"v":5000,"vw":185.3,"av":50000000,"op":185.0,"z":100,"s":{now_ms-1000},"e":{now_ms},"n":45,"otc":false}}'),
        ('MSFT', f'{{"ev":"A","sym":"MSFT","o":378.0,"h":379.0,"l":377.0,"c":378.5,"v":3000,"vw":378.2,"av":30000000,"op":378.0,"z":80,"s":{now_ms-1000},"e":{now_ms},"n":30,"otc":false}}'),
    ]
    schema = StructType([
        StructField("key", StringType()),
        StructField("value", StringType()),
    ])
    return spark.createDataFrame(data, schema)


@pytest.mark.spark
class TestStreamTransformer:

    def test_parse_kafka_messages(self, spark, sample_kafka_df):
        """Testet JSON-Parsing aus Kafka-Nachrichten."""
        # Simuliere Kafka-Spalten
        kafka_df = sample_kafka_df.select(
            F.col("key"),
            F.col("value"),
            F.lit("stocks.aggregates.second").alias("topic"),
            F.lit(0).alias("partition"),
            F.lit(1).cast("long").alias("offset"),
            F.current_timestamp().alias("timestamp"),
        )

        parsed = StreamTransformer.parse_kafka_messages(
            kafka_df, AGGREGATE_SECOND_SCHEMA
        )

        assert parsed.count() == 2
        assert "sym" in parsed.columns
        assert "o" in parsed.columns

    def test_transform_to_target_schema(self, spark):
        """Testet Schema-Transformation."""
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        data = [
            ("A", "AAPL", 185.0, 186.0, 184.0, 185.5, 5000, 185.3,
             50000000, 185.0, 100, 45, now_ms - 1000, now_ms, False),
        ]
        schema = StructType([
            StructField("ev", StringType()),
            StructField("sym", StringType()),
            StructField("o", DoubleType()),
            StructField("h", DoubleType()),
            StructField("l", DoubleType()),
            StructField("c", DoubleType()),
            StructField("v", LongType()),
            StructField("vw", DoubleType()),
            StructField("av", LongType()),
            StructField("op", DoubleType()),
            StructField("z", IntegerType()),
            StructField("n", IntegerType()),
            StructField("s", LongType()),
            StructField("e", LongType()),
            StructField("otc", BooleanType()),
        ])
        df = spark.createDataFrame(data, schema)

        result = StreamTransformer.transform_to_target_schema(df, "second")

        assert "time" in result.columns
        assert "symbol" in result.columns
        assert "open" in result.columns
        assert "close" in result.columns
        assert result.count() == 1

        row = result.first()
        assert row["symbol"] == "AAPL"
        assert row["open"] == 185.0

    def test_validate_data(self, spark):
        """Testet Datenvalidierung."""
        data = [
            # Gültig
            (datetime.now(timezone.utc), "AAPL", 185.0, 186.0, 184.0, 185.5, 5000),
            # Ungültig: negative Preise
            (datetime.now(timezone.utc), "BAD1", -1.0, 186.0, 184.0, 185.5, 5000),
            # Ungültig: High < Low
            (datetime.now(timezone.utc), "BAD2", 185.0, 180.0, 190.0, 185.5, 5000),
            # Ungültig: Kein Symbol
            (datetime.now(timezone.utc), None, 185.0, 186.0, 184.0, 185.5, 5000),
        ]
        schema = StructType([
            StructField("time", StringType()),
            StructField("symbol", StringType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
            StructField("volume", LongType()),
        ])
        df = spark.createDataFrame(data, schema)

        validated = StreamTransformer.validate_data(df)
        assert validated.count() == 1
        assert validated.first()["symbol"] == "AAPL"

    def test_add_derived_columns(self, spark):
        """Testet abgeleitete Spalten."""
        data = [
            (datetime.now(timezone.utc), "AAPL", 100.0, 110.0, 95.0, 105.0, 5000),
        ]
        schema = StructType([
            StructField("time", StringType()),
            StructField("symbol", StringType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
            StructField("volume", LongType()),
        ])
        df = spark.createDataFrame(data, schema)

        enriched = StreamTransformer.add_derived_columns(df)

        row = enriched.first()
        assert row["price_range"] == 15.0        # 110 - 95
        assert row["price_change"] == 5.0         # 105 - 100
        assert row["price_change_pct"] == 5.0     # (5/100)*100
        assert row["is_bullish"] is True          # 105 >= 100
        assert row["body_size"] == 5.0            # |105 - 100|
        assert row["upper_shadow"] == 5.0         # 110 - max(100,105) = 5
        assert row["lower_shadow"] == 5.0         # min(100,105) - 95 = 5
