#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:48:56 2026

@author: twi
"""

"""
Unit Tests für Datenqualitäts-Prüfungen.
"""

import pytest
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

from backend.spark_streaming.quality_checks import QualityChecker


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-quality-checks")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def schema():
    return StructType([
        StructField("time", TimestampType()),
        StructField("symbol", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", LongType()),
        StructField("vwap", DoubleType()),
    ])


@pytest.mark.spark
class TestQualityChecker:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.checker = QualityChecker(
            max_price=100_000.0,
            max_volume=10_000_000_000,
        )

    def test_all_good_data(self, spark, schema):
        """Testet dass gute Daten durchgelassen werden."""
        now = datetime.now(timezone.utc)
        data = [
            (now, "AAPL", 185.0, 186.0, 184.0, 185.5, 5000, 185.3),
            (now, "MSFT", 378.0, 379.0, 377.0, 378.5, 3000, 378.2),
        ]
        df = spark.createDataFrame(data, schema)

        good, bad = self.checker.check_and_split(df)

        assert good.count() == 2
        assert bad.count() == 0

    def test_reject_future_timestamps(self, spark, schema):
        """Testet Ablehnung von Timestamps in der Zukunft."""
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        data = [
            (future, "AAPL", 185.0, 186.0, 184.0, 185.5, 5000, 185.3),
        ]
        df = spark.createDataFrame(data, schema)

        good, bad = self.checker.check_and_split(df)

        assert good.count() == 0
        assert bad.count() == 1

    def test_reject_old_timestamps(self, spark, schema):
        """Testet Ablehnung von zu alten Timestamps."""
        old = datetime.now(timezone.utc) - timedelta(days=5)
        data = [
            (old, "AAPL", 185.0, 186.0, 184.0, 185.5, 5000, 185.3),
        ]
        df = spark.createDataFrame(data, schema)

        good, bad = self.checker.check_and_split(df)

        assert good.count() == 0

    def test_reject_unrealistic_prices(self, spark, schema):
        """Testet Ablehnung bei unrealistischen Preisen."""
        now = datetime.now(timezone.utc)
        data = [
            (now, "AAPL", 500000.0, 500001.0, 499999.0, 500000.5, 5000, 500000.0),
        ]
        df = spark.createDataFrame(data, schema)

        good, bad = self.checker.check_and_split(df)

        assert good.count() == 0

    def test_reject_invalid_symbol(self, spark, schema):
        """Testet Ablehnung bei ungültigem Symbol-Format."""
        now = datetime.now(timezone.utc)
        data = [
            (now, "invalid_symbol!!!", 185.0, 186.0, 184.0, 185.5, 5000, 185.3),
        ]
        df = spark.createDataFrame(data, schema)

        good, bad = self.checker.check_and_split(df)

        assert good.count() == 0

    def test_mixed_good_and_bad(self, spark, schema):
        """Testet gemischte Daten."""
        now = datetime.now(timezone.utc)
        future = now + timedelta(hours=2)
        data = [
            (now, "AAPL", 185.0, 186.0, 184.0, 185.5, 5000, 185.3),     # Gut
            (future, "MSFT", 378.0, 379.0, 377.0, 378.5, 3000, 378.2),   # Schlecht: Zukunft
            (now, "GOOGL", 141.0, 142.0, 140.0, 141.5, 2000, 141.3),    # Gut
        ]
        df = spark.createDataFrame(data, schema)

        good, bad = self.checker.check_and_split(df)

        assert good.count() == 2
        assert bad.count() == 1

    def test_empty_dataframe(self, spark, schema):
        """Testet leeren DataFrame."""
        df = spark.createDataFrame([], schema)

        good, bad = self.checker.check_and_split(df)

        assert good.count() == 0
        assert bad.count() == 0

    def test_stats_tracking(self, spark, schema):
        """Testet Statistik-Tracking."""
        now = datetime.now(timezone.utc)
        data = [
            (now, "AAPL", 185.0, 186.0, 184.0, 185.5, 5000, 185.3),
        ]
        df = spark.createDataFrame(data, schema)

        self.checker.check_and_split(df)
        stats = self.checker.get_stats()

        assert stats["total_records"] >= 1
        assert stats["batches_processed"] >= 1
        assert 0 <= stats["rejection_rate"] <= 100
