#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:12:14 2026

@author: twi
"""

"""Spark-spezifische Konfiguration."""

import os
from dataclasses import dataclass, field
from config.settings import settings


@dataclass
class SparkConfig:
    """Alle Spark-relevanten Einstellungen."""

    # --- Spark Core ---
    app_name: str = "StockStreamingPlatform"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    log_level: str = os.getenv("SPARK_LOG_LEVEL", "WARN")

    # --- Kafka ---
    kafka_bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS
    kafka_topic_second: str = settings.KAFKA_TOPIC_SECOND
    kafka_topic_minute: str = settings.KAFKA_TOPIC_MINUTE
    kafka_starting_offsets: str = "latest"
    kafka_max_offsets_per_trigger: int = 50000  # Pro Micro-Batch
    kafka_group_id_prefix: str = "spark-stock-streaming"

    # --- TimescaleDB / JDBC ---
    jdbc_url: str = f"jdbc:postgresql://{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    jdbc_user: str = settings.DB_USER
    jdbc_password: str = settings.DB_PASSWORD
    jdbc_driver: str = "org.postgresql.Driver"
    jdbc_batch_size: int = 1000
    jdbc_num_partitions: int = 4

    # --- Streaming ---
    trigger_processing_time_second: str = "2 seconds"
    trigger_processing_time_minute: str = "5 seconds"
    checkpoint_base_path: str = os.getenv(
        "SPARK_CHECKPOINT_PATH", "/tmp/spark-checkpoints"
    )
    watermark_delay_second: str = "10 seconds"
    watermark_delay_minute: str = "2 minutes"

    # --- Performance ---
    shuffle_partitions: int = 8
    executor_memory: str = "2g"
    driver_memory: str = "1g"
    executor_cores: int = 2
    max_executors: int = 4
    backpressure_enabled: bool = True

    # --- Packages (für spark-submit) ---
    packages: list = field(default_factory=lambda: [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.1",
    ])
    @property
    def packages_string(self) -> str:
        """Nicht mehr genutzt – JARs werden direkt im Docker-Image bereitgestellt."""
        return ""
    @property
    def checkpoint_path_second(self) -> str:
        return f"{self.checkpoint_base_path}/second_aggregates"

    @property
    def checkpoint_path_minute(self) -> str:
        return f"{self.checkpoint_base_path}/minute_aggregates"

    @property
    def packages_string(self) -> str:
        """Kommaseparierte Packages für spark.jars.packages."""
        return ",".join(self.packages)


spark_config = SparkConfig()
