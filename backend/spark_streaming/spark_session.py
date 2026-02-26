#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:12:42 2026

@author: twi
"""

"""
Spark Session Factory – Erstellt und konfiguriert die SparkSession.

Zentrale Stelle für alle Spark-Konfigurationen.
Wird von allen Streaming-Jobs verwendet.
"""

import logging
from pyspark.sql import SparkSession
from config.spark_config import spark_config

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """
    Factory für SparkSession mit optimierten Einstellungen
    für Structured Streaming mit Kafka und JDBC.
    """

    _instance: SparkSession = None

    @classmethod
    def get_or_create(
        cls,
        app_name_suffix: str = "",
        additional_config: dict = None,
    ) -> SparkSession:
        """
        Erstellt oder gibt die bestehende SparkSession zurück.

        Args:
            app_name_suffix: Wird an den App-Namen angehängt (z.B. "-second")
            additional_config: Zusätzliche Spark-Konfigurationen

        Returns:
            Konfigurierte SparkSession
        """
        if cls._instance is not None and cls._instance._jsc is not None:
            return cls._instance

        app_name = spark_config.app_name
        if app_name_suffix:
            app_name = f"{app_name}-{app_name_suffix}"

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(spark_config.master)

            # --- Packages ---
            .config(
                "spark.jars.packages",
                spark_config.packages_string,
            )

            # --- Streaming ---
            .config(
                "spark.sql.streaming.schemaInference",
                "false",
            )
            .config(
                "spark.sql.shuffle.partitions",
                str(spark_config.shuffle_partitions),
            )

            # --- Backpressure ---
            .config(
                "spark.streaming.backpressure.enabled",
                str(spark_config.backpressure_enabled).lower(),
            )

            # --- Memory ---
            .config("spark.driver.memory", spark_config.driver_memory)
            .config("spark.executor.memory", spark_config.executor_memory)
            .config("spark.executor.cores", str(spark_config.executor_cores))

            # --- Kafka spezifisch ---
            .config(
                "spark.sql.streaming.kafka.useDeprecatedOffsetFetching",
                "false",
            )

            # --- Graceful Shutdown ---
            .config(
                "spark.streaming.stopGracefullyOnShutdown",
                "true",
            )

            # --- Adaptive Query Execution ---
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

            # --- Serialization ---
            .config(
                "spark.serializer",
                "org.apache.spark.serializer.KryoSerializer",
            )

            # --- UI ---
            .config("spark.ui.showConsoleProgress", "true")
            .config("spark.ui.enabled", "true")

            # --- Metrics für Prometheus ---
            .config("spark.metrics.conf.*.sink.prometheusServlet.class",
                    "org.apache.spark.metrics.sink.PrometheusServlet")
            .config("spark.metrics.conf.*.sink.prometheusServlet.path",
                    "/metrics/prometheus")
            .config("spark.ui.prometheus.enabled", "true")
        )

        # Zusätzliche Konfigurationen
        if additional_config:
            for key, value in additional_config.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(spark_config.log_level)

        cls._instance = spark
        logger.info(f"SparkSession created: {app_name}")
        logger.info(f"  Master: {spark_config.master}")
        logger.info(f"  Packages: {spark_config.packages_string}")
        logger.info(f"  Shuffle Partitions: {spark_config.shuffle_partitions}")

        return spark

    @classmethod
    def stop(cls):
        """Stoppt die SparkSession sauber."""
        if cls._instance is not None:
            try:
                cls._instance.stop()
                logger.info("SparkSession stopped.")
            except Exception as e:
                logger.error(f"Error stopping SparkSession: {e}")
            finally:
                cls._instance = None
