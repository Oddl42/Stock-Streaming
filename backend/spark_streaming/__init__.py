#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 08:58:58 2026

@author: twi
"""

"""
Spark Streaming Package.

Enthält alle Komponenten für das Streaming
von Stock-Daten via Kafka → Spark → TimescaleDB.

Hinweis: Imports sind mit try/except abgesichert,
da dieses Package auch vom Panel-GUI-Container importiert wird,
in dem pyspark NICHT installiert ist.
"""

import logging

logger = logging.getLogger(__name__)

try:
    from backend.spark_streaming.second_stream_job import SecondStreamJob
except ImportError as e:
    SecondStreamJob = None
    logger.warning(f"SecondStreamJob nicht verfügbar (pyspark fehlt): {e}")

try:
    from backend.spark_streaming.minute_stream_job import MinuteStreamJob
except ImportError as e:
    MinuteStreamJob = None
    logger.warning(f"MinuteStreamJob nicht verfügbar (pyspark fehlt): {e}")

try:
    from backend.spark_streaming.stream_job_manager import stream_job_manager
except ImportError as e:
    stream_job_manager = None
    logger.warning(f"stream_job_manager nicht verfügbar (pyspark fehlt): {e}")

try:
    from backend.spark_streaming.spark_session import SparkSessionFactory
except ImportError as e:
    SparkSessionFactory = None
    logger.warning(f"SparkSessionFactory nicht verfügbar (pyspark fehlt): {e}")

__all__ = [
    "SecondStreamJob",
    "MinuteStreamJob",
    "stream_job_manager",
    "SparkSessionFactory",
]
