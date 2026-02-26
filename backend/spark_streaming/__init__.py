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
"""

from backend.spark_streaming.second_stream_job import SecondStreamJob
from backend.spark_streaming.minute_stream_job import MinuteStreamJob
from backend.spark_streaming.stream_job_manager import stream_job_manager
from backend.spark_streaming.spark_session import SparkSessionFactory

__all__ = [
    "SecondStreamJob",
    "MinuteStreamJob",
    "stream_job_manager",
    "SparkSessionFactory",
]
