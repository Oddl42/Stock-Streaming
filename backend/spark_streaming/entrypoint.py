#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:19:42 2026

@author: twi
"""

"""
CLI Entrypoint für spark-submit.

Verwendung:
    # Lokales Ausführen
    python -m backend.spark_streaming.entrypoint --job second
    python -m backend.spark_streaming.entrypoint --job minute
    python -m backend.spark_streaming.entrypoint --job both

    # Via spark-submit
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
        --master local[*] \
        --driver-memory 1g \
        --executor-memory 2g \
        --conf spark.streaming.stopGracefullyOnShutdown=true \
        backend/spark_streaming/entrypoint.py --job second

    # Kubernetes spark-submit
    spark-submit \
        --master k8s://https://<k8s-master>:6443 \
        --deploy-mode cluster \
        --name stock-streaming-second \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
        --conf spark.kubernetes.container.image=stock-streaming/spark:latest \
        --conf spark.kubernetes.namespace=stock-platform \
        --conf spark.executor.instances=2 \
        --conf spark.executor.memory=2g \
        --conf spark.driver.memory=1g \
        backend/spark_streaming/entrypoint.py --job second
"""

import argparse
import logging
import sys
import signal
import threading

from backend.spark_streaming.second_stream_job import SecondStreamJob
from backend.spark_streaming.minute_stream_job import MinuteStreamJob
from backend.spark_streaming.spark_session import SparkSessionFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-40s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stock Streaming Platform - Spark Streaming Jobs"
    )
    parser.add_argument(
        "--job",
        type=str,
        required=True,
        choices=["second", "minute", "both"],
        help="Which streaming job to run: 'second', 'minute', or 'both'",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=8090,
        help="Port for Prometheus metrics endpoint (default: 8090)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )
    return parser.parse_args()


def run_both_jobs():
    """Führt beide Jobs in separaten Threads aus."""
    second_job = SecondStreamJob()
    minute_job = MinuteStreamJob()

    second_thread = threading.Thread(
        target=second_job.start,
        name="second-stream-thread",
        daemon=True,
    )
    minute_thread = threading.Thread(
        target=minute_job.start,
        name="minute-stream-thread",
        daemon=True,
    )

    # Shutdown-Handler
    def shutdown(signum, frame):
        logger.info("Shutting down both streaming jobs...")
        second_job.stop()
        minute_job.stop()
        SparkSessionFactory.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Starte beide Threads
    logger.info("Starting BOTH streaming jobs...")
    second_thread.start()
    minute_thread.start()

    # Warte auf beide
    try:
        while second_thread.is_alive() or minute_thread.is_alive():
            second_thread.join(timeout=1)
            minute_thread.join(timeout=1)
    except KeyboardInterrupt:
        shutdown(None, None)


def main():
    args = parse_args()

    # Log Level setzen
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    logger.info("=" * 60)
    logger.info("  Stock Streaming Platform - Spark Streaming")
    logger.info(f"  Job: {args.job}")
    logger.info(f"  Metrics Port: {args.metrics_port}")
    logger.info("=" * 60)

    if args.job == "second":
        job = SecondStreamJob()
        job.start()

    elif args.job == "minute":
        job = MinuteStreamJob()
        job.start()

    elif args.job == "both":
        run_both_jobs()


if __name__ == "__main__":
    main()
