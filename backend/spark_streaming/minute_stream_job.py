#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:17:53 2026

@author: twi
"""

"""
Spark Structured Streaming Job: Minuten-Aggregation.

Analoge Pipeline zum Second-Job, aber für Minuten-Aggregationen.
Unterschiede:
- Anderes Kafka Topic
- Andere Trigger-Zeit (5s statt 2s)
- Andere Watermark-Delay (2min statt 10s)
- Andere Ziel-Tabelle
"""

import logging
import time
import signal
import sys
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from backend.spark_streaming.spark_session import SparkSessionFactory
from backend.spark_streaming.schemas import AGGREGATE_MINUTE_SCHEMA
from backend.spark_streaming.transformations import transformer
from backend.spark_streaming.quality_checks import minute_quality_checker
from backend.spark_streaming.db_sink import minute_db_sink, dead_letter_sink
from backend.spark_streaming.metrics import (
    BatchMetricsCollector,
    start_metrics_server,
    ACTIVE_STREAMS,
    BATCH_PROCESSING_TIME,
)
from config.spark_config import spark_config

logger = logging.getLogger(__name__)


class MinuteStreamJob:
    """
    Spark Structured Streaming Job für Minuten-Aggregationen.

    Pipeline:
    1. Kafka Source (stocks.aggregates.minute)
    2. JSON Parsing
    3. Transformation → Ziel-Schema
    4. Validierung & Quality Checks
    5. Deduplication (Watermark)
    6. Derived Columns (für ML)
    7. foreachBatch → TimescaleDB
    """

    def __init__(self):
        self.spark = None
        self.query: Optional[StreamingQuery] = None
        self.metrics = BatchMetricsCollector(stream_type="minute")
        self._is_running = False

    def start(self):
        """Startet den Streaming-Job."""
        logger.info("=" * 60)
        logger.info("  STARTING: Minute Aggregation Streaming Job")
        logger.info("=" * 60)

        # Metrics Server (anderer Port als Second-Job)
        start_metrics_server(port=8091)

        # SparkSession
        self.spark = SparkSessionFactory.get_or_create(
            app_name_suffix="minute-agg",
        )

        # Graceful Shutdown
        self._register_shutdown_hooks()

        # Pipeline
        self.query = self._build_and_start_pipeline()
        self._is_running = True
        ACTIVE_STREAMS.inc()

        logger.info("Minute stream job started successfully.")
        logger.info(f"  Kafka Topic: {spark_config.kafka_topic_minute}")
        logger.info(f"  Trigger: {spark_config.trigger_processing_time_minute}")
        logger.info(f"  Checkpoint: {spark_config.checkpoint_path_minute}")
        logger.info(f"  Target Table: stock_agg_minute")

        # Warte auf Terminierung
        self.query.awaitTermination()

    def stop(self):
        """Stoppt den Job sauber."""
        logger.info("Stopping minute stream job...")
        self._is_running = False

        if self.query is not None:
            try:
                self.query.stop()
                logger.info("Minute streaming query stopped.")
            except Exception as e:
                logger.error(f"Error stopping query: {e}")

        ACTIVE_STREAMS.dec()
        self._log_final_stats()

    def _build_and_start_pipeline(self) -> StreamingQuery:
        """Baut die Minuten-Streaming-Pipeline."""

        # 1. Kafka Source
        kafka_df = self._read_from_kafka()

        # 2. Parse JSON (Minuten-Schema)
        parsed_df = transformer.parse_kafka_messages(
            kafka_df=kafka_df,
            schema=AGGREGATE_MINUTE_SCHEMA,
        )

        # 3. Transform
        transformed_df = transformer.transform_to_target_schema(
            parsed_df=parsed_df,
            stream_type="minute",
        )

        # 4. Validate
        validated_df = transformer.validate_data(transformed_df)

        # 5. Deduplicate (längere Watermark für Minuten)
        deduped_df = transformer.deduplicate(
            df=validated_df,
            watermark_column="time",
            watermark_delay=spark_config.watermark_delay_minute,
        )

        # 6. Derived Columns
        enriched_df = transformer.add_derived_columns(deduped_df)

        # 7. Filter OTC
        filtered_df = transformer.filter_otc(enriched_df, include_otc=False)

        # 8. Select DB Columns
        final_df = transformer.select_db_columns(filtered_df)

        # 9. Write Stream
        query = (
            final_df.writeStream
            .foreachBatch(self._process_batch)
            .outputMode("append")
            .trigger(processingTime=spark_config.trigger_processing_time_minute)
            .option(
                "checkpointLocation",
                spark_config.checkpoint_path_minute,
            )
            .queryName("minute_aggregates_to_timescaledb")
            .start()
        )

        return query

    def _read_from_kafka(self) -> DataFrame:
        """Liest aus dem Minuten-Kafka-Topic."""
        return (
            self.spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                spark_config.kafka_bootstrap_servers,
            )
            .option("subscribe", spark_config.kafka_topic_minute)
            .option("startingOffsets", spark_config.kafka_starting_offsets)
            .option(
                "maxOffsetsPerTrigger",
                str(spark_config.kafka_max_offsets_per_trigger),
            )
            .option("failOnDataLoss", "false")
            .option(
                "kafka.group.id",
                f"{spark_config.kafka_group_id_prefix}-minute",
            )
            .option("kafka.session.timeout.ms", "30000")
            .option("kafka.heartbeat.interval.ms", "10000")
            .option("kafka.max.poll.records", "5000")
            .load()
        )

    def _process_batch(self, batch_df: DataFrame, batch_id: int):
        """Verarbeitet einen Micro-Batch der Minuten-Daten."""
        batch_start = time.time()

        if batch_df.isEmpty():
            logger.debug(f"Minute Batch {batch_id}: Empty, skipping.")
            return

        row_count = batch_df.count()
        logger.info(f"Minute Batch {batch_id}: Processing {row_count} records...")

        try:
            # Quality Check
            good_df, bad_df = minute_quality_checker.check_and_split(batch_df)

            # Write good data
            write_start = time.time()
            minute_db_sink.write_batch(good_df, batch_id)
            write_duration = time.time() - write_start

            # Write bad data to DLQ
            if not bad_df.isEmpty():
                dead_letter_sink.write_rejected(
                    bad_df, batch_id, reason="quality_check_minute"
                )
                self.metrics.track_rejection(bad_df.count())

            # Metriken
            good_count = good_df.count()
            batch_duration = time.time() - batch_start

            self.metrics.track_batch(batch_size=row_count)
            self.metrics.track_write(
                table="stock_agg_minute",
                row_count=good_count,
                duration_seconds=write_duration,
            )

            BATCH_PROCESSING_TIME.labels(
                stream_type="minute"
            ).observe(batch_duration)

            logger.info(
                f"Minute Batch {batch_id}: Completed in {batch_duration:.2f}s "
                f"({good_count} written, {row_count - good_count} rejected)"
            )

        except Exception as e:
            logger.error(f"Minute Batch {batch_id}: Failed: {e}")
            self.metrics.track_error(table="stock_agg_minute")
            raise

    def _register_shutdown_hooks(self):
        """Registriert Shutdown-Signale."""
        def handler(signum, frame):
            logger.info(f"Minute job: Received signal {signum}. Shutting down...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    def _log_final_stats(self):
        """Abschluss-Statistiken."""
        db_stats = minute_db_sink.get_stats()
        qc_stats = minute_quality_checker.get_stats()

        logger.info("=" * 60)
        logger.info("  FINAL STATISTICS: Minute Aggregation Job")
        logger.info("=" * 60)
        logger.info(f"  DB Writes:       {db_stats['total_rows_written']} rows")
        logger.info(f"  DB Batches:      {db_stats['batches_written']}")
        logger.info(f"  QC Total:        {qc_stats['total_records']} records")
        logger.info(f"  QC Rejected:     {qc_stats['rejected_records']} records")
        logger.info(f"  QC Rejection %:  {qc_stats['rejection_rate']:.2f}%")
        logger.info("=" * 60)

    def get_status(self) -> dict:
        """Gibt den aktuellen Status zurück."""
        status = {
            "is_running": self._is_running,
            "query_name": "minute_aggregates_to_timescaledb",
            "stream_type": "minute",
        }
        if self.query is not None:
            try:
                status.update({
                    "is_active": self.query.isActive,
                    "last_progress": self.query.lastProgress,
                })
            except Exception:
                pass
        return status


# ============================================================
# Standalone
# ============================================================
def run():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)-40s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    job = MinuteStreamJob()
    job.start()


if __name__ == "__main__":
    run()
