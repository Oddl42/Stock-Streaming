#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 21:50:23 2026

@author: twi
"""

"""
Spark Structured Streaming Job: Sekunden-Aggregation.

Liest aus dem Kafka Topic 'stocks.aggregates.second',
transformiert die Daten und schreibt sie in TimescaleDB.

Spark Structured Streaming creates long-running jobs
that apply transformations and push results to databases.
"""

import logging
import time
import signal
import sys
from typing import Optional

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import StreamingQuery
    PYSPARK_AVAILABLE = True
except ImportError:
    DataFrame = None
    StreamingQuery = None
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from backend.spark_streaming.spark_session import SparkSessionFactory
    from backend.spark_streaming.schemas import AGGREGATE_SECOND_SCHEMA
    from backend.spark_streaming.transformations import transformer
    from backend.spark_streaming.quality_checks import second_quality_checker
    from backend.spark_streaming.db_sink import second_db_sink, dead_letter_sink
    from backend.spark_streaming.metrics import (
        BatchMetricsCollector,
        start_metrics_server,
        ACTIVE_STREAMS,
        BATCH_PROCESSING_TIME,
    )
    from config.spark_config import spark_config

logger = logging.getLogger(__name__)


class SecondStreamJob:
    """
    Spark Structured Streaming Job für Sekunden-Aggregationen.

    Pipeline:
    1. Kafka Source (stocks.aggregates.second)
    2. JSON Parsing
    3. Transformation → Ziel-Schema
    4. Validierung & Quality Checks
    5. Deduplication (Watermark)
    6. Derived Columns (für ML)
    7. foreachBatch → TimescaleDB
    """

    def __init__(self):
        if not PYSPARK_AVAILABLE:
            raise RuntimeError(
                "pyspark ist nicht installiert. "
                "SecondStreamJob kann nur im Spark-Container ausgeführt werden."
            )
        self.spark = None
        self.query: Optional[StreamingQuery] = None
        self.metrics = BatchMetricsCollector(stream_type="second")
        self._is_running = False

    def start(self):
        """Startet den Streaming-Job."""
        logger.info("=" * 60)
        logger.info("  STARTING: Second Aggregation Streaming Job")
        logger.info("=" * 60)

        # Metrics Server starten
        start_metrics_server(port=8090)

        # SparkSession
        self.spark = SparkSessionFactory.get_or_create(
            app_name_suffix="second-agg",
        )

        # Graceful Shutdown registrieren
        self._register_shutdown_hooks()

        # Pipeline bauen und starten
        self.query = self._build_and_start_pipeline()
        self._is_running = True
        ACTIVE_STREAMS.inc()

        logger.info("Second stream job started successfully.")
        logger.info(f"  Kafka Topic: {spark_config.kafka_topic_second}")
        logger.info(f"  Trigger: {spark_config.trigger_processing_time_second}")
        logger.info(f"  Checkpoint: {spark_config.checkpoint_path_second}")
        logger.info(f"  Target Table: stock_agg_second")

        # Warte auf Terminierung
        self.query.awaitTermination()

    def stop(self):
        """Stoppt den Streaming-Job sauber."""
        logger.info("Stopping second stream job...")
        self._is_running = False

        if self.query is not None:
            try:
                self.query.stop()
                logger.info("Streaming query stopped.")
            except Exception as e:
                logger.error(f"Error stopping query: {e}")

        ACTIVE_STREAMS.dec()

        # Statistiken loggen
        self._log_final_stats()

    def _build_and_start_pipeline(self) -> StreamingQuery:
        """Baut die komplette Streaming-Pipeline."""

        # ==========================================
        # 1. Kafka Source
        # ==========================================
        kafka_df = self._read_from_kafka()

        # ==========================================
        # 2. Parse JSON
        # ==========================================
        parsed_df = transformer.parse_kafka_messages(
            kafka_df=kafka_df,
            schema=AGGREGATE_SECOND_SCHEMA,
        )

        # ==========================================
        # 3. Transform to Target Schema
        # ==========================================
        transformed_df = transformer.transform_to_target_schema(
            parsed_df=parsed_df,
            stream_type="second",
        )

        # ==========================================
        # 4. Validate
        # ==========================================
        validated_df = transformer.validate_data(transformed_df)

        # ==========================================
        # 5. Deduplicate with Watermark
        # ==========================================
        deduped_df = transformer.deduplicate(
            df=validated_df,
            watermark_column="time",
            watermark_delay=spark_config.watermark_delay_second,
        )

        # ==========================================
        # 6. Add Derived Columns (for ML)
        # ==========================================
        enriched_df = transformer.add_derived_columns(deduped_df)

        # ==========================================
        # 7. Filter OTC
        # ==========================================
        filtered_df = transformer.filter_otc(enriched_df, include_otc=False)

        # ==========================================
        # 8. Select DB Columns
        # ==========================================
        final_df = transformer.select_db_columns(filtered_df)

        # ==========================================
        # 9. Write via foreachBatch
        # ==========================================
        query = (
            final_df.writeStream
            .foreachBatch(self._process_batch)
            .outputMode("append")
            .trigger(processingTime=spark_config.trigger_processing_time_second)
            .option(
                "checkpointLocation",
                spark_config.checkpoint_path_second,
            )
            .queryName("second_aggregates_to_timescaledb")
            .start()
        )

        return query

    def _read_from_kafka(self) -> DataFrame:
        """Liest aus dem Kafka Topic."""
        return (
            self.spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                spark_config.kafka_bootstrap_servers,
            )
            .option("subscribe", spark_config.kafka_topic_second)
            .option("startingOffsets", spark_config.kafka_starting_offsets)
            .option(
                "maxOffsetsPerTrigger",
                str(spark_config.kafka_max_offsets_per_trigger),
            )
            .option("failOnDataLoss", "false")
            .option(
                "kafka.group.id",
                f"{spark_config.kafka_group_id_prefix}-second",
            )
            # Kafka Consumer Konfiguration
            .option("kafka.session.timeout.ms", "30000")
            .option("kafka.heartbeat.interval.ms", "10000")
            .option("kafka.max.poll.records", "10000")
            .option("kafka.fetch.max.bytes", "52428800")  # 50MB
            .load()
        )

    def _process_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Verarbeitet einen einzelnen Micro-Batch.
        Dies ist die Hauptfunktion für foreachBatch.
        """
        batch_start = time.time()

        if batch_df.isEmpty():
            logger.debug(f"Batch {batch_id}: Empty, skipping.")
            return

        row_count = batch_df.count()
        logger.info(f"Batch {batch_id}: Processing {row_count} records...")

        try:
            # Quality Check: Teile in gute und schlechte Daten
            good_df, bad_df = second_quality_checker.check_and_split(batch_df)

            # Schreibe gute Daten in TimescaleDB
            write_start = time.time()
            second_db_sink.write_batch(good_df, batch_id)
            write_duration = time.time() - write_start

            # Schreibe schlechte Daten in Dead Letter Queue
            if not bad_df.isEmpty():
                dead_letter_sink.write_rejected(
                    bad_df, batch_id, reason="quality_check_second"
                )
                self.metrics.track_rejection(bad_df.count())

            # Metriken
            good_count = good_df.count()
            batch_duration = time.time() - batch_start

            self.metrics.track_batch(
                batch_size=row_count,
                symbols=good_df.select("symbol").distinct().rdd.flatMap(lambda x: x).collect()
                if good_count > 0 else [],
            )
            self.metrics.track_write(
                table="stock_agg_second",
                row_count=good_count,
                duration_seconds=write_duration,
            )

            BATCH_PROCESSING_TIME.labels(
                stream_type="second"
            ).observe(batch_duration)

            logger.info(
                f"Batch {batch_id}: Completed in {batch_duration:.2f}s "
                f"({good_count} written, {row_count - good_count} rejected, "
                f"DB write: {write_duration:.2f}s)"
            )

        except Exception as e:
            logger.error(f"Batch {batch_id}: Processing failed: {e}")
            self.metrics.track_error(table="stock_agg_second")
            raise

    def _register_shutdown_hooks(self):
        """Registriert Graceful-Shutdown Signalhandler."""

        def shutdown_handler(signum, frame):
            logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
        logger.info("Shutdown hooks registered (SIGTERM, SIGINT).")

    def _log_final_stats(self):
        """Loggt abschließende Statistiken."""
        db_stats = second_db_sink.get_stats()
        qc_stats = second_quality_checker.get_stats()

        logger.info("=" * 60)
        logger.info("  FINAL STATISTICS: Second Aggregation Job")
        logger.info("=" * 60)
        logger.info(f"  DB Writes:       {db_stats['total_rows_written']} rows")
        logger.info(f"  DB Batches:      {db_stats['batches_written']}")
        logger.info(f"  QC Total:        {qc_stats['total_records']} records")
        logger.info(f"  QC Rejected:     {qc_stats['rejected_records']} records")
        logger.info(f"  QC Rejection %:  {qc_stats['rejection_rate']:.2f}%")
        logger.info("=" * 60)

    def get_status(self) -> dict:
        """Gibt den aktuellen Status des Jobs zurück."""
        status = {
            "is_running": self._is_running,
            "query_name": "second_aggregates_to_timescaledb",
            "stream_type": "second",
        }
        if self.query is not None:
            try:
                status.update({
                    "is_active": self.query.isActive,
                    "last_progress": self.query.lastProgress,
                    "recent_progress": [
                        {
                            "batch_id": p.get("batchId"),
                            "num_input_rows": p.get("numInputRows"),
                            "processing_time_ms": p.get("batchDuration"),
                        }
                        for p in (self.query.recentProgress or [])
                    ],
                })
            except Exception:
                pass
        return status


# ============================================================
# Standalone Ausführung
# ============================================================
def run():
    """Startet den Second Stream Job standalone."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)-40s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    job = SecondStreamJob()
    job.start()


if __name__ == "__main__":
    run()
