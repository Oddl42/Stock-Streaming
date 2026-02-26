#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:15:35 2026

@author: twi
"""

"""
TimescaleDB JDBC Sink für Spark Structured Streaming.

Implementiert die foreachBatch-Logik zum Schreiben in TimescaleDB.
Unterstützt Upsert (ON CONFLICT), Batching und Fehlerbehandlung.
"""

import logging
from pyspark.sql import DataFrame
from config.spark_config import spark_config

logger = logging.getLogger(__name__)


class TimescaleDBSink:
    """
    Schreibt Spark DataFrames in TimescaleDB via JDBC.
    
    Verwendet foreachBatch für Micro-Batch-Verarbeitung.
    """

    def __init__(
        self,
        table_name: str,
        mode: str = "append",
        use_upsert: bool = True,
    ):
        """
        Args:
            table_name: Ziel-Tabelle in TimescaleDB
            mode: Write Mode ("append" oder "overwrite")
            use_upsert: Wenn True, verwende INSERT ON CONFLICT
        """
        self.table_name = table_name
        self.mode = mode
        self.use_upsert = use_upsert
        self._batches_written = 0
        self._total_rows_written = 0

    @property
    def jdbc_properties(self) -> dict:
        """JDBC Connection Properties."""
        return {
            "user": spark_config.jdbc_user,
            "password": spark_config.jdbc_password,
            "driver": spark_config.jdbc_driver,
            "batchsize": str(spark_config.jdbc_batch_size),
            "rewriteBatchedStatements": "true",
            "rewriteBatchedInserts": "true",
        }

    def write_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Schreibt einen Micro-Batch in TimescaleDB.

        Dies ist die Hauptfunktion, die an foreachBatch übergeben wird.

        Args:
            batch_df: DataFrame des aktuellen Micro-Batches
            batch_id: ID des Micro-Batches
        """
        if batch_df.isEmpty():
            logger.debug(f"Batch {batch_id}: Empty, skipping write.")
            return

        row_count = batch_df.count()

        try:
            if self.use_upsert:
                self._write_with_upsert(batch_df, batch_id)
            else:
                self._write_simple_append(batch_df, batch_id)

            self._batches_written += 1
            self._total_rows_written += row_count

            logger.info(
                f"Batch {batch_id}: Wrote {row_count} rows to "
                f"'{self.table_name}' "
                f"(Total: {self._total_rows_written} rows in "
                f"{self._batches_written} batches)"
            )

        except Exception as e:
            logger.error(
                f"Batch {batch_id}: Failed to write {row_count} rows "
                f"to '{self.table_name}': {e}"
            )
            # Retry-Logik
            self._retry_write(batch_df, batch_id, max_retries=3)

    def _write_simple_append(self, batch_df: DataFrame, batch_id: int):
        """Einfaches JDBC Append."""
        (
            batch_df.write
            .format("jdbc")
            .option("url", spark_config.jdbc_url)
            .option("dbtable", self.table_name)
            .option("numPartitions", str(spark_config.jdbc_num_partitions))
            .options(**self.jdbc_properties)
            .mode(self.mode)
            .save()
        )

    def _write_with_upsert(self, batch_df: DataFrame, batch_id: int):
        """
        Schreibt mit INSERT ON CONFLICT DO UPDATE (Upsert).
        
        Verwendet eine Staging-Tabelle + SQL-Merge, da Spark JDBC
        kein natives Upsert unterstützt.
        """
        staging_table = f"staging_{self.table_name}_{batch_id}"

        try:
            # 1. Schreibe in Staging-Tabelle
            (
                batch_df.write
                .format("jdbc")
                .option("url", spark_config.jdbc_url)
                .option("dbtable", staging_table)
                .option("numPartitions", str(spark_config.jdbc_num_partitions))
                .options(**self.jdbc_properties)
                .mode("overwrite")
                .save()
            )

            # 2. Merge von Staging → Ziel-Tabelle via JDBC
            import psycopg2
            conn = psycopg2.connect(
                host=spark_config.jdbc_url.split("//")[1].split(":")[0],
                port=spark_config.jdbc_url.split(":")[-1].split("/")[0],
                dbname=spark_config.jdbc_url.split("/")[-1],
                user=spark_config.jdbc_user,
                password=spark_config.jdbc_password,
            )
            conn.autocommit = True
            cur = conn.cursor()

            # Upsert: INSERT ON CONFLICT
            merge_sql = f"""
                INSERT INTO {self.table_name}
                    (time, symbol, open, high, low, close, volume, vwap,
                     accumulated_volume, official_open, avg_trade_size,
                     num_trades, tick_start, tick_end, is_otc,
                     price_range, price_change, price_change_pct,
                     is_bullish, body_size, upper_shadow, lower_shadow)
                SELECT
                    time, symbol, open, high, low, close, volume, vwap,
                    accumulated_volume, official_open, avg_trade_size,
                    num_trades, tick_start, tick_end, is_otc,
                    price_range, price_change, price_change_pct,
                    is_bullish, body_size, upper_shadow, lower_shadow
                FROM {staging_table}
                ON CONFLICT (time, symbol) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    vwap = EXCLUDED.vwap,
                    num_trades = EXCLUDED.num_trades
            """
            cur.execute(merge_sql)

            # 3. Staging-Tabelle droppen
            cur.execute(f"DROP TABLE IF EXISTS {staging_table}")

            cur.close()
            conn.close()

        except Exception as e:
            logger.error(f"Upsert failed for batch {batch_id}: {e}")
            # Fallback zu einfachem Append
            logger.info("Falling back to simple append...")
            self._write_simple_append(batch_df, batch_id)

    def _retry_write(
        self, batch_df: DataFrame, batch_id: int, max_retries: int = 3
    ):
        """Retry-Logik mit exponentiellem Backoff."""
        import time

        for attempt in range(1, max_retries + 1):
            wait_time = 2 ** attempt  # 2, 4, 8 Sekunden
            logger.warning(
                f"Batch {batch_id}: Retry {attempt}/{max_retries} "
                f"in {wait_time}s..."
            )
            time.sleep(wait_time)

            try:
                self._write_simple_append(batch_df, batch_id)
                logger.info(
                    f"Batch {batch_id}: Retry {attempt} successful."
                )
                self._batches_written += 1
                self._total_rows_written += batch_df.count()
                return
            except Exception as e:
                logger.error(
                    f"Batch {batch_id}: Retry {attempt} failed: {e}"
                )

        logger.critical(
            f"Batch {batch_id}: All {max_retries} retries failed! "
            f"Data may be lost."
        )

    def get_stats(self) -> dict:
        """Gibt Schreib-Statistiken zurück."""
        return {
            "table_name": self.table_name,
            "batches_written": self._batches_written,
            "total_rows_written": self._total_rows_written,
        }


# ============================================================
# Dead Letter Queue Sink
# ============================================================
class DeadLetterSink:
    """
    Schreibt abgelehnte/fehlerhafte Datensätze in eine
    Dead-Letter-Tabelle für spätere Analyse.
    """

    def __init__(self):
        self.table_name = "dead_letter_queue"
        self._inner_sink = TimescaleDBSink(
            table_name=self.table_name,
            mode="append",
            use_upsert=False,
        )

    def write_rejected(self, bad_df: DataFrame, batch_id: int, reason: str = "quality_check"):
        """Schreibt abgelehnte Daten mit Grund."""
        from pyspark.sql import functions as F

        if bad_df.isEmpty():
            return

        enriched = (
            bad_df
            .withColumn("rejection_reason", F.lit(reason))
            .withColumn("rejected_at", F.current_timestamp())
            .withColumn("batch_id", F.lit(batch_id))
        )

        try:
            (
                enriched.write
                .format("jdbc")
                .option("url", spark_config.jdbc_url)
                .option("dbtable", self.table_name)
                .options(**self._inner_sink.jdbc_properties)
                .mode("append")
                .save()
            )
            logger.info(
                f"Dead Letter: Wrote {bad_df.count()} rejected records "
                f"(batch {batch_id}, reason: {reason})"
            )
        except Exception as e:
            logger.error(f"Failed to write to dead letter queue: {e}")


# Singleton Instanzen
second_db_sink = TimescaleDBSink(table_name="stock_agg_second")
minute_db_sink = TimescaleDBSink(table_name="stock_agg_minute")
dead_letter_sink = DeadLetterSink()
