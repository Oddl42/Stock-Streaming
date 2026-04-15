#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:14:53 2026

@author: twi
"""

"""
Datenqualitäts-Prüfungen für Streaming-Daten.

Wird innerhalb von foreachBatch aufgerufen, um jeden Micro-Batch zu prüfen.
Schlechte Daten werden in eine Dead-Letter-Tabelle geschrieben.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class QualityChecker:
    """Prüft die Datenqualität jedes Micro-Batches."""

    def __init__(self, max_price: float = 100_000.0, max_volume: int = 10_000_000_000):
        self.max_price = max_price
        self.max_volume = max_volume
        self._total_records = 0
        self._rejected_records = 0
        self._batches_processed = 0

    def check_and_split(
        self, batch_df: DataFrame
    ) -> tuple[DataFrame, DataFrame]:
        """
        Prüft den Batch und teilt ihn in gute und schlechte Daten.

        Args:
            batch_df: Micro-Batch DataFrame

        Returns:
            Tuple von (good_df, bad_df)
        """
        if batch_df.isEmpty():
            return batch_df, batch_df

        total = batch_df.count()
        self._total_records += total
        self._batches_processed += 1

        # Definiere Qualitäts-Regeln
        quality_filter = (
            # Zeitstempel nicht in der Zukunft (mit 1 Minute Toleranz)
            (F.col("time") <= F.current_timestamp() + F.expr("INTERVAL 1 MINUTE"))

            # Zeitstempel nicht älter als 2 Tage (Retention)
            & (F.col("time") >= F.current_timestamp() - F.expr("INTERVAL 2 DAY"))

            # Preise in realistischem Bereich
            & (F.col("open") < self.max_price)
            & (F.col("high") < self.max_price)
            & (F.col("low") < self.max_price)
            & (F.col("close") < self.max_price)

            # Volume in realistischem Bereich
            & (F.col("volume") < self.max_volume)

            # Symbol-Format (1-10 Großbuchstaben)
            & (F.col("symbol").rlike(r"^[A-Z\.]{1,10}$"))

            # VWAP-Plausibilität (sollte zwischen Low und High liegen)
            & (
                F.col("vwap").isNull()
                | (
                    (F.col("vwap") >= F.col("low") * 0.99)
                    & (F.col("vwap") <= F.col("high") * 1.01)
                )
            )
        )

        good_df = batch_df.filter(quality_filter)
        bad_df = batch_df.filter(~quality_filter)

        bad_count = bad_df.count()
        self._rejected_records += bad_count

        if bad_count > 0:
            logger.warning(
                f"Quality Check: {bad_count}/{total} records rejected "
                f"in batch #{self._batches_processed}"
            )

            # Logge Beispiele der abgelehnten Daten
            if bad_count <= 5:
                bad_samples = bad_df.select("symbol", "time", "open", "close").collect()
                for row in bad_samples:
                    logger.warning(f"  Rejected: {row}")

        logger.info(
            f"Batch #{self._batches_processed}: "
            f"{total - bad_count}/{total} records passed quality checks "
            f"(Total: {self._total_records}, Rejected: {self._rejected_records})"
        )

        return good_df, bad_df

    def get_stats(self) -> dict:
        """Gibt Statistiken zurück."""
        return {
            "total_records": self._total_records,
            "rejected_records": self._rejected_records,
            "batches_processed": self._batches_processed,
            "rejection_rate": (
                self._rejected_records / max(self._total_records, 1) * 100
            ),
        }


# Quality-Checker-Instanzen
second_quality_checker = QualityChecker()
minute_quality_checker = QualityChecker()
