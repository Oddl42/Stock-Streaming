#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 09:18:59 2026

@author: twi
"""

"""
Stream Job Manager – Zentrale Verwaltung aller Spark Streaming Jobs.

Ermöglicht das Starten, Stoppen und Monitoring
der Second- und Minute-Streaming-Jobs.
Wird von der GUI über die Callbacks angesteuert.
"""

import logging
import threading
import time
from typing import Optional
from enum import Enum

from backend.spark_streaming.second_stream_job import SecondStreamJob
from backend.spark_streaming.minute_stream_job import MinuteStreamJob
from backend.spark_streaming.spark_session import SparkSessionFactory
from backend.spark_streaming.metrics import ACTIVE_STREAMS

logger = logging.getLogger(__name__)


class JobState(Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


class StreamJobManager:
    """
    Zentrale Verwaltung aller Spark Streaming Jobs.

    Features:
    - Thread-basiertes Job-Management (Jobs laufen in separaten Threads)
    - Saubere Start/Stop Logik
    - Status-Monitoring
    - Thread-Safety
    """

    def __init__(self):
        self._second_job: Optional[SecondStreamJob] = None
        self._minute_job: Optional[MinuteStreamJob] = None
        self._second_thread: Optional[threading.Thread] = None
        self._minute_thread: Optional[threading.Thread] = None
        self._second_state: JobState = JobState.STOPPED
        self._minute_state: JobState = JobState.STOPPED
        self._lock = threading.Lock()

    # =========================================
    # Second Stream Job
    # =========================================

    def start_second_job(self) -> bool:
        """
        Startet den Sekunden-Streaming-Job in einem separaten Thread.

        Returns:
            True wenn erfolgreich gestartet, False bei Fehler
        """
        with self._lock:
            if self._second_state in (JobState.RUNNING, JobState.STARTING):
                logger.warning("Second job is already running/starting.")
                return False

            self._second_state = JobState.STARTING
            logger.info("Starting second streaming job...")

        try:
            self._second_job = SecondStreamJob()
            self._second_thread = threading.Thread(
                target=self._run_second_job,
                name="spark-second-stream",
                daemon=True,
            )
            self._second_thread.start()

            # Warte kurz, um zu prüfen ob der Start erfolgreich war
            time.sleep(3)

            with self._lock:
                if self._second_state != JobState.ERROR:
                    self._second_state = JobState.RUNNING
                    logger.info("Second streaming job is now RUNNING.")
                    return True
                return False

        except Exception as e:
            logger.error(f"Failed to start second job: {e}")
            with self._lock:
                self._second_state = JobState.ERROR
            return False

    def stop_second_job(self) -> bool:
        """Stoppt den Sekunden-Streaming-Job."""
        with self._lock:
            if self._second_state != JobState.RUNNING:
                logger.warning(f"Second job is not running (state: {self._second_state}).")
                return False

            self._second_state = JobState.STOPPING
            logger.info("Stopping second streaming job...")

        try:
            if self._second_job:
                self._second_job.stop()

            if self._second_thread and self._second_thread.is_alive():
                self._second_thread.join(timeout=30)

            with self._lock:
                self._second_state = JobState.STOPPED
                self._second_job = None
                self._second_thread = None

            logger.info("Second streaming job STOPPED.")
            return True

        except Exception as e:
            logger.error(f"Error stopping second job: {e}")
            with self._lock:
                self._second_state = JobState.ERROR
            return False

    def _run_second_job(self):
        """Interne Thread-Funktion für den Second-Job."""
        try:
            self._second_job.start()
        except Exception as e:
            logger.error(f"Second job thread crashed: {e}")
            with self._lock:
                self._second_state = JobState.ERROR

    # =========================================
    # Minute Stream Job
    # =========================================

    def start_minute_job(self) -> bool:
        """Startet den Minuten-Streaming-Job."""
        with self._lock:
            if self._minute_state in (JobState.RUNNING, JobState.STARTING):
                logger.warning("Minute job is already running/starting.")
                return False

            self._minute_state = JobState.STARTING

        try:
            self._minute_job = MinuteStreamJob()
            self._minute_thread = threading.Thread(
                target=self._run_minute_job,
                name="spark-minute-stream",
                daemon=True,
            )
            self._minute_thread.start()

            time.sleep(3)

            with self._lock:
                if self._minute_state != JobState.ERROR:
                    self._minute_state = JobState.RUNNING
                    logger.info("Minute streaming job is now RUNNING.")
                    return True
                return False

        except Exception as e:
            logger.error(f"Failed to start minute job: {e}")
            with self._lock:
                self._minute_state = JobState.ERROR
            return False

    def stop_minute_job(self) -> bool:
        """Stoppt den Minuten-Streaming-Job."""
        with self._lock:
            if self._minute_state != JobState.RUNNING:
                logger.warning(f"Minute job is not running (state: {self._minute_state}).")
                return False

            self._minute_state = JobState.STOPPING

        try:
            if self._minute_job:
                self._minute_job.stop()

            if self._minute_thread and self._minute_thread.is_alive():
                self._minute_thread.join(timeout=30)

            with self._lock:
                self._minute_state = JobState.STOPPED
                self._minute_job = None
                self._minute_thread = None

            logger.info("Minute streaming job STOPPED.")
            return True

        except Exception as e:
            logger.error(f"Error stopping minute job: {e}")
            with self._lock:
                self._minute_state = JobState.ERROR
            return False

    def _run_minute_job(self):
        """Interne Thread-Funktion für den Minute-Job."""
        try:
            self._minute_job.start()
        except Exception as e:
            logger.error(f"Minute job thread crashed: {e}")
            with self._lock:
                self._minute_state = JobState.ERROR

    # =========================================
    # Convenience Methods
    # =========================================

    def start_job(self, stream_type: str) -> bool:
        """
        Startet einen Job nach Typ.

        Args:
            stream_type: "second" oder "minute" (oder "Sekunden"/"Minuten")
        """
        normalized = stream_type.lower()
        if normalized in ("second", "sekunden"):
            return self.start_second_job()
        elif normalized in ("minute", "minuten"):
            return self.start_minute_job()
        else:
            logger.error(f"Unknown stream type: {stream_type}")
            return False

    def stop_job(self, stream_type: str) -> bool:
        """Stoppt einen Job nach Typ."""
        normalized = stream_type.lower()
        if normalized in ("second", "sekunden"):
            return self.stop_second_job()
        elif normalized in ("minute", "minuten"):
            return self.stop_minute_job()
        else:
            logger.error(f"Unknown stream type: {stream_type}")
            return False

    def stop_all(self):
        """Stoppt alle laufenden Jobs."""
        logger.info("Stopping ALL streaming jobs...")
        self.stop_second_job()
        self.stop_minute_job()
        SparkSessionFactory.stop()
        logger.info("All streaming jobs stopped. SparkSession closed.")

    # =========================================
    # Status & Monitoring
    # =========================================

    def get_status(self) -> dict:
        """Gibt den Status aller Jobs zurück."""
        status = {
            "second_stream": {
                "state": self._second_state.value,
                "thread_alive": (
                    self._second_thread.is_alive()
                    if self._second_thread else False
                ),
            },
            "minute_stream": {
                "state": self._minute_state.value,
                "thread_alive": (
                    self._minute_thread.is_alive()
                    if self._minute_thread else False
                ),
            },
        }

        # Job-spezifische Details
        if self._second_job:
            status["second_stream"].update(self._second_job.get_status())
        if self._minute_job:
            status["minute_stream"].update(self._minute_job.get_status())

        return status

    def is_any_running(self) -> bool:
        """Prüft ob mindestens ein Job läuft."""
        return (
            self._second_state == JobState.RUNNING
            or self._minute_state == JobState.RUNNING
        )

    def health_check(self) -> dict:
        """Health-Check für Kubernetes Readiness/Liveness Probes."""
        status = self.get_status()
        is_healthy = True
        issues = []

        # Prüfe ob laufende Jobs tatsächlich aktive Threads haben
        if self._second_state == JobState.RUNNING:
            if not (self._second_thread and self._second_thread.is_alive()):
                is_healthy = False
                issues.append("Second stream thread is dead but state is RUNNING")

        if self._minute_state == JobState.RUNNING:
            if not (self._minute_thread and self._minute_thread.is_alive()):
                is_healthy = False
                issues.append("Minute stream thread is dead but state is RUNNING")

        # Prüfe auf Error-States
        if self._second_state == JobState.ERROR:
            is_healthy = False
            issues.append("Second stream is in ERROR state")
        if self._minute_state == JobState.ERROR:
            is_healthy = False
            issues.append("Minute stream is in ERROR state")

        return {
            "healthy": is_healthy,
            "issues": issues,
            "status": status,
        }


# Singleton
stream_job_manager = StreamJobManager()
