#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:15:30 2026

@author: twi
"""

"""
Stream Callbacks – Leichtgewichtige Version für die GUI.

ARCHITEKTUR:
    Die GUI startet KEINE eigenen Backend-Prozesse!
    - WebSocket Producer läuft als standalone Prozess (via run_local.sh)
    - Spark Streaming Jobs laufen als standalone Prozesse (via run_local.sh)
    - Die GUI liest nur aus TimescaleDB und zeigt Daten an

    Der "Start Stream" Button in der GUI:
    → Startet das periodische Chart-Update (liest DB alle 2s)
    → Startet NICHT den Producer oder Spark!

    Der "Stop Stream" Button in der GUI:
    → Stoppt das periodische Chart-Update
    → Stoppt NICHT den Producer oder Spark!

WARUM:
    1. Producer belegt Ports 8092/8093 → zweite Instanz crasht
    2. Spark nutzt signal() → funktioniert nur im Main-Thread
    3. Saubere Trennung: Backend = Daten-Pipeline, GUI = Anzeige
"""

import logging

logger = logging.getLogger(__name__)


class StreamCallbackHandler:
    """
    Leichtgewichtiger Stream-Handler für die GUI.

    Prüft nur, ob die externen Prozesse vermutlich laufen
    (Kafka erreichbar, DB hat aktuelle Daten), startet aber
    KEINE eigenen Backend-Prozesse.
    """

    def __init__(self):
        self._active_stream_type: str = ""
        self._active_tickers: list[str] = []

    def on_stream_start(self, stream_type_str: str, tickers: list[str]):
        """
        Wird aufgerufen wenn der "Start" Button geklickt wird.

        Startet KEINE Backend-Prozesse!
        Speichert nur den aktuellen Stream-Typ und die Ticker.
        Das periodische Chart-Update wird in main_layout.py gestartet.
        """
        stream_type = (
            "second" if stream_type_str in ("Sekunden", "second")
            else "minute"
        )

        self._active_stream_type = stream_type
        self._active_tickers = tickers

        logger.info(
            f"GUI stream started: {stream_type} with {len(tickers)} tickers. "
            f"(Backend-Prozesse laufen extern via run_local.sh)"
        )

        # Optional: Prüfe ob Backend-Prozesse laufen
        self._check_backend_health()

    def on_stream_stop(self, stream_type_str: str):
        """
        Wird aufgerufen wenn der "Stop" Button geklickt wird.

        Stoppt KEINE Backend-Prozesse!
        Das periodische Chart-Update wird in main_layout.py gestoppt.
        """
        stream_type = (
            "second" if stream_type_str in ("Sekunden", "second")
            else "minute"
        )

        self._active_stream_type = ""
        self._active_tickers = []

        logger.info(
            f"GUI stream stopped: {stream_type}. "
            f"(Backend-Prozesse laufen weiter via run_local.sh)"
        )

    def on_shutdown(self):
        """Cleanup bei Session-Ende. Stoppt keine externen Prozesse."""
        self._active_stream_type = ""
        self._active_tickers = []
        logger.info("GUI session cleanup complete.")

    def _check_backend_health(self):
        """
        Prüft ob die externen Backend-Prozesse vermutlich laufen.
        Nur Logging — blockiert oder crasht nicht.
        """
        try:
            import socket

            # Kafka erreichbar?
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(("localhost", 29092))
            sock.close()
            if result == 0:
                logger.info("✅ Kafka ist erreichbar (localhost:29092)")
            else:
                logger.warning(
                    "⚠️ Kafka nicht erreichbar! "
                    "Ist 'run_local.sh infra' gestartet?"
                )

            # TimescaleDB erreichbar?
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(("localhost", 5432))
            sock.close()
            if result == 0:
                logger.info("✅ TimescaleDB ist erreichbar (localhost:5432)")
            else:
                logger.warning(
                    "⚠️ TimescaleDB nicht erreichbar! "
                    "Ist 'run_local.sh infra' gestartet?"
                )

        except Exception as e:
            logger.debug(f"Health check error (harmlos): {e}")

    def get_health(self) -> dict:
        """Health-Status."""
        return {
            "panel_gui": "running",
            "active_stream": self._active_stream_type or "none",
            "active_tickers": len(self._active_tickers),
            "backend": "external (run_local.sh)",
        }

# Singleton
stream_callback_handler = StreamCallbackHandler()
