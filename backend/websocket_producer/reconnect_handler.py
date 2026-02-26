#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:29:34 2026

@author: twi
"""

"""
Reconnection Handler mit Exponential Backoff.

Verwaltet die Wiederverbindungslogik für WebSocket-Verbindungen
mit konfigurierbarem Backoff, Jitter und maximaler Retry-Anzahl.
"""

import asyncio
import random
import logging
import time
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """Verbindungszustand."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATING = "authenticating"
    AUTHENTICATED = "authenticated"
    SUBSCRIBING = "subscribing"
    STREAMING = "streaming"
    RECONNECTING = "reconnecting"
    FAILED = "failed"
    CLOSED = "closed"


@dataclass
class ReconnectConfig:
    """Konfiguration für Reconnection-Logik."""
    initial_delay: float = 1.0          # Erste Wartezeit in Sekunden
    max_delay: float = 60.0             # Maximale Wartezeit
    multiplier: float = 2.0             # Backoff-Multiplikator
    jitter: float = 0.5                 # Zufälliger Jitter (0-1)
    max_retries: int = 50               # Maximale Anzahl Retries (0 = unbegrenzt)
    reset_after_success: float = 60.0   # Nach X Sekunden Erfolg: Retry-Counter zurücksetzen


class ReconnectHandler:
    """
    Verwaltet die Wiederverbindungslogik mit Exponential Backoff + Jitter.

    Ablauf:
    1. Verbindung bricht ab
    2. Warte initial_delay Sekunden
    3. Versuche erneut zu verbinden
    4. Bei Fehler: Wartezeit *= multiplier (bis max_delay)
    5. Nach max_retries: Aufgeben
    6. Bei Erfolg: Counter zurücksetzen nach reset_after_success
    """

    def __init__(self, config: ReconnectConfig = None):
        self.config = config or ReconnectConfig()
        self._current_delay = self.config.initial_delay
        self._retry_count = 0
        self._state = ConnectionState.DISCONNECTED
        self._last_success_time: float = 0
        self._total_reconnects = 0
        self._state_change_callbacks: list = []

    @property
    def state(self) -> ConnectionState:
        return self._state

    @state.setter
    def state(self, new_state: ConnectionState):
        old_state = self._state
        self._state = new_state
        if old_state != new_state:
            logger.info(f"Connection state: {old_state.value} → {new_state.value}")
            for callback in self._state_change_callbacks:
                try:
                    callback(old_state, new_state)
                except Exception as e:
                    logger.error(f"State change callback error: {e}")

    def on_state_change(self, callback):
        """Registriert einen Callback für Zustandsänderungen."""
        self._state_change_callbacks.append(callback)

    def on_connected(self):
        """Wird aufgerufen wenn die Verbindung erfolgreich hergestellt wurde."""
        self._last_success_time = time.time()
        self.state = ConnectionState.CONNECTED
        logger.info(
            f"Connected successfully (after {self._retry_count} retries)."
        )

    def on_authenticated(self):
        """Wird nach erfolgreicher Authentifizierung aufgerufen."""
        self.state = ConnectionState.AUTHENTICATED

    def on_streaming(self):
        """Wird aufgerufen wenn Daten gestreamt werden."""
        self.state = ConnectionState.STREAMING
        # Reset retry counter nach stabiler Verbindung
        self._check_reset_counter()

    def on_disconnected(self, reason: str = ""):
        """Wird bei Verbindungsabbruch aufgerufen."""
        self.state = ConnectionState.RECONNECTING
        logger.warning(
            f"Disconnected: {reason}. "
            f"Retry {self._retry_count + 1}"
            f"{'/' + str(self.config.max_retries) if self.config.max_retries > 0 else ''}"
        )

    async def wait_before_retry(self) -> bool:
        """
        Wartet die berechnete Backoff-Zeit ab.

        Returns:
            True wenn ein Retry versucht werden soll,
            False wenn max_retries erreicht.
        """
        # Max Retries prüfen
        if 0 < self.config.max_retries <= self._retry_count:
            logger.error(
                f"Max retries ({self.config.max_retries}) reached. Giving up."
            )
            self.state = ConnectionState.FAILED
            return False

        # Berechne Wartezeit mit Jitter
        jitter_amount = self._current_delay * self.config.jitter * random.random()
        actual_delay = self._current_delay + jitter_amount

        logger.info(
            f"Waiting {actual_delay:.1f}s before retry "
            f"(attempt {self._retry_count + 1}, "
            f"base delay: {self._current_delay:.1f}s)"
        )

        await asyncio.sleep(actual_delay)

        # Delay für nächstes Mal erhöhen
        self._current_delay = min(
            self._current_delay * self.config.multiplier,
            self.config.max_delay,
        )
        self._retry_count += 1
        self._total_reconnects += 1

        self.state = ConnectionState.CONNECTING
        return True

    def reset(self):
        """Setzt den Handler komplett zurück."""
        self._current_delay = self.config.initial_delay
        self._retry_count = 0
        self._state = ConnectionState.DISCONNECTED
        logger.debug("Reconnect handler reset.")

    def _check_reset_counter(self):
        """
        Prüft ob die Verbindung lange genug stabil war,
        um den Retry-Counter zurückzusetzen.
        """
        if self._last_success_time > 0:
            elapsed = time.time() - self._last_success_time
            if elapsed >= self.config.reset_after_success:
                logger.info(
                    f"Connection stable for {elapsed:.0f}s. "
                    f"Resetting retry counter."
                )
                self._current_delay = self.config.initial_delay
                self._retry_count = 0

    def get_stats(self) -> dict:
        """Reconnect-Statistiken."""
        return {
            "state": self._state.value,
            "retry_count": self._retry_count,
            "current_delay": self._current_delay,
            "total_reconnects": self._total_reconnects,
            "last_success": (
                datetime.fromtimestamp(self._last_success_time).isoformat()
                if self._last_success_time > 0 else None
            ),
        }
