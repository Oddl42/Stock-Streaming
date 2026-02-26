#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:30:04 2026

@author: twi
"""

"""
Rate Limiter für Massive.com API Calls.

Stellt sicher, dass API Rate Limits nicht überschritten werden.
Implementiert Token-Bucket und Sliding-Window Algorithmen.
"""

import asyncio
import time
import logging
from collections import deque

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """
    Token-Bucket Rate Limiter.

    Erlaubt bursts bis zu max_tokens,
    füllt sich mit rate Tokens pro Sekunde auf.
    """

    def __init__(
        self,
        rate: float = 5.0,       # Tokens pro Sekunde
        max_tokens: float = 10.0, # Maximale Burst-Größe
    ):
        self.rate = rate
        self.max_tokens = max_tokens
        self._tokens = max_tokens
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._total_waits = 0
        self._total_acquired = 0

    async def acquire(self, tokens: float = 1.0):
        """
        Wartet bis genügend Tokens verfügbar sind.

        Args:
            tokens: Anzahl der benötigten Tokens
        """
        async with self._lock:
            self._refill()

            while self._tokens < tokens:
                # Berechne Wartezeit
                needed = tokens - self._tokens
                wait_time = needed / self.rate
                self._total_waits += 1
                logger.debug(
                    f"Rate limit: waiting {wait_time:.2f}s "
                    f"(need {needed:.1f} tokens)"
                )
                await asyncio.sleep(wait_time)
                self._refill()

            self._tokens -= tokens
            self._total_acquired += 1

    def _refill(self):
        """Füllt Tokens basierend auf vergangener Zeit auf."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self.max_tokens,
            self._tokens + elapsed * self.rate,
        )
        self._last_refill = now

    def get_stats(self) -> dict:
        return {
            "available_tokens": self._tokens,
            "rate_per_second": self.rate,
            "total_waits": self._total_waits,
            "total_acquired": self._total_acquired,
        }


class SlidingWindowRateLimiter:
    """
    Sliding-Window Rate Limiter.

    Begrenzt die Anzahl der Requests pro Zeitfenster.
    Nützlich für API-Limits wie "5 Requests pro Sekunde".
    """

    def __init__(
        self,
        max_requests: int = 5,
        window_seconds: float = 1.0,
    ):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._timestamps: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Wartet bis ein Request erlaubt ist."""
        async with self._lock:
            now = time.monotonic()

            # Alte Timestamps entfernen
            while self._timestamps and (now - self._timestamps[0]) > self.window_seconds:
                self._timestamps.popleft()

            # Prüfe ob Limit erreicht
            if len(self._timestamps) >= self.max_requests:
                # Warte bis ältester Timestamp abläuft
                oldest = self._timestamps[0]
                wait_time = self.window_seconds - (now - oldest) + 0.01
                if wait_time > 0:
                    logger.debug(f"Sliding window limit: waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)

            self._timestamps.append(time.monotonic())


class WebSocketRateLimiter:
    """
    Kombinierter Rate Limiter für die Massive.com WebSocket API.

    Implementiert:
    - Connection Rate Limit (max Verbindungen/Minute)
    - Subscription Rate Limit (max Subscriptions/Sekunde)
    - Message Rate Limit (für ausgehende Nachrichten)
    """

    def __init__(self):
        # Connection: Max 5 Verbindungen pro Minute
        self.connection_limiter = SlidingWindowRateLimiter(
            max_requests=5, window_seconds=60.0
        )
        # Subscription: Max 100 Subscriptions pro Sekunde
        self.subscription_limiter = TokenBucketRateLimiter(
            rate=100.0, max_tokens=500.0
        )
        # Message (outgoing): Max 50 Messages pro Sekunde
        self.message_limiter = TokenBucketRateLimiter(
            rate=50.0, max_tokens=100.0
        )

    async def wait_for_connection(self):
        """Warte auf Erlaubnis eine neue Verbindung herzustellen."""
        await self.connection_limiter.acquire()

    async def wait_for_subscription(self, num_tickers: int = 1):
        """Warte auf Erlaubnis Subscriptions zu senden."""
        await self.subscription_limiter.acquire(float(num_tickers))

    async def wait_for_message(self):
        """Warte auf Erlaubnis eine Nachricht zu senden."""
        await self.message_limiter.acquire()
