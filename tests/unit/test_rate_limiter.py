#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:45:30 2026

@author: twi
"""

"""
Unit Tests für Rate Limiter.
"""

import pytest
import asyncio
import time

from backend.websocket_producer.rate_limiter import (
    TokenBucketRateLimiter,
    SlidingWindowRateLimiter,
    WebSocketRateLimiter,
)


class TestTokenBucketRateLimiter:

    @pytest.mark.asyncio
    async def test_initial_burst(self):
        """Testet dass initial max_tokens verfügbar sind."""
        limiter = TokenBucketRateLimiter(rate=10.0, max_tokens=5.0)

        # Sollte sofort 5 Tokens verbrauchen können
        for _ in range(5):
            await limiter.acquire(1.0)

        stats = limiter.get_stats()
        assert stats["total_acquired"] == 5

    @pytest.mark.asyncio
    async def test_refill_over_time(self):
        """Testet Token-Auffüllung über Zeit."""
        limiter = TokenBucketRateLimiter(rate=100.0, max_tokens=10.0)

        # Alle Tokens verbrauchen
        for _ in range(10):
            await limiter.acquire(1.0)

        # Kurz warten für Refill
        await asyncio.sleep(0.15)

        # Sollte wieder mindestens einige Tokens haben
        await limiter.acquire(1.0)
        assert limiter.get_stats()["total_acquired"] == 11

    @pytest.mark.asyncio
    async def test_rate_limiting_slows_down(self):
        """Testet dass Rate Limiting tatsächlich verzögert."""
        limiter = TokenBucketRateLimiter(rate=10.0, max_tokens=2.0)

        start = time.time()
        for _ in range(5):
            await limiter.acquire(1.0)
        elapsed = time.time() - start

        # Bei rate=10/s und 5 Requests (2 sofort, 3 warten):
        # Sollte mindestens 0.2s dauern
        assert elapsed >= 0.2


class TestSlidingWindowRateLimiter:

    @pytest.mark.asyncio
    async def test_allows_up_to_max(self):
        """Testet dass max_requests pro Fenster erlaubt sind."""
        limiter = SlidingWindowRateLimiter(max_requests=5, window_seconds=1.0)

        start = time.time()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.time() - start

        # 5 Requests sollten sofort durchgehen
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_blocks_over_max(self):
        """Testet Blockierung bei Überschreitung."""
        limiter = SlidingWindowRateLimiter(max_requests=3, window_seconds=0.5)

        start = time.time()
        for _ in range(6):
            await limiter.acquire()
        elapsed = time.time() - start

        # 6 Requests bei max 3/0.5s → mindestens 0.5s Wartezeit
        assert elapsed >= 0.4


class TestWebSocketRateLimiter:

    @pytest.mark.asyncio
    async def test_combined_limiter_exists(self):
        """Testet dass alle Sub-Limiter existieren."""
        limiter = WebSocketRateLimiter()

        assert limiter.connection_limiter is not None
        assert limiter.subscription_limiter is not None
        assert limiter.message_limiter is not None

    @pytest.mark.asyncio
    async def test_connection_rate_limit(self):
        """Testet Connection Rate Limiting."""
        limiter = WebSocketRateLimiter()
        await limiter.wait_for_connection()
        # Sollte ohne Fehler durchlaufen

    @pytest.mark.asyncio
    async def test_subscription_rate_limit(self):
        """Testet Subscription Rate Limiting."""
        limiter = WebSocketRateLimiter()
        await limiter.wait_for_subscription(num_tickers=10)
