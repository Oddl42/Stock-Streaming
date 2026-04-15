#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:45:00 2026

@author: twi
"""

"""
Unit Tests für den Reconnection Handler.
"""

import pytest
import asyncio

from backend.websocket_producer.reconnect_handler import (
    ReconnectHandler,
    ReconnectConfig,
    ConnectionState,
)


class TestReconnectHandler:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.config = ReconnectConfig(
            initial_delay=0.1,      # Kurz für Tests
            max_delay=1.0,
            multiplier=2.0,
            jitter=0.0,            # Kein Jitter für deterministische Tests
            max_retries=5,
            reset_after_success=1.0,
        )
        self.handler = ReconnectHandler(config=self.config)

    def test_initial_state(self):
        """Initialer Zustand ist DISCONNECTED."""
        assert self.handler.state == ConnectionState.DISCONNECTED

    def test_state_transitions(self):
        """Testet Zustandsübergänge."""
        self.handler.state = ConnectionState.CONNECTING
        assert self.handler.state == ConnectionState.CONNECTING

        self.handler.on_connected()
        assert self.handler.state == ConnectionState.CONNECTED

        self.handler.on_authenticated()
        assert self.handler.state == ConnectionState.AUTHENTICATED

        self.handler.on_streaming()
        assert self.handler.state == ConnectionState.STREAMING

    def test_on_disconnected(self):
        """Testet Verhalten bei Verbindungsabbruch."""
        self.handler.on_disconnected("Test disconnect")
        assert self.handler.state == ConnectionState.RECONNECTING

    @pytest.mark.asyncio
    async def test_exponential_backoff(self):
        """Testet exponentiellen Backoff."""
        delays = []
        for i in range(3):
            self.handler._current_delay = self.config.initial_delay * (
                self.config.multiplier ** i
            )
            delays.append(self.handler._current_delay)

        assert delays[0] == 0.1
        assert delays[1] == 0.2
        assert delays[2] == 0.4

    @pytest.mark.asyncio
    async def test_max_delay_cap(self):
        """Testet dass max_delay nicht überschritten wird."""
        # Simuliere viele Retries
        for _ in range(20):
            result = await self.handler.wait_before_retry()
            if not result:
                break

        assert self.handler._current_delay <= self.config.max_delay

    @pytest.mark.asyncio
    async def test_max_retries_reached(self):
        """Testet dass nach max_retries aufgegeben wird."""
        for i in range(self.config.max_retries):
            result = await self.handler.wait_before_retry()
            assert result is True

        result = await self.handler.wait_before_retry()
        assert result is False
        assert self.handler.state == ConnectionState.FAILED

    def test_reset(self):
        """Testet Reset des Handlers."""
        self.handler._retry_count = 10
        self.handler._current_delay = 30.0
        self.handler.state = ConnectionState.RECONNECTING

        self.handler.reset()

        assert self.handler._retry_count == 0
        assert self.handler._current_delay == self.config.initial_delay
        assert self.handler.state == ConnectionState.DISCONNECTED

    def test_state_change_callback(self):
        """Testet State-Change Callbacks."""
        changes = []
        self.handler.on_state_change(
            lambda old, new: changes.append((old.value, new.value))
        )

        self.handler.state = ConnectionState.CONNECTING
        self.handler.state = ConnectionState.CONNECTED

        assert len(changes) == 2
        assert changes[0] == ("disconnected", "connecting")
        assert changes[1] == ("connecting", "connected")

    def test_stats(self):
        """Testet Statistik-Ausgabe."""
        stats = self.handler.get_stats()

        assert "state" in stats
        assert "retry_count" in stats
        assert "current_delay" in stats
        assert "total_reconnects" in stats
