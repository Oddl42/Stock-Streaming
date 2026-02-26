#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:47:10 2026

@author: twi
"""

"""
Unit Tests für den Health Check Server.
"""

import pytest
import aiohttp

from backend.websocket_producer.health_check import HealthCheckServer


class TestHealthCheckServer:

    @pytest.fixture
    async def server(self):
        """Erstellt und startet einen Health Check Server."""
        srv = HealthCheckServer(port=18092, host="127.0.0.1")
        await srv.start()
        yield srv
        await srv.stop()

    @pytest.mark.asyncio
    async def test_health_endpoint_healthy(self, server):
        """Testet /health wenn gesund."""
        server.set_healthy(True)

        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:18092/health") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_health_endpoint_unhealthy(self, server):
        """Testet /health wenn ungesund."""
        server.set_healthy(False)

        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:18092/health") as resp:
                assert resp.status == 503
                data = await resp.json()
                assert data["status"] == "unhealthy"

    @pytest.mark.asyncio
    async def test_ready_endpoint_ready(self, server):
        """Testet /ready wenn bereit."""
        server.set_ready(True)

        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:18092/ready") as resp:
                assert resp.status == 200

    @pytest.mark.asyncio
    async def test_ready_endpoint_not_ready(self, server):
        """Testet /ready wenn nicht bereit."""
        server.set_ready(False)

        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:18092/ready") as resp:
                assert resp.status == 503

    @pytest.mark.asyncio
    async def test_status_endpoint(self, server):
        """Testet /status Endpoint."""
        # Provider registrieren
        class MockProvider:
            def get_stats(self):
                return {"key": "value", "count": 42}

        server.register_status_provider("test", MockProvider())

        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:18092/status") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert "components" in data
                assert "test" in data["components"]
                assert data["components"]["test"]["count"] == 42

    @pytest.mark.asyncio
    async def test_root_endpoint(self, server):
        """Testet Root Endpoint."""
        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:18092/") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert "endpoints" in data
