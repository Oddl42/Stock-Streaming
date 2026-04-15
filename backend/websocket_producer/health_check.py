#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:32:21 2026

@author: twi
"""

"""
HTTP Health Check Server für Kubernetes Liveness/Readiness Probes.

Stellt einen einfachen HTTP-Endpoint bereit:
  GET /health    → 200 OK wenn gesund
  GET /ready     → 200 OK wenn bereit
  GET /metrics   → Prometheus Metriken (via prometheus_client)
"""

import asyncio
import json
import logging
from aiohttp import web

logger = logging.getLogger(__name__)


class HealthCheckServer:
    """
    Async HTTP Server für Health Checks.

    Endpoints:
    - /health     → Liveness Probe
    - /ready      → Readiness Probe
    - /status     → Detaillierter Status (JSON)
    """

    def __init__(
        self,
        port: int = 8092,
        host: str = "0.0.0.0",
    ):
        self.port = port
        self.host = host
        self._app = web.Application()
        self._runner: web.AppRunner = None
        self._is_healthy = True
        self._is_ready = False
        self._status_providers: list = []

    def register_status_provider(self, name: str, provider):
        """
        Registriert einen Status-Provider.

        Args:
            name: Name des Providers (z.B. "websocket", "kafka")
            provider: Objekt mit get_stats() Methode
        """
        self._status_providers.append((name, provider))

    def set_healthy(self, healthy: bool):
        """Setzt den Health-Status."""
        self._is_healthy = healthy

    def set_ready(self, ready: bool):
        """Setzt den Readiness-Status."""
        self._is_ready = ready

    async def start(self):
        """Startet den Health Check Server."""
        self._app.router.add_get("/health", self._handle_health)
        self._app.router.add_get("/ready", self._handle_ready)
        self._app.router.add_get("/status", self._handle_status)
        self._app.router.add_get("/", self._handle_root)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info(f"Health check server started on {self.host}:{self.port}")

    async def stop(self):
        """Stoppt den Server."""
        if self._runner:
            await self._runner.cleanup()
            logger.info("Health check server stopped.")

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Liveness Probe: Ist der Prozess am Leben?"""
        if self._is_healthy:
            return web.json_response(
                {"status": "healthy"},
                status=200,
            )
        return web.json_response(
            {"status": "unhealthy"},
            status=503,
        )

    async def _handle_ready(self, request: web.Request) -> web.Response:
        """Readiness Probe: Ist der Service bereit Traffic zu empfangen?"""
        if self._is_ready:
            return web.json_response(
                {"status": "ready"},
                status=200,
            )
        return web.json_response(
            {"status": "not_ready"},
            status=503,
        )

    async def _handle_status(self, request: web.Request) -> web.Response:
        """Detaillierter Status aller Komponenten."""
        status = {
            "healthy": self._is_healthy,
            "ready": self._is_ready,
            "components": {},
        }

        for name, provider in self._status_providers:
            try:
                stats = provider.get_stats()
                status["components"][name] = stats
            except Exception as e:
                status["components"][name] = {"error": str(e)}

        return web.json_response(status, status=200)

    async def _handle_root(self, request: web.Request) -> web.Response:
        """Root-Endpoint mit Übersicht."""
        return web.json_response({
            "service": "stock-platform-ws-producer",
            "endpoints": ["/health", "/ready", "/status"],
        })
