#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:49:22 2026

@author: twi
"""

"""
Unit Tests für die Konfiguration.
"""

import pytest
import os

from config.settings import Settings


class TestSettings:

    def test_default_values(self):
        """Testet Standardwerte."""
        s = Settings()

        assert s.DB_PORT == 5432
        assert s.PANEL_PORT == 5006
        assert s.CHART_UPDATE_INTERVAL_MS == 2000
        assert s.CHART_MAX_POINTS == 500

    def test_db_url_format(self):
        """Testet DB-URL Format."""
        s = Settings()
        url = s.db_url

        assert url.startswith("postgresql://")
        assert str(s.DB_PORT) in url
        assert s.DB_NAME in url

    def test_env_override(self, monkeypatch):
        """Testet Environment-Variable Override."""
        monkeypatch.setenv("DB_HOST", "custom-host")
        monkeypatch.setenv("DB_PORT", "5433")

        s = Settings()

        assert s.DB_HOST == "custom-host"
        assert s.DB_PORT == 5433
