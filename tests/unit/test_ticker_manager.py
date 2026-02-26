#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:46:22 2026

@author: twi
"""

"""
Unit Tests für den Ticker Manager.
"""

import pytest
from unittest.mock import patch, PropertyMock

from backend.websocket_producer.ticker_manager import TickerManager


class TestTickerManager:

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup mit gemocktem ticker_loader."""
        self.all_symbols = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
            "META", "TSLA", "BRK.B", "UNH", "JNJ",
            "V", "XOM", "WMT", "JPM", "PG",
        ]
        self.top10 = self.all_symbols[:10]

        with patch(
            "backend.websocket_producer.ticker_manager.ticker_loader"
        ) as mock_loader:
            mock_loader.all_symbols = self.all_symbols
            mock_loader.top10_symbols = self.top10
            self.manager = TickerManager()
            self.mock_loader = mock_loader

    def test_set_tickers(self):
        """Testet das Setzen von Tickern."""
        self.manager.set_tickers(["AAPL", "MSFT", "GOOGL"])

        assert self.manager.active_count == 3
        assert "AAPL" in self.manager.active_tickers
        assert "MSFT" in self.manager.active_tickers

    def test_set_tickers_uppercase(self):
        """Testet Normalisierung auf Großbuchstaben."""
        self.manager.set_tickers(["aapl", "msft"])

        assert "AAPL" in self.manager.active_tickers
        assert "MSFT" in self.manager.active_tickers

    def test_set_tickers_deduplication(self):
        """Testet Duplikat-Entfernung."""
        self.manager.set_tickers(["AAPL", "AAPL", "MSFT", "MSFT"])

        assert self.manager.active_count == 2

    def test_set_tickers_unknown_ignored(self):
        """Testet dass unbekannte Ticker ignoriert werden."""
        self.manager.set_tickers(["AAPL", "UNKNOWN_TICKER", "MSFT"])

        assert self.manager.active_count == 2
        assert "UNKNOWN_TICKER" not in self.manager.active_tickers

    def test_set_tickers_empty(self):
        """Testet leere Ticker-Liste."""
        self.manager.set_tickers([])

        assert self.manager.active_count == 0

    def test_subscription_batches_small(self):
        """Testet Batch-Erstellung für wenige Ticker."""
        self.manager.set_tickers(["AAPL", "MSFT", "GOOGL"])
        batches = self.manager.get_subscription_batches(prefix="A")

        assert len(batches) == 1
        assert "A.AAPL" in batches[0]
        assert "A.MSFT" in batches[0]
        assert "A.GOOGL" in batches[0]

    def test_subscription_batches_minute(self):
        """Testet Batch-Erstellung mit AM-Prefix."""
        self.manager.set_tickers(["AAPL"])
        batches = self.manager.get_subscription_batches(prefix="AM")

        assert "AM.AAPL" in batches[0]

    def test_subscription_batches_large(self):
        """Testet Batching bei vielen Tickern (>100)."""
        # Simuliere 250 Ticker
        many_tickers = [f"T{i:03d}" for i in range(250)]
        with patch.object(
            self.manager, "_active_tickers", many_tickers
        ):
            # Mock dass alle als valide gelten
            batches = self.manager.get_subscription_batches(prefix="A")

        assert len(batches) == 3  # 100 + 100 + 50

    def test_subscription_batches_empty(self):
        """Testet leere Batches."""
        batches = self.manager.get_subscription_batches(prefix="A")

        assert len(batches) == 0

    def test_mark_subscribed(self):
        """Testet Markierung als abonniert."""
        self.manager.set_tickers(["AAPL", "MSFT"])
        self.manager.mark_subscribed(["AAPL", "MSFT"])

        assert self.manager.subscribed_count == 2

    def test_mark_unsubscribed_all(self):
        """Testet Abmeldung aller Ticker."""
        self.manager.mark_subscribed(["AAPL", "MSFT"])
        self.manager.mark_unsubscribed()

        assert self.manager.subscribed_count == 0

    def test_mark_unsubscribed_specific(self):
        """Testet selektive Abmeldung."""
        self.manager.mark_subscribed(["AAPL", "MSFT", "GOOGL"])
        self.manager.mark_unsubscribed(["MSFT"])

        assert self.manager.subscribed_count == 2

    def test_get_stats(self):
        """Testet Statistik-Ausgabe."""
        self.manager.set_tickers(["AAPL", "MSFT"])
        stats = self.manager.get_stats()

        assert stats["active_tickers"] == 2
        assert "sample_active" in stats
