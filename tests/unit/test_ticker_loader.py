#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:46:49 2026

@author: twi
"""

"""
Unit Tests für den Ticker CSV Loader.
"""

import pytest
import pandas as pd

from backend.data_service.ticker_loader import TickerLoader


class TestTickerLoader:

    @pytest.fixture(autouse=True)
    def setup(self, tmp_csv_file):
        """Setup mit temporärer CSV-Datei."""
        self.loader = TickerLoader(csv_path=tmp_csv_file)

    def test_load_csv(self):
        """Testet das Laden der CSV-Datei."""
        assert not self.loader.all_tickers.empty
        assert len(self.loader.all_symbols) == 5

    def test_all_symbols_returns_list(self):
        """Testet dass all_symbols eine Liste zurückgibt."""
        symbols = self.loader.all_symbols
        assert isinstance(symbols, list)
        assert "AAPL" in symbols

    def test_top10_by_market_cap(self):
        """Testet Top 10 Sortierung nach MarketCap."""
        top10 = self.loader.top10_by_market_cap
        assert len(top10) <= 10

        # Erste Position sollte höchste MarketCap haben
        if len(top10) > 1:
            assert top10.iloc[0]["MarketCap"] >= top10.iloc[1]["MarketCap"]

    def test_top10_symbols(self):
        """Testet top10_symbols Property."""
        symbols = self.loader.top10_symbols
        assert isinstance(symbols, list)
        assert len(symbols) <= 10

    def test_get_tickers_by_symbols(self):
        """Testet Filterung nach Symbolen."""
        result = self.loader.get_tickers_by_symbols(["AAPL", "MSFT"])

        assert len(result) == 2
        assert set(result["Symbol"]) == {"AAPL", "MSFT"}

    def test_get_tickers_by_unknown_symbols(self):
        """Testet Filterung mit unbekannten Symbolen."""
        result = self.loader.get_tickers_by_symbols(["UNKNOWN"])

        assert len(result) == 0

    def test_search_symbols(self):
        """Testet Symbol-Suche."""
        results = self.loader.search_symbols("AAPL")
        assert "AAPL" in results

    def test_search_by_name(self):
        """Testet Suche nach Firmennamen."""
        results = self.loader.search_symbols("Apple")
        assert "AAPL" in results

    def test_search_case_insensitive(self):
        """Testet case-insensitive Suche."""
        results = self.loader.search_symbols("aapl")
        assert "AAPL" in results

    def test_fallback_demo_data(self, tmp_path):
        """Testet Fallback auf Demo-Daten wenn CSV fehlt."""
        loader = TickerLoader(csv_path=str(tmp_path / "nonexistent.csv"))

        assert not loader.all_tickers.empty
        assert len(loader.all_symbols) > 0

    def test_all_tickers_returns_copy(self):
        """Testet dass all_tickers eine Kopie zurückgibt."""
        df1 = self.loader.all_tickers
        df2 = self.loader.all_tickers

        assert df1 is not df2
