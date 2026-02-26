#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:51:05 2026

@author: twi
"""

"""
Integration Tests: GUI Callbacks.

Testet die Interaktion zwischen GUI-Komponenten und Callbacks
ohne einen laufenden Panel-Server.
"""

import pytest
from unittest.mock import MagicMock, patch

from frontend.components.ticker_selector import TickerSelector
from frontend.components.stream_controls import StreamControls
from frontend.components.chart_type_selector import ChartTypeSelector
from frontend.components.ticker_dropdown import TickerDropdown
from frontend.components.ticker_info_table import TickerInfoTable
from frontend.callbacks.ticker_callbacks import create_ticker_callback_handler
from frontend.callbacks.chart_callbacks import ChartCallbackHandler


@pytest.mark.gui
class TestTickerSelectionFlow:

    @pytest.fixture(autouse=True)
    def setup(self):
        with patch(
            "frontend.components.ticker_selector.ticker_loader"
        ) as mock_loader:
            mock_loader.all_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
            mock_loader.top10_symbols = ["AAPL", "MSFT", "GOOGL"]

            self.selector = TickerSelector()
            self.dropdown = TickerDropdown()
            self.table = TickerInfoTable()

    def test_top10_default_selection(self):
        """Testet dass Top 10 als Default ausgewählt sind."""
        assert self.selector.selection_mode == "Top 10"
        assert len(self.selector.selected_symbols) > 0

    def test_mode_change_updates_selection(self):
        """Testet dass Moduswechsel die Auswahl ändert."""
        self.selector.selection_mode = "Top 10"
        top10_count = len(self.selector.selected_symbols)

        self.selector.selection_mode = "Alle S&P 500"
        all_count = len(self.selector.selected_symbols)

        assert all_count >= top10_count

    def test_ticker_callback_handler_wiring(self):
        """Testet die Verdrahtung des TickerCallbackHandlers."""
        handler = create_ticker_callback_handler(
            self.selector, self.dropdown, self.table
        )

        # Simuliere Ticker-Änderung
        self.selector.selected_symbols = ["AAPL", "MSFT"]

        assert self.dropdown.available_tickers == ["AAPL", "MSFT"]
        assert self.table.selected_symbols == ["AAPL", "MSFT"]


@pytest.mark.gui
class TestStreamControlsFlow:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.controls = StreamControls()

    def test_initial_state(self):
        """Testet Initialzustand."""
        assert self.controls.is_streaming is False
        assert self.controls.stream_type == "Sekunden"

    def test_stream_type_toggle(self):
        """Testet Stream-Typ Wechsel."""
        self.controls.stream_type = "Minuten"
        assert self.controls.stream_type == "Minuten"


@pytest.mark.gui
class TestChartCallbackHandler:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.handler = ChartCallbackHandler()
        self.handler._use_demo_data = True

    def test_set_symbol(self):
        """Testet Symbol-Wechsel."""
        self.handler.set_symbol("AAPL")
        assert self.handler._current_symbol == "AAPL"

    def test_set_chart_type(self):
        """Testet Chart-Typ Wechsel."""
        self.handler.set_chart_type("Linie")
        assert self.handler._current_chart_type == "Linie"

    def test_set_stream_type(self):
        """Testet Stream-Typ Wechsel."""
        self.handler.set_stream_type("Minuten")
        assert self.handler._current_stream_type == "minute"

    def test_fetch_demo_data(self):
        """Testet Demo-Daten Abruf."""
        self.handler.set_symbol("AAPL")
        self.handler._fetch_and_update()
        # Sollte ohne Fehler durchlaufen

    def test_active_chart_candlestick(self):
        """Testet dass Candlestick der Standard-Chart ist."""
        self.handler.set_chart_type("Candlestick")
        assert self.handler.active_chart == self.handler.candlestick_chart

    def test_active_chart_line(self):
        """Testet Line-Chart Auswahl."""
        self.handler.set_chart_type("Linie")
        assert self.handler.active_chart == self.handler.line_chart
