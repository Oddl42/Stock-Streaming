#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:32:24 2026

@author: twi
"""

"""Callbacks für Ticker-Auswahl und deren Auswirkungen."""

import logging

from frontend.components.ticker_selector import TickerSelector
from frontend.components.ticker_dropdown import TickerDropdown
from frontend.components.ticker_info_table import TickerInfoTable
from frontend.callbacks.chart_callbacks import ChartCallbackHandler

logger = logging.getLogger(__name__)


class TickerCallbackHandler:
    """
    Koordiniert die Ticker-Auswahl zwischen Komponenten:
    1. TickerSelector wählt Ticker aus
    2. TickerDropdown wird mit verfügbaren Tickern befüllt
    3. TickerInfoTable zeigt Details der ausgewählten Ticker
    4. ChartCallbackHandler wird über Ticker-Änderungen informiert
    """

    def __init__(
        self,
        ticker_selector: TickerSelector,
        ticker_dropdown: TickerDropdown,
        ticker_info_table: TickerInfoTable,
        chart_handler: ChartCallbackHandler,
    ):
        self.selector = ticker_selector
        self.dropdown = ticker_dropdown
        self.table = ticker_info_table
        self.chart_handler = chart_handler

        # Registriere Watchers
        self._setup_watchers()

    def _setup_watchers(self):
        """Verbindet die Komponenten miteinander."""

        # Wenn sich die Ticker-Auswahl ändert → Update Dropdown + Tabelle
        self.selector.param.watch(
            self._on_selection_changed, "selected_symbols"
        )

        # Wenn sich der Plot-Ticker ändert → Update Chart
        self.dropdown.param.watch(
            self._on_plot_ticker_changed, "selected_ticker"
        )

    def _on_selection_changed(self, event):
        """Wird aufgerufen wenn die Ticker-Auswahl sich ändert."""
        new_symbols = event.new
        logger.info(f"Ticker selection changed: {len(new_symbols)} symbols")

        # Update Dropdown
        self.dropdown.available_tickers = new_symbols

        # Update Tabelle
        self.table.selected_symbols = new_symbols

    def _on_plot_ticker_changed(self, event):
        """Wird aufgerufen wenn der Plot-Ticker sich ändert."""
        new_ticker = event.new
        if new_ticker:
            logger.info(f"Plot ticker changed to: {new_ticker}")
            self.chart_handler.set_symbol(new_ticker)


# Factory-Funktion (wird in main_layout verwendet)
def create_ticker_callback_handler(
    selector: TickerSelector,
    dropdown: TickerDropdown,
    table: TickerInfoTable,
    chart_handler: ChartCallbackHandler,
) -> TickerCallbackHandler:
    return TickerCallbackHandler(selector, dropdown, table, chart_handler)
