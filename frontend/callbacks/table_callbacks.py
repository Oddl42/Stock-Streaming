#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:32:56 2026

@author: twi
"""

"""Callbacks für Tabellen-Updates."""

import panel as pn
import logging

from frontend.components.ticker_info_table import TickerInfoTable
from config.settings import settings

logger = logging.getLogger(__name__)


class TableCallbackHandler:
    """
    Verwaltet periodische Updates der Ticker-Tabelle.
    Aktualisiert Preise in regelmäßigen Abständen.
    """

    TABLE_UPDATE_INTERVAL_MS = 10_000  # Alle 10 Sekunden

    def __init__(self, ticker_info_table: TickerInfoTable):
        self.table = ticker_info_table
        self._periodic_callback = None

    def start_periodic_update(self):
        """Startet das periodische Tabellen-Update."""
        if self._periodic_callback is not None:
            return

        self._periodic_callback = pn.state.add_periodic_callback(
            self._update_table,
            period=self.TABLE_UPDATE_INTERVAL_MS,
        )
        logger.info("Table periodic update started.")

    def stop_periodic_update(self):
        """Stoppt das periodische Tabellen-Update."""
        if self._periodic_callback is not None:
            self._periodic_callback.stop()
            self._periodic_callback = None
            logger.info("Table periodic update stopped.")

    def _update_table(self):
        """Aktualisiert die Tabellendaten."""
        try:
            self.table.refresh_prices()
        except Exception as e:
            logger.error(f"Table update error: {e}")


def create_table_callback_handler(
    ticker_info_table: TickerInfoTable,
) -> TableCallbackHandler:
    return TableCallbackHandler(ticker_info_table)
