#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:35:24 2026

@author: twi
"""

"""Ticker-Tabellen Bereich."""

import panel as pn

from frontend.components.ticker_info_table import TickerInfoTable


def create_ticker_table_area(ticker_info_table: TickerInfoTable) -> pn.Column:
    """
    Erstellt den Ticker-Tabellen Bereich unterhalb des Charts.
    """
    table_card = pn.layout.Card(
        ticker_info_table.panel(),
        title="📋 Ticker Übersicht",
        sizing_mode="stretch_width",
        collapsed=False,
        collapsible=True,
        header_background="#2a2a3e",
        header_color="white",
    )

    return table_card
