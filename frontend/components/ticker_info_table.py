#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:14:54 2026

@author: twi
"""

"""Tabelle mit Detail-Informationen der ausgewählten Ticker."""

import panel as pn
import param
import pandas as pd

from backend.data_service.ticker_loader import ticker_loader
from backend.data_service.data_provider import data_provider


class TickerInfoTable(param.Parameterized):
    """
    Zeigt eine Tabelle mit:
    Symbol, Name, Industry, Open Price, Close Price, Volume
    """

    selected_symbols = param.List(
        default=[],
        doc="Ausgewählte Symbole",
    )
    table_data = param.DataFrame(
        doc="Tabellen-Daten",
    )

    def __init__(self, **params):
        super().__init__(**params)
        self._tabulator = pn.widgets.Tabulator(
            value=pd.DataFrame(),
            name="Ausgewählte Ticker",
            sizing_mode="stretch_width",
            height=300,
            page_size=15,
            pagination="remote",
            theme="midnight",
            frozen_columns=["Symbol"],
            header_filters=True,
            selectable="checkbox",
            layout="fit_data_stretch",
            text_align={
                "Open": "right",
                "Close": "right",
                "Volume": "right",
                "MarketCap": "right",
            },
            formatters={
                "Open": {"type": "money", "symbol": "$", "precision": 2},
                "Close": {"type": "money", "symbol": "$", "precision": 2},
                "Volume": {"type": "money", "symbol": "", "precision": 0},
                "MarketCap": {"type": "money", "symbol": "$", "precision": 0},
            },
            titles={
                "Symbol": "Symbol",
                "Name": "Name",
                "Industry": "Industrie",
                "Open": "Open ($)",
                "Close": "Close ($)",
                "Volume": "Volumen",
                "MarketCap": "Marktkapitalisierung",
            },
        )

    @param.depends("selected_symbols", watch=True)
    def _update_table(self):
        """Aktualisiert die Tabelle bei Änderung der Auswahl."""
        if not self.selected_symbols:
            self._tabulator.value = pd.DataFrame()
            return

        # Basis-Daten aus CSV
        base_df = ticker_loader.get_tickers_by_symbols(self.selected_symbols)

        # Versuche Live-Preis-Daten zu holen
        try:
            price_df = data_provider.get_latest_price_info(self.selected_symbols)
            if not price_df.empty:
                price_df = price_df.rename(columns={
                    "symbol": "Symbol",
                    "open": "Open",
                    "close": "Close",
                    "volume": "Volume",
                })
                merged = base_df.merge(price_df, on="Symbol", how="left")
            else:
                merged = base_df.copy()
                merged["Open"] = None
                merged["Close"] = None
                merged["Volume"] = None
        except Exception:
            merged = base_df.copy()
            merged["Open"] = None
            merged["Close"] = None
            merged["Volume"] = None

        # Spalten auswählen und formatieren
        display_cols = ["Symbol", "Name", "Industry", "MarketCap", "Open", "Close", "Volume"]
        available_cols = [c for c in display_cols if c in merged.columns]
        self._tabulator.value = merged[available_cols].reset_index(drop=True)
        self.table_data = self._tabulator.value

    def refresh_prices(self):
        """Manuelle Aktualisierung der Preis-Daten."""
        self._update_table()

    def panel(self):
        """Erstellt die Panel-Komponente."""
        refresh_btn = pn.widgets.Button(
            name="🔄 Preise aktualisieren",
            button_type="light",
            width=180,
            height=30,
        )
        refresh_btn.on_click(lambda e: self.refresh_prices())

        return pn.Column(
            pn.Row(
                pn.pane.Markdown("### 📋 Ausgewählte Ticker"),
                pn.layout.HSpacer(),
                refresh_btn,
            ),
            self._tabulator,
            sizing_mode="stretch_width",
        )
