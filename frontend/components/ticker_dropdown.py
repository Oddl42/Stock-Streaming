#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:14:23 2026

@author: twi
"""

"""Dropdown zur Auswahl des zu plottenden Tickers."""

import panel as pn
import param


class TickerDropdown(param.Parameterized):
    """
    Dropdown-Menü mit allen ausgewählten Tickern.
    Nur der hier gewählte Ticker wird geplottet.
    """

    selected_ticker = param.String(
        default="",
        doc="Aktuell ausgewählter Ticker zum Plotten",
    )
    available_tickers = param.List(
        default=[],
        doc="Verfügbare Ticker (aus TickerSelector)",
    )

    def __init__(self, **params):
        super().__init__(**params)
        self._dropdown = pn.widgets.Select(
            name="📈 Ticker zum Plotten",
            options=[],
            value=None,
            width=300,
        )
        # Watch dropdown changes
        self._dropdown.param.watch(self._on_select, "value")

    def _on_select(self, event):
        """Wird aufgerufen wenn ein Ticker im Dropdown ausgewählt wird."""
        if event.new:
            self.selected_ticker = event.new

    @param.depends("available_tickers", watch=True)
    def _update_options(self):
        """Aktualisiert die Dropdown-Optionen."""
        self._dropdown.options = self.available_tickers
        if self.available_tickers:
            self._dropdown.value = self.available_tickers[0]
            self.selected_ticker = self.available_tickers[0]

    def panel(self):
        """Erstellt die Panel-Komponente."""
        return pn.Column(
            "### 🔽 Plot-Ticker",
            self._dropdown,
            pn.pane.Markdown(
                "*Es wird immer nur ein Ticker geplottet. "
                "Alle ausgewählten Ticker werden aber gestreamt und in die DB geschrieben.*",
                styles={"font-size": "11px", "color": "#888"},
            ),
            sizing_mode="stretch_width",
            css_classes=["sidebar-section"],
        )
