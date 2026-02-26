#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:34:24 2026

@author: twi
"""

"""Sidebar-Layout: Enthält alle Steuerelemente."""

import panel as pn

from frontend.components.ticker_selector import TickerSelector
from frontend.components.stream_controls import StreamControls
from frontend.components.chart_type_selector import ChartTypeSelector
from frontend.components.ticker_dropdown import TickerDropdown


def create_sidebar(
    ticker_selector: TickerSelector,
    stream_controls: StreamControls,
    chart_type_selector: ChartTypeSelector,
    ticker_dropdown: TickerDropdown,
) -> pn.Column:
    """
    Baut die Sidebar zusammen aus allen Steuer-Komponenten.
    """
    # Logo/Branding
    branding = pn.pane.Markdown(
        "## 🎛️ Steuerung",
        styles={"text-align": "center", "margin-bottom": "10px"},
    )

    # Dividers zwischen Sektionen
    divider = pn.layout.Divider()

    sidebar = pn.Column(
        branding,
        divider,
        ticker_selector.panel(),
        divider,
        stream_controls.panel(),
        divider,
        chart_type_selector.panel(),
        divider,
        ticker_dropdown.panel(),
        sizing_mode="stretch_width",
        scroll=True,
    )

    return sidebar
