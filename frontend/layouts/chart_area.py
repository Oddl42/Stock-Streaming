#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:34:49 2026

@author: twi
"""

"""Chart-Bereich: Zeigt den aktiven Chart (Candlestick oder Line)."""

import panel as pn
from frontend.callbacks.chart_callbacks import ChartCallbackHandler


def create_chart_area(chart_handler: ChartCallbackHandler) -> pn.Column:
    """
    Erstellt den Chart-Bereich.

    Args:
        chart_handler: Die ChartCallbackHandler-Instanz für diese Session.
                       Wird in create_main_layout() erstellt und hierher übergeben.
    """

    chart_container = pn.Column(
        chart_handler.chart_pane,
        sizing_mode="stretch_width",
    )

    return chart_container
