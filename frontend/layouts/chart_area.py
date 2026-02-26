#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:34:49 2026

@author: twi
"""

"""Chart-Bereich: Zeigt den aktiven Chart (Candlestick oder Line)."""

import panel as pn
from bokeh.models import Div

from frontend.charts.candlestick_chart import CandlestickChart
from frontend.charts.line_chart import LineChart
from frontend.callbacks.chart_callbacks import chart_callback_handler


def create_chart_area() -> pn.Column:
    """
    Erstellt den Chart-Bereich mit dynamischem Chart-Wechsel.
    """

    # Erstelle initiale Chart-Panes
    candlestick_pane = pn.pane.Bokeh(
        chart_callback_handler.candlestick_chart.figure,
        sizing_mode="stretch_width",
    )

    line_pane = pn.pane.Bokeh(
        chart_callback_handler.line_chart.figure,
        sizing_mode="stretch_width",
    )

    # Stack-Layout: Zeigt nur einen Chart
    chart_stack = pn.layout.Card(
        candlestick_pane,
        title="📊 Live Chart",
        sizing_mode="stretch_width",
        collapsed=False,
        header_background="#1a73e8",
        header_color="white",
    )

    def switch_chart(chart_type: str):
        """Wechselt zwischen Candlestick und Line Chart."""
        chart_stack.clear()
        if chart_type == "Candlestick":
            chart_stack.append(pn.pane.Bokeh(
                chart_callback_handler.candlestick_chart.figure,
                sizing_mode="stretch_width",
            ))
        else:
            chart_stack.append(pn.pane.Bokeh(
                chart_callback_handler.line_chart.figure,
                sizing_mode="stretch_width",
            ))

    # Expose switch function
    chart_stack._switch_chart = switch_chart

    return chart_stack
