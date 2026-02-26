#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:33:58 2026

@author: twi
"""

"""Header-Bereich der App."""

import panel as pn
from datetime import datetime


def create_header():
    """Erstellt den Header mit Titel und Live-Uhr."""

    title = pn.pane.Markdown(
        "# 📈 Stock Streaming Platform",
        styles={
            "font-size": "20px",
            "margin": "0",
            "color": "white",
        },
    )

    subtitle = pn.pane.Markdown(
        "*Real-time S&P 500 Streaming mit Spark & Kafka*",
        styles={
            "font-size": "12px",
            "color": "#ccc",
            "margin": "0",
        },
    )

    # Live Clock
    clock = pn.indicators.String(
        value=datetime.now().strftime("%H:%M:%S"),
        font_size="14pt",
        title="🕐",
    )

    def update_clock():
        clock.value = datetime.now().strftime("%H:%M:%S")

    pn.state.add_periodic_callback(update_clock, period=1000)

    return pn.Row(
        pn.Column(title, subtitle, margin=0),
        pn.layout.HSpacer(),
        clock,
        sizing_mode="stretch_width",
        margin=(5, 15),
    )
