#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:36:33 2026

@author: twi
"""

"""
Stock Streaming Platform - Panel App Entry Point.

Starte mit:
    panel serve frontend/app.py --show --autoreload --port 5006
    
Oder:
    python -m panel serve frontend/app.py --show --autoreload
"""

import panel as pn
import logging

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Panel Extensions
pn.extension(
    "bokeh",
    "tabulator",
    notifications=True,
    sizing_mode="stretch_width",
    loading_spinner="dots",
    loading_color="#1a73e8",
)

# Custom CSS einbinden
from frontend.styles.theme import CUSTOM_CSS, TEMPLATE_CONFIG

pn.config.raw_css.append(CUSTOM_CSS)

# Layout erstellen
from frontend.layouts.main_layout import create_main_layout


def create_app():
    """Erstellt die komplette Panel-Applikation."""

    # Material Template
    template = pn.template.MaterialTemplate(
        title=TEMPLATE_CONFIG["title"],
        sidebar_width=TEMPLATE_CONFIG["sidebar_width"],
        header_background=TEMPLATE_CONFIG["header_background"],
        accent_base_color=TEMPLATE_CONFIG["accent_base_color"],
        theme=pn.template.DarkTheme,
        busy_indicator=pn.indicators.BooleanStatus(
            value=True, color="primary", width=20, height=20
        ),
    )

    # Layout bauen und verbinden
    layout = create_main_layout()

    # Sidebar
    template.sidebar.append(layout["sidebar"])

    # Main Content
    template.main.append(layout["main"])

    logger.info("🚀 Stock Streaming Platform GUI initialized!")
    return template


# App erstellen und servable machen
app = create_app()
app.servable()
