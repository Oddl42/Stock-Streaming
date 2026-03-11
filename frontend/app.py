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
"""

import panel as pn
import param
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.info(f"Panel version: {pn.__version__}")
logger.info(f"Param version: {param.__version__}")

# Panel Extensions
try:
    pn.extension(
        "bokeh", "tabulator",
        notifications=True,
        sizing_mode="stretch_width",
        loading_spinner="dots",
        loading_color="#1a73e8",
    )
except Exception:
    pn.extension(
        "tabulator",
        notifications=True,
        sizing_mode="stretch_width",
        loading_spinner="dots",
        loading_color="#1a73e8",
    )

from frontend.styles.theme import CUSTOM_CSS, TEMPLATE_CONFIG

pn.config.raw_css.append(CUSTOM_CSS)

from frontend.layouts.main_layout import create_main_layout


def _get_template_params():
    """
    Ermittelt die gueltigen Parameter fuer MaterialTemplate.
    Kompatibel mit param 1.x und param >= 2.0.
    """
    try:
        # ✅ param >= 2.0
        valid_params = set(pn.template.MaterialTemplate.param.objects().keys())
    except AttributeError:
        try:
            # param 1.x
            valid_params = set(pn.template.MaterialTemplate.param.params().keys())
        except AttributeError:
            # Fallback: Direkt aus param dict lesen
            valid_params = set(dict(pn.template.MaterialTemplate.param).keys())

    logger.info(f"MaterialTemplate valid params: {sorted(valid_params)}")
    return valid_params


def create_app():
    """Erstellt die komplette Panel-Applikation."""

    valid_params = _get_template_params()

    template_params = {
        "title": TEMPLATE_CONFIG.get("title", "Stock Streaming Platform"),
    }

    if "sidebar_width" in valid_params:
        template_params["sidebar_width"] = TEMPLATE_CONFIG.get("sidebar_width", 330)

    if "header_background" in valid_params:
        template_params["header_background"] = TEMPLATE_CONFIG.get(
            "header_background", "#1a1a2e"
        )

    # Accent Color
    accent_color = TEMPLATE_CONFIG.get("accent", "#1a73e8")
    if "accent_base_color" in valid_params:
        template_params["accent_base_color"] = accent_color
    elif "accent" in valid_params:
        template_params["accent"] = accent_color
    else:
        logger.warning("Accent-Farbe wird uebersprungen (nicht unterstuetzt).")

    # Theme
    if "theme" in valid_params:
        if hasattr(pn.template, "DarkTheme"):
            template_params["theme"] = pn.template.DarkTheme
        else:
            template_params["theme"] = "dark"

    # busy_indicator bewusst NICHT setzen - verursacht ParamFutureWarning

    logger.info(f"Using template params: {list(template_params.keys())}")

    template = pn.template.MaterialTemplate(**template_params)

    layout = create_main_layout()

    template.sidebar.append(layout["sidebar"])
    template.main.append(layout["main"])

    logger.info("Stock Streaming Platform GUI initialized!")
    return template


app = create_app()
app.servable()
