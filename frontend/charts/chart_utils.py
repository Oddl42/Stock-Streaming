#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:03:18 2026

@author: twi
"""

"""Gemeinsame Chart Utilities und Helfer-Funktionen – Robuste Version (Bokeh 3.8.x)."""

import pandas as pd
import numpy as np
from bokeh.models import (
    ColumnDataSource,
    CrosshairTool,
    DatetimeTickFormatter,
)
from frontend.styles.theme import CHART_COLORS


def create_empty_source() -> ColumnDataSource:
    """
    Erstellt eine leere ColumnDataSource mit dem korrekten Schema.
    """
    return ColumnDataSource(data={
        "time": [],
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
    })


def create_source_from_df(df: pd.DataFrame) -> ColumnDataSource:
    """Erstellt eine ColumnDataSource aus einem DataFrame."""
    if df is None or df.empty:
        return create_empty_source()
    return ColumnDataSource(data={
        "time": df["time"].tolist(),
        "open": df["open"].tolist(),
        "high": df["high"].tolist(),
        "low": df["low"].tolist(),
        "close": df["close"].tolist(),
        "volume": df["volume"].tolist() if "volume" in df.columns else [0] * len(df),
    })


def get_crosshair_tool() -> CrosshairTool:
    """Erstellt ein Crosshair-Tool für Charts."""
    return CrosshairTool(
        line_color="#aaaaaa",
        line_alpha=0.5,
    )


def get_datetime_formatter() -> DatetimeTickFormatter:
    """
    Erstellt einen DateTime-Formatter für die X-Achse.
    """
    return DatetimeTickFormatter(
        seconds="%H:%M:%S",
        minsec="%H:%M:%S",
        minutes="%H:%M",
        hourmin="%H:%M",
        hours="%H:%M",
        days="%d %b",
        months="%b %Y",
    )


def calculate_candle_width(stream_type: str = "second") -> float:
    """
    Berechne Kerzenbreite basierend auf Stream-Typ.
    """
    if stream_type == "second":
        return 800       # 0.8 Sekunden in ms
    else:
        return 50_000    # 50 Sekunden in ms
