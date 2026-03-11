#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:03:18 2026

@author: twi
"""

"""Gemeinsame Chart Utilities und Helfer-Funktionen."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    CrosshairTool,
    Range1d,
    LinearAxis,
    NumeralTickFormatter,
    DatetimeTickFormatter,
)
from frontend.styles.theme import CHART_COLORS


def create_empty_source() -> ColumnDataSource:
    """Erstellt eine leere ColumnDataSource mit dem korrekten Schema."""
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
    if df.empty:
        return create_empty_source()
    return ColumnDataSource(data={
        "time": df["time"].tolist(),
        "open": df["open"].tolist(),
        "high": df["high"].tolist(),
        "low": df["low"].tolist(),
        "close": df["close"].tolist(),
        "volume": df["volume"].tolist(),
    })


def get_crosshair_tool() -> CrosshairTool:
    """Erstellt ein Crosshair-Tool für Charts."""
    return CrosshairTool(
        line_color="#aaaaaa",
        line_alpha=0.5,
    )


def get_datetime_formatter() -> DatetimeTickFormatter:
    """Erstellt einen DateTime-Formatter für die X-Achse."""
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
    """Berechne Kerzbreite basierend auf Stream-Typ (in Millisekunden)."""
    if stream_type == "second":
        return 800     # 0.8 Sekunden in ms
    else:
        return 50000   # 50 Sekunden in ms


def add_volume_bars(fig, source_inc, source_dec, y_range_name: str = "volume"):
    """
    Fügt Volumen-Balken zu einem bestehenden Chart hinzu.
    Nutzt eine sekundäre Y-Achse.
    """
    fig.extra_y_ranges[y_range_name] = Range1d(start=0, end=1)

    fig.add_layout(
        LinearAxis(
            y_range_name=y_range_name,
            axis_label="Volume",
            formatter=NumeralTickFormatter(format="0.0a"),
        ),
        "right",
    )

    fig.vbar(
        x="time", top="volume", width=500,
        source=source_inc,
        fill_color=CHART_COLORS["bullish"],
        fill_alpha=0.3,
        line_alpha=0,
        y_range_name=y_range_name,
    )
    fig.vbar(
        x="time", top="volume", width=500,
        source=source_dec,
        fill_color=CHART_COLORS["bearish"],
        fill_alpha=0.3,
        line_alpha=0,
        y_range_name=y_range_name,
    )


def update_volume_range(fig, df: pd.DataFrame, y_range_name: str = "volume"):
    """Aktualisiert den Volume-Y-Range basierend auf neuen Daten."""
    if not df.empty and y_range_name in fig.extra_y_ranges:
        max_vol = df["volume"].max()
        fig.extra_y_ranges[y_range_name].end = max_vol * 3  # 3x für Platz
