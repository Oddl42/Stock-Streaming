#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:04:38 2026

@author: twi
"""

"""Bokeh Line Chart für Aktienkurse."""

from bokeh.plotting import figure
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    Band,
    Span,
)
import pandas as pd
import numpy as np

from frontend.charts.chart_utils import (
    create_empty_source,
    get_crosshair_tool,
    get_datetime_formatter,
)
from frontend.styles.theme import CHART_COLORS


class LineChart:
    """Interaktiver Bokeh Line Chart mit Volumen."""

    def __init__(self, stream_type: str = "second"):
        self.stream_type = stream_type
        self.source = ColumnDataSource(data={
            "time": [],
            "close": [],
            "volume": [],
            "open": [],
            "high": [],
            "low": [],
            "upper": [],
            "lower": [],
        })
        self.figure = self._create_figure()

    def _create_figure(self):
        """Erstellt die Bokeh Line Figure."""
        p = figure(
            title="Wähle einen Ticker aus dem Dropdown",
            x_axis_type="datetime",
            width=950,
            height=500,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            toolbar_location="above",
            sizing_mode="stretch_width",
            background_fill_color=CHART_COLORS["background"],
            border_fill_color=CHART_COLORS["background"],
        )

        # Styling
        p.title.text_color = CHART_COLORS["text"]
        p.title.text_font_size = "14px"
        p.xaxis.formatter = get_datetime_formatter()
        p.xaxis.axis_label = "Zeit"
        p.yaxis.axis_label = "Preis ($)"
        p.xaxis.axis_label_text_color = CHART_COLORS["text"]
        p.yaxis.axis_label_text_color = CHART_COLORS["text"]
        p.xaxis.major_label_text_color = CHART_COLORS["text"]
        p.yaxis.major_label_text_color = CHART_COLORS["text"]
        p.grid.grid_line_color = CHART_COLORS["grid"]
        p.grid.grid_line_alpha = 0.3
        p.outline_line_color = None

        # Band (High-Low Bereich)
        band = Band(
            base="time",
            lower="lower",
            upper="upper",
            source=self.source,
            level="underlay",
            fill_alpha=0.15,
            fill_color=CHART_COLORS["line"],
            line_width=0,
        )
        p.add_layout(band)

        # Hauptlinie (Close)
        p.line(
            x="time", y="close",
            source=self.source,
            line_width=2,
            line_color=CHART_COLORS["line"],
            legend_label="Close",
        )

        # Punkte auf der Linie
        p.scatter(
            x="time", y="close",
            source=self.source,
            size=3,
            color=CHART_COLORS["line"],
            alpha=0.6,
        )

        # Hover
        hover = HoverTool(
            tooltips=[
                ("Zeit", "@time{%F %T}"),
                ("Close", "@close{$0.2f}"),
                ("Open", "@open{$0.2f}"),
                ("High", "@high{$0.2f}"),
                ("Low", "@low{$0.2f}"),
                ("Volume", "@volume{0,0}"),
            ],
            formatters={"@time": "datetime"},
            mode="vline",
        )
        p.add_tools(hover)
        p.add_tools(get_crosshair_tool())

        # Legende
        p.legend.location = "top_left"
        p.legend.background_fill_alpha = 0.7
        p.legend.background_fill_color = CHART_COLORS["background"]
        p.legend.label_text_color = CHART_COLORS["text"]
        p.legend.click_policy = "hide"

        return p

    def update(self, df: pd.DataFrame, symbol: str = ""):
        """Aktualisiert den Line Chart mit neuen Daten."""
        if df.empty:
            self.source.data = {
                "time": [], "close": [], "volume": [],
                "open": [], "high": [], "low": [],
                "upper": [], "lower": [],
            }
            self.figure.title.text = f"{symbol} – Keine Daten verfügbar"
            return

        self.source.data = {
            "time": df["time"].tolist(),
            "close": df["close"].tolist(),
            "volume": df["volume"].tolist(),
            "open": df["open"].tolist(),
            "high": df["high"].tolist(),
            "low": df["low"].tolist(),
            "upper": df["high"].tolist(),     # Band obere Grenze
            "lower": df["low"].tolist(),      # Band untere Grenze
        }

        # Titel aktualisieren
        last_price = df["close"].iloc[-1]
        change = df["close"].iloc[-1] - df["open"].iloc[0]
        change_pct = (change / df["open"].iloc[0]) * 100
        arrow = "▲" if change >= 0 else "▼"

        self.figure.title.text = (
            f"{symbol}  |  ${last_price:.2f}  "
            f"{arrow} {change:+.2f} ({change_pct:+.2f}%)"
        )

        # Durchschnittslinie hinzufügen
        if len(df) > 1:
            avg_price = df["close"].mean()
            # Entferne alte Spans
            self.figure.renderers = [
                r for r in self.figure.renderers
                if not isinstance(r, Span)
            ]
            avg_line = Span(
                location=avg_price,
                dimension="width",
                line_color=CHART_COLORS["warning"],
                line_dash="dashed",
                line_width=1,
                line_alpha=0.5,
            )
            self.figure.add_layout(avg_line)
