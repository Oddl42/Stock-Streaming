#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:03:59 2026

@author: twi
"""

"""Bokeh Candlestick Chart für Aktienkurse."""

from bokeh.plotting import figure
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    Range1d,
    Title,
)
import pandas as pd

from frontend.charts.chart_utils import (
    create_empty_source,
    create_source_from_df,
    get_crosshair_tool,
    get_datetime_formatter,
    calculate_candle_width,
    add_volume_bars,
    update_volume_range,
)
from frontend.styles.theme import CHART_COLORS


class CandlestickChart:
    """Interaktiver Bokeh Candlestick Chart."""

    def __init__(self, stream_type: str = "second"):
        self.stream_type = stream_type
        self.source_inc = create_empty_source()
        self.source_dec = create_empty_source()
        self.figure = self._create_figure()

    def _create_figure(self):
        """Erstellt die Bokeh Figure mit Candlestick-Elementen."""
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

        # Candle width
        w = calculate_candle_width(self.stream_type)

        # Wicks (Dochte) - Bullish
        p.segment(
            x0="time", y0="high", x1="time", y1="low",
            source=self.source_inc,
            color=CHART_COLORS["bullish"], line_width=1,
        )
        # Wicks (Dochte) - Bearish
        p.segment(
            x0="time", y0="high", x1="time", y1="low",
            source=self.source_dec,
            color=CHART_COLORS["bearish"], line_width=1,
        )

        # Bodies - Bullish (close >= open)
        p.vbar(
            x="time", width=w, top="close", bottom="open",
            source=self.source_inc,
            fill_color=CHART_COLORS["bullish"],
            line_color=CHART_COLORS["bullish"],
        )
        # Bodies - Bearish (open > close)
        p.vbar(
            x="time", width=w, top="open", bottom="close",
            source=self.source_dec,
            fill_color=CHART_COLORS["bearish"],
            line_color=CHART_COLORS["bearish"],
        )

        # Volumen-Balken (sekundäre Y-Achse)
        add_volume_bars(p, self.source_inc, self.source_dec)

        # Hover-Tool
        hover = HoverTool(
            tooltips=[
                ("Zeit", "@time{%F %T}"),
                ("Open", "@open{$0.2f}"),
                ("High", "@high{$0.2f}"),
                ("Low", "@low{$0.2f}"),
                ("Close", "@close{$0.2f}"),
                ("Volume", "@volume{0,0}"),
            ],
            formatters={"@time": "datetime"},
            mode="vline",
        )
        p.add_tools(hover)
        p.add_tools(get_crosshair_tool())

        return p

    def update(self, df: pd.DataFrame, symbol: str = ""):
        """Aktualisiert den Chart mit neuen Daten."""
        if df.empty:
            self.source_inc.data = create_empty_source().data
            self.source_dec.data = create_empty_source().data
            self.figure.title.text = f"{symbol} – Keine Daten verfügbar"
            return

        # Bullish / Bearish aufteilen
        inc_mask = df["close"] >= df["open"]
        dec_mask = df["open"] > df["close"]

        inc_data = df[inc_mask]
        dec_data = df[dec_mask]

        self.source_inc.data = {
            "time": inc_data["time"].tolist(),
            "open": inc_data["open"].tolist(),
            "high": inc_data["high"].tolist(),
            "low": inc_data["low"].tolist(),
            "close": inc_data["close"].tolist(),
            "volume": inc_data["volume"].tolist(),
        }

        self.source_dec.data = {
            "time": dec_data["time"].tolist(),
            "open": dec_data["open"].tolist(),
            "high": dec_data["high"].tolist(),
            "low": dec_data["low"].tolist(),
            "close": dec_data["close"].tolist(),
            "volume": dec_data["volume"].tolist(),
        }

        # Titel aktualisieren
        last_price = df["close"].iloc[-1]
        change = df["close"].iloc[-1] - df["open"].iloc[0]
        change_pct = (change / df["open"].iloc[0]) * 100
        arrow = "▲" if change >= 0 else "▼"
        color = CHART_COLORS["bullish"] if change >= 0 else CHART_COLORS["bearish"]

        self.figure.title.text = (
            f"{symbol}  |  ${last_price:.2f}  "
            f"{arrow} {change:+.2f} ({change_pct:+.2f}%)"
        )

        # Volume Range aktualisieren
        update_volume_range(self.figure, df)
