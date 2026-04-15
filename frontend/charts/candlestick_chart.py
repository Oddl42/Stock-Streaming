#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:03:59 2026

@author: twi
"""
"""
Bokeh Candlestick Chart für Aktienkurse – Robuste Version (Bokeh 3.8.x).
"""

from bokeh.plotting import figure
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    CrosshairTool,
    DatetimeTickFormatter,
)
import pandas as pd
import numpy as np

from frontend.styles.theme import CHART_COLORS

EMPTY_DATA = {
    "time": [],
    "open": [],
    "high": [],
    "low": [],
    "close": [],
    "volume": [],
}


class CandlestickChart:
    """
    Interaktiver Bokeh Candlestick Chart.
    """

    def __init__(self, stream_type: str = "second"):
        self.stream_type = stream_type
        self.source_inc = ColumnDataSource(data=dict(EMPTY_DATA))
        self.source_dec = ColumnDataSource(data=dict(EMPTY_DATA))
        self.figure = self._create_figure()

    def _get_candle_width(self) -> float:
        """Kerzenbreite in Millisekunden."""
        if self.stream_type == "second":
            return 800
        else:
            return 50_000

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
            background_fill_color=CHART_COLORS.get("background", "#1e1e2e"),
            border_fill_color=CHART_COLORS.get("background", "#1e1e2e"),
        )

        # Styling
        p.title.text_color = CHART_COLORS.get("text", "#e0e0e0")
        p.title.text_font_size = "14px"
        p.xaxis.axis_label = "Zeit"
        p.yaxis.axis_label = "Preis ($)"
        p.xaxis.axis_label_text_color = CHART_COLORS.get("text", "#e0e0e0")
        p.yaxis.axis_label_text_color = CHART_COLORS.get("text", "#e0e0e0")
        p.xaxis.major_label_text_color = CHART_COLORS.get("text", "#e0e0e0")
        p.yaxis.major_label_text_color = CHART_COLORS.get("text", "#e0e0e0")
        p.grid.grid_line_color = CHART_COLORS.get("grid", "#3a3a4e")
        p.grid.grid_line_alpha = 0.3
        p.outline_line_color = None

        # Datetime Formatter
        p.xaxis.formatter = DatetimeTickFormatter(
            seconds="%H:%M:%S",
            minsec="%H:%M:%S",
            minutes="%H:%M",
            hourmin="%H:%M",
            hours="%H:%M",
            days="%d %b",
            months="%b %Y",
        )

        w = self._get_candle_width()

        # Wicks (Dochte) – Bullish
        p.segment(
            x0="time", y0="high",
            x1="time", y1="low",
            source=self.source_inc,
            color=CHART_COLORS.get("bullish", "#26a69a"),
            line_width=1,
        )
        # Wicks (Dochte) – Bearish
        p.segment(
            x0="time", y0="high",
            x1="time", y1="low",
            source=self.source_dec,
            color=CHART_COLORS.get("bearish", "#ef5350"),
            line_width=1,
        )

        # Bodies – Bullish (close >= open)
        p.vbar(
            x="time", width=w,
            top="close", bottom="open",
            source=self.source_inc,
            fill_color=CHART_COLORS.get("bullish", "#26a69a"),
            line_color=CHART_COLORS.get("bullish", "#26a69a"),
            line_width=1,
        )
        # Bodies – Bearish (open > close)
        p.vbar(
            x="time", width=w,
            top="open", bottom="close",
            source=self.source_dec,
            fill_color=CHART_COLORS.get("bearish", "#ef5350"),
            line_color=CHART_COLORS.get("bearish", "#ef5350"),
            line_width=1,
        )

        # Hover Tool
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

        # Crosshair
        p.add_tools(CrosshairTool(
            line_color="#aaaaaa",
            line_alpha=0.5,
        ))

        return p

    def update(self, df: pd.DataFrame, symbol: str = ""):
        """
         Aktualisiert den Chart mit neuen Daten.
         """
        if df is None or df.empty:
            self.source_inc.data = dict(EMPTY_DATA)
            self.source_dec.data = dict(EMPTY_DATA)
            self.figure.title.text = f"{symbol} – Keine Daten verfügbar"
            return

        df = df.copy()

        if not pd.api.types.is_datetime64_any_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"])

        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                df[col] = 0.0
        if "volume" not in df.columns:
            df["volume"] = 0

        df = df.replace([np.inf, -np.inf], np.nan).dropna(
            subset=["open", "high", "low", "close"]
        )

        if df.empty:
            self.source_inc.data = dict(EMPTY_DATA)
            self.source_dec.data = dict(EMPTY_DATA)
            self.figure.title.text = f"{symbol} – Keine gültigen Daten"
            return

        inc = df[df["close"] >= df["open"]]
        dec = df[df["open"] > df["close"]]

        self.source_inc.data = {
            "time": inc["time"].tolist(),
            "open": inc["open"].tolist(),
            "high": inc["high"].tolist(),
            "low": inc["low"].tolist(),
            "close": inc["close"].tolist(),
            "volume": inc["volume"].tolist(),
        }

        self.source_dec.data = {
            "time": dec["time"].tolist(),
            "open": dec["open"].tolist(),
            "high": dec["high"].tolist(),
            "low": dec["low"].tolist(),
            "close": dec["close"].tolist(),
            "volume": dec["volume"].tolist(),
        }

        try:
            last_price = float(df["close"].iloc[-1])
            first_open = float(df["open"].iloc[0])

            if first_open != 0:
                change = last_price - first_open
                change_pct = (change / first_open) * 100
                arrow = "▲" if change >= 0 else "▼"
                self.figure.title.text = (
                    f"{symbol}  |  ${last_price:.2f}  "
                    f"{arrow} {change:+.2f} ({change_pct:+.2f}%)"
                )
            else:
                self.figure.title.text = f"{symbol}  |  ${last_price:.2f}"
        except (IndexError, ZeroDivisionError, ValueError):
            self.figure.title.text = f"{symbol}"
