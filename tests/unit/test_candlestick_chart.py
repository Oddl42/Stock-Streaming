#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:47:47 2026

@author: twi
"""

"""
Unit Tests für den Candlestick Chart.
"""

import pytest
import pandas as pd
from bokeh.plotting import figure as bokeh_figure
from bokeh.models import ColumnDataSource, DatetimeAxis  # ← NEU

from frontend.charts.candlestick_chart import CandlestickChart
from tests.fixtures.sample_data import generate_ohlcv_dataframe


class TestCandlestickChart:

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup für jeden Test."""
        self.chart = CandlestickChart(stream_type="second")

    def test_initialization(self):
        """Testet Chart-Initialisierung."""
        assert self.chart.figure is not None
        assert self.chart.source_inc is not None
        assert self.chart.source_dec is not None
        assert self.chart.stream_type == "second"

    def test_figure_properties(self):
        """Testet Figure-Eigenschaften."""
        fig = self.chart.figure

        # ✅ Bokeh 3.x: x_axis_type ist kein lesbares Attribut mehr.
        # Stattdessen prüfen wir, ob eine DatetimeAxis vorhanden ist.
        assert any(isinstance(axis, DatetimeAxis) for axis in fig.xaxis), \
            "X-Achse sollte eine DatetimeAxis sein"

        assert fig.title is not None
        assert len(fig.tools) > 0

    def test_update_with_data(self, sample_ohlcv_df):
        """Testet Update mit gültigen Daten."""
        self.chart.update(sample_ohlcv_df, symbol="AAPL")

        # Prüfe dass Daten in Sources geladen wurden
        inc_count = len(self.chart.source_inc.data["time"])
        dec_count = len(self.chart.source_dec.data["time"])
        total = inc_count + dec_count

        assert total == len(sample_ohlcv_df)
        assert "AAPL" in self.chart.figure.title.text

    def test_update_with_empty_data(self, sample_empty_df):
        """Testet Update mit leeren Daten."""
        self.chart.update(sample_empty_df, symbol="AAPL")

        assert len(self.chart.source_inc.data["time"]) == 0
        assert len(self.chart.source_dec.data["time"]) == 0
        assert "Keine Daten" in self.chart.figure.title.text

    def test_bullish_bearish_split(self):
        """Testet die Aufteilung in bullish/bearish Kerzen."""
        df = pd.DataFrame({
            "time": pd.date_range("2024-01-01", periods=4, freq="s"),
            "symbol": ["AAPL"] * 4,
            "open": [100, 102, 104, 106],
            "high": [103, 105, 107, 109],
            "low": [99, 101, 103, 105],
            "close": [102, 101, 106, 105],  # Bullish, Bearish, Bullish, Bearish
            "volume": [1000] * 4,
        })

        self.chart.update(df, "AAPL")

        assert len(self.chart.source_inc.data["time"]) == 2  # Bullish
        assert len(self.chart.source_dec.data["time"]) == 2  # Bearish

    def test_title_shows_price_info(self):
        """Testet dass der Titel Preisinformationen enthält."""
        df = generate_ohlcv_dataframe("AAPL", points=10)
        self.chart.update(df, "AAPL")

        title = self.chart.figure.title.text
        assert "AAPL" in title
        assert "$" in title

    def test_minute_stream_type(self):
        """Testet Chart mit Minuten-Stream-Typ."""
        chart = CandlestickChart(stream_type="minute")
        assert chart.stream_type == "minute"


class TestLineChart:
    """Tests für den Line Chart."""

    @pytest.fixture(autouse=True)
    def setup(self):
        from frontend.charts.line_chart import LineChart
        self.chart = LineChart(stream_type="second")

    def test_initialization(self):
        assert self.chart.figure is not None
        assert self.chart.source is not None

    def test_update_with_data(self, sample_ohlcv_df):
        self.chart.update(sample_ohlcv_df, symbol="AAPL")

        assert len(self.chart.source.data["time"]) == len(sample_ohlcv_df)
        assert "AAPL" in self.chart.figure.title.text

    def test_update_with_empty_data(self, sample_empty_df):
        self.chart.update(sample_empty_df, symbol="AAPL")

        assert len(self.chart.source.data["time"]) == 0
