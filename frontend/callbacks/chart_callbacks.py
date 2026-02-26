#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:16:05 2026

@author: twi
"""

"""Callbacks für periodische Chart-Updates (alle 2 Sekunden)."""

import panel as pn
import logging
from typing import Optional

from frontend.charts.candlestick_chart import CandlestickChart
from frontend.charts.line_chart import LineChart
from backend.data_service.data_provider import data_provider
from config.settings import settings

logger = logging.getLogger(__name__)


class ChartCallbackHandler:
    """
    Verwaltet das periodische Update des Charts.
    - Holt Daten aus TimescaleDB
    - Aktualisiert den aktiven Chart (Candlestick oder Line)
    - Update-Intervall: 2 Sekunden
    """

    def __init__(self):
        self.candlestick_chart = CandlestickChart(stream_type="second")
        self.line_chart = LineChart(stream_type="second")

        self._current_symbol: str = ""
        self._current_chart_type: str = "Candlestick"
        self._current_stream_type: str = "second"
        self._is_streaming: bool = False
        self._periodic_callback: Optional[pn.state.PeriodicCallback] = None
        self._use_demo_data: bool = True  # True für Entwicklung ohne DB

    @property
    def active_chart(self):
        """Gibt den aktuell aktiven Chart zurück."""
        if self._current_chart_type == "Candlestick":
            return self.candlestick_chart
        return self.line_chart

    @property
    def active_figure(self):
        """Gibt die aktive Bokeh Figure zurück."""
        return self.active_chart.figure

    def set_symbol(self, symbol: str):
        """Setzt den aktuell zu plottenden Ticker."""
        self._current_symbol = symbol
        logger.info(f"Chart symbol changed to: {symbol}")
        # Sofort updaten
        self._fetch_and_update()

    def set_chart_type(self, chart_type: str):
        """Setzt den Chart-Typ (Candlestick/Linie)."""
        self._current_chart_type = chart_type
        logger.info(f"Chart type changed to: {chart_type}")
        # Sofort updaten
        self._fetch_and_update()

    def set_stream_type(self, stream_type_str: str):
        """Setzt den Stream-Typ (Sekunden/Minuten)."""
        self._current_stream_type = (
            "second" if stream_type_str == "Sekunden" else "minute"
        )
        # Charts Stream-Typ aktualisieren
        self.candlestick_chart.stream_type = self._current_stream_type
        self.line_chart.stream_type = self._current_stream_type
        logger.info(f"Stream type changed to: {self._current_stream_type}")

    def start_periodic_update(self):
        """Startet das periodische Chart-Update."""
        if self._periodic_callback is not None:
            return  # Bereits gestartet

        self._is_streaming = True
        self._periodic_callback = pn.state.add_periodic_callback(
            self._fetch_and_update,
            period=settings.CHART_UPDATE_INTERVAL_MS,
        )
        logger.info(
            f"Periodic chart update started "
            f"(every {settings.CHART_UPDATE_INTERVAL_MS}ms)"
        )

    def stop_periodic_update(self):
        """Stoppt das periodische Chart-Update."""
        self._is_streaming = False
        if self._periodic_callback is not None:
            self._periodic_callback.stop()
            self._periodic_callback = None
            logger.info("Periodic chart update stopped.")

    def _fetch_and_update(self):
        """Holt neue Daten und aktualisiert den Chart."""
        if not self._current_symbol:
            return

        try:
            if self._use_demo_data:
                # Demo-Modus: Generiere Fake-Daten
                df = data_provider.generate_demo_data(
                    symbol=self._current_symbol,
                    points=settings.CHART_MAX_POINTS,
                )
            else:
                # Produktiv: Hole Daten aus TimescaleDB
                df = data_provider.get_latest_data(
                    symbol=self._current_symbol,
                    stream_type=self._current_stream_type,
                    limit=settings.CHART_MAX_POINTS,
                )

            # Aktualisiere den aktiven Chart
            self.active_chart.update(df, self._current_symbol)

        except Exception as e:
            logger.error(f"Chart update error: {e}")

    def get_chart_pane(self) -> pn.pane.Bokeh:
        """
        Gibt einen reaktiven Bokeh-Pane zurück, der den aktuellen Chart zeigt.
        """
        return pn.pane.Bokeh(
            self.active_figure,
            sizing_mode="stretch_width",
        )


# Singleton
chart_callback_handler = ChartCallbackHandler()
