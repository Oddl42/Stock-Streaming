#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:35:56 2026

@author: twi
"""

"""
Haupt-Layout: Orchestriert alle Komponenten und Callbacks.
Dies ist das zentrale Modul, das alles zusammenfügt.
"""

import panel as pn
import logging

from frontend.components.ticker_selector import TickerSelector
from frontend.components.stream_controls import StreamControls
from frontend.components.chart_type_selector import ChartTypeSelector
from frontend.components.ticker_dropdown import TickerDropdown
from frontend.components.ticker_info_table import TickerInfoTable

from frontend.layouts.header import create_header
from frontend.layouts.sidebar import create_sidebar
from frontend.layouts.chart_area import create_chart_area
from frontend.layouts.ticker_table import create_ticker_table_area

from frontend.callbacks.stream_callbacks import stream_callback_handler
from frontend.callbacks.chart_callbacks import chart_callback_handler
from frontend.callbacks.ticker_callbacks import create_ticker_callback_handler
from frontend.callbacks.table_callbacks import create_table_callback_handler

logger = logging.getLogger(__name__)


def create_main_layout() -> dict:
    """
    Erstellt das gesamte Layout der App und verbindet alle Callbacks.

    Returns:
        Dict mit "sidebar" und "main" Schlüsseln für das Template.
    """

    # =========================================
    # 1. Erstelle alle Komponenten
    # =========================================
    ticker_selector = TickerSelector()
    stream_controls = StreamControls()
    chart_type_selector = ChartTypeSelector()
    ticker_dropdown = TickerDropdown()
    ticker_info_table = TickerInfoTable()

    # =========================================
    # 2. Erstelle Layout-Bereiche
    # =========================================
    sidebar = create_sidebar(
        ticker_selector=ticker_selector,
        stream_controls=stream_controls,
        chart_type_selector=chart_type_selector,
        ticker_dropdown=ticker_dropdown,
    )

    chart_area = create_chart_area()
    table_area = create_ticker_table_area(ticker_info_table)
    header = create_header()

    # =========================================
    # 3. Verbinde Callbacks
    # =========================================

    # --- Ticker Callbacks ---
    ticker_handler = create_ticker_callback_handler(
        selector=ticker_selector,
        dropdown=ticker_dropdown,
        table=ticker_info_table,
    )

    # --- Table Callbacks ---
    table_handler = create_table_callback_handler(ticker_info_table)

    # --- Stream Start/Stop Callbacks ---
    def on_stream_start(event):
        """Wird aufgerufen wenn der Start-Button geklickt wird."""
        tickers = ticker_selector.selected_symbols
        if not tickers:
            pn.state.notifications.error(
                "❌ Keine Ticker ausgewählt! Bitte wähle zuerst Ticker aus.",
                duration=5000,
            )
            return

        stream_type = stream_controls.stream_type

        # Backend Stream starten
        try:
            stream_callback_handler.on_stream_start(stream_type, tickers)
        except Exception as e:
            pn.state.notifications.error(
                f"❌ Stream-Start fehlgeschlagen: {e}",
                duration=5000,
            )
            return

        # Chart Update starten
        chart_callback_handler.set_stream_type(stream_type)
        chart_callback_handler.start_periodic_update()

        # Tabellen Update starten
        table_handler.start_periodic_update()

        logger.info(
            f"Streaming started: {stream_type} with {len(tickers)} tickers"
        )

    def on_stream_stop(event):
        """Wird aufgerufen wenn der Stop-Button geklickt wird."""
        stream_type = stream_controls.stream_type

        # Chart Update stoppen
        chart_callback_handler.stop_periodic_update()

        # Tabellen Update stoppen
        table_handler.stop_periodic_update()

        # Backend Stream stoppen
        try:
            stream_callback_handler.on_stream_stop(stream_type)
        except Exception as e:
            logger.error(f"Stream stop error: {e}")

        logger.info("Streaming stopped.")

    stream_controls.param.watch(on_stream_start, "stream_started")
    stream_controls.param.watch(on_stream_stop, "stream_stopped")

    # --- Chart Type Callback ---
    def on_chart_type_change(event):
        """Wechselt den Chart-Typ."""
        chart_callback_handler.set_chart_type(event.new)
        chart_area._switch_chart(event.new)

    chart_type_selector.param.watch(on_chart_type_change, "chart_type")

    # --- Stream Type Callback ---
    def on_stream_type_change(event):
        """Wechselt den Stream-Typ."""
        chart_callback_handler.set_stream_type(event.new)

    stream_controls.param.watch(on_stream_type_change, "stream_type")

    # =========================================
    # 4. Initiale Werte setzen
    # =========================================
    # Lade Top 10 als Default
    ticker_selector.selection_mode = "Top 10"

    # =========================================
    # 5. Baue Main-Content zusammen
    # =========================================
    main_content = pn.Column(
        header,
        pn.layout.Divider(),
        chart_area,
        pn.Spacer(height=10),
        table_area,
        sizing_mode="stretch_width",
    )

    # =========================================
    # 6. Cleanup bei Shutdown
    # =========================================
    def on_session_destroyed(session_context):
        chart_callback_handler.stop_periodic_update()
        table_handler.stop_periodic_update()
        stream_callback_handler.on_shutdown()
        logger.info("Session destroyed - cleanup complete.")

    pn.state.on_session_destroyed(on_session_destroyed)

    return {
        "sidebar": sidebar,
        "main": main_content,
    }
