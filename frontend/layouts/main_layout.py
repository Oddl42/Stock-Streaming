#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:35:56 2026

@author: twi
"""

"""
Haupt-Layout: Orchestriert alle Komponenten und Callbacks.

ARCHITEKTUR:
    - Backend (Producer, Spark) läuft extern via run_local.sh
    - GUI startet nur das periodische Lesen aus TimescaleDB
    - "Start" Button → startet Chart-Updates (DB-Reads)
    - "Stop" Button → stoppt Chart-Updates
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
from frontend.callbacks.chart_callbacks import ChartCallbackHandler
from frontend.callbacks.ticker_callbacks import create_ticker_callback_handler
from frontend.callbacks.table_callbacks import create_table_callback_handler

logger = logging.getLogger(__name__)


def create_main_layout() -> dict:
    """
    Erstellt das gesamte Layout der App und verbindet alle Callbacks.

    Der ChartCallbackHandler wird pro Session erstellt (kein Singleton),
    damit jede Session eigene Bokeh-Modelle bekommt.

    Backend-Prozesse (Producer, Spark) werden NICHT aus der GUI
    gestartet — sie laufen extern via run_local.sh.

    Returns:
        Dict mit "sidebar" und "main" Schlüsseln für das Template.
    """

    # =========================================
    # 0. Chart-Handler pro Session erstellen
    # =========================================
    chart_handler = ChartCallbackHandler()

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

    chart_area = create_chart_area(chart_handler)
    table_area = create_ticker_table_area(ticker_info_table)
    header = create_header()

    # =========================================
    # 3. Verbinde Callbacks
    # =========================================

    # --- Ticker Callbacks ---
    # create_ticker_callback_handler() registriert intern einen Watcher
    # auf dropdown.selected_ticker → ruft chart_handler.set_symbol() auf
    ticker_handler = create_ticker_callback_handler(
        selector=ticker_selector,
        dropdown=ticker_dropdown,
        table=ticker_info_table,
        chart_handler=chart_handler,
    )

    # --- Table Callbacks ---
    table_handler = create_table_callback_handler(ticker_info_table)

    # --- Stream Start/Stop Callbacks ---
    def on_stream_start(event):
        """
        Wird aufgerufen wenn der Start-Button geklickt wird.

        1. Informiert stream_callback_handler (Logging, Health-Check)
        2. Startet periodisches Chart-Update (liest aus TimescaleDB)
        3. Startet periodisches Tabellen-Update

        STARTET KEINE Backend-Prozesse (Producer/Spark)!
        Die laufen extern via run_local.sh.
        """
        tickers = ticker_selector.selected_symbols
        if not tickers:
            pn.state.notifications.error(
                "❌ Keine Ticker ausgewählt! "
                "Bitte wähle zuerst Ticker aus.",
                duration=5000,
            )
            return

        stream_type = stream_controls.stream_type

        # Logging + Health-Check (keine Backend-Prozesse!)
        stream_callback_handler.on_stream_start(stream_type, tickers)

        # Chart-Updates starten (liest aus DB)
        chart_handler.set_stream_type(stream_type)
        chart_handler.start_periodic_update()

        # Tabellen-Updates starten
        table_handler.start_periodic_update()

        pn.state.notifications.success(
            f"✅ Chart-Updates gestartet für {len(tickers)} Ticker. "
            f"Daten kommen vom externen Producer (run_local.sh).",
            duration=5000,
        )

        logger.info(
            f"GUI updates started: {stream_type} "
            f"with {len(tickers)} tickers"
        )

    def on_stream_stop(event):
        """
        Wird aufgerufen wenn der Stop-Button geklickt wird.

        Stoppt nur die GUI-Updates, NICHT die Backend-Prozesse.
        """
        stream_type = stream_controls.stream_type

        # Chart-Updates stoppen
        chart_handler.stop_periodic_update()

        # Tabellen-Updates stoppen
        table_handler.stop_periodic_update()

        # Logging
        stream_callback_handler.on_stream_stop(stream_type)

        pn.state.notifications.info(
            "⏹️ Chart-Updates gestoppt. "
            "Backend-Prozesse laufen weiter.",
            duration=5000,
        )

        logger.info("GUI updates stopped.")

    stream_controls.param.watch(on_stream_start, "stream_started")
    stream_controls.param.watch(on_stream_stop, "stream_stopped")

    # --- Chart Type Callback ---
    def on_chart_type_change(event):
        """
        Wechselt den Chart-Typ (Candlestick ↔ Line).
        set_chart_type() tauscht intern _chart_pane.object aus.
        Da chart_area dieselbe Pane-Referenz nutzt, wird der
        Wechsel automatisch sichtbar.
        """
        chart_handler.set_chart_type(event.new)

    chart_type_selector.param.watch(on_chart_type_change, "chart_type")

    # --- Stream Type Callback ---
    def on_stream_type_change(event):
        """Wechselt den Stream-Typ (Sekunden ↔ Minuten)."""
        chart_handler.set_stream_type(event.new)

    stream_controls.param.watch(on_stream_type_change, "stream_type")

    # =========================================
    # 4. Initiale Werte setzen
    # =========================================
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
        chart_handler.stop_periodic_update()
        table_handler.stop_periodic_update()
        stream_callback_handler.on_shutdown()
        logger.info("Session destroyed - cleanup complete.")

    pn.state.on_session_destroyed(on_session_destroyed)

    return {
        "sidebar": sidebar,
        "main": main_content,
    }
