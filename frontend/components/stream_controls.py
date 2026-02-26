#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:06:05 2026

@author: twi
"""

"""Stream-Steuerung: Start/Stop Buttons + Sekunden/Minuten Toggle."""

import panel as pn
import param
import asyncio
import logging

logger = logging.getLogger(__name__)


class StreamControls(param.Parameterized):
    """
    Steuert das Streaming:
    - Sekunden vs. Minuten Aggregation
    - Start / Stop mit sauberer Logik
    """

    stream_type = param.Selector(
        default="Sekunden",
        objects=["Sekunden", "Minuten"],
        doc="Stream-Aggregation Typ",
    )
    is_streaming = param.Boolean(
        default=False,
        doc="Ist ein Stream aktiv?",
    )
    stream_started = param.Event(doc="Stream gestartet Event")
    stream_stopped = param.Event(doc="Stream gestoppt Event")

    def __init__(self, stream_manager=None, **params):
        super().__init__(**params)
        self.stream_manager = stream_manager
        self._status_text = pn.pane.Markdown(
            self._get_status_html(),
            styles={"font-size": "13px"},
        )

    def _get_status_html(self) -> str:
        if self.is_streaming:
            stream_label = self.stream_type
            return (
                f'🟢 **Stream aktiv** – {stream_label}-Aggregation\n\n'
                f'Daten werden empfangen...'
            )
        return '🔴 **Stream inaktiv** – Bereit zum Starten'

    def _start_stream(self, event=None):
        """Startet den Stream."""
        if self.is_streaming:
            pn.state.notifications.warning(
                "Stream läuft bereits! Stoppe zuerst den aktuellen Stream.",
                duration=3000,
            )
            return

        logger.info(f"Starting {self.stream_type} stream...")
        self.is_streaming = True
        self._status_text.object = self._get_status_html()

        # Trigger Event für Callbacks
        self.param.trigger("stream_started")

        pn.state.notifications.success(
            f"✅ {self.stream_type}-Stream gestartet!",
            duration=3000,
        )

    def _stop_stream(self, event=None):
        """Stoppt den Stream sauber."""
        if not self.is_streaming:
            pn.state.notifications.info(
                "Kein aktiver Stream zum Stoppen.",
                duration=3000,
            )
            return

        logger.info(f"Stopping {self.stream_type} stream...")
        self.is_streaming = False
        self._status_text.object = self._get_status_html()

        # Trigger Event für Callbacks
        self.param.trigger("stream_stopped")

        pn.state.notifications.info(
            f"🛑 {self.stream_type}-Stream gestoppt.",
            duration=3000,
        )

    def _on_type_change(self, event):
        """Bei Wechsel des Stream-Typs: Stoppe aktiven Stream zuerst."""
        if self.is_streaming:
            pn.state.notifications.warning(
                "⚠️ Stream-Typ gewechselt. Aktueller Stream wird gestoppt.",
                duration=3000,
            )
            self._stop_stream()

    def panel(self):
        """Erstellt die Panel-Komponente für Stream Controls."""

        # Stream-Typ Toggle
        stream_toggle = pn.widgets.RadioButtonGroup(
            name="Stream-Typ",
            options=["Sekunden", "Minuten"],
            value=self.stream_type,
            button_type="primary",
            button_style="solid",
        )

        # Bidirektionale Bindung
        stream_toggle.param.watch(
            lambda e: setattr(self, "stream_type", e.new), "value"
        )
        stream_toggle.param.watch(self._on_type_change, "value")

        # Start Button
        start_btn = pn.widgets.Button(
            name="▶  Stream starten",
            button_type="success",
            icon="player-play",
            width=160,
            height=40,
        )
        start_btn.on_click(self._start_stream)

        # Stop Button
        stop_btn = pn.widgets.Button(
            name="⏹  Stream stoppen",
            button_type="danger",
            icon="player-stop",
            width=160,
            height=40,
        )
        stop_btn.on_click(self._stop_stream)

        # Status Indicator
        status_indicator = pn.indicators.BooleanStatus(
            value=self.is_streaming,
            color="success",
            width=25,
            height=25,
        )

        # Bind status indicator
        self.param.watch(
            lambda e: setattr(status_indicator, "value", e.new),
            "is_streaming",
        )

        return pn.Column(
            "### 🔄 Stream Controls",
            pn.pane.Markdown("**Aggregation:**", margin=(5, 0, 0, 0)),
            stream_toggle,
            pn.layout.Divider(),
            pn.Row(start_btn, stop_btn),
            pn.Row(
                status_indicator,
                self._status_text,
                align="center",
            ),
            sizing_mode="stretch_width",
            css_classes=["sidebar-section"],
        )
