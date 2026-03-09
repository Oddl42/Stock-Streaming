#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:05:28 2026

@author: twi
"""

"""Ticker-Auswahl Komponente: Alle / Top 10 / Manuell."""

import panel as pn
import param

from backend.data_service.ticker_loader import ticker_loader


class TickerSelector(param.Parameterized):
    """
    Ermoeglicht die Auswahl von Tickern:
    - Alle S&P 500
    - Top 10 nach Marktkapitalisierung
    - Manuelle Eingabe
    """

    selection_mode = param.Selector(
        default="Top 10",
        objects=["Alle S&P 500", "Top 10", "Manuell"],
        doc="Ticker-Auswahl Modus",
    )
    manual_input = param.String(
        default="",
        doc="Manuelle Ticker-Eingabe (kommasepariert)",
    )
    selected_symbols = param.List(
        default=[],
        doc="Aktuell ausgewaehlte Symbole",
    )
    apply_selection = param.Event(doc="Auswahl anwenden")

    def __init__(self, **params):
        super().__init__(**params)
        self._all_symbols = ticker_loader.all_symbols
        self._top10_symbols = ticker_loader.top10_symbols
        self.selected_symbols = self._top10_symbols.copy()

    @param.depends("selection_mode", watch=True)
    def _on_mode_change(self):
        """Aktualisiere Auswahl bei Moduswechsel."""
        if self.selection_mode == "Alle S&P 500":
            self.selected_symbols = self._all_symbols.copy()
        elif self.selection_mode == "Top 10":
            self.selected_symbols = self._top10_symbols.copy()

    def _apply_manual(self, event=None):
        """Manuelle Eingabe parsen und anwenden."""
        if self.selection_mode == "Manuell" and self.manual_input:
            symbols = [
                s.strip().upper()
                for s in self.manual_input.split(",")
                if s.strip()
            ]
            valid = [s for s in symbols if s in self._all_symbols]
            invalid = [s for s in symbols if s not in self._all_symbols]

            self.selected_symbols = valid

            if invalid:
                pn.state.notifications.warning(
                    f"Unbekannte Ticker ignoriert: {', '.join(invalid)}",
                    duration=5000,
                )

    @param.depends("selected_symbols")
    def _selection_info(self):
        """Zeigt Info ueber aktuelle Auswahl."""
        count = len(self.selected_symbols)
        if count == 0:
            return pn.pane.Alert(
                "Keine Ticker ausgewaehlt!", alert_type="warning"
            )
        return pn.pane.Markdown(
            f"**{count} Ticker ausgewaehlt**",
            styles={"color": "#0f9d58", "font-size": "13px"},
        )

    def panel(self):
        """Erstellt die Panel-Komponente."""
        mode_select = pn.widgets.RadioButtonGroup.from_param(
            self.param.selection_mode,
            name="Auswahl-Modus",
            button_type="primary",
            button_style="outline",
        )

        manual_field = pn.widgets.TextAreaInput(
            name="Ticker eingeben (kommasepariert)",
            placeholder="z.B. AAPL, MSFT, GOOGL, AMZN",
            value=self.manual_input,
            max_length=5000,
            height=80,
            visible=False,
        )

        apply_btn = pn.widgets.Button(
            name="Auswahl anwenden",
            button_type="primary",
            width=200,
            visible=False,
        )

        # Panel 1.3.8: 'options' statt 'completions'
        search_field = pn.widgets.AutocompleteInput(
            name="Ticker suchen",
            options=self._all_symbols,
            min_characters=1,
            placeholder="Symbol suchen...",
            case_sensitive=False,
            visible=False,
        )

        def update_visibility(event):
            is_manual = event.new == "Manuell"
            manual_field.visible = is_manual
            apply_btn.visible = is_manual
            search_field.visible = is_manual

        mode_select.param.watch(update_visibility, "value")

        def on_apply(event):
            self.manual_input = manual_field.value
            self._apply_manual()

        apply_btn.on_click(on_apply)

        def on_search(event):
            if event.new and event.new in self._all_symbols:
                current = manual_field.value
                if current:
                    manual_field.value = f"{current}, {event.new}"
                else:
                    manual_field.value = event.new
                search_field.value = ""

        search_field.param.watch(on_search, "value")

        info = pn.panel(self._selection_info)

        return pn.Column(
            "### Ticker Auswahl",
            mode_select,
            search_field,
            manual_field,
            apply_btn,
            info,
            sizing_mode="stretch_width",
            css_classes=["sidebar-section"],
        )
