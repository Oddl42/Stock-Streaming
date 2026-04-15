#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:13:56 2026

@author: twi
"""

"""Chart-Typ Auswahl: Candlestick oder Linien-Chart."""

import panel as pn
import param


class ChartTypeSelector(param.Parameterized):
    """Toggle zwischen Candlestick und Line Chart."""

    chart_type = param.Selector(
        default="Candlestick",
        objects=["Candlestick", "Linie"],
        doc="Chart-Darstellung",
    )

    def panel(self):
        """Erstellt die Panel-Komponente."""
        toggle = pn.widgets.RadioButtonGroup(
            name="Chart-Typ",
            options=["Candlestick", "Linie"],
            value="Candlestick",
            button_type="warning",
            button_style="outline",
        )

        # Bidirektionale Bindung
        toggle.param.watch(
            lambda e: setattr(self, "chart_type", e.new), "value"
        )

        return pn.Column(
            "### 📊 Chart-Typ",
            toggle,
            sizing_mode="stretch_width",
            css_classes=["sidebar-section"],
        )
