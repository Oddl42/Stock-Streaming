#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:02:04 2026

@author: twi
"""

"""Panel Theme und CSS Konfiguration."""

# Custom CSS für die gesamte App
CUSTOM_CSS = """
/* ========== Global Styles ========== */
:root {
    --primary-color: #1a73e8;
    --success-color: #0f9d58;
    --danger-color: #db4437;
    --warning-color: #f4b400;
    --bg-dark: #1e1e2e;
    --bg-card: #2a2a3e;
    --text-primary: #e0e0e0;
    --text-secondary: #a0a0b0;
    --border-color: #3a3a4e;
}

/* ========== Sidebar Styles ========== */
.sidebar-section {
    background: var(--bg-card);
    border-radius: 8px;
    padding: 12px;
    margin-bottom: 10px;
    border: 1px solid var(--border-color);
}

.sidebar-section h3 {
    margin-top: 0;
    color: var(--text-primary);
    font-size: 14px;
    border-bottom: 1px solid var(--border-color);
    padding-bottom: 8px;
}

/* ========== Stream Status Indicator ========== */
.stream-active {
    color: var(--success-color);
    font-weight: bold;
}

.stream-inactive {
    color: var(--danger-color);
    font-weight: bold;
}

/* ========== Ticker Table ========== */
.ticker-table {
    font-size: 12px;
}

.ticker-table .tabulator-header {
    background: var(--bg-dark);
}

/* ========== Chart Area ========== */
.chart-container {
    border-radius: 8px;
    padding: 10px;
    background: var(--bg-card);
}

/* ========== Button Styles ========== */
.bk-btn-success {
    background-color: var(--success-color) !important;
}

.bk-btn-danger {
    background-color: var(--danger-color) !important;
}

/* ========== Responsive Adjustments ========== */
@media (max-width: 1200px) {
    .bk-root {
        font-size: 13px;
    }
}
"""

# Panel Template Konfiguration
TEMPLATE_CONFIG = {
    "title": "📈 Stock Streaming Platform",
    "logo": "",
    "favicon": "",
    "sidebar_width": 380,
    "header_background": "#1a73e8",
    "accent_base_color": "#1a73e8",
}

# Farbpalette für Charts
CHART_COLORS = {
    "bullish": "#26a69a",      # Grün für steigende Kurse
    "bearish": "#ef5350",      # Rot für fallende Kurse
    "line": "#1a73e8",         # Blau für Linien-Chart
    "volume": "#7986cb",       # Lila für Volumen
    "grid": "#3a3a4e",         # Grid-Linien
    "background": "#1e1e2e",   # Chart-Hintergrund
    "text": "#e0e0e0",         # Text
}
