#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 21:57:05 2026

@author: twi
"""

"""Zentrale Konfiguration für das gesamte Projekt."""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Zentrale Konfiguration – liest Werte bei jeder Instanziierung aus Environment."""

    def __init__(self):
        # Use Demo Data
        self.USE_DEMO_DATA: bool = True
        
        # Massive.com API
        self.MASSIVE_API_KEY: str = os.getenv("MASSIVE_API_KEY", "")
        self.MASSIVE_WS_URL: str = os.getenv("MASSIVE_WS_URL", "wss://delayed.massive.com")

        # PostgreSQL / TimescaleDB
        self.DB_HOST: str = os.getenv("DB_HOST", "localhost")
        self.DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
        self.DB_NAME: str = os.getenv("DB_NAME", "stock_streaming")
        self.DB_USER: str = os.getenv("DB_USER", "postgres")
        self.DB_PASSWORD: str = os.getenv("DB_PASSWORD", "password")

        # Kafka
        self.KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.KAFKA_TOPIC_SECOND: str = os.getenv("KAFKA_TOPIC_SECOND", "stocks.aggregates.second")
        self.KAFKA_TOPIC_MINUTE: str = os.getenv("KAFKA_TOPIC_MINUTE", "stocks.aggregates.minute")

        # Panel GUI
        self.PANEL_PORT: int = int(os.getenv("PANEL_PORT", "5006"))
        self.PANEL_ADDRESS: str = os.getenv("PANEL_ADDRESS", "0.0.0.0")

        # Chart
        self.CHART_UPDATE_INTERVAL_MS: int = int(os.getenv("CHART_UPDATE_INTERVAL_MS", "2000"))
        self.CHART_MAX_POINTS: int = int(os.getenv("CHART_MAX_POINTS", "500"))

        # Ticker CSV
        self.SP500_CSV_PATH: str = os.getenv("SP500_CSV_PATH", "data/sp500_tickers.csv")

    @property
    def db_url(self) -> str:
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )


# Globale Instanz für den normalen Gebrauch
settings = Settings()
