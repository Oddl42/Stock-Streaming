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
    # Massive.com API
    MASSIVE_API_KEY: str = os.getenv("MASSIVE_API_KEY", "")
    MASSIVE_WS_URL: str = os.getenv("MASSIVE_WS_URL", "wss://delayed.massive.com")

    # PostgreSQL / TimescaleDB
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "stock_streaming")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "password")

    @property
    def db_url(self) -> str:
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_SECOND: str = os.getenv("KAFKA_TOPIC_SECOND", "stocks.aggregates.second")
    KAFKA_TOPIC_MINUTE: str = os.getenv("KAFKA_TOPIC_MINUTE", "stocks.aggregates.minute")

    # Panel GUI
    PANEL_PORT: int = int(os.getenv("PANEL_PORT", "5006"))
    PANEL_ADDRESS: str = os.getenv("PANEL_ADDRESS", "0.0.0.0")

    # Chart
    CHART_UPDATE_INTERVAL_MS: int = 2000  # 2 Sekunden
    CHART_MAX_POINTS: int = 500           # Max Datenpunkte im Chart

    # Ticker CSV
    SP500_CSV_PATH: str = os.getenv("SP500_CSV_PATH", "data/sp500_tickers.csv")


settings = Settings()
