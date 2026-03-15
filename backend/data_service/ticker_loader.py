#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 22:00:09 2026

@author: twi
"""

"""Laedt S&P 500 Ticker aus CSV und stellt Auswahl-Logik bereit."""

import logging
import pandas as pd
from pathlib import Path
from config.settings import settings

logger = logging.getLogger(__name__)

COLUMN_MAPPING = {
    "Symbol":    ["Symbol", "symbol", "SYMBOL", "Ticker", "ticker"],
    "Name":      ["Name", "name", "NAME", "Company", "company", "Shortname"],
    "Sector":    ["Sector", "sector", "SECTOR", "GICS_Sector"],
    "Industry":  ["Industry", "industry", "INDUSTRY", "Sub_Industry"],
    "MarketCap": [
        "MarketCap", "Market_Cap", "Market Cap", "marketCap",
        "market_cap", "MARKETCAP", "Mkt_Cap", "MktCap", "Marketcap",
    ],
}


class TickerLoader:
    """Verwaltet das Laden und Filtern von S&P 500 Tickern."""

    def __init__(self, csv_path: str = None):
        self.csv_path = csv_path or settings.SP500_CSV_PATH
        self._df: pd.DataFrame = pd.DataFrame()
        self._load_csv()

    def _load_csv(self):
        """CSV laden und Spaltennamen standardisieren."""
        path = Path(self.csv_path)
        if not path.exists():
            logger.warning(f"CSV nicht gefunden: {path}. Verwende Demo-Daten.")
            self._df = self._create_demo_data()
            return

        self._df = pd.read_csv(path)
        logger.info(f"CSV geladen: {path} ({len(self._df)} Zeilen)")
        logger.info(f"Originale Spalten: {list(self._df.columns)}")
        self._standardize_columns()
        logger.info(f"Standardisierte Spalten: {list(self._df.columns)}")

    def _standardize_columns(self):
        """Mappt CSV-Spaltennamen auf erwartete Standardnamen."""
        self._df.columns = [c.strip() for c in self._df.columns]

        rename_map = {}
        for target_name, variants in COLUMN_MAPPING.items():
            if target_name in self._df.columns:
                continue
            for variant in variants:
                if variant in self._df.columns:
                    rename_map[variant] = target_name
                    break

        if rename_map:
            logger.info(f"Spalten umbenannt: {rename_map}")
            self._df.rename(columns=rename_map, inplace=True)

        required = ["Symbol", "Name"]
        missing = [col for col in required if col not in self._df.columns]
        if missing:
            logger.error(f"Pflicht-Spalten fehlen: {missing}")
            raise ValueError(f"CSV fehlen Pflicht-Spalten: {missing}")

        if "Sector" not in self._df.columns:
            self._df["Sector"] = "Unknown"
            logger.warning("Spalte 'Sector' fehlt - mit 'Unknown' gefuellt.")

        if "Industry" not in self._df.columns:
            self._df["Industry"] = "Unknown"
            logger.warning("Spalte 'Industry' fehlt - mit 'Unknown' gefuellt.")

        if "MarketCap" not in self._df.columns:
            self._df["MarketCap"] = 0.0
            logger.warning("Spalte 'MarketCap' fehlt - mit 0 gefuellt.")

    def _create_demo_data(self) -> pd.DataFrame:
        """Demo-Daten falls CSV nicht vorhanden."""
        data = {
            "Symbol": [
                "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
                "META", "TSLA", "BRK.B", "UNH", "JNJ",
                "V", "XOM", "WMT", "JPM", "PG",
                "MA", "HD", "CVX", "MRK", "ABBV",
                "LLY", "PEP", "KO", "COST", "AVGO",
            ],
            "Name": [
                "Apple Inc.", "Microsoft Corp.", "Alphabet Inc.", "Amazon.com Inc.",
                "NVIDIA Corp.", "Meta Platforms Inc.", "Tesla Inc.",
                "Berkshire Hathaway", "UnitedHealth Group", "Johnson & Johnson",
                "Visa Inc.", "Exxon Mobil", "Walmart Inc.", "JPMorgan Chase",
                "Procter & Gamble", "Mastercard", "Home Depot", "Chevron Corp.",
                "Merck & Co.", "AbbVie Inc.", "Eli Lilly", "PepsiCo Inc.",
                "Coca-Cola Co.", "Costco Wholesale", "Broadcom Inc.",
            ],
            "Sector": [
                "Technology", "Technology", "Technology", "Consumer Cyclical",
                "Technology", "Technology", "Consumer Cyclical",
                "Financial Services", "Healthcare", "Healthcare",
                "Financial Services", "Energy", "Consumer Defensive",
                "Financial Services", "Consumer Defensive",
                "Financial Services", "Consumer Cyclical", "Energy",
                "Healthcare", "Healthcare", "Healthcare", "Consumer Defensive",
                "Consumer Defensive", "Consumer Defensive", "Technology",
            ],
            "Industry": [
                "Consumer Electronics", "Software", "Internet Content", "Internet Retail",
                "Semiconductors", "Internet Content", "Auto Manufacturers",
                "Insurance", "Health Care Plans", "Drug Manufacturers",
                "Credit Services", "Oil & Gas", "Discount Stores",
                "Banks", "Household Products",
                "Credit Services", "Home Improvement", "Oil & Gas",
                "Drug Manufacturers", "Drug Manufacturers", "Drug Manufacturers",
                "Beverages", "Beverages", "Discount Stores", "Semiconductors",
            ],
            "MarketCap": [
                3.0e12, 2.8e12, 1.9e12, 1.8e12, 1.7e12,
                1.2e12, 0.8e12, 0.78e12, 0.52e12, 0.48e12,
                0.47e12, 0.45e12, 0.43e12, 0.42e12, 0.38e12,
                0.37e12, 0.35e12, 0.34e12, 0.30e12, 0.29e12,
                0.55e12, 0.25e12, 0.26e12, 0.24e12, 0.60e12,
            ],
        }
        return pd.DataFrame(data)

    @property
    def all_tickers(self) -> pd.DataFrame:
        return self._df.copy()

    @property
    def all_symbols(self) -> list[str]:
        return self._df["Symbol"].tolist()

    @property
    def top10_by_market_cap(self) -> pd.DataFrame:
        if "MarketCap" not in self._df.columns:
            logger.warning("MarketCap-Spalte fehlt, gebe erste 10 zurueck.")
            return self._df.head(10).reset_index(drop=True)
        return (
            self._df
            .sort_values("MarketCap", ascending=False)
            .head(10)
            .reset_index(drop=True)
        )

    @property
    def top10_symbols(self) -> list[str]:
        return self.top10_by_market_cap["Symbol"].tolist()

    def get_tickers_by_symbols(self, symbols: list[str]) -> pd.DataFrame:
        return self._df[self._df["Symbol"].isin(symbols)].reset_index(drop=True)

    def search_symbols(self, query: str) -> list[str]:
        query = query.upper()
        mask = (
            self._df["Symbol"].str.upper().str.contains(query, na=False)
            | self._df["Name"].str.upper().str.contains(query, na=False)
        )
        return self._df[mask]["Symbol"].tolist()


ticker_loader = TickerLoader()
