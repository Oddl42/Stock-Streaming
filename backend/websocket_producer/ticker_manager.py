#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:31:49 2026

@author: twi
"""

"""
Ticker Manager – Verwaltet Ticker-Subscriptions für den WebSocket.

Zuständig für:
- Laden der Ticker aus CSV
- Aufteilen in Subscription-Batches (API Limit)
- Generieren der Subscription-Parameter
- Tracking aktiver Subscriptions
"""

import logging
import math
from typing import Optional

from backend.data_service.ticker_loader import ticker_loader

logger = logging.getLogger(__name__)


class TickerManager:
    """
    Verwaltet Ticker für WebSocket-Subscriptions.

    Die Massive.com WebSocket API hat Limits für Subscriptions:
    - Max Ticker pro Subscribe-Nachricht
    - Batching notwendig bei >100 Tickern
    """

    MAX_TICKERS_PER_SUBSCRIBE = 510  # Max Ticker pro Subscribe-Call

    def __init__(self):
        self._active_tickers: list[str] = []
        self._subscribed_tickers: set[str] = set()

    @property
    def active_tickers(self) -> list[str]:
        """Aktive Ticker-Liste."""
        return self._active_tickers.copy()

    @property
    def active_count(self) -> int:
        """Anzahl aktiver Ticker."""
        return len(self._active_tickers)

    @property
    def subscribed_count(self) -> int:
        """Anzahl aktuell abonnierter Ticker."""
        return len(self._subscribed_tickers)

    def set_tickers(self, tickers: list[str]):
        """
        Setzt die zu streamenden Ticker.

        Args:
            tickers: Liste von Ticker-Symbolen
        """
        # Validiere und bereinige
        valid_tickers = []
        all_known = set(ticker_loader.all_symbols)

        for ticker in tickers:
            cleaned = ticker.strip().upper()
            if cleaned and cleaned in all_known:
                valid_tickers.append(cleaned)
            elif cleaned:
                logger.warning(f"Unknown ticker ignored: {cleaned}")

        # Duplikate entfernen, Reihenfolge beibehalten
        seen = set()
        unique_tickers = []
        for t in valid_tickers:
            if t not in seen:
                seen.add(t)
                unique_tickers.append(t)

        self._active_tickers = unique_tickers
        logger.info(f"Ticker set: {len(self._active_tickers)} symbols")

    def set_all_sp500(self):
        """Setzt alle S&P 500 Ticker."""
        self._active_tickers = ticker_loader.all_symbols
        logger.info(f"All S&P 500 tickers set: {len(self._active_tickers)}")

    def set_top10(self):
        """Setzt die Top 10 Ticker nach Marktkapitalisierung."""
        self._active_tickers = ticker_loader.top10_symbols
        logger.info(f"Top 10 tickers set: {self._active_tickers}")

    def get_subscription_batches(
        self, prefix: str = "A"
    ) -> list[str]:
        """
        Generiert Subscribe-Parameter in Batches.

        Die API erwartet: "A.AAPL,A.MSFT,A.GOOGL,..."
        Bei >100 Tickern werden mehrere Subscribe-Nachrichten benötigt.

        Args:
            prefix: "A" für Second, "AM" für Minute

        Returns:
            Liste von Subscription-Strings (je max MAX_TICKERS_PER_SUBSCRIBE Ticker)
        """
        if not self._active_tickers:
            return []

        batches = []
        num_batches = math.ceil(
            len(self._active_tickers) / self.MAX_TICKERS_PER_SUBSCRIBE
        )

        for i in range(num_batches):
            start = i * self.MAX_TICKERS_PER_SUBSCRIBE
            end = start + self.MAX_TICKERS_PER_SUBSCRIBE
            batch_tickers = self._active_tickers[start:end]

            params = ",".join(
                f"{prefix}.{ticker}" for ticker in batch_tickers
            )
            batches.append(params)

        logger.info(
            f"Created {len(batches)} subscription batch(es) "
            f"for {len(self._active_tickers)} tickers (prefix: {prefix})"
        )
        return batches

    def get_unsubscribe_params(self, prefix: str = "A") -> list[str]:
        """Generiert Unsubscribe-Parameter."""
        if not self._subscribed_tickers:
            return []

        all_params = ",".join(
            f"{prefix}.{ticker}" for ticker in self._subscribed_tickers
        )
        # Auch hier batchen
        tickers = list(self._subscribed_tickers)
        batches = []
        for i in range(0, len(tickers), self.MAX_TICKERS_PER_SUBSCRIBE):
            batch = tickers[i:i + self.MAX_TICKERS_PER_SUBSCRIBE]
            params = ",".join(f"{prefix}.{t}" for t in batch)
            batches.append(params)
        return batches

    def mark_subscribed(self, tickers: list[str]):
        """Markiert Ticker als abonniert."""
        self._subscribed_tickers.update(tickers)

    def mark_unsubscribed(self, tickers: list[str] = None):
        """Markiert Ticker als abgemeldet."""
        if tickers:
            self._subscribed_tickers -= set(tickers)
        else:
            self._subscribed_tickers.clear()

    def get_stats(self) -> dict:
        return {
            "active_tickers": len(self._active_tickers),
            "subscribed_tickers": len(self._subscribed_tickers),
            "sample_active": self._active_tickers[:5],
        }
