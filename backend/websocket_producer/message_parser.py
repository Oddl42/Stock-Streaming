#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:28:51 2026

@author: twi
"""

"""
WebSocket Message Parser & Validator.

Parst und validiert eingehende Nachrichten von der Massive.com WebSocket API.
Unterstützt folgende Event-Typen:
  - "A"      : Aggregates per Second
  - "AM"     : Aggregates per Minute
  - "status" : Verbindungsstatus-Nachrichten
  - "auth"   : Authentifizierungsantworten

Referenz:
  - https://massive.com/docs/websocket/stocks/aggregates-per-second
  - https://massive.com/docs/websocket/stocks/aggregates-per-minute
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ============================================================
# Datenklassen für geparste Nachrichten
# ============================================================

@dataclass
class AggregateData:
    """Geparste Aggregationsdaten (Second oder Minute)."""
    event_type: str          # "A" oder "AM"
    symbol: str              # Ticker Symbol
    open: float              # Open Price
    high: float              # High Price
    low: float               # Low Price
    close: float             # Close Price
    volume: int              # Volume
    vwap: Optional[float]    # Volume Weighted Average Price
    accumulated_volume: Optional[int]    # Tages-Gesamtvolumen
    official_open: Optional[float]       # Offizieller Eröffnungspreis
    avg_trade_size: Optional[int]        # Durchschnittliche Trade-Größe
    num_trades: Optional[int]            # Anzahl Trades
    tick_start_ms: int       # Tick Start (Millisekunden Epoch)
    tick_end_ms: int         # Tick End (Millisekunden Epoch)
    is_otc: bool = False     # Over-the-Counter Flag
    raw_data: dict = field(default_factory=dict)  # Originaldaten

    @property
    def tick_start_dt(self) -> datetime:
        """Tick-Start als datetime."""
        return datetime.fromtimestamp(
            self.tick_start_ms / 1000, tz=timezone.utc
        )

    @property
    def tick_end_dt(self) -> datetime:
        """Tick-End als datetime."""
        return datetime.fromtimestamp(
            self.tick_end_ms / 1000, tz=timezone.utc
        )

    def to_kafka_value(self) -> str:
        """Serialisiert die Daten als JSON für Kafka."""
        return json.dumps(self.raw_data)

    @property
    def kafka_key(self) -> str:
        """Kafka-Key: Symbol."""
        return self.symbol


@dataclass
class StatusMessage:
    """Statusnachricht von der WebSocket API."""
    status: str        # "connected", "auth_success", "auth_failed", etc.
    message: str       # Beschreibung
    raw_data: dict = field(default_factory=dict)


@dataclass
class ParseResult:
    """Ergebnis des Message-Parsings."""
    aggregates: list[AggregateData] = field(default_factory=list)
    status_messages: list[StatusMessage] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    raw_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0


# ============================================================
# Message Parser
# ============================================================

class MessageParser:
    """
    Parst und validiert WebSocket-Nachrichten.

    Die Massive.com WebSocket API sendet JSON-Arrays mit
    verschiedenen Event-Typen. Jedes Element hat ein "ev" Feld.
    """

    # Erwartete Felder für Aggregate-Events
    REQUIRED_AGGREGATE_FIELDS = {"ev", "sym", "o", "h", "l", "c", "v", "s", "e"}
    OPTIONAL_AGGREGATE_FIELDS = {"vw", "av", "op", "z", "n", "otc", "a"}

    # Gültige Event-Typen
    AGGREGATE_EVENTS = {"A", "AM"}
    STATUS_EVENTS = {"status"}

    def __init__(
        self,
        validate_prices: bool = True,
        max_price: float = 100_000.0,
        max_volume: int = 10_000_000_000,
    ):
        """
        Args:
            validate_prices: Preise validieren?
            max_price: Maximaler plausibler Preis
            max_volume: Maximales plausibles Volume
        """
        self.validate_prices = validate_prices
        self.max_price = max_price
        self.max_volume = max_volume

        # Statistiken
        self._total_parsed = 0
        self._total_valid = 0
        self._total_invalid = 0
        self._total_status = 0

    def parse(self, raw_message: str) -> ParseResult:
        """
        Parst eine rohe WebSocket-Nachricht.

        Die API sendet JSON-Arrays:
        [{"ev": "A", "sym": "AAPL", ...}, {"ev": "AM", ...}, ...]

        Args:
            raw_message: Roher JSON-String von der WebSocket

        Returns:
            ParseResult mit geparsten Daten, Status und Fehlern
        """
        result = ParseResult()

        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError as e:
            result.errors.append(f"JSON decode error: {e}")
            logger.warning(f"Failed to parse JSON: {e}")
            return result

        # Stelle sicher, dass es eine Liste ist
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            result.errors.append(f"Unexpected data type: {type(data)}")
            return result

        result.raw_count = len(data)

        for item in data:
            if not isinstance(item, dict):
                result.errors.append(f"Item is not a dict: {type(item)}")
                result.invalid_count += 1
                continue

            event_type = item.get("ev", "")

            # Aggregate-Events (A, AM)
            if event_type in self.AGGREGATE_EVENTS:
                aggregate = self._parse_aggregate(item)
                if aggregate is not None:
                    result.aggregates.append(aggregate)
                    result.valid_count += 1
                    self._total_valid += 1
                else:
                    result.invalid_count += 1
                    self._total_invalid += 1

            # Status-Events
            elif event_type in self.STATUS_EVENTS or "status" in item:
                status = self._parse_status(item)
                result.status_messages.append(status)
                self._total_status += 1

            else:
                # Unbekannter Event-Typ (z.B. Heartbeat)
                logger.debug(f"Unknown event type: {event_type}")

        self._total_parsed += result.raw_count
        return result

    def _parse_aggregate(self, data: dict) -> Optional[AggregateData]:
        """Parst ein einzelnes Aggregate-Event."""

        # Pflichtfelder prüfen
        missing = self.REQUIRED_AGGREGATE_FIELDS - set(data.keys())
        if missing:
            logger.debug(
                f"Missing required fields for {data.get('sym', '?')}: {missing}"
            )
            return None

        # Symbol validieren
        symbol = data.get("sym", "")
        if not symbol or not isinstance(symbol, str):
            logger.debug(f"Invalid symbol: {symbol}")
            return None

        # Preise extrahieren
        try:
            open_price = float(data["o"])
            high_price = float(data["h"])
            low_price = float(data["l"])
            close_price = float(data["c"])
            volume = int(data["v"])
            tick_start = int(data["s"])
            tick_end = int(data["e"])
        except (ValueError, TypeError) as e:
            logger.debug(f"Type conversion error for {symbol}: {e}")
            return None

        # Preis-Validierung
        if self.validate_prices:
            if not self._validate_prices(
                symbol, open_price, high_price, low_price, close_price, volume
            ):
                return None

        # Timestamp-Validierung
        if tick_start <= 0 or tick_end <= 0:
            logger.debug(f"Invalid timestamps for {symbol}: s={tick_start}, e={tick_end}")
            return None

        if tick_end < tick_start:
            logger.debug(f"tick_end < tick_start for {symbol}")
            return None

        return AggregateData(
            event_type=data["ev"],
            symbol=symbol,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            vwap=self._safe_float(data.get("vw")),
            accumulated_volume=self._safe_int(data.get("av")),
            official_open=self._safe_float(data.get("op")),
            avg_trade_size=self._safe_int(data.get("z")),
            num_trades=self._safe_int(data.get("n")),
            tick_start_ms=tick_start,
            tick_end_ms=tick_end,
            is_otc=bool(data.get("otc", False)),
            raw_data=data,
        )

    def _parse_status(self, data: dict) -> StatusMessage:
        """Parst eine Status-Nachricht."""
        return StatusMessage(
            status=data.get("status", data.get("ev", "unknown")),
            message=data.get("message", data.get("msg", "")),
            raw_data=data,
        )

    def _validate_prices(
        self,
        symbol: str,
        open_p: float,
        high_p: float,
        low_p: float,
        close_p: float,
        volume: int,
    ) -> bool:
        """Validiert Preise auf Plausibilität."""

        # Preise müssen positiv sein
        if any(p <= 0 for p in [open_p, high_p, low_p, close_p]):
            logger.debug(f"Non-positive price for {symbol}")
            return False

        # Preise nicht unrealistisch hoch
        if any(p > self.max_price for p in [open_p, high_p, low_p, close_p]):
            logger.debug(f"Price exceeds max for {symbol}")
            return False

        # High >= Low
        if high_p < low_p:
            logger.debug(f"High < Low for {symbol}: {high_p} < {low_p}")
            return False

        # High >= Open und High >= Close
        if high_p < open_p or high_p < close_p:
            logger.debug(f"High inconsistency for {symbol}")
            return False

        # Low <= Open und Low <= Close
        if low_p > open_p or low_p > close_p:
            logger.debug(f"Low inconsistency for {symbol}")
            return False

        # Volume
        if volume < 0 or volume > self.max_volume:
            logger.debug(f"Invalid volume for {symbol}: {volume}")
            return False

        return True

    @staticmethod
    def _safe_float(value) -> Optional[float]:
        """Sicher zu float konvertieren."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(value) -> Optional[int]:
        """Sicher zu int konvertieren."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def get_stats(self) -> dict:
        """Parser-Statistiken."""
        return {
            "total_parsed": self._total_parsed,
            "total_valid": self._total_valid,
            "total_invalid": self._total_invalid,
            "total_status": self._total_status,
            "validity_rate": (
                self._total_valid / max(self._total_parsed, 1) * 100
            ),
        }
