#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 11:44:19 2026

@author: twi
"""

"""
Unit Tests für den WebSocket Message Parser.
"""

import json
import pytest
from datetime import datetime, timezone

from backend.websocket_producer.message_parser import (
    MessageParser,
    AggregateData,
    StatusMessage,
    ParseResult,
)


class TestMessageParser:
    """Tests für MessageParser."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup für jeden Test."""
        self.parser = MessageParser(validate_prices=True)

    # ==========================================
    # Grundlegendes Parsing
    # ==========================================

    def test_parse_single_second_aggregate(self, sample_ws_second_message):
        """Testet das Parsen einer einzelnen Sekunden-Aggregation."""
        raw = json.dumps(sample_ws_second_message)
        result = self.parser.parse(raw)

        assert isinstance(result, ParseResult)
        assert result.raw_count == 2
        assert result.valid_count == 2
        assert result.invalid_count == 0
        assert len(result.aggregates) == 2
        assert len(result.errors) == 0

    def test_parse_minute_aggregate(self, sample_ws_minute_message):
        """Testet das Parsen einer Minuten-Aggregation."""
        raw = json.dumps(sample_ws_minute_message)
        result = self.parser.parse(raw)

        assert result.valid_count == 1
        assert result.aggregates[0].event_type == "AM"
        assert result.aggregates[0].symbol == "AAPL"

    def test_parse_status_message(self, sample_ws_status_message):
        """Testet das Parsen von Status-Nachrichten."""
        raw = json.dumps(sample_ws_status_message)
        result = self.parser.parse(raw)

        assert len(result.status_messages) == 1
        assert result.status_messages[0].status == "connected"
        assert "Connected" in result.status_messages[0].message

    # ==========================================
    # AggregateData Eigenschaften
    # ==========================================

    def test_aggregate_data_properties(self, sample_ws_second_message):
        """Testet AggregateData Attribute und Properties."""
        raw = json.dumps(sample_ws_second_message)
        result = self.parser.parse(raw)
        agg = result.aggregates[0]

        assert agg.symbol == "AAPL"
        assert agg.event_type == "A"
        assert agg.open == 186.00
        assert agg.high == 186.50
        assert agg.low == 185.80
        assert agg.close == 186.25
        assert agg.volume == 5000
        assert agg.vwap == 186.10
        assert agg.num_trades == 45
        assert agg.is_otc is False

    def test_aggregate_kafka_key(self, sample_ws_second_message):
        """Testet den Kafka-Key."""
        raw = json.dumps(sample_ws_second_message)
        result = self.parser.parse(raw)
        agg = result.aggregates[0]

        assert agg.kafka_key == "AAPL"

    def test_aggregate_to_kafka_value(self, sample_ws_second_message):
        """Testet die Kafka-Value Serialisierung."""
        raw = json.dumps(sample_ws_second_message)
        result = self.parser.parse(raw)
        agg = result.aggregates[0]

        value = agg.to_kafka_value()
        assert isinstance(value, str)
        parsed = json.loads(value)
        assert parsed["sym"] == "AAPL"
        assert parsed["ev"] == "A"

    def test_aggregate_tick_timestamps(self, sample_ws_second_message):
        """Testet Tick-Timestamp Properties."""
        raw = json.dumps(sample_ws_second_message)
        result = self.parser.parse(raw)
        agg = result.aggregates[0]

        assert isinstance(agg.tick_start_dt, datetime)
        assert isinstance(agg.tick_end_dt, datetime)
        assert agg.tick_end_dt > agg.tick_start_dt

    # ==========================================
    # Validierung
    # ==========================================

    def test_reject_missing_required_fields(self):
        """Testet Ablehnung bei fehlenden Pflichtfeldern."""
        incomplete = [{"ev": "A", "sym": "AAPL"}]  # Fehlende OHLCV-Felder
        raw = json.dumps(incomplete)
        result = self.parser.parse(raw)

        assert result.valid_count == 0
        assert result.invalid_count == 1

    def test_reject_negative_prices(self):
        """Testet Ablehnung bei negativen Preisen."""
        msg = [{
            "ev": "A", "sym": "AAPL",
            "o": -5.0, "h": 10.0, "l": -8.0, "c": 7.0,
            "v": 100, "s": 1000000, "e": 1001000,
        }]
        raw = json.dumps(msg)
        result = self.parser.parse(raw)

        assert result.valid_count == 0

    def test_reject_high_less_than_low(self):
        """Testet Ablehnung wenn High < Low."""
        msg = [{
            "ev": "A", "sym": "AAPL",
            "o": 5.0, "h": 3.0, "l": 8.0, "c": 6.0,
            "v": 100, "s": 1000000, "e": 1001000,
        }]
        raw = json.dumps(msg)
        result = self.parser.parse(raw)

        assert result.valid_count == 0

    def test_reject_unrealistic_price(self):
        """Testet Ablehnung bei unrealistischen Preisen."""
        msg = [{
            "ev": "A", "sym": "AAPL",
            "o": 999999.0, "h": 999999.0, "l": 999999.0, "c": 999999.0,
            "v": 100, "s": 1000000, "e": 1001000,
        }]
        raw = json.dumps(msg)
        result = self.parser.parse(raw)

        assert result.valid_count == 0

    def test_reject_empty_symbol(self):
        """Testet Ablehnung bei leerem Symbol."""
        msg = [{
            "ev": "A", "sym": "",
            "o": 5.0, "h": 8.0, "l": 3.0, "c": 6.0,
            "v": 100, "s": 1000000, "e": 1001000,
        }]
        raw = json.dumps(msg)
        result = self.parser.parse(raw)

        assert result.valid_count == 0

    def test_reject_negative_timestamps(self):
        """Testet Ablehnung bei negativen Timestamps."""
        msg = [{
            "ev": "A", "sym": "AAPL",
            "o": 5.0, "h": 8.0, "l": 3.0, "c": 6.0,
            "v": 100, "s": -1, "e": -1,
        }]
        raw = json.dumps(msg)
        result = self.parser.parse(raw)

        assert result.valid_count == 0

    def test_reject_end_before_start(self):
        """Testet Ablehnung wenn tick_end < tick_start."""
        msg = [{
            "ev": "A", "sym": "AAPL",
            "o": 5.0, "h": 8.0, "l": 3.0, "c": 6.0,
            "v": 100, "s": 2000000, "e": 1000000,
        }]
        raw = json.dumps(msg)
        result = self.parser.parse(raw)

        assert result.valid_count == 0

    # ==========================================
    # Edge Cases
    # ==========================================

    def test_parse_invalid_json(self):
        """Testet Fehlerbehandlung bei ungültigem JSON."""
        result = self.parser.parse("not valid json {{{")

        assert result.raw_count == 0
        assert len(result.errors) > 0
        assert "JSON decode error" in result.errors[0]

    def test_parse_single_dict(self, sample_ws_second_message):
        """Testet Parsing eines einzelnen Dicts (nicht in Array)."""
        single = sample_ws_second_message[0]
        raw = json.dumps(single)
        result = self.parser.parse(raw)

        assert result.valid_count == 1

    def test_parse_empty_array(self):
        """Testet leeres Array."""
        result = self.parser.parse("[]")

        assert result.raw_count == 0
        assert result.valid_count == 0

    def test_parse_mixed_valid_invalid(self, sample_ws_second_message):
        """Testet Batch mit gültigen und ungültigen Nachrichten."""
        mixed = sample_ws_second_message + [
            {"ev": "A", "sym": "BAD"},  # Ungültig
        ]
        raw = json.dumps(mixed)
        result = self.parser.parse(raw)

        assert result.valid_count == 2
        assert result.invalid_count == 1

    def test_validation_disabled(self):
        """Testet Parser ohne Preis-Validierung."""
        parser = MessageParser(validate_prices=False)
        msg = [{
            "ev": "A", "sym": "AAPL",
            "o": -5.0, "h": -3.0, "l": -8.0, "c": -6.0,
            "v": 100, "s": 1000000, "e": 1001000,
        }]
        raw = json.dumps(msg)
        result = parser.parse(raw)

        # Ohne Validierung sollte es durchgehen (Timestamps sind positiv)
        assert result.valid_count == 1

    def test_optional_fields_missing(self):
        """Testet dass optionale Felder fehlen dürfen."""
        msg = [{
            "ev": "A", "sym": "AAPL",
            "o": 185.0, "h": 186.0, "l": 184.0, "c": 185.5,
            "v": 5000, "s": 1000000000000, "e": 1000000001000,
            # Keine optionalen Felder: vw, av, op, z, n, otc
        }]
        raw = json.dumps(msg)
        result = self.parser.parse(raw)

        assert result.valid_count == 1
        agg = result.aggregates[0]
        assert agg.vwap is None
        assert agg.accumulated_volume is None
        assert agg.num_trades is None

    # ==========================================
    # Statistiken
    # ==========================================

    def test_parser_stats(self, sample_ws_second_message, sample_ws_invalid_messages):
        """Testet Parser-Statistiken."""
        raw_valid = json.dumps(sample_ws_second_message)
        raw_invalid = json.dumps(sample_ws_invalid_messages)

        self.parser.parse(raw_valid)
        self.parser.parse(raw_invalid)

        stats = self.parser.get_stats()
        assert stats["total_parsed"] > 0
        assert stats["total_valid"] == 2
        assert stats["total_invalid"] > 0
        assert 0 <= stats["validity_rate"] <= 100
