#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 14:08:13 2026

@author: twi
"""

"""
SQLAlchemy ORM Models für TimescaleDB Tabellen.

Definiert die Tabellenstruktur als Python-Klassen.
Wird verwendet für:
- Typsichere Queries
- Schema-Validierung
- Alembic Migrationen (Zukunft)
- ML-Pipeline Datenextraktion
"""

from datetime import datetime
from sqlalchemy import (
    Column,
    BigInteger,
    Boolean,
    DateTime,
    Double,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Index,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import TIMESTAMP


class Base(DeclarativeBase):
    """Basis-Klasse für alle ORM Models."""
    pass


class StockAggSecond(Base):
    """
    Sekunden-Aggregation der Stock-Daten.
    TimescaleDB Hypertable mit 2-Tage Retention.
    """

    __tablename__ = "stock_agg_second"
    __table_args__ = (
        UniqueConstraint("time", "symbol", name="uq_second_time_symbol"),
        Index("idx_second_symbol_time", "symbol", "time"),
        Index("idx_second_symbol", "symbol"),
        {"schema": "public"},
    )

    # Primäre Felder
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )
    symbol: Mapped[str] = mapped_column(
        String(10), nullable=False, primary_key=True
    )

    # OHLCV
    open: Mapped[float] = mapped_column(Double, nullable=True)
    high: Mapped[float] = mapped_column(Double, nullable=True)
    low: Mapped[float] = mapped_column(Double, nullable=True)
    close: Mapped[float] = mapped_column(Double, nullable=True)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=True)

    # Zusätzliche Marktdaten
    vwap: Mapped[float] = mapped_column(Double, nullable=True)
    accumulated_volume: Mapped[int] = mapped_column(BigInteger, nullable=True)
    official_open: Mapped[float] = mapped_column(Double, nullable=True)
    avg_trade_size: Mapped[int] = mapped_column(Integer, nullable=True)
    num_trades: Mapped[int] = mapped_column(Integer, nullable=True)

    # Tick Timestamps
    tick_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    tick_end: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    # Flags
    is_otc: Mapped[bool] = mapped_column(Boolean, default=False)

    # Derived Columns (ML Features)
    price_range: Mapped[float] = mapped_column(Double, nullable=True)
    price_change: Mapped[float] = mapped_column(Double, nullable=True)
    price_change_pct: Mapped[float] = mapped_column(Double, nullable=True)
    is_bullish: Mapped[bool] = mapped_column(Boolean, nullable=True)
    body_size: Mapped[float] = mapped_column(Double, nullable=True)
    upper_shadow: Mapped[float] = mapped_column(Double, nullable=True)
    lower_shadow: Mapped[float] = mapped_column(Double, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<StockAggSecond("
            f"time={self.time}, symbol={self.symbol}, "
            f"close={self.close}, volume={self.volume})>"
        )


class StockAggMinute(Base):
    """
    Minuten-Aggregation der Stock-Daten.
    Identische Struktur wie StockAggSecond, andere Tabelle.
    """

    __tablename__ = "stock_agg_minute"
    __table_args__ = (
        UniqueConstraint("time", "symbol", name="uq_minute_time_symbol"),
        Index("idx_minute_symbol_time", "symbol", "time"),
        Index("idx_minute_symbol", "symbol"),
        {"schema": "public"},
    )

    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )
    symbol: Mapped[str] = mapped_column(
        String(10), nullable=False, primary_key=True
    )
    open: Mapped[float] = mapped_column(Double, nullable=True)
    high: Mapped[float] = mapped_column(Double, nullable=True)
    low: Mapped[float] = mapped_column(Double, nullable=True)
    close: Mapped[float] = mapped_column(Double, nullable=True)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=True)
    vwap: Mapped[float] = mapped_column(Double, nullable=True)
    accumulated_volume: Mapped[int] = mapped_column(BigInteger, nullable=True)
    official_open: Mapped[float] = mapped_column(Double, nullable=True)
    avg_trade_size: Mapped[int] = mapped_column(Integer, nullable=True)
    num_trades: Mapped[int] = mapped_column(Integer, nullable=True)
    tick_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    tick_end: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    is_otc: Mapped[bool] = mapped_column(Boolean, default=False)
    price_range: Mapped[float] = mapped_column(Double, nullable=True)
    price_change: Mapped[float] = mapped_column(Double, nullable=True)
    price_change_pct: Mapped[float] = mapped_column(Double, nullable=True)
    is_bullish: Mapped[bool] = mapped_column(Boolean, nullable=True)
    body_size: Mapped[float] = mapped_column(Double, nullable=True)
    upper_shadow: Mapped[float] = mapped_column(Double, nullable=True)
    lower_shadow: Mapped[float] = mapped_column(Double, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<StockAggMinute("
            f"time={self.time}, symbol={self.symbol}, "
            f"close={self.close}, volume={self.volume})>"
        )


class DeadLetterQueue(Base):
    """
    Dead Letter Queue für abgelehnte/fehlerhafte Datensätze.
    Keine Hypertable, normaler PostgreSQL Table.
    """

    __tablename__ = "dead_letter_queue"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    symbol: Mapped[str] = mapped_column(String(10), nullable=True)
    open: Mapped[float] = mapped_column(Double, nullable=True)
    high: Mapped[float] = mapped_column(Double, nullable=True)
    low: Mapped[float] = mapped_column(Double, nullable=True)
    close: Mapped[float] = mapped_column(Double, nullable=True)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=True)
    vwap: Mapped[float] = mapped_column(Double, nullable=True)
    rejection_reason: Mapped[str] = mapped_column(Text, nullable=True)
    rejected_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    batch_id: Mapped[int] = mapped_column(Integer, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<DeadLetterQueue("
            f"id={self.id}, symbol={self.symbol}, "
            f"reason={self.rejection_reason})>"
        )


class SP500Ticker(Base):
    """
    S&P 500 Ticker-Informationen (aus CSV geladen).
    Dient als Lookup-Tabelle für die GUI.
    """

    __tablename__ = "sp500_tickers"

    symbol: Mapped[str] = mapped_column(
        String(10), primary_key=True
    )
    name: Mapped[str] = mapped_column(String(200), nullable=True)
    sector: Mapped[str] = mapped_column(String(100), nullable=True)
    industry: Mapped[str] = mapped_column(String(200), nullable=True)
    market_cap: Mapped[float] = mapped_column(Double, nullable=True)

    def __repr__(self) -> str:
        return f"<SP500Ticker(symbol={self.symbol}, name={self.name})>"
