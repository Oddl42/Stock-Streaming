#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 14:06:19 2026

@author: twi
"""

"""
Datenbank-Konfiguration und Connection-Helpers.

Stellt zentrale Funktionen bereit für:
- Connection Strings (psycopg2, SQLAlchemy, JDBC)
- Connection Pool Konfiguration
- SSL/TLS Settings
- Retry-Konfiguration
"""

import os
from dataclasses import dataclass
from config.settings import settings


@dataclass
class DatabaseConfig:
    """Alle DB-relevanten Einstellungen."""

    host: str = settings.DB_HOST
    port: int = settings.DB_PORT
    name: str = settings.DB_NAME
    user: str = settings.DB_USER
    password: str = settings.DB_PASSWORD

    # Connection Pool
    pool_min_size: int = int(os.getenv("DB_POOL_MIN", "2"))
    pool_max_size: int = int(os.getenv("DB_POOL_MAX", "20"))
    pool_max_overflow: int = int(os.getenv("DB_POOL_OVERFLOW", "10"))
    pool_timeout: int = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    pool_recycle: int = int(os.getenv("DB_POOL_RECYCLE", "3600"))

    # Query Settings
    statement_timeout_ms: int = int(os.getenv("DB_STATEMENT_TIMEOUT", "30000"))
    lock_timeout_ms: int = int(os.getenv("DB_LOCK_TIMEOUT", "10000"))

    # SSL (für Produktion)
    ssl_mode: str = os.getenv("DB_SSL_MODE", "disable")
    ssl_root_cert: str = os.getenv("DB_SSL_ROOT_CERT", "")

    # Retry
    max_retries: int = 3
    retry_delay_seconds: float = 2.0

    # TimescaleDB spezifisch
    retention_days: int = int(os.getenv("DB_RETENTION_DAYS", "2"))
    compression_after_days: int = int(os.getenv("DB_COMPRESSION_AFTER", "1"))
    chunk_time_interval: str = os.getenv("DB_CHUNK_INTERVAL", "1 day")

    @property
    def psycopg2_dsn(self) -> str:
        """Connection-String für psycopg2."""
        dsn = (
            f"host={self.host} port={self.port} "
            f"dbname={self.name} user={self.user} "
            f"password={self.password} "
            f"sslmode={self.ssl_mode}"
        )
        if self.ssl_root_cert:
            dsn += f" sslrootcert={self.ssl_root_cert}"
        return dsn

    @property
    def sqlalchemy_url(self) -> str:
        """Connection-String für SQLAlchemy."""
        base = (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )
        params = f"?sslmode={self.ssl_mode}"
        if self.ssl_root_cert:
            params += f"&sslrootcert={self.ssl_root_cert}"
        return base + params

    @property
    def jdbc_url(self) -> str:
        """Connection-String für Spark JDBC."""
        url = f"jdbc:postgresql://{self.host}:{self.port}/{self.name}"
        if self.ssl_mode != "disable":
            url += f"?sslmode={self.ssl_mode}"
        return url

    @property
    def jdbc_properties(self) -> dict:
        """JDBC Properties für Spark .write.jdbc()."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
            "batchsize": "1000",
            "rewriteBatchedStatements": "true",
            "rewriteBatchedInserts": "true",
            "connectTimeout": "10",
            "socketTimeout": str(self.statement_timeout_ms // 1000),
        }

    @property
    def connection_options(self) -> str:
        """PostgreSQL connection options (SET Befehle)."""
        return (
            f"-c statement_timeout={self.statement_timeout_ms} "
            f"-c lock_timeout={self.lock_timeout_ms}"
        )

    def get_pool_config(self) -> dict:
        """Konfiguration für SQLAlchemy Connection Pool."""
        return {
            "pool_size": self.pool_max_size,
            "max_overflow": self.pool_max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": True,
            "echo": False,
        }


# Singleton
db_config = DatabaseConfig()


def get_db_connection_string() -> str:
    """Shortcut: Gibt den psycopg2 DSN zurück."""
    return db_config.psycopg2_dsn


def get_sqlalchemy_url() -> str:
    """Shortcut: Gibt die SQLAlchemy URL zurück."""
    return db_config.sqlalchemy_url


def get_jdbc_url() -> str:
    """Shortcut: Gibt die JDBC URL zurück."""
    return db_config.jdbc_url
