#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 14:08:55 2026

@author: twi
"""

"""
Datenbank Connection Pool Management.

Verwaltet einen SQLAlchemy Connection Pool für die gesamte Applikation.
Bietet sowohl ORM-Sessions als auch Raw-Connections für psycopg2.

Wichtig: Connection Pooling ist essentiell, da bei 500 gestreamten
Tickern viele gleichzeitige DB-Zugriffe stattfinden.
"""

import logging
import psycopg2
import psycopg2.pool
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from config.db_config import db_config

logger = logging.getLogger(__name__)


class DatabaseConnectionManager:
    """
    Zentrale Verwaltung aller DB-Verbindungen.

    Stellt bereit:
    - SQLAlchemy Engine + Session (ORM Queries)
    - psycopg2 Connection Pool (Raw SQL, Legacy)
    - Health-Check Funktionen
    """

    def __init__(self):
        self._engine = None
        self._session_factory = None
        self._psycopg2_pool = None
        self._is_initialized = False

    def initialize(self):
        """Initialisiert Engine und Connection Pools."""
        if self._is_initialized:
            return

        logger.info(
            f"Initializing database connections: "
            f"{db_config.host}:{db_config.port}/{db_config.name}"
        )

        # SQLAlchemy Engine mit Connection Pool
        self._engine = create_engine(
            db_config.sqlalchemy_url,
            poolclass=QueuePool,
            **db_config.get_pool_config(),
            connect_args={
                "options": db_config.connection_options,
            },
        )

        # Session Factory
        self._session_factory = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )

        # psycopg2 Connection Pool (für Raw SQL)
        try:
            self._psycopg2_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=db_config.pool_min_size,
                maxconn=db_config.pool_max_size,
                dsn=db_config.psycopg2_dsn,
            )
        except psycopg2.OperationalError as e:
            logger.warning(
                f"Could not create psycopg2 pool: {e}. "
                f"Raw SQL connections will use ad-hoc connections."
            )
            self._psycopg2_pool = None

        self._is_initialized = True
        logger.info("Database connection pools initialized ✅")

    def close(self):
        """Schließt alle Verbindungen und Pools."""
        if self._engine:
            self._engine.dispose()
            logger.info("SQLAlchemy engine disposed.")

        if self._psycopg2_pool:
            self._psycopg2_pool.closeall()
            logger.info("psycopg2 pool closed.")

        self._is_initialized = False

    # =========================================
    # SQLAlchemy Session (ORM)
    # =========================================

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context Manager für SQLAlchemy Sessions.

        Verwendung:
            with db_manager.get_session() as session:
                result = session.query(StockAggSecond).filter(...)
                session.commit()
        """
        if not self._is_initialized:
            self.initialize()

        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @property
    def engine(self):
        """Gibt die SQLAlchemy Engine zurück."""
        if not self._is_initialized:
            self.initialize()
        return self._engine

    # =========================================
    # psycopg2 Raw Connection
    # =========================================

    @contextmanager
    def get_raw_connection(self):
        """
        Context Manager für psycopg2 Raw Connections.

        Verwendung:
            with db_manager.get_raw_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT ...")
        """
        if not self._is_initialized:
            self.initialize()

        conn = None
        try:
            if self._psycopg2_pool:
                conn = self._psycopg2_pool.getconn()
            else:
                conn = psycopg2.connect(db_config.psycopg2_dsn)
            yield conn
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                if self._psycopg2_pool:
                    self._psycopg2_pool.putconn(conn)
                else:
                    conn.close()

    @contextmanager
    def get_raw_cursor(self, autocommit: bool = False):
        """
        Context Manager für psycopg2 Cursor.

        Verwendung:
            with db_manager.get_raw_cursor() as cur:
                cur.execute("INSERT INTO ...")
        """
        with self.get_raw_connection() as conn:
            if autocommit:
                conn.autocommit = True
            cur = conn.cursor()
            try:
                yield cur
                if not autocommit:
                    conn.commit()
            except Exception:
                if not autocommit:
                    conn.rollback()
                raise
            finally:
                cur.close()

    # =========================================
    # Health Check
    # =========================================

    def health_check(self) -> dict:
        """Prüft die DB-Verbindung und gibt Status zurück."""
        result = {
            "healthy": False,
            "engine_pool_size": 0,
            "engine_pool_checked_out": 0,
            "psycopg2_pool_available": False,
        }

        try:
            # SQLAlchemy Check
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                conn.commit()

            pool = self._engine.pool
            result["engine_pool_size"] = pool.size()
            result["engine_pool_checked_out"] = pool.checkedout()
            result["healthy"] = True

            # psycopg2 Pool Check
            if self._psycopg2_pool and not self._psycopg2_pool.closed:
                result["psycopg2_pool_available"] = True

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Database health check failed: {e}")

        return result

    def get_stats(self) -> dict:
        """Gibt Pool-Statistiken zurück."""
        stats = {
            "initialized": self._is_initialized,
        }
        if self._engine:
            pool = self._engine.pool
            stats.update({
                "pool_size": pool.size(),
                "pool_checked_out": pool.checkedout(),
                "pool_overflow": pool.overflow(),
                "pool_checked_in": pool.checkedin(),
            })
        return stats


# Singleton
db_manager = DatabaseConnectionManager()
