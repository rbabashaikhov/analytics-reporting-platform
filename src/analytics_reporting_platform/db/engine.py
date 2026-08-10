"""SQLAlchemy engine factory for Portfolio Edition PostgreSQL databases.

Engines are created only when explicitly requested.
No connections are opened on package import.
"""

from __future__ import annotations

from typing import Literal

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

from analytics_reporting_platform.config.settings import AppSettings

LogicalDatabase = Literal["raw", "calc", "cdm"]


def create_db_engine(settings: AppSettings, logical_database: LogicalDatabase) -> Engine:
    """Create a SQLAlchemy engine for a logical database target.

    Uses ``postgresql+psycopg2`` and ``URL.create`` so credentials are not
    interpolated into a manually concatenated URL string by application code.
    """
    database = settings.database_name(logical_database)
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
        database=database,
    )
    return create_engine(url, pool_pre_ping=True)


__all__ = ["LogicalDatabase", "create_db_engine"]
