"""PostgreSQL connectivity helpers (explicit engine creation and health checks)."""

from analytics_reporting_platform.db.engine import LogicalDatabase, create_db_engine
from analytics_reporting_platform.db.health import DatabaseHealthResult, check_database_connection
from analytics_reporting_platform.db.timestamps import (
    PLATFORM_TZ,
    as_platform_tz,
    for_timestamp_without_time_zone,
    platform_now,
)

__all__ = [
    "PLATFORM_TZ",
    "DatabaseHealthResult",
    "LogicalDatabase",
    "as_platform_tz",
    "check_database_connection",
    "create_db_engine",
    "for_timestamp_without_time_zone",
    "platform_now",
]
