"""Unit tests for SQLAlchemy engine factory (no live PostgreSQL connection)."""

from __future__ import annotations

from analytics_reporting_platform.config import AppSettings
from analytics_reporting_platform.db import create_db_engine


def _settings() -> AppSettings:
    return AppSettings(
        db_host="127.0.0.1",
        db_port=15433,
        db_user="synthetic_user",
        db_password="synthetic-db-password",
        raw_db_name="raw_data",
        calc_db_name="calc_data",
        cdm_db_name="cdm_data",
        adobe_client_secret="synthetic-adobe-secret",
    )


def test_engine_url_for_raw() -> None:
    engine = create_db_engine(_settings(), "raw")
    url = engine.url

    assert url.drivername == "postgresql+psycopg2"
    assert url.host == "127.0.0.1"
    assert url.port == 15433
    assert url.username == "synthetic_user"
    assert url.database == "raw_data"
    assert "synthetic-db-password" not in str(url)
    assert "synthetic-db-password" not in repr(url)
    engine.dispose()


def test_engine_database_selection() -> None:
    settings = _settings()
    mapping = {
        "raw": "raw_data",
        "calc": "calc_data",
        "cdm": "cdm_data",
    }
    for logical, expected_db in mapping.items():
        engine = create_db_engine(settings, logical)  # type: ignore[arg-type]
        assert engine.url.database == expected_db
        assert engine.url.drivername == "postgresql+psycopg2"
        engine.dispose()


def test_pool_pre_ping_enabled() -> None:
    engine = create_db_engine(_settings(), "calc")
    assert engine.pool._pre_ping is True  # noqa: SLF001 - intentional config assertion
    engine.dispose()
