"""Unit tests for application settings loading."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from analytics_reporting_platform.config import AppSettings, SettingsError, load_settings


REQUIRED_DB_ENV = {
    "DB_HOST": "127.0.0.1",
    "DB_PORT": "15433",
    "DB_USER": "test_user",
    "DB_PASSWORD": "test-secret-password-do-not-leak",
    "RAW_DB_NAME": "raw_data",
    "CALC_DB_NAME": "calc_data",
    "CDM_DB_NAME": "cdm_data",
}


@pytest.fixture
def clean_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure settings tests do not inherit ambient process DB/Adobe vars."""
    keys = [
        "DB_HOST",
        "DB_PORT",
        "DB_USER",
        "DB_PASSWORD",
        "RAW_DB_NAME",
        "CALC_DB_NAME",
        "CDM_DB_NAME",
        "ADOBE_CLIENT_ID",
        "ADOBE_CLIENT_SECRET",
        "ADOBE_GLOBAL_COMPANY_ID",
        "ADOBE_REPORT_SUITE_ID",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def test_load_settings_from_process_env(clean_db_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_DB_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("ADOBE_CLIENT_ID", "synthetic-client-id")
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", "synthetic-adobe-secret")

    settings = load_settings()

    assert isinstance(settings, AppSettings)
    assert settings.db_host == "127.0.0.1"
    assert settings.db_port == 15433
    assert settings.db_user == "test_user"
    assert settings.raw_db_name == "raw_data"
    assert settings.calc_db_name == "calc_data"
    assert settings.cdm_db_name == "cdm_data"
    assert settings.adobe_client_id == "synthetic-client-id"
    assert settings.adobe_client_secret == "synthetic-adobe-secret"


def test_db_port_must_be_integer(clean_db_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_DB_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DB_PORT", "not-a-port")

    with pytest.raises(SettingsError, match="Invalid DB_PORT"):
        load_settings()


def test_missing_required_db_variable_fails(
    clean_db_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key, value in REQUIRED_DB_ENV.items():
        if key == "DB_HOST":
            continue
        monkeypatch.setenv(key, value)

    with pytest.raises(SettingsError, match="DB_HOST"):
        load_settings()


def test_adobe_not_required_for_db_settings(
    clean_db_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key, value in REQUIRED_DB_ENV.items():
        monkeypatch.setenv(key, value)

    settings = load_settings()
    assert settings.adobe_client_id is None
    assert settings.adobe_client_secret is None


def test_logical_database_mapping(clean_db_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_DB_ENV.items():
        monkeypatch.setenv(key, value)
    settings = load_settings()

    assert settings.database_name("raw") == "raw_data"
    assert settings.database_name("calc") == "calc_data"
    assert settings.database_name("cdm") == "cdm_data"
    assert settings.database_name("RAW") == "raw_data"


def test_unknown_logical_database_rejected(
    clean_db_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key, value in REQUIRED_DB_ENV.items():
        monkeypatch.setenv(key, value)
    settings = load_settings()

    with pytest.raises(SettingsError, match="Unsupported logical database"):
        settings.database_name("weekly")


def test_password_and_adobe_secret_absent_from_repr(
    clean_db_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key, value in REQUIRED_DB_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", "synthetic-adobe-secret")

    settings = load_settings()
    rendered = repr(settings)
    as_text = str(settings)

    assert "test-secret-password-do-not-leak" not in rendered
    assert "test-secret-password-do-not-leak" not in as_text
    assert "synthetic-adobe-secret" not in rendered
    assert "synthetic-adobe-secret" not in as_text
    assert "db_password=***" in rendered
    assert "adobe_client_secret=***" in rendered


def test_explicit_env_file_loading(
    clean_db_env: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env_path = tmp_path / "synthetic.env"
    env_path.write_text(
        "\n".join(
            [
                "DB_HOST=10.0.0.8",
                "DB_PORT=5432",
                "DB_USER=file_user",
                "DB_PASSWORD=file-secret-password",
                "RAW_DB_NAME=raw_from_file",
                "CALC_DB_NAME=calc_from_file",
                "CDM_DB_NAME=cdm_from_file",
                "ADOBE_CLIENT_SECRET=file-adobe-secret",
                "",
            ]
        ),
        encoding="utf-8",
    )

    # Ambient process values must not be required when explicit file is provided,
    # and must not silently come from a historical .env path.
    assert "HISTORICAL" not in str(env_path).upper()

    settings = load_settings(env_file=env_path)
    assert settings.db_host == "10.0.0.8"
    assert settings.db_port == 5432
    assert settings.db_user == "file_user"
    assert settings.raw_db_name == "raw_from_file"
    assert "file-secret-password" not in repr(settings)
    assert "file-adobe-secret" not in repr(settings)


def test_missing_env_file_fails(clean_db_env: None, tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.env"
    with pytest.raises(SettingsError, match="Environment file not found"):
        load_settings(env_file=missing)


def test_no_automatic_cwd_dotenv_loading(
    clean_db_env: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Without env_file, a local .env in cwd must not be auto-loaded."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(f"{k}={v}" for k, v in REQUIRED_DB_ENV.items()) + "\n",
        encoding="utf-8",
    )
    # Process env remains empty for required keys → must fail (no silent .env load).
    with pytest.raises(SettingsError):
        load_settings()
