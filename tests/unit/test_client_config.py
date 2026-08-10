"""Unit tests for client-specific Adobe YAML configuration.

Uses synthetic, anonymized logical keys only (no historical client taxonomy).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from analytics_reporting_platform.config import (
    AdobeClientConfig,
    ClientConfigError,
    load_client_config,
)


SYNTHETIC_RSID = "synthetic-rsid-not-real"
SYNTHETIC_SEGMENT = "synthetic-segment-id-not-real"
SYNTHETIC_SECTION = "synthetic-section-id-not-real"


def _write_config(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def test_valid_config_loads(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "client.yaml",
        f"""
adobe:
  report_suite_id: "{SYNTHETIC_RSID}"
  segments:
    segment_1: null
    segment_2: "{SYNTHETIC_SEGMENT}"
  section_traffic_segments:
    traffic_segment_1: null
  sections:
    product_1: "{SYNTHETIC_SECTION}"
""",
    )
    cfg = load_client_config(path)
    assert isinstance(cfg, AdobeClientConfig)
    assert cfg.report_suite_id == SYNTHETIC_RSID
    assert cfg.segment_id("segment_1") is None
    assert cfg.segment_id("segment_2") == SYNTHETIC_SEGMENT
    assert cfg.section_id("product_1") == SYNTHETIC_SECTION


def test_missing_report_suite_fails(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "client.yaml",
        """
adobe:
  report_suite_id: ""
  segments: {}
  sections: {}
""",
    )
    with pytest.raises(ClientConfigError, match="report_suite_id"):
        load_client_config(path)


def test_malformed_yaml_fails(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "bad.yaml", "adobe: [\n  - broken")
    with pytest.raises(ClientConfigError, match="Malformed"):
        load_client_config(path)


def test_unknown_logical_name_rejected(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "client.yaml",
        f"""
adobe:
  report_suite_id: "{SYNTHETIC_RSID}"
  segments:
    segment_2: "{SYNTHETIC_SEGMENT}"
  sections:
    product_1: "{SYNTHETIC_SECTION}"
""",
    )
    cfg = load_client_config(path)
    with pytest.raises(ClientConfigError, match="Unknown segments"):
        cfg.segment_id("does_not_exist")
    with pytest.raises(ClientConfigError, match="Unknown sections"):
        cfg.section_id("does_not_exist")


def test_section_traffic_lookup(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "client.yaml",
        f"""
adobe:
  report_suite_id: "{SYNTHETIC_RSID}"
  section_traffic_segments:
    traffic_segment_1: null
    traffic_segment_2: "{SYNTHETIC_SEGMENT}"
  sections: {{}}
""",
    )
    cfg = load_client_config(path)
    assert cfg.section_traffic_segment_id("traffic_segment_1") is None
    assert cfg.section_traffic_segment_id("traffic_segment_2") == SYNTHETIC_SEGMENT


def test_explicit_path_required_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "nope.yaml"
    with pytest.raises(ClientConfigError, match="not found"):
        load_client_config(missing)


def test_no_implicit_cwd_discovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "client-config.yaml").write_text(
        f'adobe:\n  report_suite_id: "{SYNTHETIC_RSID}"\n',
        encoding="utf-8",
    )
    with pytest.raises(TypeError):
        load_client_config()  # type: ignore[call-arg]


def test_repr_masks_identifiers(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "client.yaml",
        f"""
adobe:
  report_suite_id: "{SYNTHETIC_RSID}"
  segments:
    segment_2: "{SYNTHETIC_SEGMENT}"
  sections:
    product_1: "{SYNTHETIC_SECTION}"
""",
    )
    cfg = load_client_config(path)
    rendered = repr(cfg) + str(cfg)
    assert SYNTHETIC_RSID not in rendered
    assert SYNTHETIC_SEGMENT not in rendered
    assert SYNTHETIC_SECTION not in rendered
    assert "report_suite_id=***" in rendered


def test_empty_segment_string_rejected(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "client.yaml",
        f"""
adobe:
  report_suite_id: "{SYNTHETIC_RSID}"
  segments:
    segment_2: ""
""",
    )
    with pytest.raises(ClientConfigError, match="empty identifier"):
        load_client_config(path)


def test_error_does_not_echo_ids(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "client.yaml",
        f"""
adobe:
  report_suite_id: "{SYNTHETIC_RSID}"
  segments:
    segment_2: "{SYNTHETIC_SEGMENT}"
""",
    )
    cfg = load_client_config(path)
    with pytest.raises(ClientConfigError) as exc_info:
        cfg.segment_id("missing_key")
    assert SYNTHETIC_SEGMENT not in str(exc_info.value)
    assert SYNTHETIC_RSID not in str(exc_info.value)


def test_arbitrary_logical_keys_accepted(tmp_path: Path) -> None:
    """Public loader must accept any validated string keys (no fixed taxonomy)."""
    path = _write_config(
        tmp_path / "client.yaml",
        f"""
adobe:
  report_suite_id: "{SYNTHETIC_RSID}"
  segments:
    custom_channel_a: "{SYNTHETIC_SEGMENT}"
  sections:
    content_section_9: "{SYNTHETIC_SECTION}"
""",
    )
    cfg = load_client_config(path)
    assert cfg.segment_id("custom_channel_a") == SYNTHETIC_SEGMENT
    assert cfg.section_id("content_section_9") == SYNTHETIC_SECTION
