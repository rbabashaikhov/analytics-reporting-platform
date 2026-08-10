"""Typed client-specific Adobe configuration loaded from an explicit YAML path.

OAuth secrets remain in environment settings. This module holds report suite /
segment / section identifier mappings only.

No automatic discovery of CWD or historical notebook directories.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml


class ClientConfigError(ValueError):
    """Raised when client configuration is missing, malformed, or incomplete."""


def _require_mapping(name: str, value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ClientConfigError(f"Invalid client configuration: {name} must be a mapping")
    return value


def _optional_id(logical_name: str, value: Any) -> Optional[str]:
    """Normalize a segment/section ID value.

    ``null`` / missing-as-None means "no Adobe segment filter".
    Empty strings are rejected (ambiguous).
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ClientConfigError(
            f"Invalid client configuration: value for {logical_name!r} must be a string or null"
        )
    text = value.strip()
    if not text:
        raise ClientConfigError(
            f"Invalid client configuration: empty identifier for {logical_name!r}"
        )
    return text


def _parse_id_map(category: str, raw: Any) -> dict[str, Optional[str]]:
    mapping = _require_mapping(category, raw)
    parsed: dict[str, Optional[str]] = {}
    for key, value in mapping.items():
        if not isinstance(key, str) or not key.strip():
            raise ClientConfigError(
                f"Invalid client configuration: {category} keys must be non-empty strings"
            )
        logical = key.strip()
        parsed[logical] = _optional_id(f"{category}.{logical}", value)
    return parsed


@dataclass(frozen=True)
class AdobeClientConfig:
    """Client-specific Adobe Analytics identifiers (non-secret, still private)."""

    report_suite_id: str
    segments: Mapping[str, Optional[str]] = field(default_factory=dict)
    section_traffic_segments: Mapping[str, Optional[str]] = field(default_factory=dict)
    sections: Mapping[str, Optional[str]] = field(default_factory=dict)
    page_url_dimension: Optional[str] = None

    def segment_id(self, logical_name: str) -> Optional[str]:
        """Return a segment ID for an arbitrary logical key from config."""
        return _lookup(self.segments, "segments", logical_name)

    def section_traffic_segment_id(self, logical_name: str) -> Optional[str]:
        """Return a section-traffic segment ID for an arbitrary logical key."""
        return _lookup(self.section_traffic_segments, "section_traffic_segments", logical_name)

    def section_id(self, logical_name: str) -> Optional[str]:
        """Return a section ID for an arbitrary logical key from config."""
        return _lookup(self.sections, "sections", logical_name)

    def require_page_url_dimension(self) -> str:
        """Return the Adobe dimension id used for private by-URL extraction."""
        if not self.page_url_dimension or not str(self.page_url_dimension).strip():
            raise ClientConfigError(
                "Missing required client configuration: adobe.page_url_dimension"
            )
        return str(self.page_url_dimension).strip()

    def __repr__(self) -> str:
        return (
            "AdobeClientConfig("
            "report_suite_id=***, "
            f"segments={_mask_map(self.segments)}, "
            f"section_traffic_segments={_mask_map(self.section_traffic_segments)}, "
            f"sections={_mask_map(self.sections)}, "
            "page_url_dimension=***)"
        )

    __str__ = __repr__


def _lookup(
    mapping: Mapping[str, Optional[str]], category: str, logical_name: str
) -> Optional[str]:
    key = logical_name.strip()
    if key not in mapping:
        raise ClientConfigError(f"Unknown {category} logical name: {logical_name!r}")
    return mapping[key]


def _mask_map(mapping: Mapping[str, Optional[str]]) -> str:
    parts = []
    for key, value in mapping.items():
        parts.append(f"{key!r}: {'null' if value is None else '***'}")
    return "{" + ", ".join(parts) + "}"


def load_client_config(path: Path | str) -> AdobeClientConfig:
    """Load Adobe client configuration from an explicit filesystem path."""
    if path is None:  # type: ignore[comparison-overlap]
        raise ClientConfigError("Client configuration path is required")
    config_path = Path(path)
    if not config_path.is_file():
        raise ClientConfigError(f"Client configuration file not found: {config_path.name}")

    try:
        raw_text = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ClientConfigError("Unable to read client configuration file") from exc

    try:
        loaded = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ClientConfigError("Malformed client configuration YAML") from exc

    if not isinstance(loaded, Mapping):
        raise ClientConfigError("Invalid client configuration: root must be a mapping")

    adobe_raw = loaded.get("adobe")
    adobe = _require_mapping("adobe", adobe_raw)

    rsid_raw = adobe.get("report_suite_id")
    if not isinstance(rsid_raw, str) or not rsid_raw.strip():
        raise ClientConfigError("Missing required client configuration: adobe.report_suite_id")

    segments = _parse_id_map("adobe.segments", adobe.get("segments", {}))
    section_traffic = _parse_id_map(
        "adobe.section_traffic_segments", adobe.get("section_traffic_segments", {})
    )
    sections = _parse_id_map("adobe.sections", adobe.get("sections", {}))

    page_dim_raw = adobe.get("page_url_dimension")
    page_url_dimension: Optional[str]
    if page_dim_raw is None or (isinstance(page_dim_raw, str) and not page_dim_raw.strip()):
        page_url_dimension = None
    elif isinstance(page_dim_raw, str):
        page_url_dimension = page_dim_raw.strip()
    else:
        raise ClientConfigError(
            "Invalid client configuration: adobe.page_url_dimension must be a string or null"
        )

    return AdobeClientConfig(
        report_suite_id=rsid_raw.strip(),
        segments=segments,
        section_traffic_segments=section_traffic,
        sections=sections,
        page_url_dimension=page_url_dimension,
    )


__all__ = [
    "AdobeClientConfig",
    "ClientConfigError",
    "load_client_config",
]
