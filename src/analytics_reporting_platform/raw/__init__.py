"""RAW layer — Monthly Adobe extraction + PostgreSQL persistence.

``raw_aa_monthly_by_url`` is a full-private operational mart (excluded from public snapshot).
"""

from analytics_reporting_platform.raw.models import (
    MonthlyBySectionResult,
    MonthlyByUrlResult,
    MonthlyMetricsResult,
    MonthInput,
    RawExtractionError,
    adobe_month_date_range,
    month_date_range_bounds,
    parse_month_start,
)
from analytics_reporting_platform.raw.monthly_by_section import (
    BUSINESS_KEY_COLUMNS as BY_SECTION_BUSINESS_KEY_COLUMNS,
)
from analytics_reporting_platform.raw.monthly_by_section import (
    BY_SECTION_METRIC_ID,
    MonthlyBySectionRequestSpec,
    build_monthly_by_section_requests,
    build_section_payload,
    extract_monthly_by_section,
)
from analytics_reporting_platform.raw.monthly_by_section import (
    OUTPUT_COLUMNS as BY_SECTION_OUTPUT_COLUMNS,
)
from analytics_reporting_platform.raw.monthly_by_url import (
    BUSINESS_KEY_COLUMNS as BY_URL_BUSINESS_KEY_COLUMNS,
)
from analytics_reporting_platform.raw.monthly_by_url import (
    BY_URL_METRIC_ID,
    DEFAULT_URL_LIMIT,
    MonthlyByUrlRequestSpec,
    build_monthly_by_url_requests,
    build_url_payload,
    extract_monthly_by_url,
)
from analytics_reporting_platform.raw.monthly_by_url import (
    OUTPUT_COLUMNS as BY_URL_OUTPUT_COLUMNS,
)
from analytics_reporting_platform.raw.monthly_metrics import (
    BUSINESS_KEY_COLUMNS,
    MONTHLY_METRICS_METRIC_MAP,
    OUTPUT_COLUMNS,
    MonthlyMetricsRequestSpec,
    build_metrics_payload,
    build_monthly_metrics_requests,
    extract_monthly_metrics,
    metric_names_for_segment,
)
from analytics_reporting_platform.raw.wordstat_brand_queries import (
    BUSINESS_KEY_COLUMNS as WORDSTAT_BRAND_BUSINESS_KEY_COLUMNS,
    OUTPUT_COLUMNS as WORDSTAT_BRAND_OUTPUT_COLUMNS,
    REQUIRED_QUERIES as WORDSTAT_REQUIRED_QUERIES,
    WordstatBrandQueriesResult,
    extract_wordstat_brand_queries,
    extract_wordstat_brand_queries_range,
)
from analytics_reporting_platform.raw.repository import (
    MART_SPECS,
    MartSpec,
    PersistMonthResult,
    RawPersistenceError,
    count_month_rows,
    month_exists,
    persist_month,
    resolve_mart_spec,
    validate_persist_dataframe,
)

__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "BY_SECTION_BUSINESS_KEY_COLUMNS",
    "BY_SECTION_METRIC_ID",
    "BY_SECTION_OUTPUT_COLUMNS",
    "BY_URL_BUSINESS_KEY_COLUMNS",
    "BY_URL_METRIC_ID",
    "BY_URL_OUTPUT_COLUMNS",
    "DEFAULT_URL_LIMIT",
    "MART_SPECS",
    "MONTHLY_METRICS_METRIC_MAP",
    "OUTPUT_COLUMNS",
    "MartSpec",
    "MonthInput",
    "MonthlyBySectionRequestSpec",
    "MonthlyBySectionResult",
    "MonthlyByUrlRequestSpec",
    "MonthlyByUrlResult",
    "MonthlyMetricsRequestSpec",
    "MonthlyMetricsResult",
    "PersistMonthResult",
    "RawExtractionError",
    "RawPersistenceError",
    "WORDSTAT_BRAND_BUSINESS_KEY_COLUMNS",
    "WORDSTAT_BRAND_OUTPUT_COLUMNS",
    "WORDSTAT_REQUIRED_QUERIES",
    "WordstatBrandQueriesResult",
    "adobe_month_date_range",
    "build_metrics_payload",
    "build_monthly_by_section_requests",
    "build_monthly_by_url_requests",
    "build_monthly_metrics_requests",
    "build_section_payload",
    "build_url_payload",
    "count_month_rows",
    "extract_monthly_by_section",
    "extract_monthly_by_url",
    "extract_monthly_metrics",
    "extract_wordstat_brand_queries",
    "extract_wordstat_brand_queries_range",
    "metric_names_for_segment",
    "month_date_range_bounds",
    "month_exists",
    "parse_month_start",
    "persist_month",
    "resolve_mart_spec",
    "validate_persist_dataframe",
]
