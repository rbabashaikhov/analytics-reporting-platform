"""CLI package for Analytics Reporting Platform."""

from analytics_reporting_platform.cli.main import main
from analytics_reporting_platform.cli.monthly import execute_monthly_cli

__all__ = ["execute_monthly_cli", "main"]
