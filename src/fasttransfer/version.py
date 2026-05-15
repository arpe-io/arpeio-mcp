"""
Version capabilities registry for FastTransfer.

This module defines the version registry mapping FastTransfer versions
to their known capabilities (supported source/target types, parallelism
methods, and feature flags).
"""

from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional

from ..base.version_detector import ToolVersion


@dataclass(frozen=True)
class VersionCapabilities:
    """Capabilities available in a specific FastTransfer version."""

    source_types: FrozenSet[str]
    target_types: FrozenSet[str]
    parallelism_methods: FrozenSet[str]
    supports_nobanner: bool = False
    supports_version_flag: bool = False
    supports_file_input: bool = False
    supports_settings_file: bool = False
    supports_license_path: bool = False


# Static version registry: version string -> capabilities
VERSION_REGISTRY: Dict[str, VersionCapabilities] = {
    "0.16.0.0": VersionCapabilities(
        source_types=frozenset(
            [
                "clickhouse",
                "duckdb",
                "duckdbstream",
                "hana",
                "mssql",
                "msoledbsql",
                "mysql",
                "nzoledb",
                "nzsql",
                "nzcopy",
                "odbc",
                "oledb",
                "oraodp",
                "pgcopy",
                "pgsql",
                "teradata",
            ]
        ),
        target_types=frozenset(
            [
                "clickhousebulk",
                "duckdb",
                "hanabulk",
                "msbulk",
                "mysqlbulk",
                "nzbulk",
                "orabulk",
                "oradirect",
                "pgcopy",
                "pgsql",
                "teradata",
            ]
        ),
        parallelism_methods=frozenset(
            [
                "Ctid",
                "DataDriven",
                "Ntile",
                "NZDataSlice",
                "None",
                "Physloc",
                "Random",
                "RangeId",
                "Rowid",
            ]
        ),
        supports_nobanner=True,
        supports_version_flag=True,
        supports_file_input=True,
        supports_settings_file=True,
        supports_license_path=True,
    ),
    "0.17.0.0": VersionCapabilities(
        source_types=frozenset(
            [
                "clickhouse",
                "duckdb",
                "duckdbstream",
                "hana",
                "mssql",
                "msoledbsql",
                "mysql",
                "nzoledb",
                "nzsql",
                "nzcopy",
                "odbc",
                "oledb",
                "oraodp",
                "pgcopy",
                "pgsql",
                "teradata",
            ]
        ),
        target_types=frozenset(
            [
                "clickhousebulk",
                "duckdb",
                "hanabulk",
                "msbulk",
                "mysqlbulk",
                "nzbulk",
                "orabulk",
                "oradirect",
                "pgcopy",
                "pgsql",
                "teradata",
            ]
        ),
        parallelism_methods=frozenset(
            [
                "Ctid",
                "DataDriven",
                "Ntile",
                "NZDataSlice",
                "None",
                "Physloc",
                "Random",
                "RangeId",
                "Rowid",
            ]
        ),
        supports_nobanner=True,
        supports_version_flag=True,
        supports_file_input=True,
        supports_settings_file=True,
        supports_license_path=True,
    ),
}


def check_version_compatibility(
    params: dict,
    capabilities: VersionCapabilities,
    detected_version: Optional[ToolVersion],
) -> list[str]:
    """Check for version-gated features and return warning strings.

    Args:
        params: The parameters dict for the command
        capabilities: Resolved capabilities for the detected version
        detected_version: The detected version, or None

    Returns:
        List of warning strings (empty if all OK)
    """
    warnings: list[str] = []

    # No version-gated features yet -- add checks here as they appear
    # Example pattern:
    # if params.get("some_feature") and not capabilities.supports_some_feature:
    #     ver_str = str(detected_version) if detected_version else "unknown"
    #     warnings.append(f"--some_feature requires FastTransfer X.Y.Z.W+, but detected version is {ver_str}")

    return warnings
