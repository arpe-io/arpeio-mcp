"""
Version capabilities registry for FastBCP.

This module contains the version registry mapping FastBCP versions
to their known capabilities, plus a compatibility check function.
"""

from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional


@dataclass(frozen=True)
class VersionCapabilities:
    """Capabilities available in a specific FastBCP version."""

    source_types: FrozenSet[str]
    output_formats: FrozenSet[str]
    parallelism_methods: FrozenSet[str]
    storage_targets: FrozenSet[str]
    supports_nobanner: bool = False
    supports_version_flag: bool = False
    supports_cloud_profile: bool = False
    supports_merge: bool = False
    supports_config_file: bool = False


# Static version registry: version string -> capabilities
VERSION_REGISTRY: Dict[str, VersionCapabilities] = {
    "0.29.1.0": VersionCapabilities(
        source_types=frozenset(
            [
                "clickhouse",
                "hana",
                "mssql",
                "msoledbsql",
                "mysql",
                "nzcopy",
                "nzoledb",
                "nzsql",
                "odbc",
                "oledb",
                "oraodp",
                "pgcopy",
                "pgsql",
                "teradata",
            ]
        ),
        output_formats=frozenset(
            [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
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
        storage_targets=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "azure_blob",
                "azure_datalake",
                "fabric_onelake",
            ]
        ),
        supports_nobanner=True,
        supports_version_flag=True,
        supports_cloud_profile=True,
        supports_merge=True,
    ),
    "0.30.0.0": VersionCapabilities(
        source_types=frozenset(
            [
                "clickhouse",
                "hana",
                "mssql",
                "msoledbsql",
                "mysql",
                "nzcopy",
                "nzoledb",
                "nzsql",
                "odbc",
                "oledb",
                "oraodp",
                "pgcopy",
                "pgsql",
                "teradata",
            ]
        ),
        output_formats=frozenset(
            [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
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
                "Timepartition",
            ]
        ),
        storage_targets=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "azure_blob",
                "azure_datalake",
                "fabric_onelake",
                "gcs",
            ]
        ),
        supports_nobanner=True,
        supports_version_flag=True,
        supports_cloud_profile=True,
        supports_merge=True,
        supports_config_file=True,
    ),
    "0.31.0.0": VersionCapabilities(
        source_types=frozenset(
            [
                "clickhouse",
                "hana",
                "mssql",
                "msoledbsql",
                "mysql",
                "nzcopy",
                "nzoledb",
                "nzsql",
                "odbc",
                "oledb",
                "oraodp",
                "pgcopy",
                "pgsql",
                "teradata",
            ]
        ),
        output_formats=frozenset(
            [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
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
                "Timepartition",
            ]
        ),
        storage_targets=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "azure_blob",
                "azure_datalake",
                "fabric_onelake",
                "gcs",
            ]
        ),
        supports_nobanner=True,
        supports_version_flag=True,
        supports_cloud_profile=True,
        supports_merge=True,
        supports_config_file=True,
    ),
}


def check_version_compatibility(
    params: dict,
    capabilities: VersionCapabilities,
    detected_version,
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
    #     warnings.append(f"--some_feature requires FastBCP X.Y.Z.W+, but detected version is {ver_str}")

    return warnings
