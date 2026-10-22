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


# First FastBCP release accepting each ADBC connection type
ADBC_MIN_VERSIONS: Dict[str, str] = {
    "adbc_mssql": "1.0.0.0",
    "adbc_pgsql": "1.1.0.0",
    "adbc_oracle": "1.2.0.0",
    "adbc_netezza": "1.3.0.0",
    "adbc_teradata": "1.4.0.0",
}

# Engine-specific methods replaced by Physical in FastBCP 1.4 (removed in 2.0)
DEPRECATED_PHYSICAL_METHODS = frozenset(["Ctid", "Rowid", "Physloc", "NZDataSlice"])


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
    "0.32.4.0": VersionCapabilities(
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
    "1.0.0.0": VersionCapabilities(
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
                "adbc_mssql",
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
    "1.1.0.0": VersionCapabilities(
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
                "adbc_mssql",
                "adbc_pgsql",
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
    "1.2.0.0": VersionCapabilities(
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
                "adbc_mssql",
                "adbc_pgsql",
                "adbc_oracle",
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
    "1.3.0.0": VersionCapabilities(
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
                "adbc_mssql",
                "adbc_pgsql",
                "adbc_oracle",
                "adbc_netezza",
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
                "DBPartition",
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
    "1.4.0.0": VersionCapabilities(
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
                "adbc_mssql",
                "adbc_pgsql",
                "adbc_oracle",
                "adbc_netezza",
                "adbc_teradata",
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
                "DBPartition",
                "Ntile",
                "NZDataSlice",
                "None",
                "Physical",
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
    ver_str = str(detected_version) if detected_version else "unknown"

    # ADBC connection types were added one per release
    source_type = ((params.get("source") or {}).get("type") or "").lower()
    if source_type in ADBC_MIN_VERSIONS and source_type not in capabilities.source_types:
        warnings.append(
            f"Connection type '{source_type}' requires FastBCP "
            f"{ADBC_MIN_VERSIONS[source_type]}+, but detected version is {ver_str}"
        )

    method = (params.get("options") or {}).get("method")
    if method == "DBPartition" and method not in capabilities.parallelism_methods:
        warnings.append(
            f"Parallel method 'DBPartition' requires FastBCP 1.3.0.0+, "
            f"but detected version is {ver_str}"
        )
    if method == "Physical" and method not in capabilities.parallelism_methods:
        warnings.append(
            f"Parallel method 'Physical' requires FastBCP 1.4.0.0+, "
            f"but detected version is {ver_str}. Use the engine-specific method "
            f"(Ctid, Rowid, Physloc or NZDataSlice) instead"
        )
    if (
        method in DEPRECATED_PHYSICAL_METHODS
        and "Physical" in capabilities.parallelism_methods
    ):
        warnings.append(
            f"Parallel method '{method}' is deprecated since FastBCP 1.4 and will "
            f"be removed in 2.0: use 'Physical', which resolves to the same "
            f"strategy from the connection type"
        )

    return warnings
