"""
Version capabilities registry for FastBCP.

This module contains the version registry mapping FastBCP versions
to their known capabilities, plus a compatibility check function.
"""

from dataclasses import dataclass, replace
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
    # backup_mssql reads encrypted / TDE backups with the certificate's private
    # key given in --sourceuser / --sourcepassword (1.6.6+)
    supports_backup_encryption: bool = False
    # backup_rman --database takes paths and the ** pattern (1.6.8+)
    supports_backup_rman_paths: bool = False


# First FastBCP release accepting each ADBC connection type
ADBC_MIN_VERSIONS: Dict[str, str] = {
    "adbc_mssql": "1.0.0.0",
    "adbc_pgsql": "1.1.0.0",
    "adbc_oracle": "1.2.0.0",
    "adbc_netezza": "1.3.0.0",
    "adbc_teradata": "1.4.0.0",
    "adbc_db2": "1.7.0.0",
}

# First FastBCP release accepting each backup connection type
BACKUP_MIN_VERSIONS: Dict[str, str] = {
    "backup_mssql": "1.5.0.0",
    "backup_rman": "1.6.0.0",
}

# Every connection type added after 0.29, with the first release accepting it
SOURCE_TYPE_MIN_VERSIONS: Dict[str, str] = {**ADBC_MIN_VERSIONS, **BACKUP_MIN_VERSIONS}

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


def _extends(base: VersionCapabilities, new_sources=(), **flags) -> VersionCapabilities:
    """Capabilities of a release that adds connection types or features to ``base``."""
    return replace(base, source_types=base.source_types | frozenset(new_sources), **flags)


# 1.5: SQL Server backup files (backup_mssql, ArpeMSBackup). Parquet only,
# parallel methods None and Physical (resolved to Pages).
VERSION_REGISTRY["1.5.0.0"] = _extends(VERSION_REGISTRY["1.4.0.0"], ["backup_mssql"])
# 1.6.0: Oracle RMAN backup sets (backup_rman, ArpeOraBackup), same rules.
VERSION_REGISTRY["1.6.0.0"] = _extends(VERSION_REGISTRY["1.5.0.0"], ["backup_rman"])
# 1.6.6: encrypted and TDE SQL Server backups (private key in --sourceuser).
VERSION_REGISTRY["1.6.6.0"] = _extends(
    VERSION_REGISTRY["1.6.0.0"], supports_backup_encryption=True
)
# 1.6.8: incremental RMAN backups; backup_rman --database takes paths and **.
VERSION_REGISTRY["1.6.8.0"] = _extends(
    VERSION_REGISTRY["1.6.6.0"], supports_backup_rman_paths=True
)
# 1.7: IBM Db2 (adbc_db2, ArpeDb2); enumerated values case-insensitive;
# hanaexport removed (refused by --connectiontype long before).
VERSION_REGISTRY["1.7.0.0"] = _extends(VERSION_REGISTRY["1.6.8.0"], ["adbc_db2"])


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

    # ADBC and backup connection types were added one per release
    source = params.get("source") or {}
    source_type = (source.get("type") or "").strip().lower()
    if (
        source_type in SOURCE_TYPE_MIN_VERSIONS
        and source_type not in capabilities.source_types
    ):
        warnings.append(
            f"Connection type '{source_type}' requires FastBCP "
            f"{SOURCE_TYPE_MIN_VERSIONS[source_type]}+, but detected version is {ver_str}"
        )
    elif source_type == "backup_mssql" and source.get("user"):
        if not capabilities.supports_backup_encryption:
            warnings.append(
                f"Encrypted SQL Server backups (private key file in --sourceuser) "
                f"require FastBCP 1.6.6.0+, but detected version is {ver_str}"
            )
    elif source_type == "backup_rman" and _is_rman_path_pattern(source.get("database")):
        if not capabilities.supports_backup_rman_paths:
            warnings.append(
                f"backup_rman --database with paths or the '**' pattern requires "
                f"FastBCP 1.6.8.0+ (incremental RMAN backups), but detected version "
                f"is {ver_str}: give file names or patterns of the --server directory"
            )

    # Enumerated values are case-insensitive since FastBCP 1.7; the MCP
    # normalizes them to the canonical spelling, so compare the same way here.
    method = _canonical_method((params.get("options") or {}).get("method"))
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


def _canonical_method(method) -> Optional[str]:
    """Return the canonical spelling of a parallel method name (case-insensitive)."""
    if not isinstance(method, str):
        return method
    for known in _ALL_METHODS:
        if known.lower() == method.strip().lower():
            return known
    return method


def _is_rman_path_pattern(database) -> bool:
    """Whether a backup_rman --database uses paths or ** (FastBCP 1.6.8+)."""
    if not isinstance(database, str):
        return False
    return any(sep in database for sep in ("/", "\\", "**"))


_ALL_METHODS = sorted(
    {m for caps in VERSION_REGISTRY.values() for m in caps.parallelism_methods}
)
