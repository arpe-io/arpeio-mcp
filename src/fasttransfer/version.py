"""
Version capabilities registry for FastTransfer.

This module defines the version registry mapping FastTransfer versions
to their known capabilities (supported source/target types, parallelism
methods, and feature flags).
"""

from dataclasses import dataclass, replace
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
    # 0.18.2+: no trial mode, a licence file is always required
    requires_license: bool = False


# Parallel methods added after 0.16, with the first version that accepts them
METHOD_MIN_VERSIONS: Dict[str, str] = {
    "DBPartition": "0.18.0.0",
    "Timepartition": "0.18.0.0",
    "Physical": "1.0.0.0",
}

# ADBC connection types (source and target), all added in FastTransfer 1.0
ADBC_TYPES = frozenset(
    [
        "adbc_mssql",
        "adbc_pgsql",
        "adbc_oracle",
        "adbc_netezza",
        "adbc_db2",
        "adbc_teradata",
    ]
)
ADBC_MIN_VERSION = "1.0.0.0"

# Engine-specific methods replaced by Physical in FastTransfer 1.0 (removed in 2.0)
DEPRECATED_PHYSICAL_METHODS = frozenset(["Ctid", "Rowid", "Physloc", "NZDataSlice"])

# Targets whose --useworktables merge failed before 1.0: nothing was loaded
# and the run still exited 0.
WORK_TABLES_BROKEN_TARGETS = frozenset(["msbulk", "pgcopy"])


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
    "0.17.1.0": VersionCapabilities(
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
    # 0.18.0: DBPartition and Timepartition parallel methods,
    # FASTTRANSFER_DATEFORMAT environment variable (Timepartition date bounds)
    "0.18.0.0": VersionCapabilities(
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
        supports_nobanner=True,
        supports_version_flag=True,
        supports_file_input=True,
        supports_settings_file=True,
        supports_license_path=True,
    ),
    # 0.18.1: security/maintenance release (patched .NET dependencies, Ctid /
    # pgcopy snapshot consistency); no CLI changes
    "0.18.1.0": VersionCapabilities(
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
        supports_nobanner=True,
        supports_version_flag=True,
        supports_file_input=True,
        supports_settings_file=True,
        supports_license_path=True,
    ),
}

# 0.18.2: licensing release. No trial mode: a licence file is always required
# (exit code 99 without one; only --help and --version run). The default
# FastTransfer.lic is looked up in the current directory, then next to the
# binary, and multi-block licence files (MigratorXpress licences) are accepted.
# No CLI change.
VERSION_REGISTRY["0.18.2.0"] = replace(
    VERSION_REGISTRY["0.18.1.0"], requires_license=True
)


# 1.0.0: ADBC connection types (adbc_*) as sources and targets, backed by the
# bundled Arpe ADBC drivers (both ends must be adbc_*; Windows x64 and Linux x64).
# Physical parallel method, resolved to Ctid / Rowid / Physloc / NZDataSlice from
# the source connection type; those four names are deprecated (removed in 2.0).
# The sets below are those of the 1.0.0 argument parser (the Netezza copy source
# is nzbulk).
VERSION_REGISTRY["1.0.0.0"] = VersionCapabilities(
    source_types=frozenset(
        [
            "clickhouse",
            "duckdb",
            "duckdbstream",
            "hana",
            "mssql",
            "msoledbsql",
            "mysql",
            "nzbulk",
            "nzoledb",
            "nzsql",
            "odbc",
            "oledb",
            "oraodp",
            "pgcopy",
            "pgsql",
            "teradata",
            *ADBC_TYPES,
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
            *ADBC_TYPES,
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
    supports_nobanner=True,
    supports_version_flag=True,
    supports_file_input=True,
    supports_settings_file=True,
    supports_license_path=True,
    requires_license=True,
)


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
    ver_str = str(detected_version) if detected_version else "unknown"

    source = params.get("source") or {}
    target = params.get("target") or {}
    options = params.get("options") or {}
    source_type = (source.get("type") or "").strip().lower()
    target_type = (target.get("type") or "").strip().lower()
    physical_supported = "Physical" in capabilities.parallelism_methods

    # ADBC connection types (FastTransfer 1.0+)
    for side, conn_type, known in (
        ("source", source_type, capabilities.source_types),
        ("target", target_type, capabilities.target_types),
    ):
        if conn_type in ADBC_TYPES and conn_type not in known:
            warnings.append(
                f"Connection type '{conn_type}' ({side}) requires FastTransfer "
                f"{ADBC_MIN_VERSION}+, but detected version is {ver_str}"
            )

    # Parallel methods added in FastTransfer 0.18 and 1.0
    method = options.get("method")
    if method in METHOD_MIN_VERSIONS and method not in capabilities.parallelism_methods:
        hint = (
            ". Use the engine-specific method (Ctid, Rowid, Physloc or NZDataSlice) instead"
            if method == "Physical"
            else ""
        )
        warnings.append(
            f"Parallel method '{method}' requires FastTransfer "
            f"{METHOD_MIN_VERSIONS[method]}+, but detected version is {ver_str}{hint}"
        )

    if method in DEPRECATED_PHYSICAL_METHODS:
        if physical_supported and source_type == "odbc":
            # Physical refuses odbc (the engine behind the driver is unknown):
            # the deprecated Physloc is the only physical split there.
            pass
        elif physical_supported:
            warnings.append(
                f"Parallel method '{method}' is deprecated since FastTransfer 1.0 and "
                f"will be removed in 2.0: use 'Physical', which resolves to the same "
                f"strategy from the source connection type"
            )
        elif method == "NZDataSlice" and source_type in ("nzbulk", "nzoledb"):
            warnings.append(
                f"Parallel method 'NZDataSlice' with a {source_type} source requires "
                f"FastTransfer 1.0.0.0+, but detected version is {ver_str}: "
                f"use the nzsql connection type"
            )
        elif (
            method == "NZDataSlice"
            and source_type
            and not (source.get("schema") and source.get("table"))
        ):
            warnings.append(
                f"Parallel method 'NZDataSlice' requires the source schema and table "
                f"before FastTransfer 1.0.0.0, and detected version is {ver_str}"
            )
        elif method == "Physloc" and source_type == "msoledbsql":
            warnings.append(
                f"Parallel method 'Physloc' with a msoledbsql source requires "
                f"FastTransfer 1.0.0.0+, but detected version is {ver_str}: "
                f"use the mssql connection type"
            )

    # --useworktables: before 1.0 the merge into an msbulk / pgcopy target
    # failed, the error was only logged and the run exited 0 with an empty target
    if (
        options.get("use_work_tables")
        and target_type in WORK_TABLES_BROKEN_TARGETS
        and not physical_supported
    ):
        warnings.append(
            f"use_work_tables with a {target_type} target loses the data on "
            f"FastTransfer older than 1.0.0.0 (detected version is {ver_str}): the "
            f"merge fails, nothing is loaded and the exit code is still 0. Remove "
            f"use_work_tables or upgrade to 1.0.0+"
        )

    return warnings
