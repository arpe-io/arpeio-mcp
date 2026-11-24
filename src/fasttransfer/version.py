"""
Version capabilities registry for FastTransfer.

This module defines the version registry mapping FastTransfer versions
to their known capabilities (supported source/target types, parallelism
methods, and feature flags).
"""

import re
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
    # 1.1+: --loadmode Create (and Replace as its alias) and --inferdatatype
    supports_create_load_mode: bool = False
    # 1.1+: host:port accepted as a SQL Server --sourceserver / --targetserver
    # by the native types (handed to the driver as host,port)
    supports_mssql_host_colon_port: bool = False


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

# Backup source types (SQL Server .bak files, Oracle RMAN backup sets), added
# in FastTransfer 1.1, with --loadmode Create and --inferdatatype
BACKUP_TYPES = frozenset(["backup_mssql", "backup_rman"])
BACKUP_MIN_VERSION = "1.1.0.0"
CREATE_MIN_VERSION = "1.1.0.0"
CREATE_LOAD_MODES = frozenset(["Create", "Replace"])

# Native connection types whose server is a SQL Server name: host:port is
# accepted from 1.1 (adbc_mssql took it from 1.0)
MSSQL_NATIVE_SERVER_TYPES = frozenset(["mssql", "msbulk", "msoledbsql"])

# Engine-specific methods replaced by Physical in FastTransfer 1.0 (removed in 2.0)
DEPRECATED_PHYSICAL_METHODS = frozenset(["Ctid", "Rowid", "Physloc", "NZDataSlice"])
PHYSICAL_METHODS = DEPRECATED_PHYSICAL_METHODS | {"Physical"}

# host:port, host a name, an IPv4 address or a bracketed IPv6 address
_HOST_COLON_PORT = re.compile(r"^(?:\[[^\]]+\]|[^:,\\\[\]\s]+):\d{1,5}$")

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

# 1.1.0: --loadmode Create (creates the target table from the source when it
# does not exist, truncates it when it exists; Replace is its alias, it behaved
# like Append before) with --inferdatatype allvarchar | fast | best. Backup
# source types backup_mssql and backup_rman, to an adbc_* target, with the
# methods None and Physical (resolved to Pages). FASTTRANSFER_ADBC_SSLMODE for
# adbc_netezza / adbc_pgsql. SQL Server server names: host:port accepted, the
# tcp: prefix and host\INSTANCE:port refused before any connection. Same target
# types and parallel methods as 1.0.0.
VERSION_REGISTRY["1.1.0.0"] = replace(
    VERSION_REGISTRY["1.0.0.0"],
    source_types=VERSION_REGISTRY["1.0.0.0"].source_types | BACKUP_TYPES,
    supports_create_load_mode=True,
    supports_mssql_host_colon_port=True,
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

    # Backup source types (FastTransfer 1.1+)
    if source_type in BACKUP_TYPES and source_type not in capabilities.source_types:
        warnings.append(
            f"Connection type '{source_type}' (source) requires FastTransfer "
            f"{BACKUP_MIN_VERSION}+, but detected version is {ver_str}"
        )

    # --loadmode Create and --inferdatatype (FastTransfer 1.1+)
    load_mode = options.get("load_mode")
    infer = options.get("infer_data_type")
    creates = load_mode in CREATE_LOAD_MODES
    if not capabilities.supports_create_load_mode:
        if load_mode == "Create":
            warnings.append(
                f"Load mode 'Create' requires FastTransfer {CREATE_MIN_VERSION}+, but "
                f"detected version is {ver_str}: create the target table first and "
                f"use 'Truncate' or 'Append'"
            )
        elif load_mode == "Replace":
            warnings.append(
                f"Load mode 'Replace' behaves like 'Append' before FastTransfer "
                f"{CREATE_MIN_VERSION} (detected version is {ver_str}): the target "
                f"table is neither created nor truncated"
            )
        if infer:
            warnings.append(
                f"infer_data_type (--inferdatatype) requires FastTransfer "
                f"{CREATE_MIN_VERSION}+, but detected version is {ver_str}"
            )
    elif infer and not creates:
        warnings.append(
            f"infer_data_type '{infer}' is only used with load mode 'Create': "
            f"FastTransfer ignores it, with a warning, with load mode "
            f"'{load_mode or 'Append'}'"
        )
    elif creates and infer == "allvarchar" and source_type in ("odbc", "oledb"):
        warnings.append(
            f"infer_data_type 'allvarchar' needs a source whose engine is known, "
            f"not a generic {source_type} source: when the target table has to be "
            f"created, use 'fast' or a native connection type"
        )
    elif (
        creates
        and infer in ("allvarchar", "best")
        and source.get("query")
        and options.get("method") in PHYSICAL_METHODS
    ):
        warnings.append(
            f"infer_data_type '{infer}' rewrites the source query, which FastTransfer "
            f"can refuse for a query read with the Physical method (it must be a "
            f"single SELECT listing its columns): give the source schema and table "
            f"without a query, or use 'fast'"
        )

    # SQL Server host:port: before 1.1 the native types took it for a host name
    # and failed after the connect timeout (adbc_mssql accepts it from 1.0)
    if not capabilities.supports_mssql_host_colon_port:
        for side, conn, conn_type in (
            ("source", source, source_type),
            ("target", target, target_type),
        ):
            server = (conn.get("server") or "").strip()
            sql_server = conn_type in MSSQL_NATIVE_SERVER_TYPES or (
                conn_type == "oledb"
                and (conn.get("provider") or "").strip().upper().startswith(
                    ("MSOLEDBSQL", "SQLOLEDB", "SQLNCLI")
                )
            )
            if sql_server and not conn.get("connect_string") and _HOST_COLON_PORT.match(server):
                warnings.append(
                    f"SQL Server {side} server '{server}' ({conn_type}): the "
                    f"host:port form requires FastTransfer 1.1.0.0+, but detected "
                    f"version is {ver_str}, which takes it for a host name and fails "
                    f"after the connect timeout. Write it 'host,port'"
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
