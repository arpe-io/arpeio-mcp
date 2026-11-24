"""
FastTransfer command builder extending the base Arpe.io command builder.

This module provides the FastTransfer-specific command building logic,
delegating binary validation, execution, password masking, and version
detection to the base classes.
"""

import logging
from typing import Any, Dict, List

from ..base.command_builder import BaseCommandBuilder, ArpeToolError
from ..base.version_detector import BaseVersionDetector
from .validators import (
    BACKUP_TYPES,
    ConnectionConfig,
    TransferRequest,
    native_source_type,
)
from .version import VERSION_REGISTRY, VersionCapabilities


logger = logging.getLogger(__name__)


class FastTransferError(ArpeToolError):
    """FastTransfer-specific exception for backwards compatibility."""
    pass


class CommandBuilder(BaseCommandBuilder):
    """Builds FastTransfer commands from validated requests."""

    PRODUCT_NAME = "FastTransfer"
    DOWNLOAD_URL = "https://arpe.io"
    DEFAULT_TIMEOUT = 1800
    SENSITIVE_FLAGS = {
        "--sourcepassword",
        "--targetpassword",
        "-x",
        "-X",
        "--sourceconnectstring",
        "--targetconnectstring",
        "-g",
        "-G",
    }

    def _create_version_detector(self) -> BaseVersionDetector:
        """Create a FastTransfer version detector."""
        return BaseVersionDetector(
            str(self.binary_path),
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )

    def build_command(self, request: TransferRequest) -> List[str]:
        """
        Build a FastTransfer command from a validated request.

        Args:
            request: Validated transfer request

        Returns:
            Command as list of strings (suitable for subprocess)
        """
        cmd = [str(self.binary_path)]

        # Add source connection parameters
        cmd.extend(self._build_source_params(request.source))

        # Add target connection parameters
        cmd.extend(self._build_target_params(request.target))

        # Add transfer options
        cmd.extend(self._build_option_params(request.options))

        return cmd

    def _build_source_params(self, source: ConnectionConfig) -> List[str]:
        """Build source connection parameters."""
        params = []

        # Connection type
        params.extend(["--sourceconnectiontype", source.type])

        # Connection string or individual parameters
        if source.connect_string:
            params.extend(["--sourceconnectstring", source.connect_string])
        elif source.dsn:
            params.extend(["--sourcedsn", source.dsn])
        else:
            # Standard connection parameters
            if source.server:
                params.extend(["--sourceserver", source.server])
            if source.user:
                params.extend(["--sourceuser", source.user])
            if source.password:
                params.extend(["--sourcepassword", source.password])
            if source.trusted_auth:
                params.append("--sourcetrusted")

        # Database, schema, table/query/file_input
        if source.database:
            params.extend(["--sourcedatabase", source.database])
        if source.schema:
            params.extend(["--sourceschema", source.schema])
        # A query next to the table filters it (physical-location methods)
        if source.table:
            params.extend(["--sourcetable", source.table])
        if source.query:
            params.extend(["--query", source.query])
        elif source.file_input and not source.table:
            params.extend(["--fileinput", source.file_input])

        # Provider (for OleDB)
        if source.provider:
            params.extend(["--sourceprovider", source.provider])

        return params

    def _build_target_params(self, target: ConnectionConfig) -> List[str]:
        """Build target connection parameters."""
        params = []

        # Connection type
        params.extend(["--targetconnectiontype", target.type])

        # Connection string or individual parameters
        if target.connect_string:
            params.extend(["--targetconnectstring", target.connect_string])
        else:
            # Standard connection parameters
            if target.server:
                params.extend(["--targetserver", target.server])
            if target.user:
                params.extend(["--targetuser", target.user])
            if target.password:
                params.extend(["--targetpassword", target.password])
            if target.trusted_auth:
                params.append("--targettrusted")

        # Database, schema, table
        if target.database:
            params.extend(["--targetdatabase", target.database])
        if target.schema:
            params.extend(["--targetschema", target.schema])
        if target.table:
            params.extend(["--targettable", target.table])

        return params

    def _build_option_params(self, options) -> List[str]:
        """Build transfer option parameters."""
        params = []

        # Parallelism method
        params.extend(["--method", options.method.value])

        # Distribute key column. Use the documented lowercase spelling
        # (--distributekeycolumn); the binary also accepts the camelCase alias.
        if options.distribute_key_column:
            params.extend(["--distributekeycolumn", options.distribute_key_column])

        # Degree of parallelism
        params.extend(["--degree", str(options.degree)])

        # Load mode
        params.extend(["--loadmode", options.load_mode.value])

        # Type inference of load mode Create (FastTransfer 1.1+)
        if options.infer_data_type:
            params.extend(["--inferdatatype", options.infer_data_type.value])

        # Batch size
        if options.batch_size:
            params.extend(["--batchsize", str(options.batch_size)])

        # Map method
        params.extend(["--mapmethod", options.map_method.value])

        # Run ID
        if options.run_id:
            params.extend(["--runid", options.run_id])

        # Data driven query
        if options.data_driven_query:
            params.extend(["--datadrivenquery", options.data_driven_query])

        # Use work tables (a switch: only emitted when asked for)
        if options.use_work_tables:
            params.append("--useworktables")

        # Settings file
        if options.settings_file:
            params.extend(["--settingsfile", options.settings_file])

        # Log level
        if options.log_level:
            params.extend(["--loglevel", options.log_level.value])

        # No banner
        if options.no_banner:
            params.append("--nobanner")

        # License path
        if options.license_path:
            params.extend(["--license", options.license_path])

        return params

    def _get_version_capabilities(self, caps: VersionCapabilities) -> dict:
        """Format version capabilities for the API response."""
        return {
            "source_types": sorted(caps.source_types),
            "target_types": sorted(caps.target_types),
            "parallelism_methods": sorted(caps.parallelism_methods),
            "supports_nobanner": caps.supports_nobanner,
            "supports_version_flag": caps.supports_version_flag,
            "supports_file_input": caps.supports_file_input,
            "supports_settings_file": caps.supports_settings_file,
            "supports_license_path": caps.supports_license_path,
            "requires_license": caps.requires_license,
            "supports_create_load_mode": caps.supports_create_load_mode,
            "supports_mssql_host_colon_port": caps.supports_mssql_host_colon_port,
        }


def get_supported_combinations() -> Dict[str, List[str]]:
    """
    Get supported source -> target database combinations.

    Returns:
        Dictionary mapping source types to list of compatible target types
    """
    return {
        "ClickHouse": [
            "ClickHouse",
            "DuckDB",
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "Oracle",
            "SAP HANA",
            "Teradata",
        ],
        "DuckDB": [
            "DuckDB",
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "Oracle",
            "ClickHouse",
            "SAP HANA",
            "Teradata",
        ],
        "DuckDB Stream (File Import)": [
            "DuckDB",
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "Oracle",
            "ClickHouse",
            "SAP HANA",
            "Teradata",
        ],
        "MySQL": [
            "MySQL",
            "PostgreSQL",
            "SQL Server",
            "Oracle",
            "DuckDB",
            "ClickHouse",
            "SAP HANA",
            "Teradata",
        ],
        "Netezza": [
            "Netezza",
            "PostgreSQL",
            "SQL Server",
            "Oracle",
            "DuckDB",
            "SAP HANA",
            "Teradata",
        ],
        "Oracle": [
            "Oracle",
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "DuckDB",
            "ClickHouse",
            "SAP HANA",
            "Teradata",
        ],
        "PostgreSQL": [
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "Oracle",
            "DuckDB",
            "ClickHouse",
            "Netezza",
            "SAP HANA",
            "Teradata",
        ],
        "SAP HANA": [
            "SAP HANA",
            "PostgreSQL",
            "SQL Server",
            "Oracle",
            "DuckDB",
            "ClickHouse",
            "Teradata",
        ],
        "SQL Server": [
            "SQL Server",
            "PostgreSQL",
            "MySQL",
            "Oracle",
            "DuckDB",
            "ClickHouse",
            "SAP HANA",
            "Teradata",
        ],
        "Teradata": [
            "Teradata",
            "PostgreSQL",
            "SQL Server",
            "Oracle",
            "DuckDB",
            "ClickHouse",
            "SAP HANA",
        ],
        # FastTransfer 1.0+: the bundled Arpe ADBC drivers. ADBC is needed on
        # both ends: any adbc_* source with any adbc_* target, never mixed
        # with a native type. Windows x64 and Linux x64 only.
        "ADBC (adbc_mssql, adbc_pgsql, adbc_oracle, adbc_netezza, adbc_db2, "
        "adbc_teradata; FastTransfer 1.0+, adbc_* on both ends)": [
            "SQL Server (adbc_mssql)",
            "PostgreSQL (adbc_pgsql)",
            "Oracle (adbc_oracle)",
            "Netezza (adbc_netezza)",
            "IBM Db2 for LUW (adbc_db2)",
            "Teradata (adbc_teradata)",
        ],
        # FastTransfer 1.1+: backup files read without restoring them, by the
        # bundled backup drivers. Source only; the target is an adbc_* type.
        "Backup files (backup_mssql: SQL Server .bak; backup_rman: Oracle RMAN "
        "backup sets; FastTransfer 1.1+, adbc_* target only)": [
            "SQL Server (adbc_mssql)",
            "PostgreSQL (adbc_pgsql)",
            "Oracle (adbc_oracle)",
            "Netezza (adbc_netezza)",
            "IBM Db2 for LUW (adbc_db2)",
            "Teradata (adbc_teradata)",
        ],
    }


def suggest_parallelism_method(
    source_type: str,
    has_numeric_key: bool,
    has_identity_column: bool,
    table_size_estimate: str,
    physical_supported: bool = True,
) -> Dict[str, str]:
    """
    Suggest optimal parallelism method based on source database and table characteristics.

    Args:
        source_type: Source database type
        has_numeric_key: Whether table has a numeric key column
        has_identity_column: Whether table has an identity/auto-increment column
        table_size_estimate: Table size ('small', 'medium', 'large')
        physical_supported: Whether the FastTransfer binary accepts the Physical
            method (1.0+). When False, the engine-specific name (Ctid, Rowid,
            Physloc, NZDataSlice) is returned instead.

    Returns:
        Dictionary with 'method' and 'explanation' keys
    """
    # ADBC types share the parallel planning of their native counterpart
    source_lower = native_source_type(source_type)

    # Small tables - no parallelism needed
    if table_size_estimate == "small":
        return {
            "method": "None",
            "explanation": "Table is small - parallelism overhead would likely reduce performance. Use None for best results.",
        }

    # Backup files (FastTransfer 1.1+): None or Physical (resolved to Pages)
    if source_lower in BACKUP_TYPES:
        return {
            "method": "Physical",
            "explanation": (
                "Backup source detected: only None and Physical are available. Physical "
                "resolves to Pages, each stream reading its own slice of the table "
                "(pages of a rowstore, rowgroups of a columnstore, ranges of Oracle "
                "blocks); no key column needed."
            ),
        }

    def physical(engine_method: str, explanation: str, needs_table: bool = True) -> Dict[str, str]:
        needs = (
            "it needs schema and table (a query can be added to filter the table), no key column"
            if needs_table
            else "it needs no key column"
        )
        if physical_supported:
            return {
                "method": "Physical",
                "explanation": (
                    f"{explanation} Physical (FastTransfer 1.0+) resolves to "
                    f"{engine_method} for this connection type; {needs}."
                ),
            }
        return {
            "method": engine_method,
            "explanation": (
                f"{explanation} This FastTransfer version predates Physical, so use "
                f"{engine_method} directly; it needs schema and table, no key column."
            ),
        }

    # PostgreSQL - physical tuple id (Ctid)
    if source_lower in ["pgsql", "pgcopy", "postgresql"]:
        return physical(
            "Ctid",
            "PostgreSQL source detected. Splitting on the native tuple identifier (CTID) reads the table in parallel without a key column.",
        )

    # Oracle - ROWID
    if source_lower in ["oraodp"]:
        return physical(
            "Rowid",
            "Oracle source detected. Splitting on the native ROWID reads the table in parallel without a key column.",
        )

    # Netezza - data slices. nzbulk and nzoledb are accepted for it from 1.0 only.
    netezza = ["nzoledb", "nzsql", "nzbulk", "netezza"] if physical_supported else ["nzsql", "netezza"]
    if source_lower in netezza:
        return physical(
            "NZDataSlice",
            "Netezza source detected. Splitting on the Netezza data slices leverages the appliance's own distribution.",
            needs_table=False,
        )

    # SQL Server without numeric key - physical row location (Physloc).
    # msoledbsql is accepted for it from 1.0 only. odbc / oledb are left out:
    # the engine behind them is unknown, and Physical refuses both.
    sql_server = ["mssql", "msoledbsql"] if physical_supported else ["mssql"]
    if source_lower in sql_server and not has_numeric_key:
        return physical(
            "Physloc",
            "SQL Server source detected without numeric key. Splitting on the physical row location needs no key column; use it on data that is not being written (read-only table, database or snapshot).",
        )

    # If has numeric key - RangeId or Random are good choices
    if has_numeric_key:
        if has_identity_column or table_size_estimate == "large":
            return {
                "method": "RangeId",
                "explanation": "RangeId recommended for tables with numeric keys. It distributes data by dividing the numeric range into chunks, providing good load balancing for large tables.",
            }
        else:
            return {
                "method": "Random",
                "explanation": "Random method recommended. Uses modulo operation on numeric key for distribution. Works well when key values are evenly distributed.",
            }

    # No specific optimization available - use DataDriven or Ntile
    if table_size_estimate == "large":
        return {
            "method": "DataDriven",
            "explanation": "DataDriven method recommended. Distributes based on distinct values of a key column. Choose a column with good cardinality for best results.",
        }
    else:
        return {
            "method": "Ntile",
            "explanation": "Ntile method recommended. Evenly distributes data across parallel workers. Works with numeric, date, or string columns.",
        }
