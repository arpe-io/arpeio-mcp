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
from .validators import TransferRequest, ConnectionConfig
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
        if source.table:
            params.extend(["--sourcetable", source.table])
        elif source.query:
            params.extend(["--query", source.query])
        elif source.file_input:
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

        # Distribute key column
        if options.distribute_key_column:
            params.extend(["--distributeKeyColumn", options.distribute_key_column])

        # Degree of parallelism
        params.extend(["--degree", str(options.degree)])

        # Load mode
        params.extend(["--loadmode", options.load_mode.value])

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

        # Use work tables
        if options.use_work_tables is not None:
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
    }


def suggest_parallelism_method(
    source_type: str,
    has_numeric_key: bool,
    has_identity_column: bool,
    table_size_estimate: str,
) -> Dict[str, str]:
    """
    Suggest optimal parallelism method based on source database and table characteristics.

    Args:
        source_type: Source database type
        has_numeric_key: Whether table has a numeric key column
        has_identity_column: Whether table has an identity/auto-increment column
        table_size_estimate: Table size ('small', 'medium', 'large')

    Returns:
        Dictionary with 'method' and 'explanation' keys
    """
    source_lower = source_type.lower()

    # Small tables - no parallelism needed
    if table_size_estimate == "small":
        return {
            "method": "None",
            "explanation": "Table is small - parallelism overhead would likely reduce performance. Use None for best results.",
        }

    # PostgreSQL - Ctid is optimal
    if source_lower in ["pgsql", "pgcopy", "postgresql"]:
        return {
            "method": "Ctid",
            "explanation": "PostgreSQL source detected. Ctid method provides efficient parallel reading using PostgreSQL's native tuple identifier.",
        }

    # Oracle - Rowid is optimal
    if source_lower in ["oraodp"]:
        return {
            "method": "Rowid",
            "explanation": "Oracle source detected. Rowid method provides efficient parallel reading using Oracle's native row identifier.",
        }

    # Netezza - NZDataSlice is optimal
    if source_lower in ["nzoledb", "nzsql", "nzbulk", "netezza"]:
        return {
            "method": "NZDataSlice",
            "explanation": "Netezza source detected. NZDataSlice method leverages Netezza's data slicing for optimal parallel performance.",
        }

    # SQL Server without numeric key - Physloc is a good option
    if source_lower in ["mssql", "oledb", "odbc", "msoledbsql"] and not has_numeric_key:
        return {
            "method": "Physloc",
            "explanation": "SQL Server source detected without numeric key. Physloc method uses physical row location for parallel reading without requiring a key column.",
        }

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
