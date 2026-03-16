"""
FastBCP command builder extending the base Arpe.io command builder.

This module provides functionality to build, validate, and execute
FastBCP commands with proper security measures.
"""

import logging
from typing import Any, Dict, List, Optional

from src.base.command_builder import BaseCommandBuilder, ArpeToolError
from src.base.version_detector import BaseVersionDetector

from .validators import (
    ExportRequest,
    SourceConnectionConfig,
    OutputConfig,
)
from .version import VERSION_REGISTRY


logger = logging.getLogger(__name__)


class FastBCPError(ArpeToolError):
    """Exception for FastBCP operations (backwards compatibility)."""

    pass


class CommandBuilder(BaseCommandBuilder):
    """Builds FastBCP commands from validated requests."""

    PRODUCT_NAME = "FastBCP"
    DOWNLOAD_URL = "https://arpe.io"
    DEFAULT_TIMEOUT = 1800
    SENSITIVE_FLAGS = {
        "--sourcepassword",
        "-x",
        "--sourceconnectstring",
        "-g",
    }

    def _create_version_detector(self) -> BaseVersionDetector:
        """Create a FastBCP version detector."""
        return BaseVersionDetector(
            str(self.binary_path),
            VERSION_REGISTRY,
            r"FastBCP\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastBCP",
        )

    def _get_version_capabilities(self, caps) -> dict:
        """Format capabilities dict for version info response."""
        return {
            "source_types": sorted(caps.source_types),
            "output_formats": sorted(caps.output_formats),
            "parallelism_methods": sorted(caps.parallelism_methods),
            "storage_targets": sorted(caps.storage_targets),
            "supports_nobanner": caps.supports_nobanner,
            "supports_version_flag": caps.supports_version_flag,
            "supports_cloud_profile": caps.supports_cloud_profile,
            "supports_merge": caps.supports_merge,
            "supports_config_file": caps.supports_config_file,
        }

    def build_command(
        self, request: ExportRequest, config_file: Optional[str] = None
    ) -> List[str]:
        """
        Build a FastBCP command from a validated request.

        Args:
            request: Validated export request
            config_file: Optional path to a YAML config file

        Returns:
            Command as list of strings (suitable for subprocess)
        """
        cmd = [str(self.binary_path)]

        # Add source connection parameters
        cmd.extend(self._build_source_params(request.source))

        # Add output parameters
        cmd.extend(self._build_output_params(request.output))

        # Add export options
        cmd.extend(self._build_option_params(request.options))

        # Add config file if provided
        if config_file:
            cmd.extend(["--config", config_file])

        return cmd

    def _build_source_params(self, source: SourceConnectionConfig) -> List[str]:
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

        # Database, schema, table/query
        if source.database:
            params.extend(["--sourcedatabase", source.database])
        if source.schema:
            params.extend(["--sourceschema", source.schema])
        if source.table:
            params.extend(["--sourcetable", source.table])
        elif source.query:
            params.extend(["--query", source.query])

        # Provider (for OleDB)
        if source.provider:
            params.extend(["--sourceprovider", source.provider])

        # Application intent
        if source.application_intent:
            params.extend(["--applicationintent", source.application_intent.value])

        return params

    def _build_output_params(self, output: OutputConfig) -> List[str]:
        """Build output file parameters."""
        params = []

        # Output format
        params.extend(["--format", output.format.value])

        # File output or directory
        if output.file_output:
            params.extend(["--fileoutput", output.file_output])
        if output.directory:
            params.extend(["--directory", output.directory])

        # Storage target (only if not local)
        if output.storage_target.value != "local":
            params.extend(["--storagetarget", output.storage_target.value])

        # Delimiter
        if output.delimiter:
            params.extend(["--delimiter", output.delimiter])

        # Quotes
        if output.quotes:
            params.extend(["--quotes", output.quotes])

        # Encoding
        if output.encoding:
            params.extend(["--encoding", output.encoding])

        # No header
        if output.no_header:
            params.append("--noheader")

        # Decimal separator
        if output.decimal_separator:
            params.extend(["--decimalseparator", output.decimal_separator.value])

        # Date format
        if output.date_format:
            params.extend(["--dateformat", output.date_format])

        # Bool format
        if output.bool_format:
            params.extend(["--boolformat", output.bool_format.value])

        # Parquet compression
        if output.parquet_compression:
            params.extend(["--parquetcompression", output.parquet_compression.value])

        # Timestamped
        if output.timestamped:
            params.append("--timestamped")

        # Merge
        if output.merge:
            params.append("--merge")

        return params

    def _build_option_params(self, options) -> List[str]:
        """Build export option parameters."""
        params = []

        # Parallelism method
        params.extend(["--method", options.method.value])

        # Distribute key column
        if options.distribute_key_column:
            if options.method.value == "Timepartition":
                # Timepartition uses a special tuple format: (datecolumn, year, month)
                col = options.distribute_key_column
                params.extend(
                    ["--distributeKeyColumn", f"({col}, year, month)"]
                )
            else:
                params.extend(
                    ["--distributeKeyColumn", options.distribute_key_column]
                )

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

        # Cloud profile
        if options.cloud_profile:
            params.extend(["--cloudprofile", options.cloud_profile])

        return params


def get_supported_formats() -> Dict[str, Any]:
    """
    Get supported database sources, output formats, and storage targets.

    Returns:
        Dictionary with database sources, output formats, and storage targets
    """
    return {
        "Database Sources": {
            "PostgreSQL (pgsql, pgcopy)": [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ],
            "SQL Server (mssql, msoledbsql, odbc, oledb)": [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ],
            "Oracle (oraodp)": [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ],
            "MySQL (mysql)": [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ],
            "Netezza (nzcopy, nzoledb, nzsql)": [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ],
            "ClickHouse (clickhouse)": [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ],
            "SAP HANA (hana)": [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ],
            "Teradata (teradata)": [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ],
        },
        "Output Formats": ["csv", "tsv", "json", "bson", "parquet", "xlsx", "binary"],
        "Storage Targets": [
            "local",
            "s3",
            "s3compatible",
            "azure_blob",
            "azure_datalake",
            "fabric_onelake",
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
    if source_lower in ["nzcopy", "nzoledb", "nzsql", "netezza"]:
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
