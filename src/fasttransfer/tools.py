"""
FastTransfer MCP tool definitions and handlers.

This module defines all FastTransfer tools and their handler functions,
designed to be aggregated by a unified MCP server.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Callable, Awaitable

from mcp.types import Tool, ToolAnnotations, TextContent
from pydantic import ValidationError

from .command_builder import CommandBuilder, FastTransferError
from .command_builder import get_supported_combinations, suggest_parallelism_method
from .validators import (
    ADBC_TYPES,
    BACKUP_TYPES,
    CREATE_LOAD_MODES,
    DEGREE_MAX,
    DEGREE_MIN,
    PHYSICAL_METHODS,
    PHYSICAL_STRATEGIES,
    TransferRequest,
    ConnectionValidationRequest,
    ParallelismSuggestionRequest,
    SourceConnectionType,
    TargetConnectionType,
    ParallelismMethod,
    LoadMode,
    InferDataType,
    MapMethod,
    LogLevel,
    native_source_type,
)
from .version import check_version_compatibility
from src.base.error_patterns import diagnose_cli_error
from src.base.command_line import command_input_schema, join_command, parse_command
from src.base.structured import make_output_schema, respond


logger = logging.getLogger(__name__)


# Structured-output schemas shared by the preview and execute tools. Only `status`
# is required (see make_output_schema), so success and error payloads both validate.
PREVIEW_OUTPUT_SCHEMA = make_output_schema({
    "command": {
        "type": "array",
        "items": {"type": "string"},
        "description": "Full CLI argv with real credentials, ready to hand to the execute tool.",
    },
    "command_string": {
        "type": "string",
        "description": "The argv quoted into one string (POSIX shell quoting, real credentials), accepted as-is by the execute tool.",
    },
    "command_display": {
        "type": "string",
        "description": "The command with passwords masked, safe to show the user.",
    },
    "explanation": {"type": "string", "description": "Human-readable summary of what the command does."},
    "warnings": {"type": "array", "items": {"type": "string"}, "description": "Version-compatibility warnings."},
    "preview_only": {"type": "boolean", "description": "True when no binary is configured (execution unavailable)."},
    "auto_parallelism": {"type": "string", "description": "Parallelism method auto-suggested when none was given."},
    "errors": {"type": "array", "items": {"type": "object"}, "description": "Field-level validation errors (status='error')."},
    "tips": {"type": "array", "items": {"type": "string"}, "description": "Suggested next tool calls to resolve errors."},
})

EXECUTE_OUTPUT_SCHEMA = make_output_schema({
    "success": {"type": "boolean", "description": "True when the command exited 0."},
    "return_code": {"type": "integer", "description": "Process exit code."},
    "stdout": {"type": "string"},
    "stderr": {"type": "string"},
    "diagnostics": {"type": "array", "items": {"type": "string"}, "description": "Parsed hints when the command failed."},
    "log_dir": {"type": "string"},
})


def _suggest_next_steps(errors: list) -> list[str]:
    """Suggest next tools to call based on validation error fields."""
    tips = []
    error_fields = set()
    for error in errors:
        error_fields.update(str(x) for x in error["loc"])

    if "type" in error_fields or any("source" in f or "target" in f for f in error_fields):
        tips.append("Tip: Use `fasttransfer_info` with action='combinations' to see supported source→target database pairs.")
    if "method" in error_fields or "distribute_key_column" in error_fields:
        tips.append("Tip: Use `fasttransfer_info` with action='parallelism' to get the optimal parallelism method for your table.")

    return tips


def create_tools(
    command_builder: CommandBuilder,
    config: dict,
) -> Tuple[List[Tool], Callable[[str, Dict[str, Any]], Awaitable[List[TextContent]]]]:
    """Create FastTransfer tool definitions and handlers.

    Args:
        command_builder: Initialized CommandBuilder instance
        config: Dict with optional keys: timeout (int), log_dir (str/Path)

    Returns:
        Tuple of (tools_list, handle_call_async_function)
    """
    timeout = config.get("timeout", 1800)
    log_dir = config.get("log_dir")
    if log_dir is not None:
        log_dir = Path(log_dir)

    # ------------------------------------------------------------------ #
    # Tool definitions
    # ------------------------------------------------------------------ #
    tools = [
        Tool(
            name="fasttransfer_preview_transfer",
            description=(
                "Build and preview a FastTransfer database-to-database transfer command WITHOUT executing it. "
                "Shows the exact CLI command with passwords masked. "
                "If no parallelism method is specified, one is auto-suggested based on the source database type. "
                "Does not test connectivity or execute the transfer. "
                "After reviewing, pass the command to fasttransfer_execute_transfer."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            outputSchema=PREVIEW_OUTPUT_SCHEMA,
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [e.value for e in SourceConnectionType],
                                "description": "Source database connection type (e.g., 'pgsql' for PostgreSQL, 'mssql' for SQL Server, 'oraodp' for Oracle, 'mysql' for MySQL). The ADBC types 'adbc_mssql', 'adbc_pgsql', 'adbc_oracle', 'adbc_netezza', 'adbc_db2' (IBM Db2 for LUW, no IBM client needed) and 'adbc_teradata' (FastTransfer 1.0+) read through the bundled Arpe ADBC drivers; they work only with an adbc_* target (ADBC on both ends) and on Windows x64 / Linux x64. The backup types 'backup_mssql' (SQL Server .bak files: full and differential, compressed, striped, encrypted or TDE ones included) and 'backup_rman' (Oracle RMAN backup sets: full, level 0 and incremental level 1 to 4) (FastTransfer 1.1+) read the backup files without restoring them and without a database; they need an adbc_* target, take only the methods None and Physical, and run on Windows x64 / Linux x64.",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address (e.g., 'localhost:5432', 'myserver\\\\SQLEXPRESS', 'dbhost:1521'). adbc_* types: 'host', 'host:port' or 'host,port' (default ports: 1433, 5432, 1521, 5480 Netezza, 50000 Db2, 1025 Teradata); adbc_oracle: 'host[:port]/service' or a TNS alias. SQL Server (mssql, msoledbsql, oledb with a SQL Server provider, adbc_mssql): 'host', 'host:port' (native types: FastTransfer 1.1+, write 'host,port' before), 'host,port', 'host\\INSTANCE' or 'host\\INSTANCE,port'; the 'tcp:' prefix and 'host\\INSTANCE:port' are refused. backup_mssql / backup_rman: the directory of the backup files (e.g. 'D:\\MSSQLBACKUP' or '/backup/rman').",
                            },
                            "database": {
                                "type": "string",
                                "description": "Source database or service name to connect to. backup_mssql / backup_rman: the backup file names or patterns in the 'server' directory, separated by ';'. backup_mssql: the full backup, its differentials and every stripe, e.g. 'mydb_full.bak;mydb_diff_*.bak'. backup_rman: '*' and '?' in a name, '**' for any number of directories, a directory standing for its files, e.g. 'l0;l1/**'; an entry that matches nothing stops the run.",
                            },
                            "schema": {
                                "type": "string",
                                "description": "Source schema name. Omit to use the database default schema. Required with table by the Physical method on PostgreSQL, Oracle and SQL Server, and by DBPartition.",
                            },
                            "table": {
                                "type": "string",
                                "description": "Source table name. Provide this OR query OR file_input, not multiple. Exception: with the Physical method (and the deprecated Ctid, Rowid, Physloc), schema and table are required and a query can be added to filter that table. backup_mssql / backup_rman: the table to read from the backup (with schema), or a restricted query instead.",
                            },
                            "query": {
                                "type": "string",
                                "description": "Custom SQL SELECT query to use as source data instead of a full table. Use when you need filtering, joins, or column selection. With the Physical method it is given next to schema and table, and filters that table. backup_mssql / backup_rman: only 'SELECT * | col [, col...] FROM [schema.]table' (backup_mssql also takes 'TOP n'); no WHERE, no join, no other SQL.",
                            },
                            "file_input": {
                                "type": "string",
                                "description": "Absolute path to a file holding the SQL query to run on the source (--fileinput), instead of a table or an inline query. Files to import (CSV, Parquet, JSON) are not given here: they are read by a 'duckdbstream' source through a query such as SELECT * FROM read_parquet('...').",
                            },
                            "user": {"type": "string", "description": "Database username. Required unless using trusted_auth, connect_string, or dsn. Backup types need no login: for an encrypted backup_mssql backup (WITH ENCRYPTION or TDE), 'user' is the private key file of the certificate ('.pvk', '.pfx', PEM...)."},
                            "password": {"type": "string", "description": "Database password. Required when user is provided and not using trusted_auth. backup_mssql, encrypted backup: the password of the private key file given in 'user'."},
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted (integrated) authentication instead of username/password: Windows authentication for SQL Server. With the adbc_* types: Windows SSPI or Kerberos for adbc_mssql and adbc_pgsql, Kerberos for adbc_oracle, adbc_db2 and adbc_teradata; refused by adbc_netezza.",
                                "default": False,
                            },
                            "connect_string": {
                                "type": "string",
                                "description": "Full native connection string. Use instead of server/user/password for custom formats (e.g., Oracle TNS). adbc_* types take the driver's own vocabulary, not ADO.NET keywords: 'uri=postgresql://user:password@host:5432/db' (sqlserver://, oracle://, netezza://, db2://, teradata://), with driver options after ';'; percent-encode '@', ':' and '/' in the password. Prefer server/user/password for them. backup_mssql / backup_rman: the driver options given directly ('adbc.arpemsbackup.*' / 'adbc.arpeorabackup.*'), instead of server/user/password; an option there wins over the FASTTRANSFER_INDEX_DIR and FASTTRANSFER_BACKUP_RESTORE_* environment variables.",
                            },
                            "dsn": {
                                "type": "string",
                                "description": "ODBC Data Source Name. Use when connecting via a pre-configured system or user DSN.",
                            },
                            "provider": {
                                "type": "string",
                                "description": "OLE DB provider name. Only needed for OLE DB connection types.",
                            },
                        },
                        "required": ["type", "database"],
                    },
                    "target": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [e.value for e in TargetConnectionType],
                                "description": "Target database connection type (e.g., 'msbulk' for SQL Server, 'pgcopy' or 'pgsql' for PostgreSQL, 'orabulk' for Oracle, 'mysqlbulk' for MySQL). The ADBC types 'adbc_mssql', 'adbc_pgsql', 'adbc_oracle', 'adbc_netezza', 'adbc_db2' and 'adbc_teradata' (FastTransfer 1.0+) load through the bundled Arpe ADBC drivers and work only with an adbc_* source or a backup source (backup_mssql, backup_rman; 1.1+). Use `fasttransfer_info` with action='combinations' to see valid pairs.",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address in host:port format (e.g., 'localhost:5432') or host\\instance for SQL Server. SQL Server (msbulk, adbc_mssql): 'host', 'host:port' (msbulk: FastTransfer 1.1+, write 'host,port' before), 'host,port', 'host\\INSTANCE' or 'host\\INSTANCE,port'; the 'tcp:' prefix and 'host\\INSTANCE:port' are refused.",
                            },
                            "database": {
                                "type": "string",
                                "description": "Target database or service name.",
                            },
                            "schema": {"type": "string", "description": "Target schema name. Required: FastTransfer stops with 'Target Schema is mandatory' without it (for example 'dbo' on SQL Server, 'public' on PostgreSQL, 'main' on DuckDB). It must exist, also with load_mode Create. With an adbc_* target it is used exactly as typed (quoted identifier): write it in its stored case."},
                            "table": {
                                "type": "string",
                                "description": "Target table name. The table must already exist in the target database, unless load_mode is Create (FastTransfer 1.1+), which creates it from the source. With an adbc_* target it is used exactly as typed (quoted identifier): write it in its stored case ('ORDERS' on Oracle, Db2 and Teradata; 'orders' on PostgreSQL).",
                            },
                            "user": {"type": "string", "description": "Database username. Required unless using trusted_auth or connect_string."},
                            "password": {"type": "string", "description": "Database password. Required when user is provided and not using trusted_auth."},
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted (integrated) authentication instead of username/password: Windows authentication for SQL Server. With the adbc_* types: Windows SSPI or Kerberos for adbc_mssql and adbc_pgsql, Kerberos for adbc_oracle, adbc_db2 and adbc_teradata; refused by adbc_netezza.",
                                "default": False,
                            },
                            "connect_string": {
                                "type": "string",
                                "description": "Full native connection string. Use instead of server/user/password for custom formats. adbc_* types take the driver's own vocabulary ('uri=sqlserver://user:password@host:1433/?database=db', with driver options after ';'), not ADO.NET keywords.",
                            },
                        },
                        "required": ["type", "database", "schema", "table"],
                    },
                    "os_type": {
                        "type": "string",
                        "enum": ["linux", "windows"],
                        "description": "Operating system where FastTransfer will run. Affects command quoting and path separators. linux: uses single quotes and forward slashes. windows: uses double quotes and backslashes.",
                        "default": "linux",
                    },
                    "options": {
                        "type": "object",
                        "properties": {
                            "method": {
                                "type": "string",
                                "enum": [e.value for e in ParallelismMethod],
                                "description": "Parallelism method. Ntile: even distribution on numeric key. DataDriven: distinct value partitions, works with any data type. Physical: split on the physical row location, no key needed; resolved from the source connection type to Ctid (PostgreSQL), Rowid (Oracle), Physloc (SQL Server; only on data that is not being written) or NZDataSlice (Netezza); needs schema and table (not on Netezza), and a query can be added to filter that table; not available on other sources (FastTransfer 1.0+). On a backup source (backup_mssql, backup_rman; 1.1+) Physical resolves to Pages: each stream reads one slice of the table (pages of a rowstore, rowgroups of a columnstore, ranges of Oracle blocks), and it is the only method besides None. Ctid, Rowid, Physloc, NZDataSlice: the engine-specific names, deprecated since 1.0 (removed in 2.0), still needed on FastTransfer < 1.0 and for Physloc through an odbc source. RangeId: numeric min/max range. Random: modulo-based approximate distribution. DBPartition: one stream per native table partition, biggest first, for SQL Server (mssql, msoledbsql), PostgreSQL (pgsql, pgcopy), Oracle (oraodp), MySQL (mysql), Teradata (teradata, row partitioning only), their adbc_* counterparts and Db2 for LUW range partitioning (adbc_db2, 1.0+); needs schema and table (no query), no key; an unpartitioned table falls back to a single stream (FastTransfer 0.18+). Timepartition: one stream per year, month, day or ISO week of a date column, biggest first; distribute_key_column is a tuple such as '(order_date,year,month)'; works with a table or a query (FastTransfer 0.18+). None: single-threaded. Call `fasttransfer_info` with action='parallelism' if unsure.",
                                "default": "None",
                            },
                            "distribute_key_column": {
                                "type": "string",
                                "description": "Column name used to partition data across threads. Required when method is RangeId, Random, Ntile, DataDriven or Timepartition. Should be a high-cardinality column (e.g., primary key) for even distribution. For Timepartition (0.18+) it is a tuple of a date column and time parts: '(col,year)', '(col,year,month)', '(col,year,month,day)' or '(col,year,week)'. Not needed for Physical (and Ctid, Rowid, Physloc, NZDataSlice), DBPartition or None methods. RangeId and Random do not transfer the rows whose key is NULL.",
                            },
                            "degree": {
                                "type": "integer",
                                "description": "Parallelism degree, from -32 to 1024. 0 = use all CPU cores. Positive integer = exact thread count. Negative integer = cores / abs(value), e.g. -2 on 16 cores = 8 threads.",
                                "minimum": DEGREE_MIN,
                                "maximum": DEGREE_MAX,
                                "default": -2,
                            },
                            "load_mode": {
                                "type": "string",
                                "enum": [e.value for e in LoadMode],
                                "description": "Load mode. Append: add rows to existing data (safe, default). Truncate: delete all existing rows before loading (use when you want a full refresh). Create (FastTransfer 1.1+): create the target table from the source when it does not exist, then load it; when it exists (name exactly as typed) it is truncated and loaded, its structure kept; a table that exists only in another case is an error. The created table has the column names and order of the source, every column nullable, no key, index or constraint; the target schema must exist. Replace: alias of Create from 1.1 (before 1.1 it behaved like Append). Choose Truncate for idempotent full-table reloads of an existing table, Create when the target table may not exist yet, Append for incremental loads.",
                                "default": "Append",
                            },
                            "infer_data_type": {
                                "type": "string",
                                "enum": [e.value for e in InferDataType],
                                "description": "With load_mode Create (FastTransfer 1.1+), how the column types of the created table are derived; ignored with a warning with another load mode, and not used when the target table exists. fast (default): from the source metadata, no row is read; a text column with no declared length gets 256 characters (FASTTRANSFER_INFER_DEFAULT_STRING_MAXSIZE, a number or 'max'). allvarchar: every column as text, converted on the source; needs a source whose engine is known (not a generic odbc or oledb source). best: one profiling scan of the source finds the strictest type that holds every value, for Oracle sources and files read through DuckDB (the fast types for the other sources); a row that does not fit later fails the load; FASTTRANSFER_INFER_STRING_MARGIN (characters, or a factor from '2x' to '10x') and FASTTRANSFER_INFER_SAMPLE_PERCENT (above 0, up to 100) tune it. Same engine on both sides: the source types are copied unchanged, except with allvarchar. allvarchar and best rewrite the source query: with the Physical method give schema and table, not a query.",
                            },
                            "batch_size": {
                                "type": "integer",
                                "description": "Number of rows per batch insert. Higher values improve throughput but use more memory. Typical range: 1000-100000. Leave unset to use the FastTransfer default. No effect with an adbc_* target: the drivers size their own batches.",
                            },
                            "map_method": {
                                "type": "string",
                                "enum": [e.value for e in MapMethod],
                                "description": "Column mapping method. Position: maps columns by ordinal position (source col 1 to target col 1, etc.). Name: maps columns by matching column names (order-independent, safer when schemas differ).",
                                "default": "Position",
                            },
                            "run_id": {
                                "type": "string",
                                "description": "Custom run identifier included in log output. Useful for correlating transfers in monitoring systems. If omitted, FastTransfer generates one automatically.",
                            },
                            "data_driven_query": {
                                "type": "string",
                                "description": "Custom SQL query that returns the distinct partition values for DataDriven parallelism. Only used when method is DataDriven. If omitted, FastTransfer auto-generates a SELECT DISTINCT on the distribute_key_column.",
                            },
                            "use_work_tables": {
                                "type": "boolean",
                                "description": "Use intermediate work tables before final insert. For a SQL Server Clustered Columnstore Index (CCI) target, on FastTransfer 1.0+ only: before 1.0 the merge into an msbulk or pgcopy target fails, nothing is loaded and the exit code is still 0. Refused with an adbc_* target (the parallel streams load the target table directly). Not needed for heap or rowstore targets.",
                            },
                            "settings_file": {
                                "type": "string",
                                "description": "Absolute path to a custom FastTransfer settings JSON file. Overrides default settings for column mappings, data type conversions, etc. Only needed for advanced scenarios.",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Override the default log verbosity. Use Debug for troubleshooting, Verbose for detailed progress, Information for normal operation, Warning/Error for quiet runs.",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "description": "Suppress the FastTransfer startup banner in output. Useful for cleaner log parsing. No effect on transfer behavior.",
                            },
                            "license_path": {
                                "type": "string",
                                "description": "Path or URL of the FastTransfer license file, passed with --license. FastTransfer 0.18.2+ has no trial mode and does not run without a license (exit code 99; the free 30-day trial is a license file). Without license_path it looks for FastTransfer.lic in the current directory, then next to the binary. A MigratorXpress license file issued from 2026-10-26 also holds the FastTransfer license. The adbc_* types (1.0+) also need the ARPE ADBC DRIVER license block: FastTransfer hands it to the drivers when the same file holds it; without one the run exits 99. A backup source (1.1+) loads an adbc_* target, so the same block is needed.",
                            },
                        },
                    },
                },
                "required": ["source", "target"],
            },
        ),
        Tool(
            name="fasttransfer_execute_transfer",
            description=(
                "Execute a FastTransfer command that was previously built by fasttransfer_preview_transfer. "
                "Requires confirmation=true as a safety gate. "
                "Does NOT build the command — the command must come from a prior fasttransfer_preview_transfer call. "
                "Will fail if the FastTransfer binary is not installed."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=False,
                openWorldHint=True,
            ),
            outputSchema=EXECUTE_OUTPUT_SCHEMA,
            inputSchema={
                "type": "object",
                "properties": {
                    "command": command_input_schema("fasttransfer_preview_transfer"),
                    "confirmation": {
                        "type": "boolean",
                        "description": "Safety gate. Must be explicitly set to true to execute. false or omitted = execution is blocked. Always confirm with the user before setting to true.",
                    },
                },
                "required": ["command", "confirmation"],
            },
        ),
        Tool(
            name="fasttransfer_info",
            description=(
                "Get information about FastTransfer capabilities, parallelism recommendations, "
                "workflow guidance, or binary version. Use `action` to select what information you need. "
                "Read-only, does not require database connectivity."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["combinations", "parallelism", "workflow", "version"],
                        "description": (
                            "What information to retrieve. "
                            "combinations: list supported source-to-target database pairs. "
                            "parallelism: get a recommended parallelism method for a specific table (requires source_type, has_numeric_key, table_size_estimate). "
                            "workflow: get a step-by-step transfer guide with database-specific tips (requires source_type, target_type). "
                            "version: report installed binary version and capabilities."
                        ),
                    },
                    "source_type": {
                        "type": "string",
                        "enum": [e.value for e in SourceConnectionType],
                        "description": "Source database type (required for action=parallelism and action=workflow)",
                    },
                    "target_type": {
                        "type": "string",
                        "enum": [e.value for e in TargetConnectionType],
                        "description": "Target database type (required for action=workflow)",
                    },
                    "has_numeric_key": {
                        "type": "boolean",
                        "description": "Whether the table has a numeric key column (required for action=parallelism)",
                    },
                    "has_identity_column": {
                        "type": "boolean",
                        "description": "Whether the table has an identity/auto-increment column (optional, for action=parallelism)",
                        "default": False,
                    },
                    "table_size_estimate": {
                        "type": "string",
                        "enum": ["small", "medium", "large"],
                        "description": "Estimated table size (required for action=parallelism). small: <100K rows. medium: 100K-10M. large: >10M.",
                    },
                },
                "required": ["action"],
            },
        ),
    ]

    # ------------------------------------------------------------------ #
    # Handler functions
    # ------------------------------------------------------------------ #

    def _physical_supported() -> bool:
        """Whether the detected FastTransfer accepts the Physical method (1.0+)."""
        if command_builder is None:
            return True
        caps = command_builder.version_detector.capabilities
        return caps is None or "Physical" in caps.parallelism_methods

    async def handle_preview_transfer(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_preview_transfer tool."""
        if command_builder is None:
            return respond(
                (
                    "Error: FastTransfer binary not found or not accessible.\n"
                    f"Expected location: {config.get('path', '(unknown)')}\n"
                    "Please set FASTTRANSFER_PATH environment variable correctly."
                ),
                {
                    "status": "error",
                    "tool": "fasttransfer_preview_transfer",
                    "error": "FastTransfer binary not found or not accessible.",
                },
            )

        try:
            # Extract os_type before passing to TransferRequest (not part of the model)
            os_type = arguments.pop("os_type", "linux")

            # Auto-suggest parallelism if not specified
            auto_parallelism_note = None
            suggested_method = None
            options = arguments.get("options") or {}
            method = options.get("method", "None")
            if method == "None":
                source_type = (arguments.get("source") or {}).get("type", "")
                if source_type:
                    suggestion = suggest_parallelism_method(
                        source_type,
                        has_numeric_key=False,
                        has_identity_column=False,
                        table_size_estimate="medium",
                        physical_supported=_physical_supported(),
                    )
                    suggested_method = suggestion["method"]
                    # Physical-location methods split the table itself: they
                    # need schema + table. Netezza needs neither from 1.0.
                    source_args = arguments.get("source") or {}
                    has_table = bool(source_args.get("schema") and source_args.get("table"))
                    netezza_10 = (
                        suggested_method == "Physical"
                        and PHYSICAL_STRATEGIES.get(native_source_type(source_type)) == "NZDataSlice"
                    )
                    if suggested_method in PHYSICAL_METHODS and not (has_table or netezza_10):
                        suggested_method = "None"
                    # Key-based fallbacks need a column the caller did not give
                    if suggested_method not in PHYSICAL_METHODS:
                        suggested_method = "None"
                    if suggested_method != "None":
                        if "options" not in arguments:
                            arguments["options"] = {}
                        arguments["options"]["method"] = suggested_method
                        arguments["options"].setdefault("degree", 0)
                        auto_parallelism_note = (
                            f"**Auto-suggested parallelism**: `{suggested_method}` method with degree=0 (all cores). "
                            f"Rationale: {suggestion['explanation']}"
                        )

            # Validate and parse request
            request = TransferRequest(**arguments)

            # Check version compatibility
            version_warnings = check_version_compatibility(
                arguments,
                command_builder.version_detector.capabilities,
                command_builder.version_detector._detected_version,
            )

            # Build command
            command = command_builder.build_command(request)

            # Format for display (with masked passwords)
            display_command = command_builder.format_command_display(command, mask=True, os_type=os_type)

            # Create explanation
            explanation = _build_transfer_explanation(request)

            # Build response
            response = [
                "# FastTransfer Command Preview",
                "",
            ]

            if command_builder.preview_only:
                response += [
                    "**NOTE: Execution is not available (binary not configured). "
                    "Download from https://arpe.io to enable execution.**",
                    "",
                ]

            response += [
                "## What this command will do:",
                explanation,
            ]

            if auto_parallelism_note:
                response.append("")
                response.append(auto_parallelism_note)
                response.append("To override, re-call with explicit `method` and `degree` in options.")

            if version_warnings:
                response.append("")
                response.append("## Version Compatibility Warnings")
                for warning in version_warnings:
                    response.append(f"- {warning}")

            log_dir_display = str(log_dir) if log_dir else "(not configured)"

            response += [
                "",
                "## Command (passwords masked):",
                "```bash",
                display_command,
                "```",
                "",
                "## To execute this transfer:",
                "1. Review the command carefully",
                "2. Use the `fasttransfer_execute_transfer` tool with the FULL command (not the masked version)",
                "3. Set `confirmation: true` to proceed",
                "",
                "## Security Notice:",
                "- Passwords are masked in this preview (shown as ******)",
                "- The actual execution will use the real passwords you provided",
                "- All executions are logged (with masked passwords) to: "
                + log_dir_display,
                "",
                "## Full command for execution:",
                "```",
                join_command(command),
                "```",
            ]

            return respond("\n".join(response), {
                "status": "ok",
                "tool": "fasttransfer_preview_transfer",
                "command": command,
                "command_string": join_command(command),
                "command_display": display_command,
                "explanation": explanation,
                "warnings": version_warnings,
                "preview_only": bool(command_builder.preview_only),
                **({"auto_parallelism": suggested_method} if auto_parallelism_note else {}),
            })

        except ValidationError as e:
            error_msg = [
                "# Validation Error",
                "",
                "The provided parameters are invalid:",
                "",
            ]
            structured_errors = []
            for error in e.errors():
                field = " -> ".join(str(x) for x in error["loc"])
                error_msg.append(f"- **{field}**: {error['msg']}")
                structured_errors.append({"field": field, "message": error["msg"]})
            tips = _suggest_next_steps(e.errors())
            if tips:
                error_msg.append("")
                error_msg.extend(tips)
            return respond("\n".join(error_msg), {
                "status": "error",
                "tool": "fasttransfer_preview_transfer",
                "errors": structured_errors,
                "tips": tips,
            })

        except FastTransferError as e:
            return respond(f"Error: {str(e)}", {
                "status": "error",
                "tool": "fasttransfer_preview_transfer",
                "error": str(e),
            })

    async def handle_execute_transfer(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_execute_transfer tool."""
        if command_builder is None:
            return respond(
                "Error: FastTransfer binary not found. Please check FASTTRANSFER_PATH.",
                {"status": "error", "tool": "fasttransfer_execute_transfer",
                 "error": "FastTransfer binary not found."},
            )

        if command_builder.preview_only:
            return respond(
                (
                    "Execution requires the FastTransfer binary. "
                    "Download from https://arpe.io and set FASTTRANSFER_PATH to enable."
                ),
                {"status": "error", "tool": "fasttransfer_execute_transfer",
                 "error": "Binary not configured (command-builder mode).", "preview_only": True},
            )

        # Check confirmation
        if not arguments.get("confirmation", False):
            return respond(
                (
                    "# Execution Blocked\n\n"
                    "You must set `confirmation: true` to execute a transfer.\n"
                    "This safety mechanism ensures commands are only executed with explicit approval.\n\n"
                    "Please review the command carefully and confirm by setting:\n"
                    "```json\n"
                    '{"confirmation": true}\n'
                    "```"
                ),
                {"status": "error", "tool": "fasttransfer_execute_transfer",
                 "error": "confirmation=true is required to execute."},
            )

        # Get command
        raw_command = arguments.get("command", "")
        if not raw_command:
            return respond(
                "Error: No command provided. Please provide the command from fasttransfer_preview_transfer.",
                {"status": "error", "tool": "fasttransfer_execute_transfer",
                 "error": "No command provided."},
            )

        # The command comes as the argv list or as the quoted command_string
        try:
            command = parse_command(raw_command)
        except ValueError as e:
            return respond(
                f"Error parsing command: {str(e)}",
                {"status": "error", "tool": "fasttransfer_execute_transfer",
                 "error": f"Could not parse command: {str(e)}"},
            )

        # Execute
        try:
            logger.info("Starting FastTransfer execution...")
            return_code, stdout, stderr = command_builder.execute_command(
                command, timeout=timeout, log_dir=log_dir
            )

            # Format response
            success = return_code == 0
            status = "Success" if success else "Failed"
            log_dir_str = str(log_dir) if log_dir else "(not configured)"

            response = [
                f"# FastTransfer Execution - {status}",
                "",
                f"**Status**: {status}",
                f"**Return Code**: {return_code}",
                f"**Log Location**: {log_dir_str}",
                "",
                "## Output:",
                "```",
                stdout if stdout else "(no output)",
                "```",
            ]

            if stderr:
                response.extend(["", "## Error Output:", "```", stderr, "```"])

            diagnostics: list[str] = []
            if not success:
                diagnostics = diagnose_cli_error(stdout or "", stderr or "", return_code)
                if diagnostics:
                    response.append("")
                    response.append("## Diagnostics:")
                    for diag in diagnostics:
                        response.append(f"- {diag}")
                else:
                    response.extend(
                        [
                            "",
                            "## Troubleshooting:",
                            "- Check database credentials and connectivity",
                            "- Verify table/schema names exist",
                            "- Review the full log file for more information",
                        ]
                    )

            return respond("\n".join(response), {
                "status": "ok",
                "tool": "fasttransfer_execute_transfer",
                "success": success,
                "return_code": return_code,
                "stdout": stdout or "",
                "stderr": stderr or "",
                "diagnostics": diagnostics,
                "log_dir": log_dir_str,
            })

        except FastTransferError as e:
            return respond(f"# Execution Failed\n\nError: {str(e)}", {
                "status": "error",
                "tool": "fasttransfer_execute_transfer",
                "error": str(e),
            })

    async def handle_validate_connection(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_validate_connection tool."""
        try:
            # Validate request
            request = ConnectionValidationRequest(**arguments)

            # Build validation response
            connection = request.connection
            issues = []

            # Check for required fields based on connection type
            if (
                not connection.trusted_auth
                and not connection.connect_string
                and not connection.dsn
            ):
                if not connection.user:
                    issues.append(
                        "- Username is required (unless using trusted authentication, connect_string, or dsn)"
                    )

            # Check server format (only if server is provided)
            if (
                connection.server
                and ":" not in connection.server
                and "\\" not in connection.server
            ):
                issues.append(
                    f"- Server '{connection.server}' may need port (e.g., localhost:5432) or instance name"
                )

            if issues:
                response = [
                    f"# Connection Validation - {request.side.upper()}",
                    "",
                    "**Issues Found:**",
                    "",
                    *issues,
                    "",
                    "Note: This is a parameter check only. Actual connectivity is tested during transfer execution.",
                ]
            else:
                auth_method = "Trusted"
                if connection.connect_string:
                    auth_method = "Connection String"
                elif connection.dsn:
                    auth_method = "DSN"
                elif connection.trusted_auth:
                    auth_method = "Trusted"
                else:
                    auth_method = "Username/Password"

                response = [
                    f"# Connection Validation - {request.side.upper()}",
                    "",
                    "**All required parameters present**",
                    "",
                    f"- Connection Type: {connection.type}",
                    f"- Server: {connection.server or '(not specified)'}",
                    f"- Database: {connection.database}",
                    f"- Authentication: {auth_method}",
                    "",
                    "Note: This validates parameters only. Actual connectivity will be tested during transfer.",
                ]

            return [TextContent(type="text", text="\n".join(response))]

        except ValidationError as e:
            error_msg = ["# Validation Error", ""]
            for error in e.errors():
                field = " -> ".join(str(x) for x in error["loc"])
                error_msg.append(f"- **{field}**: {error['msg']}")
            tips = _suggest_next_steps(e.errors())
            if tips:
                error_msg.append("")
                error_msg.extend(tips)
            return [TextContent(type="text", text="\n".join(error_msg))]

    async def handle_list_combinations(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_list_combinations tool."""
        combinations = get_supported_combinations()

        response = [
            "# Supported Database Combinations",
            "",
            "FastTransfer supports transfers between the following database systems:",
            "",
        ]

        for source, targets in combinations.items():
            response.append(f"## {source}")
            response.append("")
            response.append("Can transfer to:")
            for target in targets:
                response.append(f"- {target}")
            response.append("")

        response.extend(
            [
                "## Notes:",
                "- All combinations support the Append and Truncate load modes; `Create` (FastTransfer 1.1+, `Replace` is its alias) creates the target table from the source when it does not exist, with `infer_data_type` fast, allvarchar or best. It is not tested yet with ClickHouse, Netezza or SAP HANA, with Teradata as a target, or with a backup source",
                "- Parallelism method availability depends on source database type",
                "- `Physical` (FastTransfer 1.0+) splits on the physical row location of PostgreSQL, Oracle, SQL Server and Netezza sources; other sources use a key-based method",
                "- ADBC (FastTransfer 1.0+): the `adbc_*` types need ADBC on both ends, an `adbc_*` source with an `adbc_*` target, and are never mixed with a native type. Windows x64 and Linux x64 only. Db2 is a target only through `adbc_db2`; file imports (`duckdbstream`) use a native target",
                "- Backup files (FastTransfer 1.1+): `backup_mssql` (SQL Server `.bak`) and `backup_rman` (Oracle RMAN backup sets) are read without restoring them and without a database, into an `adbc_*` target only. `server` is the directory of the backup files, `database` their names or patterns separated by `;`. Methods `None` and `Physical` only. Windows x64 and Linux x64 only",
            ]
        )

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_suggest_parallelism(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_suggest_parallelism tool."""
        try:
            # Validate request
            request = ParallelismSuggestionRequest(**arguments)

            # Get suggestion
            suggestion = suggest_parallelism_method(
                request.source_type,
                request.has_numeric_key,
                request.has_identity_column,
                request.table_size_estimate,
                physical_supported=_physical_supported(),
            )

            # Recommend degree based on table size
            size = request.table_size_estimate.lower()
            if size == "small":
                suggested_degree = 1
                degree_rationale = "Small table — parallelism overhead may exceed benefit."
            elif size == "medium":
                suggested_degree = 4
                degree_rationale = "Medium table — 4 threads is a good balance."
            else:
                suggested_degree = 0
                degree_rationale = "Large table — use all CPU cores (degree=0) for maximum throughput."

            response = [
                "# Parallelism Recommendation",
                "",
                f"**Method**: `{suggestion['method']}`",
                f"**Degree**: `{suggested_degree}` — {degree_rationale}",
                "",
                "## Rationale:",
                suggestion["explanation"],
                "",
                "## Next step:",
                "Call `fasttransfer_preview_transfer` with:",
                f"- `method`: `{suggestion['method']}`",
                f"- `degree`: `{suggested_degree}`",
            ]

            if suggestion['method'] in ("Ntile", "DataDriven", "RangeId", "Random"):
                response.append("- `distribute_key_column`: your key column name")
            elif request.source_type.lower() in BACKUP_TYPES:
                response.append("- source `schema` and `table` (or a `query` limited to a column list of one table)")
            elif suggestion['method'] in ("Physical", "Ctid", "Rowid", "Physloc"):
                response.append("- source `schema` and `table` (a `query` can be added to filter the table)")

            return [TextContent(type="text", text="\n".join(response))]

        except ValidationError as e:
            error_msg = ["# Validation Error", ""]
            for error in e.errors():
                field = " -> ".join(str(x) for x in error["loc"])
                error_msg.append(f"- **{field}**: {error['msg']}")
            tips = _suggest_next_steps(e.errors())
            if tips:
                error_msg.append("")
                error_msg.extend(tips)
            return [TextContent(type="text", text="\n".join(error_msg))]

    async def handle_get_version(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_get_version tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Error: FastTransfer binary not found or not accessible.\n"
                        f"Expected location: {config.get('path', '(unknown)')}\n"
                        "Please set FASTTRANSFER_PATH environment variable correctly."
                    ),
                )
            ]

        version_info = command_builder.get_version()
        caps = version_info["capabilities"]

        response = [
            "# FastTransfer Version Information",
            "",
        ]

        if version_info.get("preview_only"):
            response += [
                "**Mode**: Command builder (execution not available)",
                f"**Binary Path**: {version_info['binary_path']}",
                f"**Message**: {version_info['message']}",
                "",
                "Capabilities below are based on the latest known FastTransfer version.",
                "",
            ]
        else:
            response += [
                f"**Version**: {version_info['version'] or 'Unknown'}",
                f"**Detected**: {'Yes' if version_info['detected'] else 'No'}",
                f"**Binary Path**: {version_info['binary_path']}",
                "",
            ]

        response += [
            "## Supported Source Types:",
            ", ".join(f"`{t}`" for t in caps["source_types"]),
            "",
            "## Supported Target Types:",
            ", ".join(f"`{t}`" for t in caps["target_types"]),
            "",
            "## Supported Parallelism Methods:",
            ", ".join(f"`{m}`" for m in caps["parallelism_methods"]),
            "",
            "## Feature Flags:",
            f"- No Banner: {'Yes' if caps['supports_nobanner'] else 'No'}",
            f"- Version Flag: {'Yes' if caps['supports_version_flag'] else 'No'}",
            f"- File Input: {'Yes' if caps['supports_file_input'] else 'No'}",
            f"- Settings File: {'Yes' if caps['supports_settings_file'] else 'No'}",
            f"- License Path: {'Yes' if caps['supports_license_path'] else 'No'}",
            f"- License Required: {'Yes' if caps.get('requires_license') else 'No'}",
        ]

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_suggest_workflow(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_suggest_workflow tool."""
        source_type = arguments.get("source_type", "").lower()
        target_type = arguments.get("target_type", "").lower()
        table_size = arguments.get("table_size_estimate", "medium").lower()

        physical_ok = _physical_supported()
        native_source = native_source_type(source_type)

        def physical_tip(engine_method: str, how: str, needs_table: bool = True) -> str:
            """Tip for the physical-location split of the source engine."""
            needs = "needs schema and table; a query can be added to filter the table" if needs_table else "no schema or table needed"
            if physical_ok:
                return (
                    f"- **Recommended parallelism**: `Physical` (no key column needed; resolved to "
                    f"`{engine_method}` for this source; {needs})\n"
                    f"- {how}\n"
                    f"- `{engine_method}` is the deprecated name of the same method (removed in 2.0)"
                )
            return (
                f"- **Recommended parallelism**: `{engine_method}` (no key column needed; needs schema and table)\n"
                f"- {how}\n"
                f"- From FastTransfer 1.0, write `Physical` instead"
            )

        # Database-specific parallelism tips (ADBC types follow their native engine)
        db_tips = {
            "pgsql": (
                physical_tip("Ctid", "Works on any table by splitting on physical page ranges") + "\n"
                "- Partitioned table: `DBPartition` reads each leaf partition in its own stream (FastTransfer 0.18+)"
            ),
            "oraodp": (
                physical_tip("Rowid", "Splits by Oracle physical rowid ranges") + "\n"
                "- Partitioned table: `DBPartition` reads each partition / sub-partition in its own stream (FastTransfer 0.18+)"
            ),
            "mssql": (
                physical_tip(
                    "Physloc",
                    "Use it only on data that is not being written (read-only table, database or snapshot); "
                    "rowstore and clustered columnstore tables",
                ) + "\n"
                "- If table has an `IDENTITY` column, `RangeId` is also excellent\n"
                "- Partitioned table: `DBPartition` reads each partition of the partition scheme in its own stream (FastTransfer 0.18+)"
            ),
            "nzsql": physical_tip("NZDataSlice", "Splits on the Netezza data slices", needs_table=not physical_ok),
            "mysql": (
                "- **Recommended parallelism**: `RangeId` on the primary key\n"
                "- MySQL has no physical-location split (`Physical` is refused)\n"
                "- Partitioned table: `DBPartition` (FastTransfer 0.18+)"
            ),
            "teradata": (
                "- **Recommended parallelism**: `DBPartition` on a PPI / MLPPI table (row partitioning only, FastTransfer 0.18+), "
                "else `Ntile` or `RangeId` on a key\n"
                "- Teradata has no physical-location split (`Physical` is refused)"
            ),
            "adbc_db2": (
                "- **Recommended parallelism**: `DBPartition` on a range-partitioned Db2 for LUW table "
                "(one range predicate per data partition), else `RangeId` or `Ntile` on a key\n"
                "- Db2 has no physical-location split (`Physical` is refused)"
            ),
        }
        db_tips["pgcopy"] = db_tips["pgsql"]
        # Backup files (FastTransfer 1.1+): no SQL engine to split them
        db_tips["backup_mssql"] = db_tips["backup_rman"] = (
            "- **Recommended parallelism**: `Physical` (resolved to `Pages`): each of the `degree` streams reads one "
            "slice of the table (pages of a rowstore, rowgroups of a columnstore, ranges of Oracle blocks)\n"
            "- `None` reads the table in one stream; every other method is refused (a backup has no SQL engine)"
        )
        # Accepted for the physical split from FastTransfer 1.0 only
        if physical_ok:
            db_tips["msoledbsql"] = db_tips["mssql"]
            db_tips["nzbulk"] = db_tips["nzoledb"] = db_tips["nzsql"]

        # DBPartition / Timepartition (FastTransfer 0.18+) apply across sources
        partition_tip = (
            "- Date column: `Timepartition` with `distribute_key_column` '(date_col,year,month)' "
            "gives one stream per month, biggest first; it works with a table or a query "
            "(FastTransfer 0.18+). Pick a granularity giving several times more groups than the degree. "
            "Date bounds are sent as `yyyy-MM-dd`; set `FASTTRANSFER_DATEFORMAT=yyyyMMdd` if the source needs the compact format\n"
            "- `DBPartition` needs schema and table (no query) and falls back to a single stream "
            "if the table is not partitioned\n"
            "- `RangeId` and `Random` do not transfer the rows whose key is NULL. Before FastTransfer 1.0, "
            "`Ntile` on a date or timestamp key and `Ntile` / `DataDriven` on a key holding NULLs left rows out "
            "without an error: use a numeric NOT NULL key there, or upgrade"
        )

        if source_type in BACKUP_TYPES:
            # Timepartition, DBPartition and the key-based methods do not apply
            partition_tip = "- No key column, no `distribute_key_column`: a backup is split by the driver"

        db_tip = db_tips.get(source_type) or db_tips.get(native_source, (
            "- Call `fasttransfer_info` with action='parallelism' with your table characteristics for a specific recommendation"
        ))

        # Size-specific tips
        size_tips = {
            "small": "- For small tables, parallelism may not be needed (`None` method is fine)",
            "medium": "- Use a moderate parallelism degree (4-8 threads)",
            "large": "- Use a high parallelism degree (8-16+ threads) for best throughput",
        }

        size_tip = size_tips.get(table_size, size_tips["medium"])

        work_tables_tip = (
            "- Clustered Columnstore Index target: `use_work_tables: true` can help, on FastTransfer 1.0+ only. "
            "Before 1.0 the merge of the work tables fails on this target: nothing is loaded and the exit code is still 0"
        )
        pgcopy_limitation = (
            "- Known limitation: from a source that is not `pgcopy`, this target writes some "
            "values wrong without an error: `timestamptz` can be shifted by the server time zone, `inet` loses its prefix, "
            "and, on FastTransfer 1.0 and earlier (fixed in 1.1), fractional seconds are lost on `time` / `timestamp` and "
            "`bytea` arrives as the text `System.Byte[]`. "
            "For tables holding such columns use `pgcopy` → `pgcopy`, or `adbc_*` on both ends, and compare the values"
        )

        # Target-specific tips
        target_tips = {
            "mssql": work_tables_tip,
            "msbulk": (
                work_tables_tip + "\n"
                "- Partitioned target: `DataDriven`, `DBPartition` and `Timepartition` send a sorted hint on the "
                "partition column, except to a rowstore clustered index (FastTransfer 0.18+). Set "
                "`FASTTRANSFER_SORTEDINPUT=true` only if the source rows are really sorted on that column"
            ),
            "pgsql": "- PostgreSQL targets work well with all load modes",
            "pgcopy": (
                "- Binary COPY when the source is also `pgcopy`, text COPY otherwise\n"
                + (pgcopy_limitation if source_type != "pgcopy" else "- `pgcopy` → `pgcopy` copies every value exactly")
                + "\n- Before FastTransfer 1.0, `use_work_tables` loads nothing into this target (exit code 0): do not use it there"
            ),
            "oraodp": "- Oracle targets support direct-path loading for maximum speed",
            "orabulk": (
                "- Known limitation (FastTransfer 1.0): a native PostgreSQL source (`pgsql`, `pgcopy`) fails on a `date` column "
                "(`Unable to cast object of type 'System.DateOnly'`); use `adbc_pgsql` → `adbc_oracle` for such tables\n"
                "- A parallel load can fail with ORA-00054 (direct path lock on the target table): lower the degree"
            ),
            "adbc_mssql": (
                "- A target with a clustered index (rowstore or columnstore) is loaded without TABLOCK so the streams "
                "run together; a heap keeps TABLOCK. A heap with a nonclustered index loads one stream at a time\n"
                "- `arpemssql.bulk_*` options in the target `connect_string` win over that choice"
            ),
            "adbc_pgsql": (
                "- Converts a value whose type differs from its column (an Oracle `NUMBER` into `bigint`, text into `integer`); "
                "a value that does not fit stops the load with an error naming the column and row\n"
                "- PostgreSQL `infinity` dates and timestamps are copied exactly only from an `adbc_pgsql` source"
            ),
            "adbc_db2": (
                "- FastTransfer 1.0: parallel loads at volume can fail with SQLCODE -911 (lock escalation between the streams): "
                "use method `None`, or a `connect_string` with `adbc.arpedb2.ingest.commit_interval_rows=50000`. "
                "From 1.1 each stream of a parallel load commits every 50,000 rows\n"
                "- Known limitations: an Oracle `NUMBER` into a Db2 `BIGINT`, and `DECFLOAT` columns; test on a sample"
            ),
            "adbc_teradata": (
                "- Known limitations: `PERIOD` columns, and load speed on large tables; test on a sample"
            ),
        }

        target_tip = target_tips.get(target_type, "")

        # Backup sources (FastTransfer 1.1+)
        source_is_backup = source_type in BACKUP_TYPES
        backup_tip = ""
        if source_is_backup:
            files = (
                "the full backup, its differentials and every stripe, e.g. `mydb_full.bak;mydb_diff_*.bak`"
                if source_type == "backup_mssql"
                else "`*` and `?` in a name, `**` for any number of directories, a directory standing for its files, "
                "e.g. `l0;l1/**`"
            )
            backup_tip = (
                "- FastTransfer 1.1+: the backup files are read without restoring them and without a database "
                + ("(SQL Server full and differential backups, compressed, striped, encrypted or TDE ones included)"
                   if source_type == "backup_mssql"
                   else "(Oracle RMAN backup sets: full, level 0 and incremental level 1 to 4)")
                + ". Windows x64 and Linux x64 only\n"
                "- The target must be an `adbc_*` type (the backup drivers return Arrow data); another target is refused "
                "before anything runs\n"
                f"- `server` is the directory of the backup files, `database` the file names or patterns in it, separated by `;`: {files}. "
                "An entry that matches nothing stops the run\n"
                + ("- Encrypted backup: `user` is the private key file of the certificate (`.pvk`, `.pfx`, PEM...), "
                   "`password` its password\n" if source_type == "backup_mssql" else "")
                + "- Source: `schema` and `table`, or a `query` of the form `SELECT * | col [, col...] FROM [schema.]table`"
                + (" (`TOP n` accepted)" if source_type == "backup_mssql" else "")
                + "; no WHERE, no join\n"
                "- The latest version the files hold is read (full + differential, level 0 + incrementals). A backup that "
                "could be the newest and cannot be used stops the run with the target untouched, `Truncate` included. "
                "`FASTTRANSFER_BACKUP_RESTORE_MODE=best_effort` reads the newest usable version instead; "
                "`FASTTRANSFER_BACKUP_RESTORE_POINT=full` reads the last full (or level 0) backup only\n"
                "- The first read indexes each backup (minutes on a large compressed one) and saves the index for the next runs; "
                "`FASTTRANSFER_INDEX_DIR` (a fully qualified path) sets its directory\n"
                "- Driver options (`adbc.arpemsbackup.*` / `adbc.arpeorabackup.*`) go in a `connect_string`, which replaces "
                "`server` / `user` / `password` and wins over the environment variables"
            )

        # ADBC connection types (FastTransfer 1.0+)
        source_is_adbc = source_type in ADBC_TYPES
        target_is_adbc = target_type in ADBC_TYPES
        adbc_tip = ""
        if source_is_backup:
            if not target_is_adbc:
                adbc_tip = (
                    f"- **This pair is refused**: a backup source (`{source_type}`) needs an `adbc_*` target "
                    f"(adbc_mssql, adbc_pgsql, adbc_oracle, adbc_netezza, adbc_db2 or adbc_teradata), not `{target_type}`"
                )
            else:
                adbc_tip = (
                    "- ADBC target (bundled Arpe ADBC drivers). Windows x64 and Linux x64 only (not Linux ARM64)\n"
                    "- License: the drivers need the ARPE ADBC DRIVER block. FastTransfer hands it over when the `license_path` "
                    "file holds it next to the FASTTRANSFER block; without one the run exits 99\n"
                    "- Target `schema` and `table` are used exactly as typed: write them in their stored case "
                    "(`ORDERS` on Oracle, Db2 and Teradata; `orders` on PostgreSQL). The target table must exist, "
                    "unless `load_mode` is `Create`\n"
                    + (
                        "- This target negotiates TLS by default: against a server without TLS, set the "
                        "`FASTTRANSFER_ADBC_SSLMODE` environment variable (`disable`, `prefer`, `require`, "
                        "`verify-ca` or `verify-full`)\n"
                        if target_type in ("adbc_pgsql", "adbc_netezza")
                        else ""
                    )
                    + "- `use_work_tables` is refused and `batch_size` has no effect"
                )
        elif source_is_adbc != target_is_adbc:
            adbc_tip = (
                f"- **This pair is refused**: ADBC connection types (adbc_*) cannot be mixed with other types "
                f"(source `{source_type}`, target `{target_type}`). Use `adbc_*` on both the source and the target, or on neither. "
                "Db2 to a native-only target goes through an `odbc` source; files (`duckdbstream`) load into a native target"
            )
        elif source_is_adbc:
            adbc_tip = (
                "- ADBC on both ends (FastTransfer 1.0+): the bundled Arpe ADBC drivers move Arrow batches from the source "
                "to the target. Windows x64 and Linux x64 only (not Linux ARM64)\n"
                "- License: the drivers need the ARPE ADBC DRIVER block. FastTransfer hands it over when the `license_path` "
                "file holds it next to the FASTTRANSFER block; without one the run exits 99\n"
                "- Target `schema` and `table` are used exactly as typed: write them in their stored case "
                "(`ORDERS` on Oracle, Db2 and Teradata; `orders` on PostgreSQL). The target table must exist, "
                "unless `load_mode` is `Create` (FastTransfer 1.1+)\n"
                "- `adbc_pgsql` and `adbc_netezza` negotiate TLS by default: against a server without TLS, set the "
                "`FASTTRANSFER_ADBC_SSLMODE` environment variable (`disable`, `prefer`, `require`, `verify-ca` or "
                "`verify-full`; FastTransfer 1.1+). An `sslmode` in the `connect_string` wins\n"
                "- Connect with `server` / `database` / `user` / `password` (or `trusted_auth`, not on Netezza); "
                "a `connect_string` uses the driver's `uri=...` form, not ADO.NET keywords\n"
                "- `use_work_tables` is refused and `batch_size` has no effect\n"
                "- Time zone offsets (SQL Server `datetimeoffset`, Oracle `TIMESTAMP WITH TIME ZONE`, Teradata) arrive as the same instant in UTC"
            )

        response = [
            "# FastTransfer Workflow",
            "",
            f"**Source**: {source_type} → **Target**: {target_type} (table size: {table_size})",
            "",
            "## Step-by-Step",
            "",
            "### 1. Discover supported combinations",
            f"Call `fasttransfer_info` with action='combinations' to confirm `{source_type}` → `{target_type}` is supported.",
            "",
            "### 2. Choose parallelism method",
            "Call `fasttransfer_info` with action='parallelism' with your table characteristics.",
            "",
            db_tip,
            partition_tip,
            size_tip,
            "",
            "### 3. Preview the command",
            "Call `fasttransfer_preview_transfer` with your source, target, and options. It validates both source and target parameters while building the command.",
            "",
        ]

        response.append("### Load mode")
        response.append(
            "- `Append` (default) and `Truncate` need an existing target table. `Create` (FastTransfer 1.1+; `Replace` is its alias) "
            "creates it from the source when it does not exist, and truncates it when it exists\n"
            "- With `Create`, `infer_data_type` sets how the column types are derived: `fast` (default, from the source metadata), "
            "`allvarchar` (every column as text) or `best` (profiling scan, Oracle sources and files read through DuckDB). "
            "The created table has no key, index or constraint, and the target schema must exist"
            + ("\n- MySQL target: it must be in strict mode, and a table above the 65,535-byte row limit is not created"
               if target_type == "mysqlbulk" else "")
        )
        response.append("")

        if backup_tip:
            response.append("### Backup source")
            response.append(backup_tip)
            response.append("")

        if adbc_tip:
            response.append("### ADBC connection types")
            response.append(adbc_tip)
            response.append("")

        if target_tip:
            response.append(f"### Target tips ({target_type})")
            response.append(target_tip)
            response.append("")

        response.extend([
            "### 4. Execute",
            "Call `fasttransfer_execute_transfer` with the command from the preview.",
        ])

        return [TextContent(type="text", text="\n".join(response))]

    # ------------------------------------------------------------------ #
    # Dispatch
    # ------------------------------------------------------------------ #

    async def handle_info(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_info tool — dispatch based on action parameter."""
        action = arguments.get("action", "")
        if action == "combinations":
            return await handle_list_combinations(arguments)
        elif action == "parallelism":
            return await handle_suggest_parallelism(arguments)
        elif action == "workflow":
            return await handle_suggest_workflow(arguments)
        elif action == "version":
            return await handle_get_version(arguments)
        else:
            return [TextContent(
                type="text",
                text=f"Unknown action '{action}'. Valid actions: combinations, parallelism, workflow, version.",
            )]

    async def handle_call(name: str, arguments: Dict[str, Any]) -> list[TextContent]:
        """Route a tool call to the appropriate handler."""
        try:
            if name == "fasttransfer_preview_transfer":
                return await handle_preview_transfer(arguments)
            elif name == "fasttransfer_execute_transfer":
                return await handle_execute_transfer(arguments)
            elif name == "fasttransfer_info":
                return await handle_info(arguments)
            else:
                return None
        except Exception as e:
            logger.exception(f"Error handling tool '{name}': {e}")
            # Return structured content so tools that declare an outputSchema
            # (preview/execute) still satisfy it on an unexpected error.
            return respond(f"Error: {str(e)}", {"status": "error", "tool": name, "error": str(e)})

    return tools, handle_call


def _build_transfer_explanation(request: TransferRequest) -> str:
    """Build a human-readable explanation of what the transfer will do."""
    parts = []

    # Source
    source_is_backup = request.source.type.lower() in BACKUP_TYPES
    if source_is_backup:
        what = (
            "SQL Server backup files" if request.source.type.lower() == "backup_mssql"
            else "Oracle RMAN backup sets"
        )
        if request.source.query:
            read = "the query"
        elif request.source.table:
            read = "table " + (
                f"{request.source.schema}.{request.source.table}"
                if request.source.schema
                else request.source.table
            )
        else:
            read = f"the query of file '{request.source.file_input}'"
        where = (
            "the files of the connect_string"
            if request.source.connect_string
            else f"files '{request.source.database}' in directory '{request.source.server or '.'}'"
        )
        parts.append(
            f"Read {read} from {what} ({request.source.type}), without restoring them: {where}. "
            f"The latest version the files hold is read; the first read indexes each backup "
            f"(FASTTRANSFER_INDEX_DIR sets the index directory)"
        )
    elif request.source.file_input:
        parts.append(
            f"Import file '{request.source.file_input}' via {request.source.type} into {request.source.database}"
        )
    elif request.source.query and request.source.table:
        source_table = (
            f"{request.source.schema}.{request.source.table}"
            if request.source.schema
            else request.source.table
        )
        parts.append(
            f"Read from {request.source.type} table: {request.source.database}.{source_table}, "
            f"with the query as a filter on that table"
        )
    elif request.source.query:
        server_info = (
            f" ({request.source.server}/{request.source.database})"
            if request.source.server
            else f" ({request.source.database})"
        )
        parts.append(f"Execute query on {request.source.type}{server_info}")
    else:
        source_table = (
            f"{request.source.schema}.{request.source.table}"
            if request.source.schema
            else request.source.table
        )
        parts.append(
            f"Read from {request.source.type} table: {request.source.database}.{source_table}"
        )

    # Target
    target_table = (
        f"{request.target.schema}.{request.target.table}"
        if request.target.schema
        else request.target.table
    )
    parts.append(
        f"Write to {request.target.type} table: {request.target.database}.{target_table}"
    )
    if request.target.type.lower() in ADBC_TYPES:
        parts.append(
            "ADBC transfer (bundled Arpe ADBC drivers on both ends): the license must hold "
            "the ARPE ADBC DRIVER block, and the target schema and table are matched in "
            "their exact stored case"
        )

    # Load mode
    if request.options.load_mode.value in CREATE_LOAD_MODES:
        infer = (
            request.options.infer_data_type.value
            if request.options.infer_data_type
            else "fast"
        )
        alias = " (Replace is an alias of Create)" if request.options.load_mode.value == "Replace" else ""
        parts.append(
            f"Mode: CREATE{alias}: the target table is created from the source when it does not "
            f"exist (column types: {infer}), then loaded; when it exists it is TRUNCATED "
            f"(all existing data will be deleted) and loaded, its structure kept"
        )
    elif request.options.load_mode.value == "Truncate":
        parts.append(
            "Mode: TRUNCATE target table before loading (all existing data will be deleted)"
        )
    else:
        parts.append("Mode: APPEND to existing target table data")

    # Parallelism
    if request.options.method.value != "None":
        parallel_desc = f"Parallelism: {request.options.method.value} method"
        if request.options.method.value == "Timepartition":
            parallel_desc += (
                f" on {request.options.distribute_key_column} (one stream per time group, "
                f"biggest first; date bounds sent as yyyy-MM-dd unless "
                f"FASTTRANSFER_DATEFORMAT=yyyyMMdd)"
            )
        elif request.options.method.value == "DBPartition":
            parallel_desc += (
                " (one stream per native partition, biggest first; "
                "a single stream if the table is not partitioned)"
            )
        elif request.options.method.value == "Physical":
            strategy = PHYSICAL_STRATEGIES.get(native_source_type(request.source.type))
            if strategy == "Pages":
                parallel_desc += " (resolved to Pages: one slice of the table per stream)"
            else:
                parallel_desc += f" (physical row location, resolved to {strategy})"
        elif request.options.distribute_key_column:
            parallel_desc += f" on column '{request.options.distribute_key_column}'"
        parallel_desc += f" with degree {request.options.degree}"
        parts.append(parallel_desc)
    else:
        parts.append("Parallelism: None (single-threaded transfer)")

    # Mapping
    parts.append(f"Column mapping: {request.options.map_method.value}")

    return "\n".join(f"{i+1}. {part}" for i, part in enumerate(parts))
