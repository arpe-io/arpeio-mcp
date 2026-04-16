"""
FastBCP MCP tool handlers.

This module provides the tool definitions and handler functions
for FastBCP MCP tools, extracted from the original server.py.
"""

import logging
import shlex
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from mcp.types import Tool, ToolAnnotations, TextContent
from pydantic import ValidationError

from .validators import (
    ExportRequest,
    ConnectionValidationRequest,
    ParallelismSuggestionRequest,
    SourceConnectionType,
    OutputFormat,
    ParallelismMethod,
    StorageTarget,
    ParquetCompression,
    LoadMode,
    MapMethod,
    LogLevel,
    DecimalSeparator,
    ApplicationIntent,
    BoolFormat,
)
from src.base.error_patterns import diagnose_cli_error
from .command_builder import CommandBuilder, FastBCPError, get_supported_formats, suggest_parallelism_method
from .version import check_version_compatibility


logger = logging.getLogger(__name__)


def _suggest_next_steps(errors: list) -> list[str]:
    """Suggest next tools to call based on validation error fields."""
    tips = []
    error_fields = set()
    error_msgs = set()
    for error in errors:
        error_fields.update(str(x) for x in error["loc"])
        error_msgs.add(error["msg"].lower())

    if "type" in error_fields or any("source" in f for f in error_fields):
        tips.append("Tip: Use `fastbcp_info` with action='formats' to see supported source database types and output formats.")
    if "method" in error_fields or "distribute_key_column" in error_fields:
        tips.append("Tip: Use `fastbcp_info` with action='parallelism' to get the optimal parallelism method for your table.")
    if any("storage" in m or "cloud" in m for m in error_msgs):
        tips.append("Tip: Use `fastbcp_info` with action='formats' to see supported storage targets.")

    return tips


def _build_export_explanation(request: ExportRequest) -> str:
    """Build a human-readable explanation of what the export will do."""
    parts = []

    # Source
    if request.source.query:
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

    # Output format and destination
    output = request.output
    dest = output.file_output or output.directory or "(not specified)"
    parts.append(f"Export to {output.format.value.upper()} format: {dest}")

    # Storage target
    if output.storage_target.value != "local":
        parts.append(f"Storage target: {output.storage_target.value}")

    # Load mode
    if request.options.load_mode.value == "Truncate":
        parts.append("Mode: TRUNCATE before export")
    else:
        parts.append("Mode: APPEND to existing output")

    # Parallelism
    if request.options.method.value != "None":
        parallel_desc = f"Parallelism: {request.options.method.value} method"
        if request.options.distribute_key_column:
            parallel_desc += f" on column '{request.options.distribute_key_column}'"
        parallel_desc += f" with degree {request.options.degree}"
        parts.append(parallel_desc)
    else:
        parts.append("Parallelism: None (single-threaded export)")

    # Special options
    if output.timestamped:
        parts.append("Timestamped output filename enabled")
    if output.merge:
        parts.append("Merge parallel output files enabled")
    if output.parquet_compression:
        parts.append(f"Parquet compression: {output.parquet_compression.value}")
    if output.no_header:
        parts.append("Header row omitted")

    return "\n".join(f"{i+1}. {part}" for i, part in enumerate(parts))


def create_tools(command_builder: CommandBuilder, config: dict) -> Tuple[list, Any]:
    """Create FastBCP MCP tools and their handler function.

    Args:
        command_builder: Initialized CommandBuilder instance
        config: Configuration dict with keys:
            - timeout (int): Execution timeout in seconds (default: 1800)
            - log_dir (Path): Directory for execution logs
            - path (str): Path to the FastBCP binary

    Returns:
        Tuple of (tools_list, handle_call_async_function)
    """
    timeout = config.get("timeout", 1800)
    log_dir = config.get("log_dir")
    fastbcp_path = config.get("path", "")

    tools = [
        Tool(
            name="fastbcp_preview_export",
            description=(
                "Build and preview a FastBCP export command WITHOUT executing it. "
                "Shows the exact CLI command with passwords masked. "
                "If no parallelism method is specified, one is auto-suggested based on the source database type. "
                "Does NOT test connectivity or execute the export. "
                "After reviewing, pass the command to fastbcp_execute_export."
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
                    "source": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [e.value for e in SourceConnectionType],
                                "description": "Source database connection type (e.g., 'pgsql' for PostgreSQL, 'mssql' for SQL Server, 'oraodp' for Oracle, 'mysql' for MySQL)",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address (e.g., 'localhost:5432', 'myserver\\\\SQLEXPRESS', 'dbhost:1521')",
                            },
                            "database": {
                                "type": "string",
                                "description": "Database name",
                            },
                            "schema": {
                                "type": "string",
                                "description": "Schema name (optional)",
                            },
                            "table": {
                                "type": "string",
                                "description": "Table name (optional if query provided)",
                            },
                            "query": {
                                "type": "string",
                                "description": "SQL query (alternative to table)",
                            },
                            "user": {"type": "string", "description": "Username"},
                            "password": {"type": "string", "description": "Password"},
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted authentication",
                                "default": False,
                            },
                            "connect_string": {
                                "type": "string",
                                "description": "Full connection string (alternative to server/user/password)",
                            },
                            "dsn": {
                                "type": "string",
                                "description": "ODBC DSN name",
                            },
                            "provider": {
                                "type": "string",
                                "description": "OleDB provider name",
                            },
                            "application_intent": {
                                "type": "string",
                                "enum": [e.value for e in ApplicationIntent],
                                "description": "SQL Server application intent",
                            },
                        },
                        "required": ["type", "database"],
                    },
                    "output": {
                        "type": "object",
                        "properties": {
                            "format": {
                                "type": "string",
                                "enum": [e.value for e in OutputFormat],
                                "description": "Output file format. csv/tsv: delimited text. parquet: columnar, compressed, ideal for analytics. json: one JSON object per line (JSONL). bson: binary JSON. xlsx: Excel spreadsheet. binary: raw binary format.",
                            },
                            "file_output": {
                                "type": "string",
                                "description": "Output file path",
                            },
                            "directory": {
                                "type": "string",
                                "description": "Output directory path",
                            },
                            "storage_target": {
                                "type": "string",
                                "enum": [e.value for e in StorageTarget],
                                "description": "Where to write output files. local: local filesystem. s3/s3compatible: Amazon S3 or compatible. azure_blob/azure_datalake: Azure storage. fabric_onelake: Microsoft Fabric. gcs: Google Cloud Storage. Cloud targets require a cloud_profile.",
                                "default": "local",
                            },
                            "delimiter": {
                                "type": "string",
                                "description": "Field delimiter (CSV/TSV)",
                            },
                            "quotes": {
                                "type": "string",
                                "description": "Quote character",
                            },
                            "encoding": {
                                "type": "string",
                                "description": "Output file encoding",
                            },
                            "no_header": {
                                "type": "boolean",
                                "description": "Omit header row (CSV/TSV)",
                                "default": False,
                            },
                            "decimal_separator": {
                                "type": "string",
                                "enum": [e.value for e in DecimalSeparator],
                                "description": "Decimal separator for numeric values",
                            },
                            "date_format": {
                                "type": "string",
                                "description": "Date format string",
                            },
                            "bool_format": {
                                "type": "string",
                                "enum": [e.value for e in BoolFormat],
                                "description": "Boolean output format",
                            },
                            "parquet_compression": {
                                "type": "string",
                                "enum": [e.value for e in ParquetCompression],
                                "description": "Parquet compression algorithm",
                            },
                            "timestamped": {
                                "type": "boolean",
                                "description": "Add timestamp to output filename",
                                "default": False,
                            },
                            "merge": {
                                "type": "boolean",
                                "description": "Merge parallel output files into one. true = slower but produces a single file. false (default) = faster, produces N files (one per thread), better for parallel downstream import.",
                                "default": False,
                            },
                        },
                        "required": ["format"],
                    },
                    "options": {
                        "type": "object",
                        "properties": {
                            "method": {
                                "type": "string",
                                "enum": [e.value for e in ParallelismMethod],
                                "description": "Parallelism method. Ntile: even distribution on numeric key. DataDriven: distinct value partitions, works with any data type. Physloc: SQL Server physical location (no key needed). Ctid: PostgreSQL physical tuple ID (no key needed). Rowid: Oracle physical ROWID (no key needed). RangeId: numeric min/max range. Random: modulo-based approximate distribution. Timepartition: partition by time column (FastBCP 0.30+). NZDataSlice: Netezza native slicing. None: single-threaded. Call `fastbcp_suggest_parallelism` if unsure.",
                                "default": "None",
                            },
                            "distribute_key_column": {
                                "type": "string",
                                "description": "Column for data distribution. Required when method is RangeId, Random, Ntile, or DataDriven.",
                            },
                            "degree": {
                                "type": "integer",
                                "description": "Parallelism degree. 0 = use all CPU cores. Positive integer = exact thread count. Negative integer = cores / abs(value), e.g. -2 on 16 cores = 8 threads. Default 1 = single-threaded.",
                                "default": 1,
                            },
                            "load_mode": {
                                "type": "string",
                                "enum": [e.value for e in LoadMode],
                                "description": "How to handle existing output. Append: add to existing files. Truncate: overwrite existing output before writing.",
                                "default": "Append",
                            },
                            "batch_size": {
                                "type": "integer",
                                "description": "Batch size for export operations",
                            },
                            "map_method": {
                                "type": "string",
                                "enum": [e.value for e in MapMethod],
                                "description": "Column mapping method",
                                "default": "Position",
                            },
                            "run_id": {
                                "type": "string",
                                "description": "Run ID for logging",
                            },
                            "data_driven_query": {
                                "type": "string",
                                "description": "Custom SQL query for DataDriven parallelism method",
                            },
                            "settings_file": {
                                "type": "string",
                                "description": "Path to custom settings JSON file",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Override log level",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "description": "Suppress the FastBCP banner",
                            },
                            "license_path": {
                                "type": "string",
                                "description": "Path or URL to license file",
                            },
                            "cloud_profile": {
                                "type": "string",
                                "description": "Cloud storage profile name",
                            },
                        },
                    },
                    "config_file": {
                        "type": "string",
                        "description": "Path to a YAML configuration file (--config parameter, requires FastBCP 0.30+)",
                    },
                    "os_type": {
                        "type": "string",
                        "enum": ["linux", "windows"],
                        "description": "Target operating system for command formatting",
                        "default": "linux",
                    },
                },
                "required": ["source", "output"],
            },
        ),
        Tool(
            name="fastbcp_execute_export",
            description=(
                "Execute a FastBCP export command that was previously built by fastbcp_preview_export. "
                "Requires confirmation=true as a safety gate. "
                "Does NOT build the command — the command string must come from a prior fastbcp_preview_export call. "
                "Will fail if the FastBCP binary is not installed."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=False,
                openWorldHint=True,
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The full command string from fastbcp_preview_export output (with actual passwords, not the masked version).",
                    },
                    "confirmation": {
                        "type": "boolean",
                        "description": "Safety gate — must be true to execute. Confirms the user has reviewed the command from fastbcp_preview_export.",
                    },
                },
                "required": ["command", "confirmation"],
            },
        ),
        Tool(
            name="fastbcp_info",
            description=(
                "Get information about FastBCP capabilities, parallelism recommendations, "
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
                        "enum": ["formats", "parallelism", "workflow", "version"],
                        "description": (
                            "What information to retrieve. "
                            "formats: list supported source databases, output formats, and storage targets. "
                            "parallelism: get a recommended parallelism method for a specific table (requires source_type, has_numeric_key, table_size_estimate). "
                            "workflow: get a step-by-step export guide with database-specific tips (requires source_type, output_format). "
                            "version: report installed binary version and capabilities."
                        ),
                    },
                    "source_type": {
                        "type": "string",
                        "enum": [e.value for e in SourceConnectionType],
                        "description": "Source database type (required for action=parallelism and action=workflow)",
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
                    "output_format": {
                        "type": "string",
                        "enum": [e.value for e in OutputFormat],
                        "description": "Desired output format (required for action=workflow)",
                    },
                    "storage_target": {
                        "type": "string",
                        "enum": [e.value for e in StorageTarget],
                        "description": "Storage target (optional, for action=workflow, default: local)",
                        "default": "local",
                    },
                },
                "required": ["action"],
            },
        ),
    ]

    # --- Handler functions (closures over command_builder, timeout, log_dir, fastbcp_path) ---

    async def handle_preview_export(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fastbcp_preview_export tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Error: FastBCP server failed to initialize.\n"
                        f"Expected binary location: {fastbcp_path}\n"
                        "Please set FASTBCP_PATH environment variable correctly."
                    ),
                )
            ]

        try:
            # Extract os_type before passing to ExportRequest (not part of the model)
            os_type = arguments.pop("os_type", "linux")

            # Extract config_file before passing to ExportRequest (not part of the model)
            config_file = arguments.pop("config_file", None)

            # Auto-suggest parallelism if not specified
            auto_parallelism_note = None
            options = arguments.get("options") or {}
            method = options.get("method", "None")
            if method == "None" and options.get("degree", 1) == 1:
                source_type = (arguments.get("source") or {}).get("type", "")
                if source_type:
                    suggestion = suggest_parallelism_method(
                        source_type,
                        has_numeric_key=False,
                        has_identity_column=False,
                        table_size_estimate="medium",
                    )
                    suggested_method = suggestion["method"]
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
            request = ExportRequest(**arguments)

            # Check version compatibility
            version_warnings = check_version_compatibility(
                arguments,
                command_builder.version_detector.capabilities,
                command_builder.version_detector._detected_version,
            )

            # Build command
            command = command_builder.build_command(request, config_file=config_file)

            # Format for display (with masked passwords)
            display_command = command_builder.format_command_display(command, mask=True, os_type=os_type)

            # Create explanation
            explanation = _build_export_explanation(request)

            # Build response
            response = [
                "# FastBCP Command Preview",
                "",
            ]

            if command_builder._preview_only:
                response += [
                    "**NOTE: Execution is not available** (binary not configured). "
                    "Command preview is available. "
                    "Download from https://arpe.io to enable execution.",
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

            log_dir_str = str(log_dir) if log_dir else "(not configured)"

            response += [
                "",
                "## Command (passwords masked):",
                "```bash",
                display_command,
                "```",
                "",
                "## To execute this export:",
                "1. Review the command carefully",
                "2. Use the `fastbcp_execute_export` tool with the FULL command (not the masked version)",
                "3. Set `confirmation: true` to proceed",
                "",
                "## Security Notice:",
                "- Passwords are masked in this preview (shown as ******)",
                "- The actual execution will use the real passwords you provided",
                "- All executions are logged (with masked passwords) to: "
                + log_dir_str,
                "",
                "## Full command for execution:",
                "```",
                " ".join(command),
                "```",
            ]

            return [TextContent(type="text", text="\n".join(response))]

        except ValidationError as e:
            error_msg = [
                "# Validation Error",
                "",
                "The provided parameters are invalid:",
                "",
            ]
            for error in e.errors():
                field = " -> ".join(str(x) for x in error["loc"])
                error_msg.append(f"- **{field}**: {error['msg']}")
            tips = _suggest_next_steps(e.errors())
            if tips:
                error_msg.append("")
                error_msg.extend(tips)
            return [TextContent(type="text", text="\n".join(error_msg))]

        except FastBCPError as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def handle_execute_export(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fastbcp_execute_export tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text="Error: FastBCP server failed to initialize. Please check FASTBCP_PATH.",
                )
            ]

        if command_builder._preview_only:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Execution requires the FastBCP binary. "
                        "Download from https://arpe.io and set FASTBCP_PATH to enable."
                    ),
                )
            ]

        # Check confirmation
        if not arguments.get("confirmation", False):
            return [
                TextContent(
                    type="text",
                    text=(
                        "# Execution Blocked\n\n"
                        "You must set `confirmation: true` to execute an export.\n"
                        "This safety mechanism ensures commands are only executed with explicit approval.\n\n"
                        "Please review the command carefully and confirm by setting:\n"
                        "```json\n"
                        '{"confirmation": true}\n'
                        "```"
                    ),
                )
            ]

        # Get command
        command_str = arguments.get("command", "")
        if not command_str:
            return [
                TextContent(
                    type="text",
                    text="Error: No command provided. Please provide the command from fastbcp_preview_export.",
                )
            ]

        # Parse command string into list
        try:
            command = shlex.split(command_str)
        except ValueError as e:
            return [TextContent(type="text", text=f"Error parsing command: {str(e)}")]

        # Execute
        try:
            logger.info("Starting FastBCP execution...")
            return_code, stdout, stderr = command_builder.execute_command(
                command, timeout=timeout, log_dir=log_dir
            )

            # Format response
            success = return_code == 0
            log_dir_str = str(log_dir) if log_dir else "(not configured)"

            response = [
                f"# FastBCP Export {'Completed' if success else 'Failed'}",
                "",
                f"**Status**: {'Success' if success else 'Failed'}",
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
                            "- Check output path is writable",
                            "- Review the full log file for more information",
                        ]
                    )

            return [TextContent(type="text", text="\n".join(response))]

        except FastBCPError as e:
            return [TextContent(type="text", text=f"# Execution Failed\n\nError: {str(e)}")]

    async def handle_validate_connection(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fastbcp_validate_connection tool."""
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
                    "Note: This is a parameter check only. Actual connectivity is tested during export execution.",
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
                    "Note: This validates parameters only. Actual connectivity will be tested during export.",
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

    async def handle_list_formats(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fastbcp_list_formats tool."""
        formats = get_supported_formats()

        response = [
            "# Supported Formats and Sources",
            "",
            "FastBCP supports exporting from the following database systems to files:",
            "",
        ]

        # Database sources
        response.append("## Database Sources")
        response.append("")
        for source, output_formats in formats["Database Sources"].items():
            response.append(f"### {source}")
            response.append(f"Supported formats: {', '.join(output_formats)}")
            response.append("")

        # Output formats
        response.append("## Output Formats")
        response.append("")
        for fmt in formats["Output Formats"]:
            response.append(f"- `{fmt}`")
        response.append("")

        # Storage targets
        response.append("## Storage Targets")
        response.append("")
        for target in formats["Storage Targets"]:
            response.append(f"- `{target}`")
        response.append("")

        response.extend(
            [
                "## Notes:",
                "- All source databases support all output formats",
                "- Parallelism method availability depends on source database type",
                "- Cloud storage targets require a cloud_profile configuration",
            ]
        )

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_suggest_parallelism(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fastbcp_suggest_parallelism tool."""
        try:
            # Validate request
            request = ParallelismSuggestionRequest(**arguments)

            # Get suggestion
            suggestion = suggest_parallelism_method(
                request.source_type,
                request.has_numeric_key,
                request.has_identity_column,
                request.table_size_estimate,
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
                "Call `fastbcp_preview_export` with:",
                f"- `method`: `{suggestion['method']}`",
                f"- `degree`: `{suggested_degree}`",
            ]

            if suggestion['method'] in ("Ntile", "DataDriven", "RangeId", "Random"):
                response.append("- `distribute_key_column`: your key column name")


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
        """Handle fastbcp_get_version tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Error: FastBCP server failed to initialize.\n"
                        f"Expected binary location: {fastbcp_path}\n"
                        "Please set FASTBCP_PATH environment variable correctly."
                    ),
                )
            ]

        version_info = command_builder.get_version()
        caps = version_info["capabilities"]

        response = [
            "# FastBCP Version Information",
            "",
        ]

        if version_info.get("preview_only"):
            response += [
                "**Mode**: Command builder (execution not available)",
                f"**Binary Path**: {version_info['binary_path']}",
                f"**Message**: {version_info['message']}",
                "",
                "Capabilities below are based on the latest known FastBCP version.",
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
            "## Supported Output Formats:",
            ", ".join(f"`{f}`" for f in caps["output_formats"]),
            "",
            "## Supported Parallelism Methods:",
            ", ".join(f"`{m}`" for m in caps["parallelism_methods"]),
            "",
            "## Supported Storage Targets:",
            ", ".join(f"`{t}`" for t in caps["storage_targets"]),
            "",
            "## Feature Flags:",
            f"- No Banner: {'Yes' if caps['supports_nobanner'] else 'No'}",
            f"- Version Flag: {'Yes' if caps['supports_version_flag'] else 'No'}",
            f"- Cloud Profile: {'Yes' if caps['supports_cloud_profile'] else 'No'}",
            f"- Merge: {'Yes' if caps['supports_merge'] else 'No'}",
            f"- Config File: {'Yes' if caps['supports_config_file'] else 'No'}",
        ]

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_suggest_workflow(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fastbcp_suggest_workflow tool."""
        source_type = arguments.get("source_type", "").lower()
        output_format = arguments.get("output_format", "").lower()
        storage_target = arguments.get("storage_target", "local").lower()

        # Database-specific parallelism tips
        db_tips = {
            "pgsql": (
                "- **Recommended parallelism**: `Ctid` (no key column needed, PostgreSQL-native)\n"
                "- Ctid splits the table by physical page ranges — works on any table\n"
                "- Alternative: `RangeId` or `Ntile` if you have a numeric key with good distribution"
            ),
            "oraodp": (
                "- **Recommended parallelism**: `Rowid` (no key column needed, Oracle-native)\n"
                "- Rowid splits by Oracle physical rowid ranges — works on any table\n"
                "- Alternative: `RangeId` or `Ntile` with a numeric/date key"
            ),
            "mssql": (
                "- **Recommended parallelism**: `Physloc` (no key column needed, SQL Server-native)\n"
                "- If table has an `IDENTITY` column, `RangeId` is also excellent\n"
                "- Alternative: `Ntile` with any indexed column"
            ),
            "mysql": (
                "- **Recommended parallelism**: `RangeId` on the primary key\n"
                "- MySQL does not support Ctid/Rowid/Physloc\n"
                "- Alternative: `Ntile` or `DataDriven` with a well-distributed column"
            ),
        }

        db_tip = db_tips.get(source_type, (
            "- Call `fastbcp_suggest_parallelism` with your table characteristics for a specific recommendation"
        ))

        # Format-specific tips
        format_tips = {
            "parquet": "- Parquet is columnar and highly compressed — ideal for analytics and cloud storage",
            "csv": "- CSV is universally compatible — consider setting delimiter, encoding, and quote character",
            "tsv": "- TSV uses tab delimiters — good for data with commas in values",
            "json": "- JSON output produces one JSON object per line (JSONL format)",
            "avro": "- Avro includes schema in the file — good for schema evolution workflows",
        }

        format_tip = format_tips.get(output_format, "")

        # Cloud storage tips
        cloud_tips = ""
        if storage_target != "local":
            cloud_tips = (
                f"\n### Cloud Storage ({storage_target})\n"
                f"- Set up a `cloud_profile` in your FastBCP configuration\n"
                f"- The profile contains credentials for {storage_target} access\n"
                f"- Pass `cloud_profile` in the options when building the export command"
            )

        response = [
            "# FastBCP Export Workflow",
            "",
            f"**Source**: {source_type} → **Format**: {output_format} → **Target**: {storage_target}",
            "",
            "## Step-by-Step",
            "",
            f"### 1. Discover capabilities",
            f"Call `fastbcp_list_formats` to confirm `{source_type}` and `{output_format}` are supported.",
            "",
            f"### 2. Choose parallelism method",
            f"Call `fastbcp_suggest_parallelism` with your table characteristics.",
            "",
            db_tip,
            "",
            f"### 3. Validate connection",
            f"Call `fastbcp_validate_connection` to verify your database parameters.",
            "",
            f"### 4. Preview the command",
            f"Call `fastbcp_preview_export` with your source, output, and options.",
            "",
            format_tip,
            "",
            f"### 5. Execute",
            f"Call `fastbcp_execute_export` with the command from the preview.",
            "",
        ]

        if cloud_tips:
            response.append(cloud_tips)

        return [TextContent(type="text", text="\n".join(response))]

    # --- Dispatcher ---

    async def handle_info(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fastbcp_info tool — dispatch based on action parameter."""
        action = arguments.get("action", "")
        if action == "formats":
            return await handle_list_formats(arguments)
        elif action == "parallelism":
            return await handle_suggest_parallelism(arguments)
        elif action == "workflow":
            return await handle_suggest_workflow(arguments)
        elif action == "version":
            return await handle_get_version(arguments)
        else:
            return [TextContent(
                type="text",
                text=f"Unknown action '{action}'. Valid actions: formats, parallelism, workflow, version.",
            )]

    async def handle_call(name: str, arguments: Dict[str, Any]):
        """Route tool calls to the appropriate handler.

        Args:
            name: Tool name
            arguments: Tool arguments

        Returns:
            List of TextContent, or None if the tool name is not handled.
        """
        if name == "fastbcp_preview_export":
            return await handle_preview_export(arguments)
        elif name == "fastbcp_execute_export":
            return await handle_execute_export(arguments)
        elif name == "fastbcp_info":
            return await handle_info(arguments)
        return None  # not our tool

    return tools, handle_call
