"""
FastTransfer MCP tool definitions and handlers.

This module defines all FastTransfer tools and their handler functions,
designed to be aggregated by a unified MCP server.
"""

import logging
import shlex
from pathlib import Path
from typing import Any, Dict, List, Tuple, Callable, Awaitable

from mcp.types import Tool, ToolAnnotations, TextContent
from pydantic import ValidationError

from .command_builder import CommandBuilder, FastTransferError
from .command_builder import get_supported_combinations, suggest_parallelism_method
from .validators import (
    TransferRequest,
    ConnectionValidationRequest,
    ParallelismSuggestionRequest,
    SourceConnectionType,
    TargetConnectionType,
    ParallelismMethod,
    LoadMode,
    MapMethod,
    LogLevel,
)
from .version import check_version_compatibility


logger = logging.getLogger(__name__)


def _suggest_next_steps(errors: list) -> list[str]:
    """Suggest next tools to call based on validation error fields."""
    tips = []
    error_fields = set()
    for error in errors:
        error_fields.update(str(x) for x in error["loc"])

    if "type" in error_fields or any("source" in f or "target" in f for f in error_fields):
        tips.append("Tip: Use `fasttransfer_list_combinations` to see supported source→target database pairs.")
    if "method" in error_fields or "distribute_key_column" in error_fields:
        tips.append("Tip: Use `fasttransfer_suggest_parallelism` to get the optimal parallelism method for your table.")

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
                "Build and preview a FastTransfer command WITHOUT executing it. "
                "This shows the exact command that will be run, with passwords masked. "
                "Call `fasttransfer_suggest_parallelism` first to choose the optimal parallelism method. "
                "After previewing, use `fasttransfer_execute_transfer` to run the command."
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
                                "description": "Table name (optional if query or file_input provided)",
                            },
                            "query": {
                                "type": "string",
                                "description": "SQL query (alternative to table)",
                            },
                            "file_input": {
                                "type": "string",
                                "description": "File path for data input (alternative to table/query)",
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
                        },
                        "required": ["type", "database"],
                    },
                    "target": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [e.value for e in TargetConnectionType],
                                "description": "Target database connection type",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address (host:port or host\\instance)",
                            },
                            "database": {
                                "type": "string",
                                "description": "Database name",
                            },
                            "schema": {"type": "string", "description": "Schema name"},
                            "table": {
                                "type": "string",
                                "description": "Table name (required)",
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
                        },
                        "required": ["type", "database", "table"],
                    },
                    "os_type": {
                        "type": "string",
                        "enum": ["linux", "windows"],
                        "description": "Target operating system for command formatting",
                        "default": "linux",
                    },
                    "options": {
                        "type": "object",
                        "properties": {
                            "method": {
                                "type": "string",
                                "enum": [e.value for e in ParallelismMethod],
                                "description": "Parallelism method. Call `fasttransfer_suggest_parallelism` first to get the optimal method for your source database and table.",
                                "default": "None",
                            },
                            "distribute_key_column": {
                                "type": "string",
                                "description": "Column for data distribution. Required when method is RangeId, Random, Ntile, or DataDriven.",
                            },
                            "degree": {
                                "type": "integer",
                                "description": "Parallelism degree",
                                "default": -2,
                            },
                            "load_mode": {
                                "type": "string",
                                "enum": [e.value for e in LoadMode],
                                "description": "Load mode",
                                "default": "Append",
                            },
                            "batch_size": {
                                "type": "integer",
                                "description": "Batch size for bulk operations",
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
                            "use_work_tables": {
                                "type": "boolean",
                                "description": "Use intermediate work tables for CCI",
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
                                "description": "Suppress the FastTransfer banner",
                            },
                            "license_path": {
                                "type": "string",
                                "description": "Path or URL to license file",
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
                "Execute a FastTransfer command that was previously previewed. "
                "IMPORTANT: You must set confirmation=true to execute. "
                "Call `fasttransfer_preview_transfer` first to review the command before executing."
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
                        "description": "The exact command from fasttransfer_preview_transfer (including actual passwords)",
                    },
                    "confirmation": {
                        "type": "boolean",
                        "description": "Must be true to execute. This confirms the user has reviewed the command.",
                    },
                },
                "required": ["command", "confirmation"],
            },
        ),
        Tool(
            name="fasttransfer_validate_connection",
            description=(
                "Validate database connection parameters before building a transfer command. "
                "This checks that all required parameters are provided but does NOT "
                "actually test connectivity (would require database access). "
                "Call this before `fasttransfer_preview_transfer` to catch parameter issues early."
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
                    "connection": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "description": "Connection type",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address",
                            },
                            "database": {
                                "type": "string",
                                "description": "Database name",
                            },
                            "user": {"type": "string", "description": "Username"},
                            "password": {"type": "string", "description": "Password"},
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
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted authentication",
                            },
                            "file_input": {
                                "type": "string",
                                "description": "File path for data input",
                            },
                        },
                        "required": ["type", "database"],
                    },
                    "side": {
                        "type": "string",
                        "enum": ["source", "target"],
                        "description": "Connection side",
                    },
                },
                "required": ["connection", "side"],
            },
        ),
        Tool(
            name="fasttransfer_list_combinations",
            description=(
                "List all supported source to target database combinations. "
                "Call this first to discover which database pairs FastTransfer supports before building a transfer command."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="fasttransfer_suggest_parallelism",
            description=(
                "Suggest the optimal parallelism method based on source database type "
                "and table characteristics. Call this before `fasttransfer_preview_transfer` to choose "
                "the right method and degree for best performance."
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
                    "source_type": {
                        "type": "string",
                        "description": "Source database type (e.g., 'pgsql', 'oraodp', 'mssql')",
                    },
                    "has_numeric_key": {
                        "type": "boolean",
                        "description": "Whether the table has a numeric key column",
                    },
                    "has_identity_column": {
                        "type": "boolean",
                        "description": "Whether the table has an identity/auto-increment column",
                        "default": False,
                    },
                    "table_size_estimate": {
                        "type": "string",
                        "enum": ["small", "medium", "large"],
                        "description": "Estimated table size",
                    },
                },
                "required": ["source_type", "has_numeric_key", "table_size_estimate"],
            },
        ),
        Tool(
            name="fasttransfer_get_version",
            description=(
                "Get the detected FastTransfer binary version, capabilities, "
                "and supported source/target types. "
                "Use this to check which features are available in the installed version."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="fasttransfer_suggest_workflow",
            description=(
                "Get a step-by-step workflow guide for transferring data between databases with FastTransfer, "
                "including database-specific tips and recommended parallelism methods. "
                "Call this early in your workflow to plan the right sequence of tool calls."
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
                    "source_type": {
                        "type": "string",
                        "description": "Source database type (e.g., 'pgsql', 'mssql', 'oraodp', 'mysql')",
                    },
                    "target_type": {
                        "type": "string",
                        "description": "Target database type (e.g., 'pgsql', 'mssql', 'oraodp', 'mysql')",
                    },
                    "table_size_estimate": {
                        "type": "string",
                        "enum": ["small", "medium", "large"],
                        "description": "Estimated table size to tailor parallelism advice",
                        "default": "medium",
                    },
                },
                "required": ["source_type", "target_type"],
            },
        ),
    ]

    # ------------------------------------------------------------------ #
    # Handler functions
    # ------------------------------------------------------------------ #

    async def handle_preview_transfer(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_preview_transfer tool."""
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

        try:
            # Extract os_type before passing to TransferRequest (not part of the model)
            os_type = arguments.pop("os_type", "linux")

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

        except FastTransferError as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def handle_execute_transfer(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_execute_transfer tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text="Error: FastTransfer binary not found. Please check FASTTRANSFER_PATH.",
                )
            ]

        if command_builder.preview_only:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Execution requires the FastTransfer binary. "
                        "Download from https://arpe.io and set FASTTRANSFER_PATH to enable."
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
                        "You must set `confirmation: true` to execute a transfer.\n"
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
                    text="Error: No command provided. Please provide the command from fasttransfer_preview_transfer.",
                )
            ]

        # Parse command string into list
        try:
            command = shlex.split(command_str)
        except ValueError as e:
            return [TextContent(type="text", text=f"Error parsing command: {str(e)}")]

        # Execute
        try:
            logger.info("Starting FastTransfer execution...")
            return_code, stdout, stderr = command_builder.execute_command(
                command, timeout=timeout, log_dir=log_dir
            )

            # Format response
            success = return_code == 0
            status = "Success" if success else "Failed"

            response = [
                f"# FastTransfer Execution - {status}",
                "",
                f"**Status**: {status}",
                f"**Return Code**: {return_code}",
                f"**Log Location**: {log_dir}",
                "",
                "## Output:",
                "```",
                stdout if stdout else "(no output)",
                "```",
            ]

            if stderr:
                response.extend(["", "## Error Output:", "```", stderr, "```"])

            if not success:
                response.extend(
                    [
                        "",
                        "## Troubleshooting:",
                        "- Check database credentials and connectivity",
                        "- Verify table/schema names exist",
                        "- Check FastTransfer documentation for error details",
                        "- Review the full log file for more information",
                    ]
                )

            return [TextContent(type="text", text="\n".join(response))]

        except FastTransferError as e:
            return [TextContent(type="text", text=f"# Execution Failed\n\nError: {str(e)}")]

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
                "- All combinations support both Append and Truncate load modes",
                "- Parallelism method availability depends on source database type",
                "- Some database-specific features (like Ctid for PostgreSQL) only work with specific sources",
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
            )

            response = [
                "# Parallelism Method Recommendation",
                "",
                f"**Recommended Method**: `{suggestion['method']}`",
                "",
                "## Explanation:",
                suggestion["explanation"],
                "",
                "## Your Table Characteristics:",
                f"- Source Database: {request.source_type}",
                f"- Has Numeric Key: {'Yes' if request.has_numeric_key else 'No'}",
                f"- Has Identity Column: {'Yes' if request.has_identity_column else 'No'}",
                f"- Table Size: {request.table_size_estimate.capitalize()}",
                "",
                "## Other Considerations:",
                "- **Ctid**: Best for PostgreSQL (no key column needed)",
                "- **Rowid**: Best for Oracle (no key column needed)",
                "- **Physloc**: Best for SQL Server without numeric key",
                "- **RangeId**: Requires numeric key with good distribution",
                "- **Random**: Requires numeric key, uses modulo distribution",
                "- **DataDriven**: Works with any data type, uses distinct values",
                "- **Ntile**: Even distribution, works with numeric/date/string columns",
                "- **None**: Single-threaded, best for small tables or troubleshooting",
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
        ]

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_suggest_workflow(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle fasttransfer_suggest_workflow tool."""
        source_type = arguments.get("source_type", "").lower()
        target_type = arguments.get("target_type", "").lower()
        table_size = arguments.get("table_size_estimate", "medium").lower()

        # Database-specific parallelism tips
        db_tips = {
            "pgsql": (
                "- **Recommended parallelism**: `Ctid` (PostgreSQL-native, no key column needed)\n"
                "- Works on any table by splitting on physical page ranges"
            ),
            "oraodp": (
                "- **Recommended parallelism**: `Rowid` (Oracle-native, no key column needed)\n"
                "- Splits by Oracle physical rowid ranges"
            ),
            "mssql": (
                "- **Recommended parallelism**: `Physloc` (SQL Server-native, no key column needed)\n"
                "- If table has an `IDENTITY` column, `RangeId` is also excellent"
            ),
            "mysql": (
                "- **Recommended parallelism**: `RangeId` on the primary key\n"
                "- MySQL does not support Ctid/Rowid/Physloc"
            ),
        }

        db_tip = db_tips.get(source_type, (
            "- Call `fasttransfer_suggest_parallelism` with your table characteristics for a specific recommendation"
        ))

        # Size-specific tips
        size_tips = {
            "small": "- For small tables, parallelism may not be needed (`None` method is fine)",
            "medium": "- Use a moderate parallelism degree (4-8 threads)",
            "large": "- Use a high parallelism degree (8-16+ threads) for best throughput",
        }

        size_tip = size_tips.get(table_size, size_tips["medium"])

        # Target-specific tips
        target_tips = {
            "mssql": "- Consider using `use_work_tables: true` for Clustered Columnstore Index targets",
            "pgsql": "- PostgreSQL targets work well with all load modes",
            "oraodp": "- Oracle targets support direct-path loading for maximum speed",
        }

        target_tip = target_tips.get(target_type, "")

        response = [
            "# FastTransfer Workflow",
            "",
            f"**Source**: {source_type} → **Target**: {target_type} (table size: {table_size})",
            "",
            "## Step-by-Step",
            "",
            "### 1. Discover supported combinations",
            f"Call `fasttransfer_list_combinations` to confirm `{source_type}` → `{target_type}` is supported.",
            "",
            "### 2. Choose parallelism method",
            "Call `fasttransfer_suggest_parallelism` with your table characteristics.",
            "",
            db_tip,
            size_tip,
            "",
            "### 3. Validate connections",
            "Call `fasttransfer_validate_connection` for both source and target.",
            "",
            "### 4. Preview the command",
            "Call `fasttransfer_preview_transfer` with your source, target, and options.",
            "",
        ]

        if target_tip:
            response.append(f"### Target tips ({target_type})")
            response.append(target_tip)
            response.append("")

        response.extend([
            "### 5. Execute",
            "Call `fasttransfer_execute_transfer` with the command from the preview.",
        ])

        return [TextContent(type="text", text="\n".join(response))]

    # ------------------------------------------------------------------ #
    # Dispatch
    # ------------------------------------------------------------------ #

    async def handle_call(name: str, arguments: Dict[str, Any]) -> list[TextContent]:
        """Route a tool call to the appropriate handler."""
        try:
            if name == "fasttransfer_preview_transfer":
                return await handle_preview_transfer(arguments)
            elif name == "fasttransfer_execute_transfer":
                return await handle_execute_transfer(arguments)
            elif name == "fasttransfer_validate_connection":
                return await handle_validate_connection(arguments)
            elif name == "fasttransfer_list_combinations":
                return await handle_list_combinations(arguments)
            elif name == "fasttransfer_suggest_parallelism":
                return await handle_suggest_parallelism(arguments)
            elif name == "fasttransfer_get_version":
                return await handle_get_version(arguments)
            elif name == "fasttransfer_suggest_workflow":
                return await handle_suggest_workflow(arguments)
            else:
                return None
        except Exception as e:
            logger.exception(f"Error handling tool '{name}': {e}")
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    return tools, handle_call


def _build_transfer_explanation(request: TransferRequest) -> str:
    """Build a human-readable explanation of what the transfer will do."""
    parts = []

    # Source
    if request.source.file_input:
        parts.append(
            f"Import file '{request.source.file_input}' via {request.source.type} into {request.source.database}"
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

    # Load mode
    if request.options.load_mode.value == "Truncate":
        parts.append(
            "Mode: TRUNCATE target table before loading (all existing data will be deleted)"
        )
    else:
        parts.append("Mode: APPEND to existing target table data")

    # Parallelism
    if request.options.method.value != "None":
        parallel_desc = f"Parallelism: {request.options.method.value} method"
        if request.options.distribute_key_column:
            parallel_desc += f" on column '{request.options.distribute_key_column}'"
        parallel_desc += f" with degree {request.options.degree}"
        parts.append(parallel_desc)
    else:
        parts.append("Parallelism: None (single-threaded transfer)")

    # Mapping
    parts.append(f"Column mapping: {request.options.map_method.value}")

    return "\n".join(f"{i+1}. {part}" for i, part in enumerate(parts))
