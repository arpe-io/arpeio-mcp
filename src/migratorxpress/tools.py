"""
MigratorXpress MCP tool definitions and handlers.

This module provides the tool list and async handler function for the
MigratorXpress tools, to be registered with a unified MCP server.
"""

import json
import shlex
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Callable, Awaitable

from mcp.types import Tool, ToolAnnotations, TextContent
from pydantic import ValidationError

from .validators import (
    MigrationParams,
    TaskType,
    SourceDatabaseType,
    TargetDatabaseType,
    MigrationDbMode,
    LoadMode,
    FkMode,
    LogLevel,
)
from .command_builder import (
    CommandBuilder,
    MigratorXpressError,
    get_supported_capabilities,
    suggest_workflow,
)
from .version import check_version_compatibility
from src.base.error_patterns import diagnose_cli_error


logger = logging.getLogger(__name__)


def _suggest_next_steps(errors: list) -> list[str]:
    """Suggest next tools to call based on validation error fields."""
    tips = []
    error_fields = set()
    for error in errors:
        error_fields.update(str(x) for x in error["loc"])

    if "auth_file" in error_fields or "source_db_auth_id" in error_fields or "target_db_auth_id" in error_fields:
        tips.append("Tip: Use `migratorxpress_validate_auth_file` to verify your credentials file is valid.")
    if "task_list" in error_fields:
        tips.append("Tip: Use `migratorxpress_suggest_workflow` to get the recommended task sequence for your migration.")
    if any("source" in f or "target" in f for f in error_fields):
        tips.append("Tip: Use `migratorxpress_list_capabilities` to see supported source and target databases.")

    return tips


def _build_command_explanation(params: MigrationParams) -> str:
    """Build a human-readable explanation of what the command will do."""
    parts = []

    parts.append(
        f"Migrate from source database '{params.source_db_name}' to target database '{params.target_db_name}'"
    )

    # Source/target schemas
    if params.source_schema_name:
        parts.append(f"Source schema: {params.source_schema_name}")
    if params.target_schema_name:
        parts.append(f"Target schema: {params.target_schema_name}")

    # Tasks
    if params.task_list:
        parts.append(f"Tasks: {', '.join(params.task_list)}")
    else:
        parts.append("No specific tasks selected (defaults will apply)")

    # FastTransfer
    if params.fasttransfer_dir_path:
        ft_info = f"FastTransfer enabled (path: {params.fasttransfer_dir_path})"
        if params.fasttransfer_p is not None:
            ft_info += f", parallelism: {params.fasttransfer_p}"
        parts.append(ft_info)

    # Table filters
    filters = []
    if params.include_tables:
        filters.append(f"include: {params.include_tables}")
    if params.exclude_tables:
        filters.append(f"exclude: {params.exclude_tables}")
    if params.min_rows is not None:
        filters.append(f"min rows: {params.min_rows}")
    if params.max_rows is not None:
        filters.append(f"max rows: {params.max_rows}")
    if filters:
        parts.append(f"Table filters: {', '.join(filters)}")

    # Resume
    if params.resume:
        parts.append(f"Resuming previous run: {params.resume}")

    # Force
    if params.force:
        parts.append("WARNING: Force flag is set -- existing data may be overwritten")

    # License
    if params.license:
        parts.append("License key provided (masked in display)")

    return "\n".join(f"{i+1}. {part}" for i, part in enumerate(parts))


def create_tools(
    command_builder: CommandBuilder,
    config: Dict[str, Any],
) -> Tuple[List[Tool], Callable[[str, Dict[str, Any]], Awaitable[Any]]]:
    """Create MigratorXpress MCP tools and return (tools_list, handle_call).

    Args:
        command_builder: Initialized MigratorXpress CommandBuilder instance
        config: Dict with keys: timeout, log_dir, path

    Returns:
        Tuple of (tools_list, async handle_call function)
    """
    timeout = config.get("timeout", 3600)
    log_dir = Path(config.get("log_dir", "./logs"))
    binary_path = config.get("path", "./MigratorXpress")

    tools = [
        Tool(
            name="migratorxpress_preview_command",
            description=(
                "Build and preview a MigratorXpress migration command WITHOUT executing it. "
                "Call this after migratorxpress_validate_auth_file and migratorxpress_suggest_workflow. "
                "Shows the exact CLI command with passwords masked. "
                "Does NOT execute the migration or validate database connectivity. "
                "After reviewing, pass the command to migratorxpress_execute_command."
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
                    "auth_file": {
                        "type": "string",
                        "description": "Path to authentication/credentials JSON file",
                    },
                    "source_db_auth_id": {
                        "type": "string",
                        "description": "Source database credential ID from the auth file",
                    },
                    "source_db_name": {
                        "type": "string",
                        "description": "Source database name to migrate from",
                    },
                    "target_db_auth_id": {
                        "type": "string",
                        "description": "Target database credential ID from the auth file",
                    },
                    "target_db_name": {
                        "type": "string",
                        "description": "Target database name to migrate to",
                    },
                    "migration_db_auth_id": {
                        "type": "string",
                        "description": "Migration tracking database credential ID from the auth file",
                    },
                    "source_schema_name": {
                        "type": "string",
                        "description": "Source schema name. If omitted, all schemas are migrated",
                    },
                    "target_schema_name": {
                        "type": "string",
                        "description": "Target schema name. Defaults to source schema name if omitted",
                    },
                    "task_list": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [t.value for t in TaskType],
                            "description": "translate: convert DDL from source to target syntax. create: create tables in target. transfer: copy data with FastTransfer. diff: compare row counts. copy_pk/copy_ak/copy_fk: copy constraints. all: run full migration.",
                        },
                        "description": "Tasks to run (e.g., ['translate', 'create', 'transfer'] for full migration, or ['diff'] for validation). Call migratorxpress_suggest_workflow to get the recommended task sequence.",
                    },
                    "resume": {
                        "type": "string",
                        "description": "Resume a previous run by RUN_ID",
                    },
                    "fasttransfer_dir_path": {
                        "type": "string",
                        "description": "Path to FastTransfer binary directory for parallel data transfer",
                    },
                    "fasttransfer_p": {
                        "type": "integer",
                        "description": "FastTransfer parallel degree (number of threads per table transfer)",
                    },
                    "ft_large_table_th": {
                        "type": "integer",
                        "description": "Row count threshold above which FastTransfer parallelism is used",
                    },
                    "n_jobs": {
                        "type": "integer",
                        "description": "Number of concurrent table transfers",
                    },
                    "cci_threshold": {
                        "type": "integer",
                        "description": "Row count threshold for clustered columnstore index creation on target",
                    },
                    "aci_threshold": {
                        "type": "integer",
                        "description": "Row count threshold for auto-created indexes on target",
                    },
                    "migration_db_mode": {
                        "type": "string",
                        "enum": [m.value for m in MigrationDbMode],
                        "description": "How to handle existing target database objects. preserve: keep existing objects. truncate: empty tables before loading. drop: drop and recreate objects.",
                    },
                    "compute_nbrows": {
                        "type": "string",
                        "enum": ["true", "false"],
                        "description": "Compute row counts for source tables before transfer",
                    },
                    "drop_tables_if_exists": {
                        "type": "string",
                        "enum": ["true", "false"],
                        "description": "Drop target tables before creating them",
                    },
                    "load_mode": {
                        "type": "string",
                        "enum": [m.value for m in LoadMode],
                        "description": "How to load data into target tables. truncate: clear target before loading. append: add to existing data.",
                    },
                    "include_tables": {
                        "type": "string",
                        "description": "Table include patterns, comma-separated. Supports wildcards",
                    },
                    "exclude_tables": {
                        "type": "string",
                        "description": "Table exclude patterns, comma-separated. Supports wildcards",
                    },
                    "min_rows": {
                        "type": "integer",
                        "description": "Minimum row count filter -- only migrate tables with at least this many rows",
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Maximum row count filter -- only migrate tables with at most this many rows",
                    },
                    "forced_int_id_prefixes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column name prefixes to force integer identity mapping",
                    },
                    "forced_int_id_suffixes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column name suffixes to force integer identity mapping",
                    },
                    "profiling_sample_pc": {
                        "type": "number",
                        "description": "Percentage of rows to sample for data profiling (0-100)",
                    },
                    "p_query": {
                        "type": "number",
                        "description": "Parallelism degree for profiling queries",
                    },
                    "min_sample_pc_profile": {
                        "type": "number",
                        "description": "Minimum sample percentage for profiling small tables",
                    },
                    "force": {
                        "type": "boolean",
                        "default": False,
                        "description": "Force overwrite of existing migration data",
                    },
                    "basic_diff": {
                        "type": "boolean",
                        "default": False,
                        "description": "Use basic diff mode (row counts only, no checksum)",
                    },
                    "without_xid": {
                        "type": "boolean",
                        "default": False,
                        "description": "Disable transaction ID tracking during transfer",
                    },
                    "fk_mode": {
                        "type": "string",
                        "enum": [m.value for m in FkMode],
                        "description": "How to handle foreign key constraints. trusted: create as trusted. untrusted: create and verify. disabled: skip FK creation.",
                    },
                    "log_level": {
                        "type": "string",
                        "enum": [level.value for level in LogLevel],
                        "description": "Logging verbosity level",
                    },
                    "log_dir": {
                        "type": "string",
                        "description": "Directory for log files",
                    },
                    "no_banner": {
                        "type": "boolean",
                        "default": False,
                        "description": "Suppress the startup banner",
                    },
                    "no_progress": {
                        "type": "boolean",
                        "default": False,
                        "description": "Disable progress bar display",
                    },
                    "quiet_ft": {
                        "type": "boolean",
                        "default": False,
                        "description": "Suppress FastTransfer console output during data transfer",
                    },
                    "license": {
                        "type": "string",
                        "description": "License key (will be masked in display)",
                    },
                    "license_file": {
                        "type": "string",
                        "description": "Path to license key file",
                    },
                    "os_type": {
                        "type": "string",
                        "enum": ["linux", "windows"],
                        "description": "Target operating system for command formatting",
                        "default": "linux",
                    },
                },
                "required": [
                    "auth_file",
                    "source_db_auth_id",
                    "source_db_name",
                    "target_db_auth_id",
                    "target_db_name",
                    "migration_db_auth_id",
                ],
            },
        ),
        Tool(
            name="migratorxpress_execute_command",
            description=(
                "Execute a MigratorXpress command that was previously built by migratorxpress_preview_command. "
                "Requires confirmation=true as a safety gate. "
                "The command string must come from a prior migratorxpress_preview_command call. "
                "Will fail if the MigratorXpress binary is not installed."
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
                        "description": "The full MigratorXpress command string copied from the migratorxpress_preview_command output (space-separated arguments).",
                    },
                    "confirmation": {
                        "type": "boolean",
                        "description": "Safety gate: must be set to true to allow execution. Confirms the user has reviewed the previewed command.",
                    },
                },
                "required": ["command", "confirmation"],
            },
        ),
        Tool(
            name="migratorxpress_validate_auth_file",
            description=(
                "Validate that a MigratorXpress JSON credentials file is well-formed and contains "
                "the required fields (auth_id, db_type, connection parameters). "
                "Call this before migratorxpress_preview_command to catch credential issues early. "
                "Does NOT test actual database connectivity."
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
                    "file_path": {
                        "type": "string",
                        "description": "Absolute or relative path to the MigratorXpress JSON credentials file to validate.",
                    },
                    "required_auth_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of auth_id values that must exist in the credentials file. Use this to verify source, target, and migration DB credentials are all present before building a command.",
                    },
                },
                "required": ["file_path"],
            },
        ),
        Tool(
            name="migratorxpress_list_capabilities",
            description=(
                "List all supported source and target database platforms, migration tasks, "
                "and modes for MigratorXpress. Call this when the user asks what databases or "
                "migration paths are supported. Returns structured capability information. "
                "Does not require any parameters."
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
            name="migratorxpress_suggest_workflow",
            description=(
                "Get the recommended task sequence for a cross-platform database migration "
                "(e.g., Oracle -> PostgreSQL). Call this FIRST to understand which tasks to run "
                "and in what order (translate -> create -> transfer -> copy_pk -> copy_ak -> copy_fk). "
                "Does not execute anything."
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
                        "enum": [e.value for e in SourceDatabaseType],
                        "description": "Source database platform to migrate from",
                    },
                    "target_type": {
                        "type": "string",
                        "enum": [e.value for e in TargetDatabaseType],
                        "description": "Target database platform to migrate to",
                    },
                    "include_constraints": {
                        "type": "boolean",
                        "default": True,
                        "description": "Whether to include constraint copy steps (copy_pk, copy_ak, copy_fk) in the suggested workflow. Set to false for data-only migrations.",
                    },
                },
                "required": ["source_type", "target_type"],
            },
        ),
        Tool(
            name="migratorxpress_get_version",
            description=(
                "Report the installed MigratorXpress binary version and its supported capabilities. "
                "Call this to check feature availability or diagnose version-related issues. "
                "Does not require database connectivity."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
    ]

    # --- Handler functions ---

    async def handle_preview_command(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle migratorxpress_preview_command tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Error: MigratorXpress could not be initialized.\n"
                        f"Expected binary location: {binary_path}\n"
                        "Please set MIGRATORXPRESS_PATH environment variable correctly.\n"
                        "Install the binary from https://arpe.io"
                    ),
                )
            ]

        try:
            # Extract os_type before passing to MigrationParams (not part of the model)
            os_type = arguments.pop("os_type", "linux")

            # Validate and parse parameters
            params = MigrationParams(**arguments)

            # Check version compatibility
            version_warnings = check_version_compatibility(
                arguments,
                command_builder.version_detector.capabilities,
                command_builder.version_detector._detected_version,
            )

            # Build command
            command = command_builder.build_command(params)

            # Format for display (with license masking)
            display_command = command_builder.format_command_display(command, mask=True, os_type=os_type)

            # Create explanation
            explanation = _build_command_explanation(params)

            # Build response
            response = [
                "# MigratorXpress Command Preview",
                "",
            ]

            if command_builder.preview_only:
                response += [
                    "**NOTE: Execution is not available (binary not configured). "
                    "Command preview is available. "
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

            response += [
                "",
                "## Command:",
                "```bash",
                display_command,
                "```",
                "",
                "## To execute this command:",
                "1. Review the command carefully",
                "2. Use the `migratorxpress_execute_command` tool with the FULL command",
                "3. Set `confirmation: true` to proceed",
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

        except MigratorXpressError as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def handle_execute_command(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle migratorxpress_execute_command tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Error: MigratorXpress could not be initialized. "
                        "Please check MIGRATORXPRESS_PATH.\n"
                        "Install the binary from https://arpe.io"
                    ),
                )
            ]

        if command_builder.preview_only:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Execution requires the MigratorXpress binary. "
                        "Download from https://arpe.io and set MIGRATORXPRESS_PATH to enable."
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
                        "You must set `confirmation: true` to execute a command.\n"
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
                    text="Error: No command provided. Please provide the command from preview_command.",
                )
            ]

        # Parse command string into list
        try:
            command = shlex.split(command_str)
        except ValueError as e:
            return [TextContent(type="text", text=f"Error parsing command: {str(e)}")]

        # Execute
        try:
            logger.info("Starting MigratorXpress execution...")
            return_code, stdout, stderr = command_builder.execute_command(
                command, timeout=timeout, log_dir=log_dir
            )

            # Format response
            success = return_code == 0

            response = [
                f"# MigratorXpress {'Completed' if success else 'Failed'}",
                "",
                f"**Status**: {'Success' if success else 'Failed'}",
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
                            "- Check the authentication file and database credentials",
                            "- Verify source and target database connectivity",
                            "- Review the full log file for more information",
                        ]
                    )

            return [TextContent(type="text", text="\n".join(response))]

        except MigratorXpressError as e:
            return [TextContent(type="text", text=f"# Execution Failed\n\nError: {str(e)}")]

    async def handle_validate_auth_file(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle migratorxpress_validate_auth_file tool."""
        file_path = arguments.get("file_path", "")
        required_auth_ids = arguments.get("required_auth_ids", [])

        issues = []
        auth_data = None

        # Check file exists
        path = Path(file_path)
        if not path.exists():
            issues.append(f"- File not found: {file_path}")
        elif not path.is_file():
            issues.append(f"- Path is not a file: {file_path}")
        else:
            # Try to parse as JSON
            try:
                with open(path) as f:
                    auth_data = json.load(f)
            except json.JSONDecodeError as e:
                issues.append(f"- Invalid JSON: {e}")
            except PermissionError:
                issues.append(f"- Permission denied reading: {file_path}")

        # Check required auth IDs
        if auth_data is not None and required_auth_ids:
            if isinstance(auth_data, dict):
                for auth_id in required_auth_ids:
                    if auth_id not in auth_data:
                        issues.append(f"- Missing auth_id: '{auth_id}'")
            elif isinstance(auth_data, list):
                found_ids = set()
                for entry in auth_data:
                    if isinstance(entry, dict) and "id" in entry:
                        found_ids.add(entry["id"])
                for auth_id in required_auth_ids:
                    if auth_id not in found_ids:
                        issues.append(f"- Missing auth_id: '{auth_id}'")

        if issues:
            response = [
                "# Auth File Validation - Issues Found",
                "",
                f"**File**: {file_path}",
                "",
                *issues,
            ]
        else:
            entry_count = 0
            if isinstance(auth_data, dict):
                entry_count = len(auth_data)
            elif isinstance(auth_data, list):
                entry_count = len(auth_data)

            response = [
                "# Auth File Validation - OK",
                "",
                f"**File**: {file_path}",
                "**Valid JSON**: Yes",
                f"**Entries**: {entry_count}",
            ]
            if required_auth_ids:
                response.append(
                    f"**Required auth_ids present**: {', '.join(required_auth_ids)}"
                )

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_list_capabilities(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle migratorxpress_list_capabilities tool."""
        caps = get_supported_capabilities()

        response = [
            "# MigratorXpress Capabilities",
            "",
        ]

        # Source databases
        response.append("## Source Databases")
        response.append("")
        for db in caps["Source Databases"]:
            response.append(f"- {db}")
        response.append("")

        # Target databases
        response.append("## Target Databases")
        response.append("")
        for db in caps["Target Databases"]:
            response.append(f"- {db}")
        response.append("")

        # Migration database
        response.append("## Migration Database")
        response.append("")
        for db in caps["Migration Database"]:
            response.append(f"- {db}")
        response.append("")

        # Tasks
        response.append("## Available Tasks")
        response.append("")
        for task_name, task_desc in caps["Tasks"].items():
            response.append(f"- **{task_name}**: {task_desc}")
        response.append("")

        # Migration DB modes
        response.append("## Migration DB Modes")
        response.append("")
        for mode_name, mode_desc in caps["Migration DB Modes"].items():
            response.append(f"- **{mode_name}**: {mode_desc}")
        response.append("")

        # Load modes
        response.append("## Load Modes")
        response.append("")
        for mode_name, mode_desc in caps["Load Modes"].items():
            response.append(f"- **{mode_name}**: {mode_desc}")
        response.append("")

        # FK modes
        response.append("## FK Modes")
        response.append("")
        for mode_name, mode_desc in caps["FK Modes"].items():
            response.append(f"- **{mode_name}**: {mode_desc}")
        response.append("")

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_suggest_workflow(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle migratorxpress_suggest_workflow tool."""
        source_type = arguments.get("source_type", "")
        target_type = arguments.get("target_type", "")
        include_constraints = arguments.get("include_constraints", True)

        workflow = suggest_workflow(source_type, target_type, include_constraints)

        response = [
            "# MigratorXpress Workflow Suggestion",
            "",
            f"**Source**: {workflow['source_type']}",
            f"**Target**: {workflow['target_type']}",
            f"**Include Constraints**: {'Yes' if workflow['include_constraints'] else 'No'}",
            "",
            "## Steps:",
            "",
        ]

        for step in workflow["steps"]:
            response.append(f"### Step {step['step']}: {step['task']}")
            response.append(f"{step['description']}")
            response.append("")
            response.append("```bash")
            response.append(step["example"])
            response.append("```")
            response.append("")

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_get_version(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle migratorxpress_get_version tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Error: MigratorXpress could not be initialized.\n"
                        f"Expected location: {binary_path}\n"
                        "Please set MIGRATORXPRESS_PATH environment variable correctly.\n"
                        "Install the binary from https://arpe.io"
                    ),
                )
            ]

        version_info = command_builder.get_version()
        caps = version_info["capabilities"]

        response = [
            "# MigratorXpress Version Information",
            "",
        ]

        if version_info.get("preview_only"):
            response += [
                "**Mode**: Command builder (execution not available)",
                f"**Binary Path**: {version_info['binary_path']}",
                f"**Message**: {version_info['message']}",
                "",
                "Capabilities below are based on the latest known version.",
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
            "## Supported Source Databases:",
            ", ".join(f"`{d}`" for d in caps["source_databases"]),
            "",
            "## Supported Target Databases:",
            ", ".join(f"`{d}`" for d in caps["target_databases"]),
            "",
            "## Migration Database Types:",
            ", ".join(f"`{d}`" for d in caps["migration_db_types"]),
            "",
            "## Available Tasks:",
            ", ".join(f"`{t}`" for t in caps["tasks"]),
            "",
            "## FK Modes:",
            ", ".join(f"`{m}`" for m in caps["fk_modes"]),
            "",
            "## Migration DB Modes:",
            ", ".join(f"`{m}`" for m in caps["migration_db_modes"]),
            "",
            "## Load Modes:",
            ", ".join(f"`{m}`" for m in caps["load_modes"]),
            "",
            "## Feature Flags:",
            f"- No Banner: {'Yes' if caps['supports_no_banner'] else 'No'}",
            f"- Version Flag: {'Yes' if caps['supports_version_flag'] else 'No'}",
            f"- FastTransfer: {'Yes' if caps['supports_fasttransfer'] else 'No'}",
            f"- License: {'Yes' if caps['supports_license'] else 'No'}",
            f"- No Progress: {'Yes' if caps.get('supports_no_progress') else 'No'}",
            f"- Quiet FT: {'Yes' if caps.get('supports_quiet_ft') else 'No'}",
            f"- Log Dir: {'Yes' if caps.get('supports_log_dir') else 'No'}",
        ]

        return [TextContent(type="text", text="\n".join(response))]

    # --- Dispatch ---

    async def handle_call(name: str, arguments: Dict[str, Any]):
        if name == "migratorxpress_preview_command":
            return await handle_preview_command(arguments)
        elif name == "migratorxpress_execute_command":
            return await handle_execute_command(arguments)
        elif name == "migratorxpress_validate_auth_file":
            return await handle_validate_auth_file(arguments)
        elif name == "migratorxpress_list_capabilities":
            return await handle_list_capabilities(arguments)
        elif name == "migratorxpress_suggest_workflow":
            return await handle_suggest_workflow(arguments)
        elif name == "migratorxpress_get_version":
            return await handle_get_version(arguments)
        return None

    return tools, handle_call
