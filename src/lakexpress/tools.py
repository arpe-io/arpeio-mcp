"""
LakeXpress MCP tool definitions and handlers.

This module defines the MCP tools for LakeXpress and their handler functions,
extracted from the original server.py for use in the unified arpeio-mcp server.
"""

import json
import logging
import shlex
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from mcp.types import Tool, TextContent
from pydantic import ValidationError

from .validators import (
    LakeXpressRequest,
    CommandType,
    CompressionType,
    ErrorAction,
    LogLevel,
    PublishMethod,
    CleanupStatus,
)
from .command_builder import (
    CommandBuilder,
    LakeXpressError,
    get_supported_capabilities,
    suggest_workflow,
)
from .version import check_version_compatibility


logger = logging.getLogger(__name__)


def create_tools(command_builder: CommandBuilder, config: dict) -> Tuple[List[Tool], Callable]:
    """Create LakeXpress MCP tools and their handler function.

    Args:
        command_builder: Initialized LakeXpress CommandBuilder instance
        config: Configuration dict with keys: timeout, log_dir, path, fastbcp_dir_path

    Returns:
        Tuple of (tools_list, handle_call_async_function)
    """
    timeout = config.get("timeout", 3600)
    log_dir = Path(config.get("log_dir", "./logs"))
    binary_path = config.get("path", "./LakeXpress")
    fastbcp_dir_path = config.get("fastbcp_dir_path", "")

    tools = [
        Tool(
            name="lakexpress_preview_command",
            description=(
                "Build and preview a LakeXpress CLI command WITHOUT executing it. "
                "This shows the exact command that will be run. "
                "Use this FIRST before executing any command."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "enum": [e.value for e in CommandType],
                        "description": "The LakeXpress command to execute",
                    },
                    "logdb_init": {
                        "type": "object",
                        "description": "Initialize the log database schema. Creates required tables for sync management.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "logdb_drop": {
                        "type": "object",
                        "description": "Drop the log database schema. WARNING: permanently deletes all sync history and configuration.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "confirm": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "logdb_truncate": {
                        "type": "object",
                        "description": "Clear all data from the log database while keeping the schema.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "sync_id": {
                                "type": "string",
                                "description": "Truncate only data for this sync_id",
                            },
                            "confirm": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "logdb_locks": {
                        "type": "object",
                        "description": "Show currently locked tables in the log database.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "sync_id": {
                                "type": "string",
                                "description": "Show locks for this sync_id only",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "logdb_release_locks": {
                        "type": "object",
                        "description": "Release stale or stuck table locks in the log database.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "max_age_hours": {
                                "type": "integer",
                                "description": "Release locks older than this many hours",
                            },
                            "table_id": {
                                "type": "string",
                                "description": "Release lock for a specific table ID",
                            },
                            "confirm": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "lxdb_init": {
                        "type": "object",
                        "description": "Initialize the log database schema (0.3.0+ replacement for logdb_init).",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "lxdb_drop": {
                        "type": "object",
                        "description": "Drop the log database schema (0.3.0+ replacement for logdb_drop). WARNING: permanently deletes all sync history.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "confirm": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "lxdb_truncate": {
                        "type": "object",
                        "description": "Clear all data from the log database while keeping the schema (0.3.0+ replacement for logdb_truncate).",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "sync_id": {
                                "type": "string",
                                "description": "Truncate only data for this sync_id",
                            },
                            "confirm": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "lxdb_locks": {
                        "type": "object",
                        "description": "Show currently locked tables in the log database (0.3.0+ replacement for logdb_locks).",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "sync_id": {
                                "type": "string",
                                "description": "Show locks for this sync_id only",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "lxdb_release_locks": {
                        "type": "object",
                        "description": "Release stale or stuck table locks (0.3.0+ replacement for logdb_release_locks).",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "max_age_hours": {
                                "type": "integer",
                                "description": "Release locks older than this many hours",
                            },
                            "table_id": {
                                "type": "string",
                                "description": "Release lock for a specific table ID",
                            },
                            "confirm": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "config_create": {
                        "type": "object",
                        "description": (
                            "Create a new sync configuration. Requires source DB credentials "
                            "and either output_dir (local) or target_storage_id (cloud). "
                            "Add publish_target for data lake publishing."
                        ),
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "source_db_auth_id": {
                                "type": "string",
                                "description": "Source database credential ID",
                            },
                            "source_db_name": {
                                "type": "string",
                                "description": "Source database name. Recommended: always specify explicitly to avoid relying on the credential default",
                            },
                            "source_schema_name": {
                                "type": "string",
                                "description": "Source schema(s), comma-separated. Supports wildcards (e.g., 'tpch_%'). If omitted, all schemas are exported",
                            },
                            "include": {
                                "type": "string",
                                "description": "Table include patterns",
                            },
                            "exclude": {
                                "type": "string",
                                "description": "Table exclude patterns",
                            },
                            "min_rows": {"type": "integer"},
                            "max_rows": {"type": "integer"},
                            "incremental_table": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Incremental table configs. Format: 'schema.table:column:type[:i|:e][@start][!strategy]'",
                            },
                            "incremental_safety_lag": {
                                "type": "integer",
                                "description": "Safety lag in seconds for incremental exports",
                            },
                            "output_dir": {
                                "type": "string",
                                "description": "Local output directory. Mutually exclusive with target_storage_id",
                            },
                            "target_storage_id": {
                                "type": "string",
                                "description": "Cloud storage credential ID. Mutually exclusive with output_dir",
                            },
                            "sub_path": {
                                "type": "string",
                                "description": "Sub-path within the storage location",
                            },
                            "fastbcp_dir_path": {
                                "type": "string",
                                "description": "Path to FastBCP binary directory (auto-filled from FASTBCP_DIR_PATH env var if not set)",
                            },
                            "fastbcp_p": {
                                "type": "integer",
                                "description": "FastBCP parallel degree (number of threads per table export)",
                            },
                            "n_jobs": {
                                "type": "integer",
                                "description": "Number of concurrent table exports",
                            },
                            "compression_type": {
                                "type": "string",
                                "enum": [e.value for e in CompressionType],
                                "description": "Parquet compression type",
                            },
                            "large_table_threshold": {
                                "type": "integer",
                                "description": "Row count threshold for large table handling",
                            },
                            "fastbcp_table_config": {
                                "type": "string",
                                "description": "Per-table FastBCP config. Format: 'table1:method:key:degree;table2:method:key:degree'",
                            },
                            "publish_target": {
                                "type": "string",
                                "description": "Publish target credential ID. Required when publish_method is set",
                            },
                            "publish_method": {
                                "type": "string",
                                "enum": [e.value for e in PublishMethod],
                                "description": "Publish method: 'external' (external tables) or 'internal' (loaded tables). Requires publish_target",
                            },
                            "publish_database_name": {
                                "type": "string",
                                "description": "Database name for publishing",
                            },
                            "publish_schema_pattern": {
                                "type": "string",
                                "description": "Schema naming pattern for publishing. Supports placeholders: {schema}, {subpath}, {date}",
                            },
                            "publish_table_pattern": {
                                "type": "string",
                                "description": "Table naming pattern for publishing. Supports placeholders: {schema}, {table}",
                            },
                            "no_views": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable view creation when publishing external tables",
                            },
                            "pk_constraints": {
                                "type": "boolean",
                                "default": False,
                                "description": "Enable primary key constraints when publishing",
                            },
                            "generate_metadata": {
                                "type": "boolean",
                                "default": False,
                                "description": "Generate CDM metadata files",
                            },
                            "manifest_name": {
                                "type": "string",
                                "description": "Manifest file name for metadata",
                            },
                            "sync_id": {
                                "type": "string",
                                "description": "Custom sync ID (1-64 chars, alphanumeric/underscore/hyphen)",
                            },
                            "error_action": {
                                "type": "string",
                                "enum": [e.value for e in ErrorAction],
                                "description": "Action on table export error: fail (stop), continue (skip and continue), skip",
                            },
                            "env_name": {
                                "type": "string",
                                "description": "Environment name for configuration isolation",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                        },
                        "required": [
                            "auth_file",
                            "log_db_auth_id",
                            "source_db_auth_id",
                        ],
                    },
                    "config_delete": {
                        "type": "object",
                        "description": "Delete a sync configuration.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "sync_id": {
                                "type": "string",
                                "description": "The sync_id to delete",
                            },
                            "confirm": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id", "sync_id"],
                    },
                    "config_list": {
                        "type": "object",
                        "description": "List all sync configurations.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "env_name": {
                                "type": "string",
                                "description": "Environment name for configuration isolation",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "sync": {
                        "type": "object",
                        "description": "Run a full sync (export + publish). Exports data and publishes if configured.",
                        "properties": {
                            "sync_id": {
                                "type": "string",
                                "description": "The sync_id to execute. If omitted, uses the last created config",
                            },
                            "resume": {
                                "type": "boolean",
                                "default": False,
                                "description": "Resume an incomplete or failed run",
                            },
                            "run_id": {
                                "type": "string",
                                "description": "Specific run_id to resume",
                            },
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "fastbcp_dir_path": {
                                "type": "string",
                                "description": "Path to FastBCP binary directory (auto-filled from FASTBCP_DIR_PATH env var if not set)",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "quiet_fbcp": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress FastBCP console output (requires LakeXpress 0.2.9+)",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                            "env_name": {
                                "type": "string",
                                "description": "Environment name for configuration isolation (0.3.0+)",
                            },
                        },
                    },
                    "sync_export": {
                        "type": "object",
                        "description": "Run export-only sync (no publish). Exports data to local or cloud storage.",
                        "properties": {
                            "sync_id": {
                                "type": "string",
                                "description": "The sync_id to export. If omitted, uses the last created config",
                            },
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "fastbcp_dir_path": {
                                "type": "string",
                                "description": "Path to FastBCP binary directory (auto-filled from FASTBCP_DIR_PATH env var if not set)",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "quiet_fbcp": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress FastBCP console output (requires LakeXpress 0.2.9+)",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                            "env_name": {
                                "type": "string",
                                "description": "Environment name for configuration isolation (0.3.0+)",
                            },
                        },
                    },
                    "sync_publish": {
                        "type": "object",
                        "description": "Run publish-only sync. Publishes previously exported data to the configured target.",
                        "properties": {
                            "sync_id": {
                                "type": "string",
                                "description": "The sync_id to publish. If omitted, uses the last created config",
                            },
                            "run_id": {
                                "type": "string",
                                "description": "Specific run_id to publish",
                            },
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                            "env_name": {
                                "type": "string",
                                "description": "Environment name for configuration isolation (0.3.0+)",
                            },
                        },
                    },
                    "run": {
                        "type": "object",
                        "description": "Run a multi-step pipeline from a YAML config file.",
                        "properties": {
                            "config": {
                                "type": "string",
                                "description": "Path to YAML config file",
                            },
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                            "license": {
                                "type": "string",
                                "description": "Path to license file (0.3.0+)",
                            },
                            "env_name": {
                                "type": "string",
                                "description": "Environment name for configuration isolation (0.3.0+)",
                            },
                        },
                        "required": ["config"],
                    },
                    "status": {
                        "type": "object",
                        "description": "Show sync run status and history.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "sync_id": {
                                "type": "string",
                                "description": "Show runs for this sync configuration",
                            },
                            "run_id": {
                                "type": "string",
                                "description": "Show details for a specific run",
                            },
                            "verbose": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id"],
                    },
                    "cleanup": {
                        "type": "object",
                        "description": "Clean up old or failed sync runs.",
                        "properties": {
                            "auth_file": {
                                "type": "string",
                                "description": "Path to authentication/credentials JSON file",
                            },
                            "log_db_auth_id": {
                                "type": "string",
                                "description": "Log database credential ID",
                            },
                            "sync_id": {
                                "type": "string",
                                "description": "The sync_id to clean up",
                            },
                            "older_than": {
                                "type": "string",
                                "description": "Duration filter, e.g., 7d, 24h, 30m",
                            },
                            "status": {
                                "type": "string",
                                "enum": [e.value for e in CleanupStatus],
                                "description": "Only delete runs with this status",
                            },
                            "dry_run": {"type": "boolean", "default": False},
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Logging verbosity level",
                            },
                            "log_dir": {
                                "type": "string",
                                "description": "Directory for log files",
                            },
                            "no_progress": {
                                "type": "boolean",
                                "default": False,
                                "description": "Disable progress bar display",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "default": False,
                                "description": "Suppress the startup banner",
                            },
                        },
                        "required": ["auth_file", "log_db_auth_id", "sync_id"],
                    },
                    "os_type": {
                        "type": "string",
                        "enum": ["linux", "windows"],
                        "description": "Target operating system for command formatting",
                        "default": "linux",
                    },
                },
                "required": ["command"],
            },
        ),
        Tool(
            name="lakexpress_execute_command",
            description=(
                "Execute a LakeXpress command that was previously previewed. "
                "IMPORTANT: You must set confirmation=true to execute. "
                "This is a safety mechanism to prevent accidental execution."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The exact command from preview_command (space-separated)",
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
            name="lakexpress_list_capabilities",
            description=(
                "List supported source databases, log databases, storage backends, "
                "publishing targets, compression types, and available commands."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="lakexpress_suggest_workflow",
            description=(
                "Given a use case (source DB type, storage destination, optional publish target), "
                "suggest the full sequence of LakeXpress commands with example parameters."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_type": {
                        "type": "string",
                        "description": "Source database type (e.g., 'sqlserver', 'postgresql', 'oracle')",
                    },
                    "destination": {
                        "type": "string",
                        "description": "Storage destination (e.g., 'local', 's3', 'azure_adls', 'gcs')",
                    },
                    "publish_target": {
                        "type": "string",
                        "description": "Optional publishing target (e.g., 'snowflake', 'databricks', 'fabric')",
                    },
                },
                "required": ["source_type", "destination"],
            },
        ),
        Tool(
            name="lakexpress_get_version",
            description=(
                "Get the detected LakeXpress binary version, capabilities, "
                "and supported databases, storage backends, and publishing targets."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
    ]

    # --- Handler functions ---

    async def handle_preview_command(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle lakexpress_preview_command tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Error: LakeXpress server failed to initialize.\n"
                        f"Expected binary location: {binary_path}\n"
                        "Please set LAKEXPRESS_PATH environment variable correctly."
                    ),
                )
            ]

        try:
            # Extract os_type before passing to LakeXpressRequest (not part of the model)
            os_type = arguments.pop("os_type", "linux")

            # Auto-fill fastbcp_dir_path from env var if not explicitly provided
            if fastbcp_dir_path:
                for key in ("config_create", "sync", "sync_export"):
                    sub = arguments.get(key)
                    if sub is not None and not sub.get("fastbcp_dir_path"):
                        sub["fastbcp_dir_path"] = fastbcp_dir_path

            # Validate and parse request
            request = LakeXpressRequest(**arguments)

            # Check version compatibility
            cmd_type = arguments.get("command", "")
            cmd_params = arguments.get(cmd_type, {}) or {}
            version_warnings = check_version_compatibility(
                cmd_type,
                cmd_params,
                command_builder.version_detector.capabilities,
                command_builder.version_detector._detected_version,
            )

            # Build command
            command = command_builder.build_command(request)

            # Format for display
            display_command = command_builder.format_command_display(command, os_type=os_type)

            # Create explanation
            explanation = _build_command_explanation(request)

            # Build response
            response = [
                "# LakeXpress Command Preview",
                "",
            ]

            if command_builder.preview_only:
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
                "2. Use the `lakexpress_execute_command` tool with the FULL command",
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
            return [TextContent(type="text", text="\n".join(error_msg))]

        except LakeXpressError as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def handle_execute_command(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle lakexpress_execute_command tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text="Error: LakeXpress server failed to initialize. Please check LAKEXPRESS_PATH.",
                )
            ]

        if command_builder.preview_only:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Execution requires the LakeXpress binary. "
                        "Download from https://arpe.io and set LAKEXPRESS_PATH to enable."
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
                    text="Error: No command provided. Please provide the command from lakexpress_preview_command.",
                )
            ]

        # Parse command string into list
        try:
            command = shlex.split(command_str)
        except ValueError as e:
            return [TextContent(type="text", text=f"Error parsing command: {str(e)}")]

        # Execute
        try:
            logger.info("Starting LakeXpress execution...")
            return_code, stdout, stderr = command_builder.execute_command(
                command, timeout=timeout, log_dir=log_dir
            )

            # Format response
            success = return_code == 0

            response = [
                f"# LakeXpress {'Completed' if success else 'Failed'}",
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
                response.extend(
                    [
                        "",
                        "## Troubleshooting:",
                        "- Check the auth file path and credential IDs",
                        "- Verify log database connectivity",
                        "- Check sync_id exists in the configuration",
                        "- Review the full log file for more information",
                    ]
                )

            return [TextContent(type="text", text="\n".join(response))]

        except LakeXpressError as e:
            return [TextContent(type="text", text=f"# Execution Failed\n\nError: {str(e)}")]

    async def handle_list_capabilities(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle lakexpress_list_capabilities tool."""
        caps = get_supported_capabilities()

        response = [
            "# LakeXpress Capabilities",
            "",
        ]

        # Source databases
        response.append("## Source Databases")
        response.append("")
        for db in caps["Source Databases"]:
            response.append(f"- {db}")
        response.append("")

        # Log databases
        response.append("## Log Databases")
        response.append("")
        for db in caps["Log Databases"]:
            response.append(f"- {db}")
        response.append("")

        # Storage backends
        response.append("## Storage Backends")
        response.append("")
        for backend in caps["Storage Backends"]:
            response.append(f"- {backend}")
        response.append("")

        # Publishing targets
        response.append("## Publishing Targets")
        response.append("")
        for target in caps["Publishing Targets"]:
            response.append(f"- {target}")
        response.append("")

        # Compression types
        response.append("## Compression Types")
        response.append("")
        for comp in caps["Compression Types"]:
            response.append(f"- `{comp}`")
        response.append("")

        # Commands
        response.append("## Available Commands")
        response.append("")
        for cmd_name, cmd_desc in caps["Commands"].items():
            response.append(f"- **{cmd_name}**: {cmd_desc}")
        response.append("")

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_suggest_workflow(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle lakexpress_suggest_workflow tool."""
        source_type = arguments.get("source_type", "")
        destination = arguments.get("destination", "")
        publish_target = arguments.get("publish_target")

        workflow = suggest_workflow(source_type, destination, publish_target)

        response = [
            "# LakeXpress Workflow Suggestion",
            "",
            f"**Source**: {workflow['source_type']}",
            f"**Destination**: {workflow['destination']}",
        ]
        if workflow["publish_target"]:
            response.append(f"**Publish Target**: {workflow['publish_target']}")
        response.append("")

        response.append("## Steps:")
        response.append("")
        for step in workflow["steps"]:
            response.append(f"### Step {step['step']}: {step['command']}")
            response.append(f"{step['description']}")
            response.append("")
            response.append("```bash")
            response.append(step["example"])
            response.append("```")
            response.append("")

        return [TextContent(type="text", text="\n".join(response))]

    async def handle_get_version(arguments: Dict[str, Any]) -> list[TextContent]:
        """Handle lakexpress_get_version tool."""
        if command_builder is None:
            return [
                TextContent(
                    type="text",
                    text=(
                        "Error: LakeXpress server failed to initialize.\n"
                        f"Expected binary location: {binary_path}\n"
                        "Please set LAKEXPRESS_PATH environment variable correctly."
                    ),
                )
            ]

        version_info = command_builder.get_version()
        caps = version_info["capabilities"]

        response = [
            "# LakeXpress Version Information",
            "",
        ]

        if version_info.get("preview_only"):
            response += [
                "**Mode**: Command builder (execution not available)",
                f"**Binary Path**: {version_info['binary_path']}",
                f"**Message**: {version_info.get('message', '')}",
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
            "## Supported Log Databases:",
            ", ".join(f"`{d}`" for d in caps["log_databases"]),
            "",
            "## Supported Storage Backends:",
            ", ".join(f"`{b}`" for b in caps["storage_backends"]),
            "",
            "## Supported Publishing Targets:",
            ", ".join(f"`{t}`" for t in caps["publish_targets"]),
            "",
            "## Compression Types:",
            ", ".join(f"`{c}`" for c in caps["compression_types"]),
            "",
            "## Feature Flags:",
            f"- No Banner: {'Yes' if caps['supports_no_banner'] else 'No'}",
            f"- Version Flag: {'Yes' if caps['supports_version_flag'] else 'No'}",
            f"- Incremental: {'Yes' if caps['supports_incremental'] else 'No'}",
            f"- Cleanup: {'Yes' if caps['supports_cleanup'] else 'No'}",
            f"- Quiet FastBCP: {'Yes' if caps['supports_quiet_fbcp'] else 'No'}",
            f"- No Progress: {'Yes' if caps['supports_no_progress'] else 'No'}",
            f"- Resume: {'Yes' if caps['supports_resume'] else 'No'}",
            f"- License: {'Yes' if caps['supports_license'] else 'No'}",
            f"- Env Name: {'Yes' if caps['supports_env_name'] else 'No'}",
        ]

        return [TextContent(type="text", text="\n".join(response))]

    # --- Dispatch function ---

    async def handle_call(name: str, arguments: Any) -> Optional[list[TextContent]]:
        """Route tool calls to the appropriate handler."""
        if name == "lakexpress_preview_command":
            return await handle_preview_command(arguments)
        elif name == "lakexpress_execute_command":
            return await handle_execute_command(arguments)
        elif name == "lakexpress_list_capabilities":
            return await handle_list_capabilities(arguments)
        elif name == "lakexpress_suggest_workflow":
            return await handle_suggest_workflow(arguments)
        elif name == "lakexpress_get_version":
            return await handle_get_version(arguments)
        return None

    return tools, handle_call


def _build_command_explanation(request: LakeXpressRequest) -> str:
    """Build a human-readable explanation of what the command will do."""
    cmd = request.command
    parts = []

    if cmd in (CommandType.LOGDB_INIT, CommandType.LXDB_INIT):
        parts.append("Initialize the log database schema")
        parts.append("This creates the required tables for LakeXpress sync management")

    elif cmd in (CommandType.LOGDB_DROP, CommandType.LXDB_DROP):
        parts.append("Drop the log database schema")
        parts.append(
            "WARNING: This will permanently delete all sync history and configuration"
        )
        drop_params = request.logdb_drop or request.lxdb_drop
        if drop_params and drop_params.confirm:
            parts.append("Confirmation flag is set -- operation will proceed")

    elif cmd in (CommandType.LOGDB_TRUNCATE, CommandType.LXDB_TRUNCATE):
        parts.append("Clear all data from the log database while keeping the schema")
        truncate_params = request.logdb_truncate or request.lxdb_truncate
        if truncate_params and truncate_params.sync_id:
            parts.append(
                f"Only data for sync_id '{truncate_params.sync_id}' will be cleared"
            )

    elif cmd in (CommandType.LOGDB_LOCKS, CommandType.LXDB_LOCKS):
        parts.append("Show currently locked tables in the log database")
        locks_params = request.logdb_locks or request.lxdb_locks
        if locks_params and locks_params.sync_id:
            parts.append(f"Filtering by sync_id: {locks_params.sync_id}")

    elif cmd in (CommandType.LOGDB_RELEASE_LOCKS, CommandType.LXDB_RELEASE_LOCKS):
        parts.append("Release stale or stuck table locks")
        release_params = request.logdb_release_locks or request.lxdb_release_locks
        if release_params:
            if release_params.max_age_hours is not None:
                parts.append(
                    f"Only locks older than {release_params.max_age_hours} hours"
                )
            if release_params.table_id:
                parts.append(f"For table_id: {release_params.table_id}")

    elif cmd == CommandType.CONFIG_CREATE:
        p = request.config_create
        if p:
            parts.append("Create a new sync configuration")
            parts.append(f"Source database: {p.source_db_auth_id}")
            if p.source_schema_name:
                parts.append(f"Source schema(s): {p.source_schema_name}")
            if p.output_dir:
                parts.append(f"Output to local directory: {p.output_dir}")
            elif p.target_storage_id:
                parts.append(f"Output to cloud storage: {p.target_storage_id}")
            if p.publish_target:
                parts.append(f"Publish to: {p.publish_target}")
            if p.n_jobs and p.n_jobs > 1:
                parts.append(f"Concurrent table exports: {p.n_jobs}")
            if p.compression_type:
                parts.append(f"Compression: {p.compression_type.value}")
            if p.incremental_table:
                parts.append(
                    f"Incremental tables configured: {len(p.incremental_table)}"
                )

    elif cmd == CommandType.CONFIG_DELETE:
        if request.config_delete:
            parts.append(f"Delete sync configuration: {request.config_delete.sync_id}")

    elif cmd == CommandType.CONFIG_LIST:
        parts.append("List all sync configurations")
        if request.config_list and request.config_list.env_name:
            parts.append(f"Filtered by environment: {request.config_list.env_name}")

    elif cmd == CommandType.SYNC:
        parts.append("Execute full sync (export + publish)")
        if request.sync:
            if request.sync.sync_id:
                parts.append(f"Sync ID: {request.sync.sync_id}")
            if request.sync.resume:
                parts.append("Resuming an incomplete run")

    elif cmd == CommandType.SYNC_EXPORT:
        parts.append("Execute export phase only")
        if request.sync_export and request.sync_export.sync_id:
            parts.append(f"Sync ID: {request.sync_export.sync_id}")

    elif cmd == CommandType.SYNC_PUBLISH:
        parts.append("Execute publish phase only")
        if request.sync_publish and request.sync_publish.sync_id:
            parts.append(f"Sync ID: {request.sync_publish.sync_id}")
        if request.sync_publish and request.sync_publish.run_id:
            parts.append(f"Run ID: {request.sync_publish.run_id}")

    elif cmd == CommandType.RUN:
        if request.run:
            parts.append(f"Run export from YAML config: {request.run.config}")

    elif cmd == CommandType.STATUS:
        parts.append("Query sync/run status")
        if request.status:
            if request.status.sync_id:
                parts.append(f"Sync ID: {request.status.sync_id}")
            if request.status.run_id:
                parts.append(f"Run ID: {request.status.run_id}")
            if request.status.verbose:
                parts.append("Verbose output enabled")

    elif cmd == CommandType.CLEANUP:
        if request.cleanup:
            parts.append(
                f"Clean up orphaned runs for sync_id: {request.cleanup.sync_id}"
            )
            if request.cleanup.older_than:
                parts.append(f"Only runs older than: {request.cleanup.older_than}")
            if request.cleanup.status:
                parts.append(f"Only runs with status: {request.cleanup.status.value}")
            if request.cleanup.dry_run:
                parts.append("DRY RUN -- no actual deletions")

    return "\n".join(f"{i+1}. {part}" for i, part in enumerate(parts))
