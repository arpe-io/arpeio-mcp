#!/usr/bin/env python3
"""
Arpe.io Unified MCP Server

A Model Context Protocol (MCP) server that exposes all Arpe.io data tools:
- FastBCP: High-performance parallel database export to files and cloud
- FastTransfer: High-performance parallel data transfer between databases
- LakeXpress: Automated database-to-cloud data pipeline as Parquet
- MigratorXpress: Cross-platform database migration with parallel transfer
"""

import os
import sys
import logging
import asyncio
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    from mcp.server import Server
    from mcp.types import (
        Tool, ToolAnnotations, TextContent,
        Prompt, PromptArgument, PromptMessage, GetPromptResult,
        Resource, TextResourceContents,
    )
except ImportError as e:
    print(f"Error: Required package not found: {e}", file=sys.stderr)
    print("Please run: pip install arpeio-mcp", file=sys.stderr)
    sys.exit(1)

from src.instructions import INSTRUCTIONS
from src.fastbcp.command_builder import CommandBuilder as FastBCPCommandBuilder
from src.fastbcp.tools import create_tools as create_fastbcp_tools
from src.fasttransfer.command_builder import CommandBuilder as FastTransferCommandBuilder
from src.fasttransfer.tools import create_tools as create_fasttransfer_tools
from src.lakexpress.command_builder import CommandBuilder as LakeXpressCommandBuilder
from src.lakexpress.tools import create_tools as create_lakexpress_tools
from src.migratorxpress.command_builder import CommandBuilder as MigratorXpressCommandBuilder
from src.migratorxpress.tools import create_tools as create_migratorxpress_tools
from src.doc_search import SearchEngine, create_tools as create_doc_search_tools
from src.base.release_notes_handler import build_release_notes_tool

# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)

# Configuration per tool
TOOL_CONFIGS = {
    "fastbcp": {
        "path": os.getenv("FASTBCP_PATH", "./fastbcp/FastBCP"),
        "timeout": int(os.getenv("FASTBCP_TIMEOUT", "1800")),
        "log_dir": Path(os.getenv("FASTBCP_LOG_DIR", "./logs")),
    },
    "fasttransfer": {
        "path": os.getenv("FASTTRANSFER_PATH", "./fasttransfer/FastTransfer"),
        "timeout": int(os.getenv("FASTTRANSFER_TIMEOUT", "1800")),
        "log_dir": Path(os.getenv("FASTTRANSFER_LOG_DIR", "./logs")),
    },
    "lakexpress": {
        "path": os.getenv("LAKEXPRESS_PATH", "./LakeXpress"),
        "timeout": int(os.getenv("LAKEXPRESS_TIMEOUT", "3600")),
        "log_dir": Path(os.getenv("LAKEXPRESS_LOG_DIR", "./logs")),
        "fastbcp_dir_path": os.getenv("FASTBCP_DIR_PATH", ""),
    },
    "migratorxpress": {
        "path": os.getenv("MIGRATORXPRESS_PATH", "./MigratorXpress"),
        "timeout": int(os.getenv("MIGRATORXPRESS_TIMEOUT", "3600")),
        "log_dir": Path(os.getenv("MIGRATORXPRESS_LOG_DIR", "./logs")),
        "fasttransfer_dir_path": os.getenv("FASTTRANSFER_DIR_PATH", ""),
    },
}

# Initialize MCP server
app = Server("arpeio-mcp", instructions=INSTRUCTIONS)

# Collect all tools and handlers
all_tools: list[Tool] = []
tool_handlers: list = []


def _init_tool(name, builder_cls, create_tools_fn, config):
    """Initialize a single tool's command builder and register its tools."""
    try:
        builder = builder_cls(config["path"])
        if builder.preview_only:
            logger.info(f"{name}: command-builder mode (binary not configured, execution not available)")
        else:
            version_info = builder.get_version()
            if version_info["detected"]:
                logger.info(f"{name}: version {version_info['version']}")
            else:
                logger.warning(f"{name}: version could not be detected")

        tools, handler = create_tools_fn(builder, config)
        all_tools.extend(tools)
        tool_handlers.append(handler)
        logger.info(f"{name}: {len(tools)} tools registered")

    except Exception as e:
        logger.error(f"{name}: failed to initialize — {e}")


# Initialize all tools
_init_tool("FastBCP", FastBCPCommandBuilder, create_fastbcp_tools, TOOL_CONFIGS["fastbcp"])
_init_tool("FastTransfer", FastTransferCommandBuilder, create_fasttransfer_tools, TOOL_CONFIGS["fasttransfer"])
_init_tool("LakeXpress", LakeXpressCommandBuilder, create_lakexpress_tools, TOOL_CONFIGS["lakexpress"])
_init_tool("MigratorXpress", MigratorXpressCommandBuilder, create_migratorxpress_tools, TOOL_CONFIGS["migratorxpress"])

# Add the meta tools
all_tools.append(
    Tool(
        name="arpe_get_status",
        description=(
            "Show the status of all four Arpe.io tools "
            "(FastBCP, FastTransfer, LakeXpress, MigratorXpress). "
            "All tools work in command-builder mode by default (no binary needed). "
            "If a binary is installed, execution is also available. "
            "Does not require database connectivity or any parameters."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
        inputSchema={"type": "object", "properties": {}},
    )
)

all_tools.append(
    Tool(
        name="arpe_quick_start",
        description=(
            "Determine which Arpe.io tool to use and get a step-by-step workflow guide. "
            "Call this when the user's intent is unclear or they are new to arpe.io tools. "
            "Accepts a plain English use case description and returns the recommended tool, "
            "required parameters, and the sequence of tool calls to make. "
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
                "use_case": {
                    "type": "string",
                    "description": "Plain English description of what the user wants to do (e.g., 'export a large Oracle table to S3 as Parquet', 'migrate SQL Server schema to PostgreSQL'). Used to auto-detect the right tool.",
                },
                "product": {
                    "type": "string",
                    "enum": ["fastbcp", "fasttransfer", "lakexpress", "migratorxpress", "all"],
                    "description": "Override auto-detection and show the workflow for a specific product. If omitted, the tool selects based on use_case.",
                },
            },
            "required": ["use_case"],
        },
    )
)

# Initialize documentation search
search_engine = SearchEngine()
_doc_tools, _doc_handler = create_doc_search_tools(search_engine)
all_tools.extend(_doc_tools)
tool_handlers.append(_doc_handler)
logger.info(f"DocSearch: {len(_doc_tools)} tools registered")

# Unified release-notes tool (replaces 4 per-product tools)
_rn_product_handlers: dict[str, Any] = {}
for _product in ("fastbcp", "fasttransfer", "lakexpress", "migratorxpress"):
    _, _rn_handler = build_release_notes_tool(_product, search_engine)
    _rn_product_handlers[_product] = _rn_handler

all_tools.append(
    Tool(
        name="arpe_release_notes",
        description=(
            "Return release-notes chunks for any Arpe.io product. "
            "Pass `product` to select which tool and optionally `version` to filter "
            "to a specific release. Read-only; queries the local docs cache."
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
                "product": {
                    "type": "string",
                    "enum": ["fastbcp", "fasttransfer", "lakexpress", "migratorxpress"],
                    "description": "Which product's release notes to retrieve.",
                },
                "version": {
                    "type": "string",
                    "description": "Optional version string (e.g. '0.31', '0.4'). Omit for the newest indexed version.",
                },
            },
            "required": ["product"],
        },
    )
)
logger.info("ReleaseNotes: 1 unified tool registered")


async def _release_notes_dispatch(name: str, arguments: dict):
    """Route arpe_release_notes to the matching per-product handler."""
    if name != "arpe_release_notes":
        return None
    product = (arguments or {}).get("product", "")
    handler = _rn_product_handlers.get(product)
    if handler is None:
        return [TextContent(
            type="text",
            text=f"Unknown product '{product}'. Valid: fastbcp, fasttransfer, lakexpress, migratorxpress.",
        )]
    return await handler(arguments or {})


tool_handlers.append(_release_notes_dispatch)


# ------------------------------------------------------------------ #
# MCP Prompts — guided workflow starters
# ------------------------------------------------------------------ #

_PROMPTS = [
    Prompt(
        name="export-table",
        description="Export a database table to a file (CSV, Parquet, JSON, etc.) using FastBCP",
        arguments=[
            PromptArgument(name="source_type", description="Source database type (e.g. pgsql, mssql, oraodp, mysql)", required=True),
            PromptArgument(name="output_format", description="Output format (e.g. parquet, csv, json)", required=True),
        ],
    ),
    Prompt(
        name="transfer-data",
        description="Transfer data between two databases using FastTransfer",
        arguments=[
            PromptArgument(name="source_type", description="Source database type (e.g. pgsql, mssql, oraodp)", required=True),
            PromptArgument(name="target_type", description="Target database type (e.g. pgsql, mssql, mysql)", required=True),
        ],
    ),
    Prompt(
        name="lakehouse-pipeline",
        description="Set up a database-to-cloud lakehouse pipeline using LakeXpress",
        arguments=[
            PromptArgument(name="source_type", description="Source database type (e.g. pgsql, mssql, oraodp)", required=True),
            PromptArgument(name="publish_target", description="Cloud target (e.g. snowflake, databricks, fabric, bigquery, redshift)", required=True),
        ],
    ),
    Prompt(
        name="migrate-database",
        description="Migrate schema and data across database platforms using MigratorXpress",
        arguments=[
            PromptArgument(name="source_type", description="Source database (e.g. oracle, postgresql, sqlserver)", required=True),
            PromptArgument(name="target_type", description="Target database (e.g. postgresql, mysql, sqlserver)", required=True),
        ],
    ),
    Prompt(
        name="troubleshoot",
        description="Troubleshoot a failed Arpe.io command using docs search and error diagnostics",
        arguments=[
            PromptArgument(name="error_message", description="The error message or output from the failed command", required=True),
        ],
    ),
]


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    return _PROMPTS


@app.get_prompt()
async def get_prompt(name: str, arguments: dict | None = None) -> GetPromptResult:
    args = arguments or {}
    if name == "export-table":
        return GetPromptResult(
            description="Export a database table to a file",
            messages=[PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=(
                        f"I want to export data from a {args.get('source_type', 'database')} table "
                        f"to {args.get('output_format', 'a file')} format using FastBCP. "
                        "Please guide me through the process step by step. "
                        "Start by getting the workflow guidance, then help me build and preview the export command."
                    ),
                ),
            )],
        )
    elif name == "transfer-data":
        return GetPromptResult(
            description="Transfer data between databases",
            messages=[PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=(
                        f"I want to transfer data from a {args.get('source_type', 'source')} database "
                        f"to a {args.get('target_type', 'target')} database using FastTransfer. "
                        "Please guide me through the process step by step."
                    ),
                ),
            )],
        )
    elif name == "lakehouse-pipeline":
        return GetPromptResult(
            description="Set up a lakehouse pipeline",
            messages=[PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=(
                        f"I want to set up a data pipeline from a {args.get('source_type', 'database')} "
                        f"to {args.get('publish_target', 'a cloud lakehouse')} using LakeXpress. "
                        "Please guide me through the full setup: auth file, lxdb init, config create, and sync."
                    ),
                ),
            )],
        )
    elif name == "migrate-database":
        return GetPromptResult(
            description="Migrate database schema and data",
            messages=[PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=(
                        f"I want to migrate schema and data from {args.get('source_type', 'a source database')} "
                        f"to {args.get('target_type', 'a target database')} using MigratorXpress. "
                        "Please guide me through the complete migration workflow."
                    ),
                ),
            )],
        )
    elif name == "troubleshoot":
        return GetPromptResult(
            description="Troubleshoot a failed command",
            messages=[PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=(
                        "I ran an Arpe.io command and it failed with this error:\n\n"
                        f"```\n{args.get('error_message', '(no error provided)')}\n```\n\n"
                        "Please search the documentation and help me diagnose what went wrong."
                    ),
                ),
            )],
        )
    else:
        raise ValueError(f"Unknown prompt: {name}")


# ------------------------------------------------------------------ #
# MCP Resources — static capability data
# ------------------------------------------------------------------ #

from src.fastbcp.command_builder import get_supported_formats as get_fastbcp_formats
from src.fasttransfer.command_builder import get_supported_combinations as get_fasttransfer_combinations
from src.lakexpress.command_builder import get_supported_capabilities as get_lakexpress_capabilities
from src.migratorxpress.command_builder import get_supported_capabilities as get_migratorxpress_capabilities

_RESOURCE_BASE = "arpeio://capabilities"

_RESOURCES = [
    Resource(
        uri=f"{_RESOURCE_BASE}/fastbcp-formats",
        name="FastBCP Supported Formats",
        description="Supported source databases, output formats, and storage targets for FastBCP",
        mimeType="application/json",
    ),
    Resource(
        uri=f"{_RESOURCE_BASE}/fasttransfer-combinations",
        name="FastTransfer Supported Combinations",
        description="Supported source-to-target database pairs for FastTransfer",
        mimeType="application/json",
    ),
    Resource(
        uri=f"{_RESOURCE_BASE}/lakexpress-capabilities",
        name="LakeXpress Capabilities",
        description="Supported source databases, storage backends, and publish targets for LakeXpress",
        mimeType="application/json",
    ),
    Resource(
        uri=f"{_RESOURCE_BASE}/migratorxpress-capabilities",
        name="MigratorXpress Capabilities",
        description="Supported source/target databases, tasks, and modes for MigratorXpress",
        mimeType="application/json",
    ),
]


@app.list_resources()
async def list_resources() -> list[Resource]:
    return _RESOURCES


@app.read_resource()
async def read_resource(uri: str) -> list[TextResourceContents]:
    import json as _json

    if uri == f"{_RESOURCE_BASE}/fastbcp-formats":
        data = get_fastbcp_formats()
    elif uri == f"{_RESOURCE_BASE}/fasttransfer-combinations":
        data = get_fasttransfer_combinations()
    elif uri == f"{_RESOURCE_BASE}/lakexpress-capabilities":
        data = get_lakexpress_capabilities()
    elif uri == f"{_RESOURCE_BASE}/migratorxpress-capabilities":
        data = get_migratorxpress_capabilities()
    else:
        raise ValueError(f"Unknown resource: {uri}")

    return [TextResourceContents(
        uri=uri,
        mimeType="application/json",
        text=_json.dumps(data, indent=2, default=str),
    )]


# ------------------------------------------------------------------ #
# Tool listing and dispatch
# ------------------------------------------------------------------ #

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List all available MCP tools."""
    return all_tools


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls by dispatching to the appropriate handler."""
    logger.info(f"call_tool invoked: name={name!r}, handlers={len(tool_handlers)}, tools={len(all_tools)}")
    try:
        # Meta tools
        if name == "arpe_get_status":
            return await handle_arpe_status()
        if name == "arpe_quick_start":
            return await handle_arpe_quick_start(arguments)

        # Try each registered handler
        for i, handler in enumerate(tool_handlers):
            result = await handler(name, arguments)
            logger.info(f"  handler[{i}] returned: {result is not None}")
            if result is not None:
                return result

        logger.warning(f"No handler matched tool '{name}'")
        return [TextContent(type="text", text=f"Error: Unknown tool '{name}'")]

    except Exception as e:
        logger.exception(f"Error handling tool '{name}': {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_arpe_status() -> list[TextContent]:
    """Handle arpe_get_status tool — show status of all tools."""
    response = [
        "# Arpe.io Tools Status",
        "",
    ]

    tool_info = [
        ("FastBCP", FastBCPCommandBuilder, TOOL_CONFIGS["fastbcp"]),
        ("FastTransfer", FastTransferCommandBuilder, TOOL_CONFIGS["fasttransfer"]),
        ("LakeXpress", LakeXpressCommandBuilder, TOOL_CONFIGS["lakexpress"]),
        ("MigratorXpress", MigratorXpressCommandBuilder, TOOL_CONFIGS["migratorxpress"]),
    ]

    for name, builder_cls, config in tool_info:
        response.append(f"## {name}")
        response.append(f"- **Binary Path**: `{config['path']}`")

        try:
            builder = builder_cls(config["path"])
            if builder.preview_only:
                response.append("- **Mode**: Command builder — build, preview, and validate commands (default mode, fully functional)")
                response.append("- **Execution**: Install the binary from https://arpe.io to also run commands directly")
            else:
                version_info = builder.get_version()
                version_str = version_info.get("version", "Unknown")
                response.append(f"- **Mode**: Full (command builder + execution)")
                response.append(f"- **Version**: {version_str}")
        except Exception as e:
            response.append(f"- **Status**: Error ({e})")

        response.append("")

    response.append(f"**Total tools registered**: {len(all_tools)}")

    return [TextContent(type="text", text="\n".join(response))]


async def handle_arpe_quick_start(arguments: dict) -> list[TextContent]:
    """Handle arpe_quick_start tool — detect the right tool and return workflow guide."""
    use_case = arguments.get("use_case", "")
    product_override = arguments.get("product")

    workflows = {
        "fastbcp": {
            "name": "FastBCP",
            "purpose": "High-performance parallel database export to files and cloud storage",
            "when": "Export a database table or query result to CSV, Parquet, JSON, TSV, BSON, XLSX, or binary files",
            "warning": None,
            "params": "source (type, server, database, table/query, credentials), output (format, path/directory), options (method, degree)",
            "steps": [
                "1. `fastbcp_info` (action='workflow') — get database-specific export guidance",
                "2. `fastbcp_preview_export` — build and review the export command (parallelism auto-suggested if not specified)",
                "3. `fastbcp_execute_export` — run the export command",
            ],
        },
        "fasttransfer": {
            "name": "FastTransfer",
            "purpose": "High-performance parallel data transfer between databases",
            "when": "Copy data directly from one database to another without intermediate files",
            "warning": None,
            "params": "source (type, server, database, table/query, credentials), target (type, server, database, table, credentials), options (method, degree)",
            "steps": [
                "1. `fasttransfer_info` (action='workflow') — get database-specific transfer guidance",
                "2. `fasttransfer_preview_transfer` — build and review the transfer command (parallelism auto-suggested if not specified)",
                "3. `fasttransfer_execute_transfer` — run the transfer command",
            ],
        },
        "lakexpress": {
            "name": "LakeXpress",
            "purpose": "Automated database-to-cloud data pipeline as Parquet",
            "when": "Sync database tables to a cloud lakehouse (Snowflake, Databricks, Fabric, BigQuery, Redshift, MotherDuck)",
            "warning": "LakeXpress uses FastBCP internally for extraction — do NOT also use FastBCP for the same task.",
            "params": "auth_file (JSON credentials), lxdb_auth_id, source config, storage backend, publish target",
            "steps": [
                "1. `lakexpress_info` (action='workflow') — get the full command sequence",
                "2. `lakexpress_preview_command` — build each command (lxdb_init, config_create, sync / sync_export / sync_publish)",
                "3. `lakexpress_execute_command` — run each command in sequence",
            ],
        },
        "migratorxpress": {
            "name": "MigratorXpress",
            "purpose": "Cross-platform database migration with DDL translation and parallel data transfer",
            "when": "Migrate schema + data from one database platform to another (e.g., Oracle → PostgreSQL)",
            "warning": "MigratorXpress uses FastTransfer internally — do NOT also use FastTransfer for the same task.",
            "params": "auth_file (JSON credentials), source/target db auth IDs, schema names, task list",
            "steps": [
                "1. `migratorxpress_info` (action='workflow') — get the recommended task sequence",
                "2. `migratorxpress_validate_auth_file` — verify credentials file",
                "3. `migratorxpress_preview_command` — build and review the migration command",
                "4. `migratorxpress_execute_command` — run the migration command",
            ],
        },
    }

    # Keyword-based tool detection
    _TOOL_KEYWORDS = {
        "fastbcp": ["export", "dump", "extract", "file", "csv", "parquet", "json", "tsv",
                     "bson", "xlsx", "binary", "output file", "to file", "to disk"],
        "fasttransfer": ["transfer", "replicate", "copy data", "database to database",
                         "db to db", "etl", "load data", "insert into"],
        "lakexpress": ["lake", "lakehouse", "snowflake", "databricks", "fabric",
                       "redshift", "bigquery", "motherduck", "glue", "ducklake",
                       "teradata", "pipeline", "sync to cloud", "s3 parquet", "data lake"],
        "migratorxpress": ["migrate", "migration", "schema translation", "convert schema",
                           "cross-platform", "ddl", "translate schema", "copy constraints",
                           "primary key", "foreign key"],
    }

    def _detect_product(text: str) -> str | None:
        text_lower = text.lower()
        scores = {}
        for product, keywords in _TOOL_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score > 0:
                scores[product] = score
        if not scores:
            return None
        return max(scores, key=lambda k: scores[k])

    # Determine which product to show
    if product_override and product_override in workflows:
        selected = product_override
    elif product_override == "all":
        selected = "all"
    else:
        selected = _detect_product(use_case) if use_case else None

    response = ["# Arpe.io Quick Start Guide", ""]

    if selected == "all" or selected is None:
        if selected is None and use_case:
            response.append(f"Could not determine the best tool for: \"{use_case}\"")
            response.append("Here are all available tools — pick the one that matches your goal:")
            response.append("")
        for key in ("fastbcp", "fasttransfer", "lakexpress", "migratorxpress"):
            wf = workflows[key]
            response.append(f"## {wf['name']} — {wf['purpose']}")
            response.append(f"**Use when**: {wf['when']}")
            response.append("")
            response.extend(wf["steps"])
            response.append("")
    else:
        wf = workflows[selected]
        response.append(f"## Recommended: {wf['name']}")
        response.append(f"**{wf['purpose']}**")
        response.append("")
        if use_case:
            response.append(f"Based on your use case: \"{use_case}\"")
            response.append("")
        if wf["warning"]:
            response.append(f"**Warning**: {wf['warning']}")
            response.append("")
        response.append(f"**Required parameters**: {wf['params']}")
        response.append("")
        response.append("## Recommended sequence:")
        response.extend(wf["steps"])

    return [TextContent(type="text", text="\n".join(response))]


async def _run():
    """Async server startup logic."""
    logger.info("Starting Arpe.io Unified MCP Server...")
    logger.info(f"Tools registered: {len(all_tools)}")

    # Start documentation search index loading in the background (non-blocking)
    asyncio.create_task(search_engine.initialize(TOOL_CONFIGS))

    from mcp.server.stdio import stdio_server

    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


def main():
    """Entry point for the MCP server (console script)."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
