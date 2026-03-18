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
    from mcp.types import Tool, ToolAnnotations, TextContent
except ImportError as e:
    print(f"Error: Required package not found: {e}", file=sys.stderr)
    print("Please run: pip install arpeio-mcp", file=sys.stderr)
    sys.exit(1)

from src.fastbcp.command_builder import CommandBuilder as FastBCPCommandBuilder
from src.fastbcp.tools import create_tools as create_fastbcp_tools
from src.fasttransfer.command_builder import CommandBuilder as FastTransferCommandBuilder
from src.fasttransfer.tools import create_tools as create_fasttransfer_tools
from src.lakexpress.command_builder import CommandBuilder as LakeXpressCommandBuilder
from src.lakexpress.tools import create_tools as create_lakexpress_tools
from src.migratorxpress.command_builder import CommandBuilder as MigratorXpressCommandBuilder
from src.migratorxpress.tools import create_tools as create_migratorxpress_tools

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
    },
}

# Initialize MCP server
app = Server("arpeio-mcp")

# Collect all tools and handlers
all_tools: list[Tool] = []
tool_handlers: list = []


def _init_tool(name, builder_cls, create_tools_fn, config):
    """Initialize a single tool's command builder and register its tools."""
    try:
        builder = builder_cls(config["path"])
        if builder.preview_only:
            logger.warning(f"{name}: binary not configured — execution disabled, command building available")
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
            "Get the status of all Arpe.io tools (installed/command-builder-only, version, capabilities summary). "
            "Call this first to see which tools are available and their versions."
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
            "Get a step-by-step workflow guide for any Arpe.io product. "
            "Call this first to understand the recommended tool sequence for your use case."
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
                    "enum": ["fastbcp", "fasttransfer", "lakexpress", "migratorxpress", "all"],
                    "description": "Which product workflow to show (or 'all' for all products)",
                },
            },
            "required": ["product"],
        },
    )
)


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
                response.append("- **Status**: Command builder (execution not available)")
                response.append("- **Download**: https://arpe.io")
            else:
                version_info = builder.get_version()
                version_str = version_info.get("version", "Unknown")
                response.append(f"- **Status**: Installed")
                response.append(f"- **Version**: {version_str}")
        except Exception as e:
            response.append(f"- **Status**: Error ({e})")

        response.append("")

    response.append(f"**Total tools registered**: {len(all_tools)}")

    return [TextContent(type="text", text="\n".join(response))]


async def handle_arpe_quick_start(arguments: dict) -> list[TextContent]:
    """Handle arpe_quick_start tool — return workflow guide per product."""
    product = arguments.get("product", "all")

    workflows = {
        "fastbcp": [
            "## FastBCP (database → file export)",
            "",
            "1. `fastbcp_list_formats` — discover supported databases, output formats, and storage targets",
            "2. `fastbcp_suggest_parallelism` — get the optimal parallelism method for your table",
            "3. `fastbcp_validate_connection` — verify your connection parameters",
            "4. `fastbcp_suggest_workflow` — get a step-by-step workflow with DB-specific tips",
            "5. `fastbcp_preview_export` — build and review the export command",
            "6. `fastbcp_execute_export` — run the export command",
        ],
        "fasttransfer": [
            "## FastTransfer (database → database transfer)",
            "",
            "1. `fasttransfer_list_combinations` — discover supported source→target database pairs",
            "2. `fasttransfer_suggest_parallelism` — get the optimal parallelism method for your table",
            "3. `fasttransfer_validate_connection` — verify source and target connection parameters",
            "4. `fasttransfer_suggest_workflow` — get a step-by-step workflow with transfer tips",
            "5. `fasttransfer_preview_transfer` — build and review the transfer command",
            "6. `fasttransfer_execute_transfer` — run the transfer command",
        ],
        "lakexpress": [
            "## LakeXpress (database → cloud data lake pipeline)",
            "",
            "1. `lakexpress_list_capabilities` — discover supported databases, storage backends, and publishing targets",
            "2. `lakexpress_suggest_workflow` — get the full command sequence for your use case",
            "3. `lakexpress_preview_command` — build and review each command in the sequence",
            "4. `lakexpress_execute_command` — run each command",
        ],
        "migratorxpress": [
            "## MigratorXpress (cross-platform database migration)",
            "",
            "1. `migratorxpress_list_capabilities` — discover supported databases, tasks, and modes",
            "2. `migratorxpress_suggest_workflow` — get the recommended task sequence for your migration",
            "3. `migratorxpress_validate_auth_file` — verify your credentials file is valid",
            "4. `migratorxpress_preview_command` — build and review the migration command",
            "5. `migratorxpress_execute_command` — run the migration command",
        ],
    }

    response = ["# Arpe.io Quick Start Guide", ""]

    if product == "all":
        for key in ("fastbcp", "fasttransfer", "lakexpress", "migratorxpress"):
            response.extend(workflows[key])
            response.append("")
    elif product in workflows:
        response.extend(workflows[product])
    else:
        response.append(f"Unknown product: {product}. Choose from: fastbcp, fasttransfer, lakexpress, migratorxpress, all")

    return [TextContent(type="text", text="\n".join(response))]


async def _run():
    """Async server startup logic."""
    logger.info("Starting Arpe.io Unified MCP Server...")
    logger.info(f"Tools registered: {len(all_tools)}")

    from mcp.server.stdio import stdio_server

    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


def main():
    """Entry point for the MCP server (console script)."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
