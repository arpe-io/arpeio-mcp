"""
Arpe ADBC drivers MCP tool.

The drivers are libraries loaded by an ADBC driver manager, so there is
nothing to preview or execute: a single read-only `adbc_info` tool lists
the family and returns per-driver connection and install guidance.
"""

import logging
from typing import Any, Dict, List, Tuple

from mcp.types import Tool, ToolAnnotations

from src.base.structured import make_output_schema, respond
from .drivers import (
    ADBC_DRIVERS,
    DOCS_BASE_URL,
    LICENCE_SOURCES,
    UPCOMING_DRIVERS,
    AdbcDriver,
    find_driver,
    get_supported_capabilities,
    install_commands,
)

logger = logging.getLogger(__name__)

_DRIVER_CHOICES = ", ".join(f"{d.name} ({d.load_name})" for d in ADBC_DRIVERS.values())


def _python_example(drv: AdbcDriver) -> str:
    return (
        "import adbc_driver_manager.dbapi as dbapi\n"
        f'with dbapi.connect(driver="{drv.load_name}", db_kwargs={{"uri": "{drv.uri_example}"}}) as conn:\n'
        "    cur = conn.cursor()\n"
        '    cur.execute("SELECT ...")\n'
        "    table = cur.fetch_arrow_table()"
    )


def create_tools() -> Tuple[List[Tool], Any]:
    """Create the adbc_info tool and its handler."""
    tools = [
        Tool(
            name="adbc_info",
            description=(
                "Get information about the Arpe ADBC drivers: ArpeMSSQL (SQL Server), ArpePGSQL "
                "(PostgreSQL), ArpeOracle (Oracle), ArpeNetezza (Netezza) and ArpeTeradata (Teradata). "
                "They are native, licence-gated ADBC drivers that return Apache Arrow from any ADBC "
                "client (Python, C/C++, C#, Go, Java, R, Rust) and back FastBCP's adbc_* connection types. "
                "Omit `driver` to list the family with current versions; pass it for the load name, "
                "connection URI, authentication, install command and docs links of one driver. "
                "Read-only, does not require database connectivity."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            outputSchema=make_output_schema({
                "drivers": {
                    "type": "array",
                    "description": "Driver records (all drivers, or the requested one).",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "description": "Commercial name, e.g. ArpeTeradata."},
                            "load_name": {"type": "string"},
                            "version": {"type": "string"},
                            "database": {"type": "string"},
                            "uri_example": {"type": "string"},
                            "fastbcp_type": {"type": ["string", "null"]},
                            "docs_url": {"type": "string"},
                        },
                    },
                },
                "install": {"type": "object", "description": "One-line installers (single driver only)."},
            }),
            inputSchema={
                "type": "object",
                "properties": {
                    "driver": {
                        "type": "string",
                        "description": (
                            f"Optional driver: {_DRIVER_CHOICES}. The commercial name, the load name "
                            "or the FastBCP connection type (e.g. 'adbc_teradata') are all accepted."
                        ),
                    },
                },
            },
        ),
    ]

    def _list_drivers() -> Tuple[list, dict]:
        lines = [
            "# Arpe ADBC Drivers",
            "",
            "Native ADBC drivers that speak each database's wire protocol directly and stream "
            "Apache Arrow both ways (read and bulk ingest). Linux x64 and Windows x64. "
            "Every driver needs a valid Arpeio licence at runtime (sales@arpe.io).",
            "",
            "| Driver | Database | Load name | Version | FastBCP type |",
            "|--------|----------|-----------|---------|--------------|",
        ]
        for drv in ADBC_DRIVERS.values():
            fastbcp = f"`{drv.fastbcp_type}` ({drv.fastbcp_min_version}+)" if drv.fastbcp_type else "-"
            lines.append(
                f"| **{drv.name}** | {drv.database} ({drv.also_covers}) | `{drv.load_name}` "
                f"| {drv.version} | {fastbcp} |"
            )
        for up in UPCOMING_DRIVERS:
            lines.append(f"| {up['name']} | {up['database']} | `{up['load_name']}` | {up['status']} | - |")
        lines.extend([
            "",
            "Install any driver by load name: "
            "`curl -fsSL https://raw.githubusercontent.com/arpe-io/adbc-drivers/main/install.sh "
            "| sh -s -- <load_name> --license /path/to/your.lic` "
            "(`--list` shows the latest published versions).",
            "",
            f"Docs: {DOCS_BASE_URL}",
            "Call `adbc_info` with `driver` for connection details, or `arpe_release_notes` "
            "with the driver's load name for its release history.",
        ])
        caps = get_supported_capabilities()
        return respond(lines, {"status": "ok", "tool": "adbc_info", **caps})

    def _driver_detail(drv: AdbcDriver) -> Tuple[list, dict]:
        install = install_commands(drv.load_name)
        lines = [
            f"# {drv.name} — {drv.database} (v{drv.version})",
            "",
            f"- **Load name**: `{drv.load_name}` (options under `{drv.option_prefix}*`)",
            f"- **Covers**: {drv.database}, {drv.also_covers}",
            f"- **Protocol**: {drv.protocol}",
            f"- **Authentication**: {drv.authentication}",
            f"- **URI**: `{drv.uri_example}`",
        ]
        if drv.fastbcp_type:
            lines.append(
                f"- **FastBCP**: connection type `{drv.fastbcp_type}` (FastBCP {drv.fastbcp_min_version}+), "
                "Parquet output only"
            )
        lines.extend([
            "",
            "## Install",
            f"- Linux: `{install['linux']}`",
            f"- Windows (PowerShell): `{install['windows']}`",
            "",
            "## Connect (Python)",
            "```python",
            _python_example(drv),
            "```",
            "",
            "## Licence (first source found wins)",
        ])
        lines.extend(f"{i}. {src}" for i, src in enumerate(LICENCE_SOURCES, 1))
        lines.extend([
            "",
            f"Docs: {drv.docs_url}",
            f"What's new: {drv.whats_new_url}",
        ])
        return respond(lines, {
            "status": "ok",
            "tool": "adbc_info",
            "drivers": [drv.to_dict()],
            "install": install,
            "licence_sources": LICENCE_SOURCES,
        })

    async def handle_info(arguments: Dict[str, Any]):
        requested = (arguments or {}).get("driver")
        if not requested:
            return _list_drivers()
        drv = find_driver(requested)
        if drv is None:
            upcoming = next(
                (u for u in UPCOMING_DRIVERS
                 if requested.strip().lower() in (u["name"].lower(), u["load_name"])),
                None,
            )
            if upcoming:
                msg = f"{upcoming['name']} ({upcoming['database']}) is announced but not published yet."
            else:
                msg = f"Unknown ADBC driver '{requested}'. Valid: {_DRIVER_CHOICES}."
            return respond(msg, {"status": "error", "tool": "adbc_info", "error": msg})
        return _driver_detail(drv)

    async def handle_call(name: str, arguments: Dict[str, Any]):
        if name == "adbc_info":
            return await handle_info(arguments)
        return None  # not our tool

    return tools, handle_call
