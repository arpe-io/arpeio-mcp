---
title: Arpeio MCP
emoji: 🛠️
colorFrom: blue
colorTo: indigo
sdk: docker
pinned: false
---

# arpeio-mcp

<!-- mcp-name: io.github.arpe-io/arpeio-mcp -->

Unified MCP server for [Arpe.io](https://arpe.io) data tools.

## Tools

| Tool | Description |
|------|-------------|
| **FastBCP** | High-performance parallel database export to files and cloud |
| **FastTransfer** | High-performance parallel data transfer between databases |
| **LakeXpress** | Automated database-to-cloud data pipeline as Parquet |
| **MigratorXpress** | Cross-platform database migration with parallel transfer |

## Installation

```bash
pip install arpeio-mcp
```

## Usage

Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "arpeio": {
      "command": "arpeio-mcp",
      "env": {
        "FASTBCP_PATH": "/path/to/FastBCP",
        "FASTTRANSFER_PATH": "/path/to/FastTransfer",
        "LAKEXPRESS_PATH": "/path/to/LakeXpress",
        "MIGRATORXPRESS_PATH": "/path/to/MigratorXpress"
      }
    }
  }
}
```

All binary paths are optional. Missing binaries activate **preview-only mode** for that tool — preview and informational tools work normally, while execution tools return helpful install instructions.

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `FASTBCP_PATH` | Path to FastBCP binary | No |
| `FASTTRANSFER_PATH` | Path to FastTransfer binary | No |
| `LAKEXPRESS_PATH` | Path to LakeXpress binary | No |
| `MIGRATORXPRESS_PATH` | Path to MigratorXpress binary | No |
| `FASTBCP_DIR_PATH` | FastBCP directory for LakeXpress | No |
| `*_TIMEOUT` | Per-tool execution timeout (seconds) | No |
| `*_LOG_DIR` | Per-tool log directory | No |
| `LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING/ERROR) | No |

## Available Tools (25)

### FastBCP (6 tools)
- `fastbcp_preview_export` — Preview export command
- `fastbcp_execute_export` — Execute export
- `fastbcp_validate_connection` — Validate source connection
- `fastbcp_list_formats` — List supported formats/databases/targets
- `fastbcp_suggest_parallelism` — Recommend parallelism method
- `fastbcp_get_version` — Report version and capabilities

### FastTransfer (6 tools)
- `fasttransfer_preview_transfer` — Preview transfer command
- `fasttransfer_execute_transfer` — Execute transfer
- `fasttransfer_validate_connection` — Validate connection
- `fasttransfer_list_combinations` — List supported database pairs
- `fasttransfer_suggest_parallelism` — Recommend parallelism method
- `fasttransfer_get_version` — Report version and capabilities

### LakeXpress (5 tools)
- `lakexpress_preview_command` — Preview any LakeXpress command
- `lakexpress_execute_command` — Execute command
- `lakexpress_list_capabilities` — List supported databases/backends
- `lakexpress_suggest_workflow` — Recommend workflow
- `lakexpress_get_version` — Report version and capabilities

### MigratorXpress (6 tools)
- `migratorxpress_preview_command` — Preview migration command
- `migratorxpress_execute_command` — Execute migration
- `migratorxpress_validate_auth_file` — Validate auth JSON file
- `migratorxpress_list_capabilities` — List databases/tasks/modes
- `migratorxpress_suggest_workflow` — Recommend migration workflow
- `migratorxpress_get_version` — Report version and capabilities

### Meta (1 tool)
- `arpe_get_status` — Status of all tools (installed/preview-only)

## License

MIT
