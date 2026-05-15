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

[![PyPI version](https://img.shields.io/pypi/v/arpeio-mcp)](https://pypi.org/project/arpeio-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Unified MCP server for [Arpe.io](https://arpe.io) data tools — build, preview, and execute high-performance data commands through AI assistants.

| Tool | Description |
|------|-------------|
| **FastBCP** | High-performance parallel database export to files and cloud |
| **FastTransfer** | High-performance parallel data transfer between databases |
| **LakeXpress** | Automated database-to-cloud data pipeline as Parquet |
| **MigratorXpress** | Cross-platform database migration with parallel transfer |

> **No binaries required.** All tools work in **command builder mode** out of the box — command building, preview, and informational tools work without any Arpe.io binary installed. To enable execution, download the binaries from [arpe.io](https://arpe.io) and set the corresponding `*_PATH` environment variables.

## Connect your AI assistant

A hosted instance is available at `https://arpe-io-arpeio-mcp.hf.space/sse` — no installation required. For local installation with execution support, use the stdio transport via `pip install arpeio-mcp`.

[ChatGPT](#chatgpt) | [Claude Code](#claude-code) | [Claude Desktop](#claude-desktop) | [Cursor](#cursor) | [Gemini CLI](#gemini-cli) | [HuggingChat](#huggingchat) | [Kiro IDE](#kiro-ide) | [Le Chat (Mistral)](#le-chat-mistral) | [VS Code](#vs-code-github-copilot) | [Windsurf](#windsurf)

### ChatGPT

*Available for paid plans only (Plus, Pro, Team, and Enterprise).*

1. Open ChatGPT in your browser, go to **Settings** > **Apps and connectors**.
2. Open **Advanced settings** and enable **Developer mode**.
3. Go to **Connectors** > **Browse connectors** > **Add a new connector**.
4. Set the URL to `https://arpe-io-arpeio-mcp.hf.space/sse` and save.

### Claude Code

```shell
claude mcp add --transport sse arpeio https://arpe-io-arpeio-mcp.hf.space/sse
```

Or for local installation with execution support:

```shell
pip install arpeio-mcp
claude mcp add arpeio arpeio-mcp
```

### Claude Desktop

Add the following to your Claude Desktop configuration file:
- **Linux**: `~/.config/Claude/claude_desktop_config.json`
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

#### Remote (no installation)

```json
{
  "mcpServers": {
    "arpeio": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://arpe-io-arpeio-mcp.hf.space/sse"
      ]
    }
  }
}
```

#### Local (with execution support)

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

### Cursor

1. Open Cursor Settings and search for "MCP".
2. Add a new MCP server with the following configuration:

```json
{
  "mcpServers": {
    "arpeio": {
      "url": "https://arpe-io-arpeio-mcp.hf.space/sse"
    }
  }
}
```

### Gemini CLI

Add the following to your `~/.gemini/settings.json` file:

```json
{
  "mcpServers": {
    "arpeio": {
      "uri": "https://arpe-io-arpeio-mcp.hf.space/sse"
    }
  }
}
```

### HuggingChat

1. In the chat interface, click the **+** icon, select **MCP Servers**, then **Manage MCP Servers**.
2. Click **Add Server**.
3. Set the **Server Name** to `Arpe.io` and the **Server URL** to `https://arpe-io-arpeio-mcp.hf.space/sse`.
4. Click **Add Server** and verify the health check shows **Connected**.

### Kiro IDE

Add the following to your Kiro MCP configuration file (`.kiro/settings/mcp.json` in your workspace):

```json
{
  "mcpServers": {
    "arpeio": {
      "url": "https://arpe-io-arpeio-mcp.hf.space/sse"
    }
  }
}
```

### Le Chat (Mistral)

*Available on all plans, including free.*

1. Go to **Intelligence** > **Connectors**.
2. Click **Add connector** > **Custom MCP Connector**.
3. Set the name to `Arpe.io` and the URL to `https://arpe-io-arpeio-mcp.hf.space/sse`.
4. Leave authentication disabled and click **Create**.

### VS Code (GitHub Copilot)

Add the following to your VS Code MCP configuration. Run **MCP: Open User Configuration** from the Command Palette to open it.
- **Linux**: `~/.config/Code/User/mcp.json`
- **macOS**: `~/Library/Application Support/Code/User/mcp.json`
- **Windows**: `%APPDATA%\Code\User\mcp.json`

```json
{
  "servers": {
    "arpeio": {
      "url": "https://arpe-io-arpeio-mcp.hf.space/sse",
      "type": "sse"
    }
  }
}
```

### Windsurf

Add the following to your Windsurf configuration file:
- **Linux**: `~/.codeium/windsurf/mcp_config.json`
- **macOS**: `~/.codeium/windsurf/mcp_config.json`
- **Windows**: `%USERPROFILE%\.codeium\windsurf\mcp_config.json`

```json
{
  "mcpServers": {
    "arpeio": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "https://arpe-io-arpeio-mcp.hf.space/sse"
      ]
    }
  }
}
```

## Local Installation

For full execution support (not just command building), install locally:

```bash
pip install arpeio-mcp
```

Then configure your AI assistant to use the `arpeio-mcp` command (stdio transport) with optional binary paths — see the [Claude Desktop local configuration](#local-with-execution-support) for an example.

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `FASTBCP_PATH` | Path to FastBCP binary | No |
| `FASTTRANSFER_PATH` | Path to FastTransfer binary | No |
| `LAKEXPRESS_PATH` | Path to LakeXpress binary | No |
| `MIGRATORXPRESS_PATH` | Path to MigratorXpress binary | No |
| `FASTBCP_DIR_PATH` | FastBCP directory for LakeXpress | No |
| `FASTTRANSFER_DIR_PATH` | FastTransfer directory for MigratorXpress | No |
| `*_TIMEOUT` | Per-tool execution timeout (seconds) | No |
| `*_LOG_DIR` | Per-tool log directory | No |
| `LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING/ERROR) | No |

## Available Tools (17)

Read-only advisory tools were consolidated into one `*_info` tool per product (with an `action` enum) and per-product release-notes tools were merged into a single `arpe_release_notes`. Auto-parallelism is now suggested inside `preview` so the typical workflow is **2 calls** (`preview` → `execute`) instead of 4.

### FastBCP (3 tools)
- `fastbcp_info` — Read-only advisory: `action="formats" | "parallelism" | "workflow" | "version"`
- `fastbcp_preview_export` — Validate parameters and render the command (auto-suggests parallelism when `method` is omitted)
- `fastbcp_execute_export` — Run the export

### FastTransfer (3 tools)
- `fasttransfer_info` — Read-only advisory: `action="combinations" | "parallelism" | "workflow" | "version"`
- `fasttransfer_preview_transfer` — Validate parameters and render the command (auto-suggests parallelism)
- `fasttransfer_execute_transfer` — Run the transfer

### LakeXpress (3 tools)
- `lakexpress_info` — Read-only advisory: `action="capabilities" | "workflow" | "version"`
- `lakexpress_preview_command` — Build any LakeXpress command (`lxdb_*`, `config_*`, `sync`, `sync[export]`, `sync[publish]`, `run`, `status`, `cleanup`). On v0.4.0+ binaries, warns when `-a` / `--lxdb_auth_id` / `--sync_id` are missing on sync-family calls
- `lakexpress_execute_command` — Run the command

### MigratorXpress (4 tools)
- `migratorxpress_info` — Read-only advisory: `action="capabilities" | "workflow" | "version"`
- `migratorxpress_validate_auth_file` — Validate the JSON auth file (only file-I/O advisory tool kept separate)
- `migratorxpress_preview_command` — Build the migrate command. Accepts the new `project` tag (v0.6.30+); warns on `migration_db_type="postgres"` against pre-0.6.32 binaries
- `migratorxpress_execute_command` — Run the migration

### Meta (4 tools)
- `arpe_get_status` — Status of all four CLIs (installed / command-builder-only)
- `arpe_quick_start` — Detect the right tool from a plain-English use case and return a workflow guide
- `arpe_release_notes` — Return release-notes chunks for any product (`product="fastbcp" | "fasttransfer" | "lakexpress" | "migratorxpress"`, optional `version`)
- `search_docs` — BM25 full-text search over arpe.io docs sites and blog

## Prompts (5)

Conversation starters surfaced by clients that support MCP prompts (Claude Desktop, Cursor, etc.):
`export-table`, `transfer-data`, `lakehouse-pipeline`, `migrate-database`, `troubleshoot`.

## Resources (4)

Static capability matrices served as MCP resources so clients can prefetch them without a tool call:
`arpeio://capabilities/fastbcp-formats`, `fasttransfer-combinations`, `lakexpress-capabilities`, `migratorxpress-capabilities`.

## License

MIT
