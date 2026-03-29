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

## Available Tools (28)

### FastBCP (7 tools)
- `fastbcp_list_formats` — List supported databases, formats, and storage targets
- `fastbcp_suggest_parallelism` — Recommend parallelism method for your table
- `fastbcp_suggest_workflow` — Step-by-step export workflow with DB-specific tips
- `fastbcp_validate_connection` — Validate source connection parameters
- `fastbcp_preview_export` — Build and preview export command
- `fastbcp_execute_export` — Execute export
- `fastbcp_get_version` — Report version and capabilities

### FastTransfer (7 tools)
- `fasttransfer_list_combinations` — List supported source-to-target database pairs
- `fasttransfer_suggest_parallelism` — Recommend parallelism method for your table
- `fasttransfer_suggest_workflow` — Step-by-step transfer workflow with tips
- `fasttransfer_validate_connection` — Validate connection parameters
- `fasttransfer_preview_transfer` — Build and preview transfer command
- `fasttransfer_execute_transfer` — Execute transfer
- `fasttransfer_get_version` — Report version and capabilities

### LakeXpress (5 tools)
- `lakexpress_list_capabilities` — List supported databases, backends, and targets
- `lakexpress_suggest_workflow` — Recommend full command sequence
- `lakexpress_preview_command` — Build and preview any LakeXpress command
- `lakexpress_execute_command` — Execute command
- `lakexpress_get_version` — Report version and capabilities

### MigratorXpress (6 tools)
- `migratorxpress_list_capabilities` — List databases, tasks, and modes
- `migratorxpress_suggest_workflow` — Recommend migration task sequence
- `migratorxpress_validate_auth_file` — Validate auth JSON file
- `migratorxpress_preview_command` — Build and preview migration command
- `migratorxpress_execute_command` — Execute migration
- `migratorxpress_get_version` — Report version and capabilities

### Meta (3 tools)
- `arpe_get_status` — Status of all tools (installed/command-builder-only)
- `arpe_quick_start` — Detect the right tool from a use case description and get a workflow guide
- `search_docs` — Search arpe.io documentation and blog with BM25 full-text search

## License

MIT
