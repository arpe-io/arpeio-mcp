# Changelog

## [0.1.4] - 2026-03-30

### Changed

- **LakeXpress parallelism guidance**: Added explicit documentation at multiple layers (MCP instructions, tool schemas, workflow output) clarifying that LakeXpress configures parallelism via `--fastbcp_p` and `--fastbcp_table_config`, not via non-existent `--parallelmethod`/`--parallelkey` flags. Prevents AI assistants from hallucinating CLI arguments.
- **Command-builder mode messaging**: Reframed status output and tool descriptions to present command-builder mode as the normal fully functional default, not a problem. AI assistants previously misinterpreted "execution not available" as an error requiring user action.

## [0.1.3] - 2026-03-29

### Added

- **MCP instructions field**: Rich `instructions` string sent to AI clients on connect, covering tool selection, parallelism methods, `--paralleldegree` convention, `--merge` trade-off, and recommended workflow sequence.
- **`search_docs` tool**: BM25 full-text search over all arpe.io documentation sites and blog. Indexes are cached locally (`~/.cache/arpeio-mcp/`) and refreshed in the background on server startup. (28 tools total, up from 27.)
- **CLI error diagnostics**: Execute tools now pattern-match known CLI errors (connection refused, license issues, OOM, permission denied, authentication failures, etc.) and return actionable remediation steps instead of generic troubleshooting.
- **`FASTTRANSFER_DIR_PATH` environment variable**: Auto-fills the FastTransfer directory path into MigratorXpress commands, mirroring `FASTBCP_DIR_PATH` for LakeXpress.
- New dependencies: `rank-bm25`, `beautifulsoup4`, `httpx`.

### Changed

- **`arpe_quick_start` tool**: Now accepts a `use_case` parameter (plain English description) and auto-detects the right tool via keyword matching. The `product` parameter is kept as an optional override for backward compatibility.
- **`suggest_parallelism` tools** (FastBCP, FastTransfer): Return a single opinionated recommendation with a suggested `--paralleldegree` value and the next tool to call, instead of listing all available methods.
- **All tool descriptions** (27 tools): Rewritten to answer three questions — when should the AI call this tool, what does it NOT do, and what must be true before calling it.
- **Parameter schemas**: Added enums to `source_type`, `output_format`, `storage_target` in suggest/workflow tools. Enhanced descriptions for `method` (per-method guidance), `degree` (0/negative/positive semantics), `merge` (speed vs single-file trade-off), `load_mode`, and other parameters.

### Fixed

- README now lists all 28 tools and documents `FASTTRANSFER_DIR_PATH`.

## [0.1.2] - 2026-03-16

### Added

- MCP tool annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) on all tools.
- `arpe_quick_start` and `arpe_get_status` meta-tools.
- `os_type` parameter for platform-specific command formatting.
- Workflow context and error guidance in tool responses.
- PyPI version and license badges in README.
- Platform-specific MCP setup instructions (ChatGPT, Claude, Cursor, Gemini CLI, VS Code, Windsurf, etc.).
