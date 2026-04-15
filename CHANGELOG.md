# Changelog

## [0.2.0] - 2026-04-15

### Added

- **FastBCP 0.31 support**: New version entry in the FastBCP capability registry. 0.31 is an internal Parquet schema-precision improvement (`GetSchemaTable` replaces `GetColumnSchema`); capability surface is unchanged from 0.30. Doc-search index now crawls the 0.31 release-notes page.
- **LakeXpress 0.4 support**: New version entry. Adds **Teradata** as a supported source database (`teradata`) and **Amazon Redshift** as a publish target (`redshift`, with `--publish_method internal|external`). `check_version_compatibility` now warns when these are requested against a pre-0.4 binary.
- **Per-product release-notes tools**: New `fastbcp_release_notes`, `fasttransfer_release_notes`, `lakexpress_release_notes`, and `migratorxpress_release_notes` tools. Each returns release-notes chunks from the local docs cache, optionally filtered by version (e.g. `version="0.31"`). Tool count rises from 28 to 32.
- **Shared helper `SearchEngine.get_release_notes`**: Filters indexed chunks by `release-notes` URL path for a given product, with automatic newest-version selection when no version is provided.

### Changed

- **LakeXpress subcommand rename (`logdb` → `lxdb`)**: The 0.4.0 CLI renamed the metadata-database subcommand group. `lakexpress_preview_command` now emits `lxdb init`, `lxdb drop`, `lxdb truncate`, `lxdb locks`, and `lxdb release-locks`. Legacy `logdb_*` `command_type` values are still accepted and transparently route to the new `lxdb_*` builders so existing agent scripts keep working.
- **LakeXpress auth-id flag rename (`--log_db_auth_id` → `--lxdb_auth_id`)**: All emitted commands use the new flag name regardless of which legacy alias was requested.
- **`suggest_workflow` and `get_supported_capabilities` (LakeXpress)**: Updated to list Teradata as a source, Redshift as a publish target, and to show `lxdb init` + `--lxdb_auth_id` in all example commands. Marks `run` as legacy in 0.4.0+.
- **MCP instructions**: New "LakeXpress 0.4.0+ notes" section covering the subcommand rename, the flag rename, Teradata, and Redshift internal vs external publishing. Recommended-workflow section now mentions the `*_release_notes` tools.
- **`arpe_quick_start` keyword routing**: Adds `teradata` to LakeXpress keywords; updates the LakeXpress workflow text to reference `lxdb_init` and `lxdb_auth_id`.

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
