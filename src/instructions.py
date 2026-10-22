"""
MCP server instructions injected into the AI's system context on connect.

This string is sent automatically in the InitializeResult response to
supporting clients (Claude, Cursor, etc.) — no tool call needed.
"""

INSTRUCTIONS = """\
arpeio-mcp provides four high-performance data tools for database export, \
transfer, lake pipelines, and cross-platform migration. Each tool wraps a \
native CLI binary with parallel execution support. It also documents the \
Arpe ADBC drivers (ArpeMSSQL, ArpePGSQL, ArpeOracle, ArpeNetezza, \
ArpeTeradata), the native drivers behind FastBCP's `adbc_*` connection types.

## Tool Selection

Choose the right tool based on what the user wants to accomplish:

| Goal | Tool | Notes |
|------|------|-------|
| Export DB table/query to files (CSV, Parquet, JSON, etc.) | **FastBCP** | Outputs to local disk or cloud storage (S3, Azure, GCS, OneLake) |
| Transfer data from one database to another | **FastTransfer** | Direct DB-to-DB, no intermediate files |
| Pipeline from DB to a cloud lakehouse as Parquet | **LakeXpress** | Targets: Snowflake, Databricks, Fabric, BigQuery, Redshift, MotherDuck, Glue, DuckLake |
| Migrate schema + data across database platforms | **MigratorXpress** | Handles DDL translation, PK/FK/AK copy, and parallel data transfer |
| Read / bulk-load a database as Apache Arrow from code | **Arpe ADBC drivers** | Libraries, not CLIs: call `adbc_info`; no preview/execute step |

Important relationships:
- LakeXpress uses FastBCP internally for extraction — do NOT suggest both for the same task.
- MigratorXpress uses FastTransfer internally for data movement — do NOT suggest both for the same task.
- When the user's intent is unclear, call `arpe_quick_start` with a description of their use case.
- When the user asks "what changed in version X", call `arpe_release_notes` with the product name instead of guessing.

## Parallelism Methods

All tools support parallel extraction/transfer. Choose the method based on the \
source database and available key columns:

| Method | When to use | Key required? |
|--------|-------------|---------------|
| **Ntile** | Numeric key column with roughly uniform distribution | Yes (numeric) |
| **DataDriven** | Date column or non-uniformly distributed key; pre-computes partitions from distinct values | Yes (any type) |
| **PhysLoc** | SQL Server only; splits by physical page location | No |
| **Ctid** | PostgreSQL only; splits by physical tuple ID | No |
| **Rowid** | Oracle only; splits by physical ROWID | No |
| **RangeId** | Numeric key with known min/max range | Yes (numeric) |
| **Random** | No suitable key; approximate distribution via modulo | Yes (numeric) |
| **NZDataSlice** | Netezza only; uses native data slicing | No |
| **Timepartition** | FastBCP 0.30+, FastTransfer 0.18+; partitions by time column (FastTransfer key: tuple such as `(date_col,year,month)`) | Yes (datetime) |
| **DBPartition** | FastBCP 1.3+, FastTransfer 0.18+; one chunk per native table partition (SQL Server, PostgreSQL, Oracle, MySQL, Teradata); needs schema + table | No |
| **Physical** | FastBCP 1.4+; physical row location, resolved to PhysLoc / Ctid / Rowid / NZDataSlice from the connection type (SQL Server, PostgreSQL, Oracle, Netezza only); needs schema + table | No |

Default recommendations:
- Numeric primary key exists → **Ntile**
- Date/timestamp key exists → **DataDriven**
- SQL Server, no key → **PhysLoc**
- PostgreSQL, no key → **Ctid**
- Oracle, no key → **Rowid**
- FastBCP 1.4+: use **Physical** instead of PhysLoc / Ctid / Rowid / \
NZDataSlice, which are deprecated in FastBCP (removed in 2.0). FastBCP < 1.4 \
and FastTransfer keep the engine-specific names.
- When unsure, call `{product}_info` with action='parallelism' for FastBCP/FastTransfer.

## --paralleldegree Convention

The `--paralleldegree` (or `degree`) parameter controls how many parallel \
threads are used:
- **0** = use all available CPU cores
- **Positive integer** (e.g. 8) = use exactly that many threads
- **Negative integer** (e.g. -2) = use cores / abs(value). On a 16-core \
machine, -2 means 8 threads; -4 means 4 threads. Useful to leave headroom \
for other processes.

## --merge Flag

The `--merge` flag controls whether parallel output files are merged:
- **merge=true**: Slower, but produces a single output file. Use when the \
downstream consumer expects one file.
- **merge=false** (default): Faster, produces N files (one per parallel \
thread). Better when the downstream system can import multiple files in \
parallel (e.g. Spark, Snowflake COPY INTO, bulk loaders).

## Arpe ADBC drivers

Native, licence-gated ADBC drivers (Linux x64, Windows x64) that speak each \
database's wire protocol directly (no ODBC or vendor client) and stream Apache \
Arrow both ways. Always use the commercial names below; load them by load name \
from any ADBC driver manager.

| Driver | Database | Load name | Latest | FastBCP type |
|--------|----------|-----------|--------|--------------|
| **ArpeMSSQL** | SQL Server, Azure SQL, Fabric | `arpemssql` | 0.7.6 | `adbc_mssql` |
| **ArpePGSQL** | PostgreSQL and compatibles | `arpepgsql` | 0.4.7 | `adbc_pgsql` |
| **ArpeOracle** | Oracle, Oracle Cloud ADB | `arpeoracle` | 0.4.2 | `adbc_oracle` |
| **ArpeNetezza** | IBM Netezza / Cloud NPS | `arpenz` | 0.3.7 | `adbc_netezza` |
| **ArpeTeradata** | Teradata | `arpeteradata` | 0.3.5 | `adbc_teradata` |

ArpeDb2 (IBM Db2) is announced but not published. Call `adbc_info` for \
URIs, install commands and licence setup, and `arpe_release_notes` with the \
load name for a driver's release history. Docs: https://adbc-drivers-docs.arpe.io/

## FastBCP ADBC connection types

FastBCP 1.x adds ADBC connection types that read through the Arpe ADBC \
drivers: `adbc_mssql` (ArpeMSSQL, 1.0+), `adbc_pgsql` (ArpePGSQL, 1.1+), \
`adbc_oracle` (ArpeOracle, 1.2+), `adbc_netezza` (ArpeNetezza, 1.3+) and \
`adbc_teradata` (ArpeTeradata, 1.4+). They only write **Parquet** \
output; use the native type (`mssql`, `pgsql`, `oraodp`, `nzsql`, `teradata`) \
for CSV, JSON and the other formats. Parallelism methods follow the native type \
(Physical on SQL Server, PostgreSQL, Oracle and Netezza; no Physical on Teradata). \
`adbc_teradata` refuses trusted authentication.

## LakeXpress 0.4.0+ notes

- The metadata-database subcommand was renamed from `logdb` to `lxdb` (e.g. \
`lxdb init`, `lxdb drop`, `lxdb release-locks`). Legacy `logdb_*` command \
types are still accepted by `lakexpress_preview_command` and route to the \
new `lxdb` subcommands automatically.
- The auth-id flag was renamed from `--log_db_auth_id` to `--lxdb_auth_id`. \
The MCP emits the new flag for all commands regardless of which legacy \
alias was requested.
- **Teradata** is a supported source database as of 0.4.0.
- **Amazon Redshift** is a supported publish target as of 0.4.0. Use \
`--publish_method internal` for COPY into Redshift tables, or `external` \
for Spectrum-style external tables sitting on S3.

## Parallelism in LakeXpress

LakeXpress does NOT accept `--parallelmethod`, `--parallelkey`, or \
`--paralleldegree` flags directly. Instead, configure parallelism through \
`config create` parameters:
- `--fastbcp_p <degree>` — global parallel degree for all tables.
- `--fastbcp_table_config 'schema.table:method:key:degree;...'` — per-table \
parallelism override. Example: \
`'dbo.orders:Timepartition:o_orderdate:16;dbo.lineitem:Ntile:l_orderkey:8'`.

Do NOT invent LakeXpress CLI flags. Always use `lakexpress_preview_command` \
to build the correct command.

## Recommended Workflow

Always follow this sequence when building a command:

1. **Get guidance** (optional) — call `{product}_info` with action='workflow' \
to get database-specific tips and the recommended command sequence.
2. **Preview** — call the `preview_*` tool to build and review the exact CLI \
command with passwords masked. If no parallelism method is specified, one is \
auto-suggested based on the source database type. Never skip this step.
3. **Execute** — only after the user has reviewed the preview, call the \
`execute_*` tool with `confirmation=true`.

If the user's intent is unclear or they are new to arpe.io tools, start with \
`arpe_quick_start` to determine which tool and workflow to recommend.
"""
