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
ArpeTeradata, ArpeDb2), the native drivers behind FastBCP's `adbc_*` connection types.

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
| **DBPartition** | FastBCP 1.3+, FastTransfer 0.18+; one chunk per native table partition (SQL Server, PostgreSQL, Oracle, MySQL, Teradata; FastBCP 1.7+ also Db2 for LUW through `adbc_db2`); needs schema + table | No |
| **Physical** | FastBCP 1.4+; physical row location, resolved to PhysLoc / Ctid / Rowid / NZDataSlice from the connection type (SQL Server, PostgreSQL, Oracle, Netezza only), and to Pages for the backup types `backup_mssql` / `backup_rman`; needs schema + table | No |

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
| **ArpeMSSQL** | SQL Server, Azure SQL, Fabric | `arpemssql` | 0.8.2 | `adbc_mssql` |
| **ArpePGSQL** | PostgreSQL and compatibles | `arpepgsql` | 0.5.4 | `adbc_pgsql` |
| **ArpeOracle** | Oracle, Oracle Cloud ADB | `arpeoracle` | 0.4.7 | `adbc_oracle` |
| **ArpeNetezza** | IBM Netezza / Cloud NPS | `arpenz` | 0.3.11 | `adbc_netezza` |
| **ArpeTeradata** | Teradata | `arpeteradata` | 0.3.8 | `adbc_teradata` |
| **ArpeDb2** | IBM Db2 (LUW, Db2 for i) | `arpedb2` | 0.4.2 | `adbc_db2` |

Call `adbc_info` for \
URIs, install commands and licence setup, and `arpe_release_notes` with the \
load name for a driver's release history. Docs: https://adbc-drivers-docs.arpe.io/

## FastBCP ADBC connection types

FastBCP 1.x adds ADBC connection types that read through the Arpe ADBC \
drivers: `adbc_mssql` (ArpeMSSQL, 1.0+), `adbc_pgsql` (ArpePGSQL, 1.1+), \
`adbc_oracle` (ArpeOracle, 1.2+), `adbc_netezza` (ArpeNetezza, 1.3+), \
`adbc_teradata` (ArpeTeradata, 1.4+) and `adbc_db2` (ArpeDb2, 1.7+, IBM Db2 \
with no IBM client). They only write **Parquet** \
output; use the native type (`mssql`, `pgsql`, `oraodp`, `nzsql`, `teradata`) \
for CSV, JSON and the other formats. Parallelism methods follow the native type \
(Physical on SQL Server, PostgreSQL, Oracle and Netezza; no Physical on Teradata \
or Db2). `adbc_db2` takes `host[:port]` (port 50000 by default) and requires the \
database name; it offers None, Random, DataDriven, RangeId, Ntile, Timepartition \
and DBPartition (range-partitioned Db2 for LUW tables). `adbc_teradata` and \
`adbc_db2` refuse trusted authentication.

## FastBCP backup connection types

FastBCP reads tables straight from backups, without restoring them and \
without a database server: `backup_mssql` (1.5+, SQL Server `.bak` files: full, \
striped, differential from 1.6.7, encrypted or TDE from 1.6.6) and \
`backup_rman` (1.6+, Oracle RMAN backup sets: full, incremental from 1.6.8). \
`server` is the directory of the backup files and `database` names the files \
(names or patterns separated by `;`; `backup_rman` 1.6.8+ also paths and \
`**`). No login: `backup_mssql` takes the certificate's private key file in \
`user` (and its password) for encrypted backups; `backup_rman` takes no \
credentials. Parquet output only; parallel methods **None** and **Physical** \
(resolved to Pages) only; give `schema` + `table`, or a `query` limited to \
`SELECT * | col1, col2, ... FROM [schema.]table` (`backup_rman`: \
`[[container.]owner.]table`).

FastBCP 1.7+ accepts connection types, parallel methods and the other \
enumerated values in any case. Use the spellings listed in the tool schemas: \
the MCP always emits the canonical spelling, which FastBCP < 1.7 requires.

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

## LakeXpress 0.6 notes

- **A license file is required** from 0.6.0, also for the free 30-day trial: \
without one every command except `--help` and `--version` exits 13. LakeXpress \
reads it from `LAKEXPRESS_LICENSE`, `--license` (text), `--license_file` (path), \
`~/.lakexpress/license.lic`, `%APPDATA%\\LakeXpress\\license.lic` or \
`./license.lic`. Pass a path with `license_file`, not `license`. The same file \
licenses the FastBCP bundled in `engine/` (1.6.8 in LakeXpress 0.6.5 to 0.6.8).
- **Exit codes** (0.6.2+): 40 some tables failed (resume), 41 every table \
failed, 44 no table matched, 45 no run to resume or publish, 46 publishing \
failed, 47 export and publishing both had failures, 34 FastBCP older than 1.4.0. \
From 0.6.8, 43 when the export job of a table cannot be created.
- **ADBC extraction** (0.6.3+) is the default for SQL Server, PostgreSQL and \
Teradata sources; Oracle uses it only on request. It is set in the credentials \
file, not on the command line: `"use_adbc": true` or `false` next to `ds_type` \
of the source entry. Some Parquet types differ between ADBC and the native driver.
- **Oracle to Snowflake** (0.6.5+): set `"use_adbc": true` on the source, and \
use `unbounded_number_type` on `config_create` for `NUMBER` columns without \
precision: `decimal` (default, `NUMBER(38,18)`, rounded beyond 18 decimal \
places), `varchar` (every digit kept) or `decimal_strict` (the export fails on a \
value that does not fit; needs ADBC).
- **Snowflake time zone** (0.6.6+): every Snowflake session is opened with \
`TIMEZONE = 'UTC'`, so timestamps without a time zone keep the source wall \
clock. Tables loaded by an earlier version keep their shifted values until \
they are published again.
- **Incremental with a publish target** (0.6.8+): the watermark of a table \
moves when the table is published, not when it is exported, so a run that \
was exported but not published is read again from the source. With \
`sync[export]` then `sync[publish]`, two exports with no publish in between \
overlap: publish the latest run, as `sync[publish]` does by default. \
Use 0.6.8+ for incremental `append`, `upsert` or `delete_insert` into Snowflake.

## MigratorXpress 0.8 and FastTransfer 0.18.2 notes

- **A license file is required** from MigratorXpress 0.8.0, also for the free \
30-day trial: without one every command except `--help` and `--version` exits \
13. It is read from `MIGRATORXPRESS_LICENSE`, `--license` (text), \
`--license_file` (path), `~/.migratorxpress/license.lic`, \
`%APPDATA%\\MigratorXpress\\license.lic` or `./license.lic`. A license issued \
from 2026-10-26 also licenses the FastTransfer bundled in `engine/` (0.18.1 in \
MigratorXpress 0.8.0 to 0.8.2), so `fasttransfer_dir_path` can be omitted.
- **FastTransfer 0.18.2+ has no trial mode**: without a license it exits 99. It \
reads `--license` (`license_path`), else `FastTransfer.lic` in the current \
directory, then next to the binary.
- **`with_xid`** (MigratorXpress 0.8.0+): Oracle and Netezza tables are now \
always transferred. `with_xid=true` skips a table whose max `ROWID` / \
`CREATEXID` is unchanged since the last run and whose target holds rows. \
`without_xid` is deprecated and ignored from 0.8.0.
- **Diff** (0.8.2+): a table that could not be compared fails the `diff` step, \
and the run exits 40 or 41. A table that merely differs does not fail the run.

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
