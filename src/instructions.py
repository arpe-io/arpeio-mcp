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
ArpeTeradata, ArpeDb2), the native drivers behind the `adbc_*` \
connection types of FastBCP and FastTransfer.

## Tool Selection

Choose the right tool based on what the user wants to accomplish:

| Goal | Tool | Notes |
|------|------|-------|
| Export DB table/query to files (CSV, Parquet, JSON, etc.) | **FastBCP** | Outputs to local disk or cloud storage (S3, Azure, GCS, OneLake) |
| Transfer data from one database to another | **FastTransfer** | Direct DB-to-DB, no intermediate files; 1.1+ can create the target table and read SQL Server / Oracle RMAN backup files |
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
| **DBPartition** | FastBCP 1.3+, FastTransfer 0.18+; one chunk per native table partition (SQL Server, PostgreSQL, Oracle, MySQL, Teradata; FastBCP 1.7+ and FastTransfer 1.0+ also Db2 for LUW through `adbc_db2`); needs schema + table | No |
| **Physical** | FastBCP 1.4+, FastTransfer 1.0+; physical row location, resolved to PhysLoc / Ctid / Rowid / NZDataSlice from the connection type (SQL Server, PostgreSQL, Oracle, Netezza only), and to Pages for the backup types `backup_mssql` / `backup_rman` (FastBCP 1.5+ / 1.6+, FastTransfer 1.1+); needs schema + table (not on Netezza in FastTransfer, where a query can also be added to filter the table) | No |

Default recommendations:
- Numeric primary key exists → **Ntile**
- Date/timestamp key exists → **DataDriven**
- SQL Server, no key → **PhysLoc**
- PostgreSQL, no key → **Ctid**
- Oracle, no key → **Rowid**
- FastBCP 1.4+ and FastTransfer 1.0+: use **Physical** instead of PhysLoc / \
Ctid / Rowid / NZDataSlice, which are deprecated there (removed in 2.0). \
FastBCP < 1.4 and FastTransfer < 1.0 only know the engine-specific names.
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
licenses the FastBCP bundled in `engine/` (1.6.8 in LakeXpress 0.6.5 to 0.6.9).
- **Exit codes** (0.6.2+): 40 some tables failed (resume), 41 every table \
failed, 44 no table matched, 45 no run to resume or publish, 46 publishing \
failed, 47 export and publishing both had failures, 34 FastBCP older than 1.4.0. \
From 0.6.8, 43 when the export job of a table cannot be created. Before 0.6.9, \
a run of more than 100 tables with `n_jobs` above 1 exports every table but \
reports those beyond 100 as failed and exits 40: upgrade to 0.6.9.
- **ADBC extraction** (0.6.3+) is the default for SQL Server, PostgreSQL and \
Teradata sources; Oracle uses it only on request. It is set in the credentials \
file, not on the command line: `"use_adbc": true` or `false` next to `ds_type` \
of the source entry. Some Parquet types differ between ADBC and the native driver.
- **Oracle to Snowflake** (0.6.5+): set `"use_adbc": true` on the source, and \
use `unbounded_number_type` on `config_create` for `NUMBER` columns without \
precision: `decimal` (default, `NUMBER(38,18)`, rounded beyond 18 decimal \
places), `varchar` (every digit kept) or `decimal_strict` (the export fails on a \
value that does not fit; needs ADBC).
- **Oracle source, large tables**: a table above `large_table_threshold` is \
split with `Rowid`, which needs `SELECT_CATALOG_ROLE` on the source user: \
FastBCP reads the extents of the table from `DBA_EXTENTS`, `DBA_SEGMENTS` and \
`DBA_OBJECTS` to build the `ROWID` ranges (`SELECT` on these three views is \
enough). Without it the table fails with "The user must have \
SELECT_CATALOG_ROLE" (`ORA-00942`). To do without the role, set \
`fastbcp_table_config` to `SCHEMA.TABLE:None::1` or to a key-based method. \
Tables below the threshold and syncs with `fastbcp_p` 1 do not need the role. \
Up to 0.6.8 an incremental table is read by a single thread unless a key-based \
method is set, and does not need the role; 0.6.9+ splits it with `Rowid` like \
a full export, so an incremental sync that worked without the role fails on \
its large tables after the upgrade: grant the role or set `None::1` first.
- **Large incremental tables** (0.6.9+): a table above \
`large_table_threshold` set with `incremental_table` is split between the \
`fastbcp_p` threads like a full export (`Ctid` on PostgreSQL, `Physloc` on \
SQL Server, `Rowid` on Oracle), for the baseline and for the increments. Up \
to 0.6.8 it is read by a single thread unless a key-based method is set. A \
run of such a table holds one file per thread, and some can be empty. Set \
`fastbcp_table_config` to `SCHEMA.TABLE:None::1` to keep a single thread for \
a table.
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

## FastTransfer 1.0 notes

- **ADBC connection types** (1.0+): `adbc_mssql`, `adbc_pgsql`, `adbc_oracle`, \
`adbc_netezza`, `adbc_db2` (Db2 for LUW) and `adbc_teradata` are source and \
target types backed by the Arpe ADBC drivers bundled in FastTransfer \
(ArpeMSSQL 0.8.2, ArpePGSQL 0.5.4, ArpeOracle 0.4.7, ArpeNetezza 0.3.11, \
ArpeDb2 0.4.2, ArpeTeradata 0.3.8 in 1.0.0). ADBC is needed on both ends: an \
`adbc_*` source with an `adbc_*` target, never mixed with a native type (the \
run is refused before anything is truncated). Windows x64 and Linux x64 only. \
So Db2 is a target only from an `adbc_*` source, Db2 to a native-only target \
(MySQL...) goes through `odbc`, and file imports (`duckdbstream`) use a native \
target.
- **When to use them**: when the user asks for ADBC, when Db2 is one end and \
the other end is one of the six engines, or when the table is known to hit a \
limitation of the native pair (below, e.g. the column types named there). \
Otherwise keep the native types, and when both ends are ADBC engines and the \
native pair has a limitation, mention it and the `adbc_*` alternative, which \
needs the ADBC driver block in the license.
- **ADBC rules**: the license file must also hold the ARPE ADBC DRIVER block \
(exit code 99 without it); the target table must exist (unless `load_mode` \
is `Create`, 1.1+), and the target schema \
and table are matched in their exact stored case (`ORDERS` on Oracle, Db2 and \
Teradata; `orders` on PostgreSQL); `use_work_tables` is refused and \
`batch_size` has no effect; trusted authentication is not available on \
Netezza; a `connect_string` is in the driver's vocabulary (`uri=...`), not \
ADO.NET keywords. Time zone offsets arrive as the same instant in UTC.
- **Physical** (1.0+) replaces Ctid / Rowid / Physloc / NZDataSlice, which log \
a deprecation warning. On SQL Server it now requires schema and table (a query \
alone is refused; a query next to them filters the table) and should only be \
used on data that is not being written. Through an `odbc` source to SQL Server \
only the deprecated `Physloc` works.
- **`use_work_tables`**: before 1.0, with an `msbulk` or `pgcopy` target, the \
merge fails, nothing is loaded and the exit code is still 0. Do not use it \
there. From 1.0 a failed merge fails the run.
- **Ntile / DataDriven**: before 1.0, `Ntile` on a date or timestamp key and \
`Ntile` / `DataDriven` on a key holding NULLs left rows out without an error; \
fixed in 1.0. `RangeId` and `Random` never transfer the rows whose key is NULL.
- **Known limitations in 1.0**: a native `pgcopy` target fed from a source \
that is not `pgcopy` writes some values wrong without an error (fractional \
seconds of `time` / `timestamp`, `timestamptz` shifted, `bytea` as text; the \
fractional seconds and `bytea` are fixed in 1.1, not the time zone shift): use \
`pgcopy` to `pgcopy` or `adbc_*` on both ends for such columns. A native \
PostgreSQL source into `orabulk` fails on a `date` column: use `adbc_pgsql` \
to `adbc_oracle`. An Oracle `--query` with `FROM t PARTITION (P1)` now fails \
at parse time: filter on the partition key instead.
- **Exit codes**: 99 = no or invalid license (or no ADBC driver block for an \
`adbc_*` run), 100 = license expired.

## FastTransfer 1.1 notes

- **`load_mode` Create** (1.1+): creates the target table from the source \
(table, query or files) when it does not exist, then loads it; when it exists \
under the name exactly as typed it is truncated and loaded, its structure \
kept; a table that exists only in another case is an error. The created table \
has the source's column names and order, every column nullable, no key, index \
or constraint; the target schema must exist. `Replace` is an alias of `Create` \
(before 1.1 it behaved like `Append`). Use `Truncate` or `Append` when the \
table is known to exist, `Create` when it may not. Not tested yet with \
ClickHouse, Netezza or SAP HANA, with Teradata as a target, or with a backup \
source.
- **`infer_data_type`** (`--inferdatatype`, only with `Create`; ignored with a \
warning otherwise): `fast` (default) uses the source metadata, no row is read, \
and a text column with no declared length gets 256 characters; `allvarchar` \
creates every column as text (not from a generic `odbc` / `oledb` source); \
`best` profiles Oracle sources and files read through DuckDB to find the \
strictest type holding every value (the `fast` types for the other sources), \
and a row that does not fit later fails the load. Between two ends of the \
same engine the source types are copied unchanged, except with `allvarchar`. \
`allvarchar` and `best` rewrite the source query: with `Physical`, give schema \
and table rather than a query. Environment variables, set before the run: \
`FASTTRANSFER_INFER_DEFAULT_STRING_MAXSIZE` (default 256, or `max`), \
`FASTTRANSFER_INFER_STRING_MARGIN` (characters, or a factor `2x` to `10x`, for \
`best`), `FASTTRANSFER_INFER_SAMPLE_PERCENT` (above 0 up to 100, for `best`).
- **Backup sources** (1.1+): `backup_mssql` (SQL Server `.bak`: full and \
differential, compressed, striped, encrypted or TDE ones included) and \
`backup_rman` (Oracle RMAN backup sets: full, level 0, incremental level 1 to \
4) are read without restoring them and without a database. The target must be \
an `adbc_*` type; Windows x64 and Linux x64 only. `server` is the directory of \
the backup files and `database` their names or patterns separated by `;` \
(`backup_mssql`: the full backup, its differentials and every stripe; \
`backup_rman`: `*`, `?`, `**` and directories, e.g. `l0;l1/**`). No login: for \
an encrypted `backup_mssql` backup, `user` is the certificate's private key \
file and `password` its password. Source: `schema` + `table`, or a `query` \
limited to `SELECT * | col [, col...] FROM [schema.]table` (`backup_mssql` \
also `TOP n`), no WHERE, no join. Parallel methods **None** and **Physical** \
(resolved to Pages) only. The latest version the files hold is read, and a \
backup that cannot be used stops the run with the target untouched; \
`FASTTRANSFER_BACKUP_RESTORE_MODE=best_effort` reads the newest usable \
version, `FASTTRANSFER_BACKUP_RESTORE_POINT=full` the last full (or level 0) \
backup only. The first read indexes each backup (minutes on a large \
compressed one); `FASTTRANSFER_INDEX_DIR` sets the index directory. Driver \
options (`adbc.arpemsbackup.*`, `adbc.arpeorabackup.*`) go in a \
`connect_string`, given instead of `server` / `user` / `password`. Unlike \
FastBCP, the output is a database table, not Parquet files.
- **Bundled drivers in 1.1.0**: ArpeMSSQL 0.8.4, ArpePGSQL 0.5.6, ArpeOracle \
0.4.8, ArpeNetezza 0.3.12, ArpeDb2 0.4.3, ArpeTeradata 0.3.8, ArpeMSBackup \
0.4.1, ArpeOraBackup 0.3.1.
- **TLS on `adbc_pgsql` / `adbc_netezza`** (1.1+): both negotiate TLS by \
default, which fails against a server without TLS. The \
`FASTTRANSFER_ADBC_SSLMODE` environment variable (`disable`, `prefer`, \
`require`, `verify-ca`, `verify-full`) sets the driver's `sslmode` on both \
ends; an `sslmode` already in a `connect_string` wins; any other value stops \
the run. It does not apply to `adbc_mssql` or `adbc_oracle`.
- **SQL Server `server`** (`mssql`, `msbulk`, `msoledbsql`, `oledb` with a SQL \
Server provider, `adbc_mssql`): `host`, `host:port`, `host,port`, \
`host\\INSTANCE` or `host\\INSTANCE,port`. From 1.1 `host:port` works with the \
native types too (before, write `host,port`), and the `tcp:` prefix and \
`host\\INSTANCE:port` are refused before any connection.
- **Fixed in 1.1**: `Ntile` on an Oracle or MySQL source (it failed with a \
query and split the rows wrongly with a table: use another method before \
1.1); a query with `QUALIFY`, a named `WINDOW` or `CONNECT BY` lost that \
clause under a parallel method; an `adbc_db2` target now commits every 50,000 \
rows on each stream of a parallel load.

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
