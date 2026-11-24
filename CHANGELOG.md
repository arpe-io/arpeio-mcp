# Changelog

## [0.3.15] - 2026-11-24

### Added

- **FastTransfer 1.1.0 support**: new registry entry `1.1.0.0`, now the latest known version. The README says "up to 1.1.0".
  - New load modes `Create` and `Replace` (`--loadmode`). `Create` creates the target table from the source when it does not exist, then loads it; when the table exists it is truncated and loaded. `Replace` is an alias of `Create`. The preview explanation says that an existing table is truncated.
  - New `infer_data_type` option (`--inferdatatype`): `fast`, `allvarchar` or `best`, how `Create` derives the column types. The description names the three environment variables `FASTTRANSFER_INFER_DEFAULT_STRING_MAXSIZE`, `FASTTRANSFER_INFER_STRING_MARGIN` and `FASTTRANSFER_INFER_SAMPLE_PERCENT`.
  - New `backup_mssql` (SQL Server `.bak` files) and `backup_rman` (Oracle RMAN backup sets) source types, read without restoring them. The target must be an `adbc_*` type; `server` is the directory of the backup files and `database` their names or patterns separated by `;`; no login is asked, and `user` / `password` are the private key file of the certificate and its password for an encrypted `backup_mssql` backup. Only the methods `None` and `Physical` (resolved to Pages) are accepted, and a `query` is limited to `SELECT * | col [, col...] FROM [schema.]table` (`TOP n` for `backup_mssql`). A backup type as a target is refused. Parallelism suggestions give `Physical` for them.
  - SQL Server `server` (`mssql`, `msbulk`, `msoledbsql`, `adbc_mssql`, and `oledb` with a SQL Server provider): the `tcp:` prefix and `host\INSTANCE:port` are refused with the form to use, as FastTransfer 1.1 does before any connection.
  - Version-compatibility warnings: a backup source, `Create` or `infer_data_type` against a binary older than 1.1; `Replace` against a binary older than 1.1, where it behaves like `Append`; `host:port` as the SQL Server `server` of a native type (`mssql`, `msbulk`, `msoledbsql`, `oledb` with a SQL Server provider) against a binary older than 1.1, which takes it for a host name. On 1.1+: `infer_data_type` with another load mode than `Create` (ignored by FastTransfer), `allvarchar` with a generic `odbc` or `oledb` source, and `allvarchar` or `best` with a query read by a physical method.
  - `fasttransfer_info`: `action="combinations"` lists the backup sources and the `Create` load mode; `action="workflow"` gains a "Load mode" section, a "Backup source" section for the two backup types (files to give, encrypted backups, restricted query, version read, index, the `FASTTRANSFER_BACKUP_RESTORE_MODE`, `FASTTRANSFER_BACKUP_RESTORE_POINT` and `FASTTRANSFER_INDEX_DIR` environment variables) and the `FASTTRANSFER_ADBC_SSLMODE` environment variable for `adbc_pgsql` and `adbc_netezza`; `action="version"` capabilities gain `supports_create_load_mode` and `supports_mssql_host_colon_port`.
  - The server instructions gain a "FastTransfer 1.1 notes" section, with the driver versions bundled in 1.1.0 (ArpeMSSQL 0.8.4, ArpePGSQL 0.5.6, ArpeOracle 0.4.8, ArpeNetezza 0.3.12, ArpeDb2 0.4.3, ArpeTeradata 0.3.8, ArpeMSBackup 0.4.1, ArpeOraBackup 0.3.1), and the parallelism table says that `Physical` resolves to Pages for the backup types in FastTransfer 1.1+ too.
  - FastTransfer 1.1 docs added to doc search, served at `/latest/` since 1.1 is the latest version of the docs site; the 1.0 pages moved to `/1.0/`. A search with a 1.1 binary (or no binary) crawls the 1.1 pages.

### Changed

- **FastTransfer workflow tips**: the `pgcopy` target tip says that the lost fractional seconds and the `bytea` values written as `System.Byte[]` are fixed in 1.1 (the `timestamptz` shift is not); the `adbc_db2` target tip says that from 1.1 each stream of a parallel load commits every 50,000 rows; the ADBC tips say that the target table must exist unless `load_mode` is `Create`.
- The FastTransfer `server` descriptions give the SQL Server forms (`host`, `host:port`, `host,port`, `host\INSTANCE`, `host\INSTANCE,port`).

### Fixed

- **FastTransfer `file_input` description**: `--fileinput` is a file holding the SQL query to run on the source, not a data file to import. Files are imported through a `duckdbstream` source and a query.
- **Execute tools (FastTransfer, FastBCP, LakeXpress, MigratorXpress) ran another command than the one previewed** (#4): the preview joined the argv with spaces and the execute tool split it again with POSIX rules, so backslashes were dropped (`D:\MSSQL\BACKUPS` became `D:MSSQLBACKUPS`, `host\SS2025` became `hostSS2025`) and a query was cut at each space. The preview now quotes `command_string` with `shlex.join`, and the execute tools take the `command` list as well as the string.
- **FastTransfer `log_level`** (#5): `fatal`, which FastTransfer refuses, is replaced by `verbose`, which it accepts (`error`, `warning`, `information`, `debug`, `verbose`).

## [0.3.14] - 2026-11-19

### Added

- **FastTransfer 1.0.0 support**: new registry entry `1.0.0.0`, now the latest known version. The README says "up to 1.0.0".
  - New `adbc_mssql`, `adbc_pgsql`, `adbc_oracle`, `adbc_netezza`, `adbc_db2` (Db2 for LUW) and `adbc_teradata` connection types, as sources and as targets, backed by the Arpe ADBC drivers bundled in FastTransfer. ADBC is needed on both ends: an `adbc_*` type with a native type is refused with FastTransfer's message, and so is `use_work_tables` with an ADBC target, and `trusted_auth` with `adbc_netezza`. `server` is `host`, `host:port` or `host,port`; a `connect_string` uses the driver's `uri=...` form.
  - New `Physical` parallel method (1.0+): the physical row location, resolved from the source connection type to Ctid (`pgsql`, `pgcopy`, `adbc_pgsql`), Rowid (`oraodp`, `adbc_oracle`), Physloc (`mssql`, `msoledbsql`, `adbc_mssql`) or NZDataSlice (`nzsql`, `nzbulk`, `nzoledb`, `adbc_netezza`); other sources are rejected, `odbc` and `oledb` with a hint. Parallelism suggestions, the auto-suggestion of `fasttransfer_preview_transfer` and the workflow tips recommend `Physical` on a 1.0+ binary and the engine-specific name on an older one.
  - `Physical`, `Ctid`, `Rowid` and `Physloc` require the source `schema` and `table`, as FastTransfer 1.0 does (Netezza needs neither). A `query` can be given next to them: it filters the table, and the command then carries `--sourcetable` and `--query`. Any other method still takes a table or a query, not both.
  - `DBPartition` accepts the `adbc_mssql`, `adbc_pgsql`, `adbc_oracle` and `adbc_teradata` sources, and `adbc_db2` (range-partitioned Db2 for LUW tables).
  - Version-compatibility warnings: an `adbc_*` type or `Physical` against a binary older than 1.0; `Ctid`, `Rowid`, `Physloc` or `NZDataSlice` against 1.0+ (deprecated, removed in 2.0; not for `Physloc` through an `odbc` source, where `Physical` is refused); `NZDataSlice` without schema and table or with a `nzbulk` / `nzoledb` source, or `Physloc` with a `msoledbsql` source, against a binary older than 1.0; and `use_work_tables` with an `msbulk` or `pgcopy` target against a binary older than 1.0, where the merge fails, nothing is loaded and the exit code is still 0.
  - `fasttransfer_info`: `action="combinations"` lists the ADBC types and the pairing rule; `action="workflow"` gives ADBC notes (license block, exact case of the target names, connection, platforms), flags a mixed pair, and adds target tips for `adbc_mssql`, `adbc_pgsql`, `adbc_db2`, `adbc_teradata`, `pgcopy` and `orabulk`, with the known limitations of 1.0.
  - The server instructions gain a "FastTransfer 1.0 notes" section, and the parallelism table says that `Physical` covers FastTransfer 1.0+.
  - FastTransfer 1.0 docs added to doc search: 1.0 is the version served at `/latest/`, and 0.18 is now crawled from `/0.18/`. The docs cache is keyed by doc version: the first search with a 1.0 binary (or no binary) crawls the FastTransfer pages again.
- **LakeXpress 0.6.9 support**: new registry entry, with no option change. The README says "up to 0.6.9".
  - A large table set with `incremental_table` is split between the `fastbcp_p` threads from 0.6.9, like a full export (`Ctid` on PostgreSQL, `Physloc` on SQL Server, `Rowid` on Oracle); up to 0.6.8 it is read by a single thread. `lakexpress_info` with `action="workflow"` says so for these three sources, and the "LakeXpress 0.6 notes" of the server instructions gain a "Large incremental tables" entry: one file per thread, and `SCHEMA.TABLE:None::1` in `fastbcp_table_config` to keep a single thread.
  - The exit codes of the server instructions say that before 0.6.9 a run of more than 100 tables with `n_jobs` above 1 exports every table but reports those beyond 100 as failed, with exit code 40.
- **LakeXpress, Oracle source**: `lakexpress_info` with `action="workflow"` and the "LakeXpress 0.6 notes" of the server instructions say that the `Rowid` split of a large Oracle table needs `SELECT_CATALOG_ROLE` (FastBCP reads `DBA_EXTENTS`, `DBA_SEGMENTS` and `DBA_OBJECTS` to build the `ROWID` ranges), what the error looks like without it, and how to do without the role (`SCHEMA.TABLE:None::1` or a key-based method in `fastbcp_table_config`). Tables below `large_table_threshold` and syncs with `fastbcp_p` 1 do not need it. Up to LakeXpress 0.6.8 an incremental table is read by a single thread and does not need the role; 0.6.9+ splits it like a full export, so an incremental sync that worked without the role fails on its large tables after the upgrade.

### Changed

- **FastTransfer parallelism suggestions**: a generic `odbc` or `oledb` source without a numeric key is no longer given `Physloc` (the engine behind the driver is unknown, and `oledb` never accepted it); a key-based method is suggested instead. `Physloc` is rejected with an `oledb` source. On a binary older than 1.0, `msoledbsql`, `nzbulk` and `nzoledb` sources get a key-based method too, as these versions accept the physical split with `mssql` and `nzsql` only.
- The `use_work_tables` description and the SQL Server target tip say that it is for FastTransfer 1.0+ only with an `msbulk` or `pgcopy` target.

### Fixed

- **FastTransfer `use_work_tables: false` emitted `--useworktables`**: the switch is now only added when the option is true.
- **`fasttransfer_preview_transfer` without a `method` failed for sources with no physical-location split** (MySQL, Teradata, SAP HANA, ClickHouse, DuckDB...): `Ntile` was auto-suggested without a key column, and the call ended in a validation error ("Method 'Ntile' requires distribute_key_column"). The auto-suggestion is now limited to the physical-location methods, and only when they can run (schema and table given, or a Netezza source on 1.0+); otherwise the command keeps method `None`.

## [0.3.13] - 2026-11-19

### Changed

- **MigratorXpress doc search has a `0.8` doc version**: a detected 0.8.x binary (or no binary) is now matched to `0.8` instead of falling back to `0.7`. The docs site has no page set per series for 0.7 and 0.8 (`/0.7/` and `/0.8/` return 404, `/latest/` documents both), so the two doc versions are crawled from `/latest/`. A 0.7.x binary keeps `0.7`, a 0.6.x binary keeps `0.6` at `/0.6/`. The docs cache is keyed by doc version: the first search after the upgrade crawls the MigratorXpress pages again.

### Fixed

- **MigratorXpress intro page missing from doc search**: the docs sitemap lists the root page of a version without a trailing slash (`/latest`), and the crawl kept only the URLs starting with `/latest/`. The page is now crawled, so `search_docs` finds its overview and `arpe_release_notes` returns its "What's new in 0.7" section again. A docs cache written by an earlier version is replaced when it expires (7 days).
- **No MigratorXpress docs with a 0.6.x binary**: the docs sitemap no longer lists the `/0.6/` pages, so nothing was indexed. When the sitemap lists no page of the selected doc version, the crawler now starts at the root page of that version and follows the links that stay under it (19 pages for 0.6).

## [0.3.12] - 2026-11-18

### Added

- **LakeXpress 0.5.0 and 0.6.0 to 0.6.8 support**: new registry entries. The sources, storage backends, publish targets and commands are those of 0.4.10.
  - New `unbounded_number_type` option on `config_create` (0.6.5+) builds `--unbounded_number_type {decimal,varchar,decimal_strict}`: the Snowflake type of an Oracle `NUMBER` declared without precision. `varchar` and `decimal_strict` are refused without a `publish_target`, as LakeXpress stops such a sync with exit code 10. A version-compatibility warning is raised against a binary older than 0.6.5.
  - New `license_file` option on every command builds `--license_file <path>`. LakeXpress 0.6.0+ does not run without a license file.
  - `lakexpress_info` reports two new feature flags with `action="version"` (license required, unbounded number type), the extraction path of each source (ADBC or native) with `action="capabilities"`, and notes on the license, ADBC extraction and Oracle to Snowflake with `action="workflow"`.
  - The server instructions gain a "LakeXpress 0.6 notes" section: license, exit codes 41, 43 (0.6.8+) and 44 to 47, ADBC extraction, Oracle to Snowflake, Snowflake sessions in UTC (0.6.6+), and the incremental watermark that moves at publish for a sync with a publish target (0.6.8+).
  - LakeXpress 0.6.6 to 0.6.8 change no option. `lakexpress_info` with `action="workflow"` notes the Snowflake session time zone and the 0.6.8 incremental fixes for a Snowflake target, and the watermark change for any publish target.
- **MigratorXpress 0.8.0, 0.8.1 and 0.8.2 support**: new registry entries. The sources, targets and tasks are those of 0.7.3.
  - New `with_xid` option (0.8.0+) builds `--with_xid`: skip an Oracle or Netezza table whose max `ROWID` / `CREATEXID` is unchanged since the last run. From 0.8.0 this skip is off by default, and `without_xid` is deprecated and ignored. Version-compatibility warnings are raised when `with_xid` is used against a binary older than 0.8.0, when `without_xid` is used against 0.8.0+, and when `with_xid` is combined with `drop_tables_if_exists=true`, which makes MigratorXpress ignore it.
  - `migratorxpress_info` reports two new feature flags with `action="version"` (`with_xid`, license required), and notes on the license and on `with_xid` (Oracle and Netezza sources) with `action="workflow"`.
  - The `license_file` description says that MigratorXpress 0.8.0+ does not run without a license file and where it is looked for. The `fasttransfer_dir_path` description says that the 0.8.0+ package bundles FastTransfer in `engine/`.
- **FastTransfer 0.18.2 support**: new registry entry `0.18.2.0`, with no option change. This version has no trial mode and exits with code 99 without a license file: `fasttransfer_info` reports a "License Required" feature flag with `action="version"`, and the `license_path` description says where the license is looked for.
- The server instructions gain a "MigratorXpress 0.8 and FastTransfer 0.18.2 notes" section: license files, `with_xid`, and the exit codes 40 and 41 of a `diff` step with tables that could not be compared (0.8.2+).

### Changed

- **Latest ADBC driver versions**: ArpeMSSQL 0.8.2, ArpePGSQL 0.5.4, ArpeOracle 0.4.7, ArpeNetezza 0.3.11, ArpeTeradata 0.3.8, ArpeDb2 0.4.2, as listed by the installer (`install.sh --list`). ArpeOracle's authentication says that the token methods (OCI IAM token, OAuth2 / Microsoft Entra ID) need a TLS connection that verifies the server certificate, as the driver requires from 0.4.6.

### Fixed

- **LakeXpress `license` was described as a path** but is emitted as `--license`, which takes the license text. The description now says so, and a path goes in the new `license_file`.
- **LakeXpress doc search** crawled doc versions that are no longer published (0.2, 0.4). The site publishes 0.3 and 0.6, the latter at `/latest/`.

## [0.3.11] - 2026-11-16

### Added

- **FastBCP 1.5, 1.6 and 1.7 support** (registry entries `1.5.0.0`, `1.6.0.0`, `1.6.6.0`, `1.6.8.0` and `1.7.0.0`):
  - New `adbc_db2` connection type (1.7+), read through the embedded ArpeDb2 ADBC driver, no IBM client needed. `server` is `host[:port]` (port 50000 by default), `database` is required, user/password or `connect_string`; trusted authentication is refused with FastBCP's message. Parquet only. Parallel methods `None`, `Random`, `DataDriven`, `RangeId`, `Ntile`, `Timepartition` and `DBPartition` (range-partitioned Db2 for LUW tables); `Physical` is rejected. Workflow tip like the `adbc_teradata` one.
  - New `backup_mssql` connection type (1.5+): tables read straight from SQL Server `.bak` files, without restoring them. `server` is the backup directory, `database` the file names or patterns separated by `;`; no login, except the certificate's private key file in `user` (and its password) for encrypted or TDE backups (1.6.6+). A `connect_string` (driver options) can go with them.
  - New `backup_rman` connection type (1.6+): tables read straight from Oracle RMAN backup sets. `server` is the directory of the pieces, `database` their names or patterns separated by `;` (paths and `**` since 1.6.8, for incremental chains); no credentials.
  - Both backup types write Parquet only, accept only the `None` and `Physical` methods (`Physical` resolves to Pages and needs `schema` and `table`), and limit `query` to `SELECT * | col1, col2, ... FROM [schema.]table` (`[[container.]owner.]table` for `backup_rman`), as FastBCP does. Parallelism suggestions recommend `Physical` for them. Workflow tips cover the file naming, the key file and the `FASTBCP_BACKUP_RESTORE_POINT`, `FASTBCP_BACKUP_RESTORE_MODE` and `FASTBCP_INDEX_DIR` variables.
  - Version-compatibility warnings when `backup_mssql`, `backup_rman` or `adbc_db2` is used against an older binary, when a `backup_mssql` key file is given to a pre-1.6.6 binary, and when a `backup_rman` `database` uses paths or `**` on a pre-1.6.8 binary.
  - FastBCP 1.5, 1.6 and 1.7 docs added to doc search; 1.7 is now the version served at `/latest/` (1.6 at `/1.6/`, 1.4 at `/1.4/`).

### Changed

- FastBCP commands are built with `--parallelmethod` and `--paralleldegree`, the names the FastBCP docs use, instead of their aliases `--method` and `--degree` (accepted by every supported FastBCP version).
- Connection types, parallel methods, output formats, Parquet compressions, bool formats, log levels and application intents are matched case-insensitively by the validators, like FastBCP 1.7, and always emitted in their canonical spelling (which older FastBCP versions require). The tool input schemas still list the canonical spellings.
- `adbc_teradata` with trusted authentication is now rejected, with FastBCP's message.
- **ArpeDb2** backs the FastBCP `adbc_db2` connection type (FastBCP 1.7+): `adbc_info`, the ADBC capabilities resource and the server instructions name it.
- Server instructions: `DBPartition` covers Db2 for LUW, `Physical` resolves to Pages on backups, and a new section describes the backup connection types. `fastbcp_info` formats lists the backup types and says which types write Parquet only.

## [0.3.10] - 2026-11-13

### Added

- **ArpeDb2 0.4.0** (IBM Db2 for LUW and Db2 for i, load name `arpedb2`) is listed as a published driver: `adbc_info` returns its load name, URI, authentication, installers and docs links, and `arpe_release_notes` accepts `arpedb2`. It has no FastBCP connection type.

### Changed

- **Latest ADBC driver versions**: ArpeMSSQL 0.8.0, ArpePGSQL 0.5.2, ArpeOracle 0.4.5, ArpeNetezza 0.3.9, ArpeTeradata 0.3.6. ArpePGSQL and ArpeOracle list Kerberos on Linux among their authentication methods.

## [0.3.9] - 2026-10-22

### Added

- **FastTransfer 0.18 support** (registry entries `0.18.0.0` and `0.18.1.0`; 0.18.1 is a security and maintenance release with no CLI changes):
  - New `DBPartition` parallel method (0.18+): one stream per native table partition, biggest first, for SQL Server (`mssql`, `msoledbsql`), PostgreSQL (`pgsql`, `pgcopy`), Oracle (`oraodp`), MySQL (`mysql`) and Teradata (`teradata`, row partitioning only) sources. It needs `schema` and `table` (no query or file input) and no key column. The validator rejects other sources, which FastTransfer would silently run as a single stream.
  - New `Timepartition` parallel method (0.18+): one stream per year, month, day or ISO week of a date column, biggest first. `distribute_key_column` is a tuple, `(date_col,year)`, `(date_col,year,month)`, `(date_col,year,month,day)` or `(date_col,year,week)`; the validator checks it and normalizes spacing and case of the time parts. Works with a table or a query.
  - Version-compatibility warnings when `DBPartition` or `Timepartition` is used against a pre-0.18 binary.
  - `FASTTRANSFER_DATEFORMAT` (0.18+, `yyyy-MM-dd` or `yyyyMMdd`, format of the `Timepartition` date bounds) is mentioned in the preview explanation and the workflow tips.
  - Workflow tips: `DBPartition` for partitioned SQL Server, PostgreSQL, Oracle, MySQL and Teradata tables (new Teradata tip), `Timepartition` for date columns, and an `msbulk` target tip on the sorted hint, no longer sent to a partitioned rowstore clustered index since 0.18 (`FASTTRANSFER_SORTEDINPUT=true` forces it).
  - FastTransfer 0.18 docs added to doc search; 0.18 is now the version served at `/latest/` (0.17 stays at `/0.17/`).

### Changed

- Server instructions list `DBPartition` and `Timepartition` for FastTransfer 0.18+ as well as FastBCP.
- **MigratorXpress docs moved to [migratorxpress-docs.arpe.io](https://migratorxpress-docs.arpe.io/)**: the old GitHub Pages site (aetperf.github.io/MigratorXpress-Documentation) is unpublished, so `search_docs` returned no MigratorXpress pages. The new site is crawled from its root `sitemap.xml`, limited to the pages of one doc version (new `DOC_URL_SCOPES` setting): a detected 0.7.x binary (or no binary) gets the 0.7 docs at `/latest/`, a 0.6.x binary the 0.6 docs at `/0.6/`. MigratorXpress doc versions are now `0.6` and `0.7` instead of one per patch release.
- `arpe_release_notes` for `migratorxpress`: the docs publish no release-notes pages (the old ones already returned 404). The tool now returns the "What's new in 0.7" section of the 0.7 docs home page when it is indexed (for `version` `0.7` or a `0.7.x` patch), and otherwise says that MigratorXpress has no published release notes and links to the docs, instead of suggesting a crawl. Both answers carry `release_notes_published: false` and `docs_url` in the structured output.

## [0.3.8] - 2026-10-22

### Added

- **Arpe ADBC drivers**, under their commercial names and latest published versions: ArpeMSSQL 0.7.6 (SQL Server, Azure SQL, Fabric), ArpePGSQL 0.4.7 (PostgreSQL), ArpeOracle 0.4.2 (Oracle, Oracle Cloud ADB), ArpeNetezza 0.3.7 (IBM Netezza, load name `arpenz`) and the new ArpeTeradata 0.3.5 (Teradata). ArpeDb2 is listed as announced.
  - New `adbc_info` tool: lists the family, or returns one driver's load name, option prefix, connection URI, authentication methods, one-line installers, licence lookup order, FastBCP `adbc_*` connection type and docs links. Accepts the commercial name, the load name or the FastBCP type.
  - New `arpeio://capabilities/adbc-drivers` resource with the same data.
  - [adbc-drivers-docs.arpe.io](https://adbc-drivers-docs.arpe.io/) added to `search_docs` (crawled from its XML sitemap).
  - `arpe_release_notes` accepts a driver load name (`arpemssql`, `arpepgsql`, `arpeoracle`, `arpenz`, `arpeteradata`) and returns that driver's "What's new" history, or the releases matching `version` (`0.3.5`, or a prefix such as `0.3`).
  - `arpe_quick_start` recognises ADBC / Arrow / dataframe use cases and recommends the drivers (or FastBCP's `adbc_*` types for file exports).
  - Server instructions gain an "Arpe ADBC drivers" section, and the FastBCP ADBC section names the driver behind each connection type.

### Changed

- FastBCP workflow tips and the `source_type` description name the driver behind each `adbc_*` type (ArpeMSSQL, ArpePGSQL, ArpeOracle, ArpeNetezza, ArpeTeradata).
- Doc search strips the zero-width anchor character Docusaurus appends to headings.

## [0.3.7] - 2026-10-22

### Added

- **FastBCP 1.4 support** (registry entry `1.4.0.0`):
  - New `adbc_teradata` connection type (1.4+), read through the embedded ArpeTeradata ADBC driver. Like the other ADBC types it writes Parquet only; parallelism follows the native `teradata` type (`DBPartition` on PPI tables, `Ntile`, `RangeId`…). Workflow tips cover the TLS / TTU logon and the refused trusted authentication.
  - New `Physical` parallel method (1.4+): splits on the physical row location, resolved by FastBCP to `Ctid` (PostgreSQL), `Rowid` (Oracle), `Physloc` (SQL Server) or `NZDataSlice` (Netezza), native and ADBC. It needs `schema` and `table` (no query) and no key column. Other sources (`odbc`, `oledb`, `mysql`, `clickhouse`, `hana`, `teradata`, `adbc_teradata`) are rejected, as FastBCP does.
  - Version-compatibility warnings when `adbc_teradata` or `Physical` is used against a pre-1.4 binary, and a deprecation warning when `Ctid`, `Rowid`, `Physloc` or `NZDataSlice` is used against 1.4+ (removed in FastBCP 2.0).
  - FastBCP 1.4 docs added to doc search; 1.4 is now the version served at `/latest/` (1.3 moved to `/1.3/`).

### Changed

- Parallelism suggestions recommend `Physical` instead of `Ctid` / `Rowid` / `Physloc` / `NZDataSlice` when the detected binary is 1.4+ (or undetected), and keep the engine-specific names for older binaries. `odbc` / `oledb` sources no longer get `Physloc` (FastBCP cannot tell the engine behind them). A numeric non-identity key on a medium table now gets `Ntile` instead of `Random`, which the FastBCP parallel-method guide advises against in production.
- The preview tool only auto-suggests a physical-location method when both `schema` and `table` are given.
- `Rowid` and `Physloc` require `schema` and `table`, matching FastBCP 1.4.
- Evaluation questions 1-4 updated for `Physical` (answer key: `Physical`, then the strategies it resolves to on Oracle, SQL Server and Netezza).

## [0.3.6] - 2026-10-13

### Added

- **FastBCP 1.3 support** (registry entry `1.3.0.0`, also covering the 1.3.1 bug-fix release):
  - New `adbc_netezza` connection type (1.3+), read through the Arpe Netezza ADBC driver. Like the other ADBC types it writes Parquet only, and parallelism follows the native `nzsql` type (`NZDataSlice`).
  - New `DBPartition` parallel method (1.3+): one chunk per native table partition, for SQL Server, PostgreSQL, Oracle, MySQL and Teradata sources (native and ADBC). It needs `schema` and `table` (no query) and no key column. The validator rejects other sources, which FastBCP would silently run unpartitioned.
  - Version-compatibility warnings when `adbc_netezza` or `DBPartition` is used against a pre-1.3 binary.
  - FastBCP 1.3 docs added to doc search; 1.3 is now the version served at `/latest/` (1.2 moved to `/1.2/`).
- **FastTransfer 0.17.1 support**: registry entry. Bug-fix release (Oracle Ntile date boundaries, 300s timeout on parallel planning queries); no CLI changes.
- **MigratorXpress 0.7.2 / 0.7.3 support**: registry entries and doc versions.
  - `source_db_name` and `target_db_name` are optional for a migration run (0.7.2+ reads them from the auth entries). A warning is emitted when they are omitted on an older binary, and `source_db_name` is flagged as deprecated on 0.7.2+.
  - 0.7.2+ capabilities list SQL Server and PostgreSQL as the only targets (MySQL and Oracle are marked pre-0.7.2). The PostgreSQL tracking DB is now listed.
  - Workflow suggestions follow the new pre-flight rules: no `copy_fk` (or `all`) for PostgreSQL targets, no constraint step for Netezza sources, and notes for unsupported source/target pairs.

### Fixed

- **MigratorXpress `drop_tables_if_exists="false"` dropped tables on pre-0.7.2 binaries**, which read any value as true. The command builder now leaves the flag out when it is `false` (the default in every version).

## [0.3.5] - 2026-09-24

### Fixed

- **Server crashed on startup with mcp 2.x** (`AttributeError: 'Server' object has no attribute 'list_prompts'`). The `mcp` dependency had no upper bound, so fresh installs (including the Hugging Face Space build) pulled mcp 2.2.0, which removed the low-level server decorator API. Pinned `mcp>=1.0.0,<2`.

## [0.3.4] - 2026-09-24

### Added

- **FastBCP 1.0 / 1.1 / 1.2 support** with the new ADBC connection types, which read through the Arpe ADBC drivers:
  - `adbc_mssql` (1.0+), `adbc_pgsql` (1.1+), `adbc_oracle` (1.2+) added to the `source.type` enum and to the capability registry (new entries `1.0.0.0`, `1.1.0.0`, `1.2.0.0`).
  - ADBC types only produce Parquet output (FastBCP rejects other formats), so the validator refuses non-parquet formats with a hint to use the native type.
  - Parallelism validation and suggestions follow the native type behind each ADBC type (`adbc_pgsql` → Ctid, `adbc_oracle` → Rowid, `adbc_mssql` → Physloc).
  - Version-compatibility warning when an ADBC type is used against a FastBCP binary that predates it.
  - Workflow tips and server instructions describe the ADBC types; `fastbcp_info` formats lists them as parquet-only.
  - FastBCP docs `1.0`, `1.1`, `1.2` added to doc search. 1.2 is served at `/latest/`, so a new `DOC_URL_VERSION_ALIASES` map rewrites the crawl URL.
- **MigratorXpress 0.7.0 / 0.7.1 support**: new registry entries and doc versions.
  - New `upgrade_migdb` option (0.7.0+) builds `--upgrade_migdb`, which upgrades an existing tracking DB in place to the per-run (`run_id`) metadata schema and exits. Only `auth_file` and `migration_db_auth_id` are needed; it cannot be combined with `task_list` or `resume`. A warning is emitted against pre-0.7.0 binaries.
  - The preview tool schema now lists the `project` run tag (0.6.30+), which was already supported but missing from the schema.
- **LakeXpress 0.4.5, 0.4.7, 0.4.8, 0.4.9, 0.4.10 support**: new registry entries (0.4.6 was never released). No CLI surface changes. The version detector handles the 0.4.9+ `LakeXpress X.Y.Z (build <date>)` output.

### Changed

- **MigratorXpress** source/target identifiers are now required by the validator rather than the JSON schema, so upgrade-only commands can omit them; normal migration runs still require them.

### Fixed

- **FastBCP enum values that the CLI rejects**: `bool_format` now uses the real values (`automatic`, `true/false`, `1/0`, `t/f` instead of `TrueFalse`/`OneZero`/`YesNo`); `parquet_compression` drops `Lzo` and adds `Brotli`/`Lz4Raw`; `log_level` adds `Verbose`.
- **FastBCP `fastbcp_info` formats** now lists the `gcs` storage target.

### Notes

- FastTransfer is unchanged: 0.17.0 is still the latest release (its ADBC work is only in a 0.17.1 release candidate).
- MigratorXpress 0.7.0 changes the tracking-DB schema; older tracking DBs are upgraded automatically on the first run, or on demand with `upgrade_migdb`.

## [0.3.3] - 2026-06-29

### Fixed

- **FastBCP command builder emitted flags the binary does not support**, producing invalid commands. The builder had been copied from FastTransfer and carried over flags that do not exist in FastBCP. Validated every emitted flag against the real FastBCP binary `--help` and source:
  - Dropped `--format` — FastBCP infers the output format from the `--fileoutput` extension; the builder now always emits `--fileoutput` so the chosen format is expressed.
  - Dropped `--storagetarget` — cloud output is driven by a cloud `--directory` URL plus `--cloudprofile`.
  - Dropped `--loadmode`, `--batchsize`, `--mapmethod` — these are FastTransfer-only flags.
  - Use the documented lowercase `--distributekeycolumn` (FastBCP rejects the camelCase form).
- **LakeXpress emitted `--env_name` on `sync`/`run`**, which the CLI rejects (`unrecognized arguments`). It is only valid on `config create` / `config list`, where it is still emitted.

### Changed

- **FastBCP tool schema** no longer advertises `load_mode`, `map_method`, `batch_size`, or `storage_target` as settable export options — they had no effect on the generated command. Removed the `LoadMode`/`MapMethod` enums; kept `StorageTarget` and `caps.storage_targets`, which back the read-only `fastbcp_info` workflow guidance and version capabilities.
- **FastTransfer command builder** normalized `--distributeKeyColumn` to the documented lowercase `--distributekeycolumn` (the binary accepts both as aliases; no behavior change). All other FastTransfer flags were verified correct.
- **MigratorXpress** verified against the real CLI — no changes needed.

### Notes

- All four command builders are now aligned with their respective real CLIs, each verified against the actual binary `--help`/source rather than by analogy. Full test suite: 595 passed.

## [0.3.2] - 2026-06-07

### Added

- **Structured tool output (`outputSchema` + `structuredContent`)**: The command-building, execution, and discovery tools now return machine-readable structured content alongside the human-readable markdown, so MCP clients can chain calls without scraping prose. Covered tools: `fastbcp_preview_export` / `fastbcp_execute_export`, `fasttransfer_preview_transfer` / `fasttransfer_execute_transfer`, `lakexpress_preview_command` / `lakexpress_execute_command`, `migratorxpress_preview_command` / `migratorxpress_execute_command`, `search_docs`, `arpe_get_status`, `arpe_quick_start`, and `arpe_release_notes`.
  - Preview tools return the built `command` (argv), `command_string`, masked `command_display`, `explanation`, and version `warnings` — the exact command can be handed straight to the matching execute tool.
  - Execute tools return `success`, `return_code`, `stdout`, `stderr`, and parsed `diagnostics`.
  - Every schema uses a permissive `status` discriminator (`ok`/`error`) so success, validation-error, and execution-error paths all conform.
- **Evaluation suite** (`evaluations/arpeio_eval.xml`): 10 independent, read-only, verifiable questions exercising tool selection, parallelism recommendations, capability lookups, and version gating, plus a guard test (`tests/test_evaluations.py`) that re-derives every answer from the registries so the answer key cannot silently drift.
- **MigratorXpress 0.6.33 / 0.6.34 support** and **LakeXpress 0.4.4 support**: New capability-registry entries (and doc-search version lists) so version detection resolves correctly. All three are supply-chain / CI / trial-mechanism releases with no new user-facing CLI surface.

### Changed

- **Shared structured-output helpers** (`src/base/structured.py`): `make_output_schema()` and `respond()` centralize the `(content, structured)` return contract; the server's global tool dispatcher now returns structured error content so `outputSchema`-bearing tools stay schema-valid even on unexpected errors.
- **Sharper tool guidance**: workflow/next-step text and parameter descriptions no longer reference tools removed in the 0.3.0 consolidation (e.g. `*_suggest_parallelism`, `*_list_formats`, `*_validate_connection`); they now point at the consolidated `*_info` actions and the preview tools, preventing failed tool calls.

### Notes

- MigratorXpress 0.6.33/0.6.34 and LakeXpress 0.4.4 add no new sources, targets, commands, parameters, or feature flags; only registry entries were added.

## [0.3.1] - 2026-05-15

### Added

- **FastBCP 0.32.4.0 support**: New version entry in the FastBCP capability registry. 0.32.x is an internal upgrade to Parquet.Net v6, an internal `rowgroupsize` setter, and S3-init retry/backoff — capability surface (sources, formats, parallelism methods, storage targets) is unchanged from 0.31.
- **FastTransfer 0.17.0.0 support**: New version entry. 0.17.0 is a `Program.Main()` refactor (six helper blocks extracted to `src/main/MainHelpers.*.cs`), DuckDB target perf improvements, and C# 14 / .NET 7 drop — capability surface unchanged from 0.16.
- **LakeXpress 0.4.1 / 0.4.2 / 0.4.3 support**: Three new version entries. All three are Teradata→Redshift schema-mapper fixes (NUMBER precision, Unicode CHAR, VARBYTE mapping, intervals/periods/XML/JSON/TIMETZ/TIMESTAMPTZ) — capability surface unchanged from 0.4.0.
- **MigratorXpress 0.6.27–0.6.32 support**: Six new version entries.
  - **0.6.30** introduces the `--project` CLI tag — now exposed as a new `project` parameter on `migratorxpress_preview_command` / `migratorxpress_execute_command`. The tag is validated client-side (`^[A-Za-z0-9_-]{1,64}$`) before the command is built.
  - **0.6.32** introduces PostgreSQL as a tracking-DB backend (`ds_type: postgres` on the migration-DB auth entry) — added to the `migration_db_types` capability set.
- **`log_db_auth_id` field on LakeXpress sync models**: `SyncParams`, `SyncExportParams`, and `SyncPublishParams` now accept `log_db_auth_id`, which is emitted as `--lxdb_auth_id <value>` on every `sync` / `sync[export]` / `sync[publish]` command. Required on LakeXpress 0.4.0+ (the local sync registry `~/.lakexpress/syncs.json` was removed in 0.4.0).

### Changed

- **LakeXpress `check_version_compatibility`**: Now emits a warning when a `sync`, `sync[export]`, or `sync[publish]` command is built against a 0.4.0+ binary and any of `-a` / `--lxdb_auth_id` / `--sync_id` is missing. A new `supports_sync_registry` capability flag (default `True`, set `False` on 0.4.x entries) tracks this gate.
- **MigratorXpress `check_version_compatibility`**: Now emits a warning when `--project` is used against a pre-0.6.30 binary, and when `migration_db_type='postgres'` is requested against a pre-0.6.32 binary.

### Notes

- FastBCP 0.32 and FastTransfer 0.17 are internal refactors with no new user-facing CLI surface; only the registry entries were added so version-detection resolves correctly.
- LakeXpress 0.4.1–0.4.3 are Teradata→Redshift mapping fixes and do not introduce new sources, publish targets, commands, or feature flags.

## [0.3.0] - 2026-04-16

### Changed

- **Tool consolidation (32 → 17 tools)**: Merged read-only advisory tools (`list_formats`, `suggest_parallelism`, `suggest_workflow`, `get_version`) into one `{product}_info` tool per product with an `action` enum parameter. Removed redundant `validate_connection` tools from FastBCP and FastTransfer (preview already validates parameters). Unified 4 per-product `*_release_notes` tools into a single `arpe_release_notes` tool with a `product` parameter.
- **Auto-parallelism in preview tools**: `fastbcp_preview_export` and `fasttransfer_preview_transfer` now auto-suggest a parallelism method when none is specified, based on the source database type. The suggestion and rationale are included in the preview output. Users can override by specifying explicit `method` and `degree`.
- **Streamlined workflow**: Default workflow reduced from 4 steps (validate → suggest → preview → execute) to 2-3 steps (optional info → preview → execute). MCP instructions updated accordingly.

### Added

- **MCP Prompts**: 5 guided workflow prompts (`export-table`, `transfer-data`, `lakehouse-pipeline`, `migrate-database`, `troubleshoot`) that appear as conversation starters in Claude Desktop, Cursor, and other clients supporting MCP prompts.
- **MCP Resources**: 4 static capability resources (`fastbcp-formats`, `fasttransfer-combinations`, `lakexpress-capabilities`, `migratorxpress-capabilities`) exposed as JSON. Clients that support MCP resources can prefetch these without a tool call.

### Fixed

- `server.json` version now stays in sync with `pyproject.toml`.

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
