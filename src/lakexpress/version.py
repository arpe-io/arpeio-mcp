"""
Version capabilities registry for LakeXpress.

This module defines the version registry mapping LakeXpress versions
to their known capabilities (supported source databases, log databases,
storage backends, publish targets, compression types, and feature flags).
"""

from dataclasses import dataclass, replace
from typing import Dict, FrozenSet, Optional

from ..base.version_detector import ToolVersion


@dataclass(frozen=True)
class VersionCapabilities:
    """Capabilities available in a specific LakeXpress version."""

    source_databases: FrozenSet[str]
    log_databases: FrozenSet[str]
    storage_backends: FrozenSet[str]
    publish_targets: FrozenSet[str]
    compression_types: FrozenSet[str]
    commands: FrozenSet[str]
    supports_no_banner: bool = False
    supports_version_flag: bool = False
    supports_incremental: bool = False
    supports_cleanup: bool = False
    supports_quiet_fbcp: bool = False
    supports_no_progress: bool = False
    supports_resume: bool = False
    supports_license: bool = False
    supports_env_name: bool = False
    # The local sync registry (~/.lakexpress/syncs.json) was removed in v0.4.0,
    # so -a, --lxdb_auth_id, and --sync_id are mandatory on every sync call.
    # Pre-0.4 versions defaulted to True (registry-backed); 0.4+ flips this off.
    supports_sync_registry: bool = True
    # 0.6.0+ does not run without a licence file: the build-date trial is gone
    # and the free trial is a 30-day licence file. Every command except
    # --help / --version exits 13 without one.
    requires_license: bool = False
    # 0.6.5+: config create --unbounded_number_type {decimal,varchar,decimal_strict}
    # (Oracle NUMBER without precision, published to Snowflake).
    supports_unbounded_number_type: bool = False


# Static version registry: version string -> capabilities
VERSION_REGISTRY: Dict[str, VersionCapabilities] = {
    "0.2.8": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "mariadb",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "logdb_init",
                "logdb_drop",
                "logdb_truncate",
                "logdb_locks",
                "logdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
    ),
    "0.2.9": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "mariadb",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "logdb_init",
                "logdb_drop",
                "logdb_truncate",
                "logdb_locks",
                "logdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
    ),
    "0.3.0": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
    ),
    "0.4.0": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",  # legacy in 0.4.0+
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    "0.4.1": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    "0.4.2": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    "0.4.3": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    # 0.4.4: supply-chain/release-pipeline hardening (SBOM-in-zip, TruffleHog gate,
    # private full-zip ACLs, prerelease gating). No CLI surface change vs 0.4.3.
    "0.4.4": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    # 0.4.5: write-strategy routing (append/upsert/delete_insert now honoured on
    # every publish target, pre-flight rejection of unsupported combos), security
    # hardening (FastBCP argv/shell=False, mssql TLS verification by default --
    # set "trust_server_certificate": true for self-signed certs), opt-in
    # "use_adbc" credential flag for mssql/pgsql sources. No CLI surface change.
    "0.4.5": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    # 0.4.7: Linux onefile OpenSSL pairing fix + Windows build toolchain fix
    # (0.4.6 was rolled into 0.4.7 and never tagged). No CLI surface change.
    "0.4.7": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    # 0.4.8: SQL Server pymssql fallback placeholder fix; reverted identifier-
    # quoting changes. No CLI surface change.
    "0.4.8": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    # 0.4.9: trial-expiry build fix; relative --output_dir now resolved to an
    # absolute path (was written under the FastBCP engine/ dir); --version
    # prints the build date. No new flags.
    "0.4.9": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
    # 0.4.10: fixes the dev version string reported by 0.4.9 binaries.
    # No CLI surface change.
    "0.4.10": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
                "saphana",
                "teradata",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
                "redshift",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "lxdb_init",
                "lxdb_drop",
                "lxdb_truncate",
                "lxdb_locks",
                "lxdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
        supports_quiet_fbcp=True,
        supports_no_progress=True,
        supports_resume=True,
        supports_license=True,
        supports_env_name=True,
        supports_sync_registry=False,
    ),
}

# 0.5.0: exit codes reflect the cause (FastBCP not found = 30, too old = 34),
# FastBCP engine check before each export. No CLI surface change.
VERSION_REGISTRY["0.5.0"] = replace(VERSION_REGISTRY["0.4.10"])

# 0.6.0: a licence file is required (also for the free trial), one package per
# platform with FastBCP bundled in engine/, the binary returns its exit codes.
# 0.6.1: bug and security fixes.
# 0.6.2: exit codes 41 and 44-47, FastBCP 1.4.0 minimum, --log_dir honoured.
# 0.6.3: ADBC extraction by default for SQL Server, PostgreSQL and Teradata
#        ("use_adbc" in the credentials file, not on the command line).
# 0.6.4: security fix.
# No CLI surface change in any of them.
for _version in ("0.6.0", "0.6.1", "0.6.2", "0.6.3", "0.6.4"):
    VERSION_REGISTRY[_version] = replace(
        VERSION_REGISTRY["0.5.0"], requires_license=True
    )

# 0.6.5: config create --unbounded_number_type; Oracle to Snowflake column
# types changed; FastBCP 1.6.8 bundled.
VERSION_REGISTRY["0.6.5"] = replace(
    VERSION_REGISTRY["0.6.4"], supports_unbounded_number_type=True
)

# 0.6.6: Snowflake sessions opened with TIMEZONE = 'UTC'; FastBCP launched
#        through its absolute path (Windows).
# 0.6.7: cleanup fix (RUN_DELETE_FAILED on a run with exported files).
# 0.6.8: incremental watermark moved at publish, not at export, for a sync with
#        a publish target; Snowflake incremental fixes (append, upsert,
#        delete_insert); exit code 43 when an export job cannot be created.
# 0.6.9: a large table set with --incremental_table is split between the
#        FastBCP threads like a full export (Ctid, Rowid, Physloc), so on
#        Oracle it now needs SELECT_CATALOG_ROLE; a run of more than 100
#        tables with --n_jobs > 1 no longer reports exported tables as failed.
# No CLI surface change in any of them; FastBCP 1.6.8 still bundled.
for _version in ("0.6.6", "0.6.7", "0.6.8", "0.6.9"):
    VERSION_REGISTRY[_version] = replace(VERSION_REGISTRY["0.6.5"])


def check_version_compatibility(
    command: str,
    params: dict,
    capabilities: VersionCapabilities,
    detected_version: Optional[ToolVersion],
) -> list[str]:
    """Check for version-gated features and return warning strings.

    Args:
        command: The command type (e.g. "sync", "sync_export", "config_create")
        params: The parameters dict for that command
        capabilities: Resolved capabilities for the detected version
        detected_version: The detected LakeXpress version, or None

    Returns:
        List of warning strings (empty if all OK)
    """
    warnings: list[str] = []

    # quiet_fbcp requires LakeXpress 0.2.9+
    if command in ("sync", "sync_export") and params.get("quiet_fbcp"):
        if not capabilities.supports_quiet_fbcp:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"--quiet_fbcp requires LakeXpress 0.2.9+, "
                f"but detected version is {ver_str}"
            )

    # --no_progress requires LakeXpress 0.3.0+
    if params.get("no_progress"):
        if not capabilities.supports_no_progress:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"--no_progress requires LakeXpress 0.3.0+, "
                f"but detected version is {ver_str}"
            )

    # --resume requires LakeXpress 0.3.0+
    if command == "sync" and params.get("resume"):
        if not capabilities.supports_resume:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"--resume requires LakeXpress 0.3.0+, "
                f"but detected version is {ver_str}"
            )

    # --license requires LakeXpress 0.3.0+
    if params.get("license"):
        if not capabilities.supports_license:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"--license requires LakeXpress 0.3.0+, "
                f"but detected version is {ver_str}"
            )

    # --env_name requires LakeXpress 0.3.0+
    if params.get("env_name"):
        if not capabilities.supports_env_name:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"--env_name requires LakeXpress 0.3.0+, "
                f"but detected version is {ver_str}"
            )

    # --unbounded_number_type requires LakeXpress 0.6.5+
    unbounded = params.get("unbounded_number_type")
    if command == "config_create" and unbounded:
        if not capabilities.supports_unbounded_number_type:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"--unbounded_number_type requires LakeXpress 0.6.5+, "
                f"but detected version is {ver_str}"
            )

    # teradata source requires LakeXpress 0.4.0+
    if "teradata" in str(params.get("source_db_type", "")).lower():
        if "teradata" not in capabilities.source_databases:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"Teradata source requires LakeXpress 0.4.0+, "
                f"but detected version is {ver_str}"
            )

    # redshift publish target requires LakeXpress 0.4.0+
    if "redshift" in str(params.get("publish_target", "")).lower():
        if "redshift" not in capabilities.publish_targets:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"Redshift publish target requires LakeXpress 0.4.0+, "
                f"but detected version is {ver_str}"
            )

    # LakeXpress 0.4.0+ removed the local sync registry, so -a / --lxdb_auth_id /
    # --sync_id are mandatory on every sync, sync_export, sync_publish call.
    if command in ("sync", "sync_export", "sync_publish") and not capabilities.supports_sync_registry:
        missing: list[str] = []
        if not params.get("sync_id"):
            missing.append("--sync_id")
        if not params.get("log_db_auth_id"):
            missing.append("--lxdb_auth_id")
        if not params.get("auth_file"):
            missing.append("-a (auth_file)")
        if missing:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"LakeXpress {ver_str} requires {', '.join(missing)} on every "
                f"{command} call -- the local sync registry "
                f"(~/.lakexpress/syncs.json) was removed in 0.4.0."
            )

    return warnings
