"""
Version capabilities registry for LakeXpress.

This module defines the version registry mapping LakeXpress versions
to their known capabilities (supported source databases, log databases,
storage backends, publish targets, compression types, and feature flags).
"""

from dataclasses import dataclass
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
}


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

    return warnings
