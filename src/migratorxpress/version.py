"""
Version capabilities registry for MigratorXpress.

This module defines the version capabilities dataclass and the static
registry mapping known MigratorXpress versions to their capabilities.
Version detection is handled by the base BaseVersionDetector.
"""

from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional


@dataclass(frozen=True)
class VersionCapabilities:
    """Capabilities available in a specific MigratorXpress version."""

    source_databases: FrozenSet[str]
    target_databases: FrozenSet[str]
    migration_db_types: FrozenSet[str]
    tasks: FrozenSet[str]
    fk_modes: FrozenSet[str]
    migration_db_modes: FrozenSet[str]
    load_modes: FrozenSet[str]
    supports_no_banner: bool = False
    supports_version_flag: bool = False
    supports_fasttransfer: bool = False
    supports_license: bool = False
    supports_no_progress: bool = False
    supports_quiet_ft: bool = False
    supports_log_dir: bool = False
    supports_project: bool = False
    supports_postgres_migration_db: bool = False


# Static version registry: version string -> capabilities
VERSION_REGISTRY: Dict[str, VersionCapabilities] = {
    "0.6.24": VersionCapabilities(
        source_databases=frozenset(
            [
                "oracle",
                "postgresql",
                "sqlserver",
                "netezza",
            ]
        ),
        target_databases=frozenset(
            [
                "postgresql",
                "sqlserver",
            ]
        ),
        migration_db_types=frozenset(
            [
                "sqlserver",
            ]
        ),
        tasks=frozenset(
            [
                "translate",
                "create",
                "transfer",
                "diff",
                "copy_pk",
                "copy_ak",
                "copy_fk",
                "all",
            ]
        ),
        fk_modes=frozenset(
            [
                "trusted",
                "untrusted",
                "disabled",
            ]
        ),
        migration_db_modes=frozenset(
            [
                "preserve",
                "truncate",
                "drop",
            ]
        ),
        load_modes=frozenset(
            [
                "truncate",
                "append",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_fasttransfer=True,
        supports_license=True,
    ),
    "0.6.26": VersionCapabilities(
        source_databases=frozenset(
            [
                "oracle",
                "postgresql",
                "sqlserver",
                "netezza",
            ]
        ),
        target_databases=frozenset(
            [
                "postgresql",
                "sqlserver",
                "mysql",
                "oracle",
            ]
        ),
        migration_db_types=frozenset(
            [
                "sqlserver",
            ]
        ),
        tasks=frozenset(
            [
                "translate",
                "create",
                "transfer",
                "diff",
                "copy_pk",
                "copy_ak",
                "copy_fk",
                "all",
            ]
        ),
        fk_modes=frozenset(
            [
                "trusted",
                "untrusted",
                "disabled",
            ]
        ),
        migration_db_modes=frozenset(
            [
                "preserve",
                "truncate",
                "drop",
            ]
        ),
        load_modes=frozenset(
            [
                "truncate",
                "append",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_fasttransfer=True,
        supports_license=True,
        supports_no_progress=True,
        supports_quiet_ft=True,
        supports_log_dir=True,
    ),
    "0.6.27": VersionCapabilities(
        source_databases=frozenset(["oracle", "postgresql", "sqlserver", "netezza"]),
        target_databases=frozenset(["postgresql", "sqlserver", "mysql", "oracle"]),
        migration_db_types=frozenset(["sqlserver"]),
        tasks=frozenset(
            ["translate", "create", "transfer", "diff", "copy_pk", "copy_ak", "copy_fk", "all"]
        ),
        fk_modes=frozenset(["trusted", "untrusted", "disabled"]),
        migration_db_modes=frozenset(["preserve", "truncate", "drop"]),
        load_modes=frozenset(["truncate", "append"]),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_fasttransfer=True,
        supports_license=True,
        supports_no_progress=True,
        supports_quiet_ft=True,
        supports_log_dir=True,
    ),
    "0.6.28": VersionCapabilities(
        source_databases=frozenset(["oracle", "postgresql", "sqlserver", "netezza"]),
        target_databases=frozenset(["postgresql", "sqlserver", "mysql", "oracle"]),
        migration_db_types=frozenset(["sqlserver"]),
        tasks=frozenset(
            ["translate", "create", "transfer", "diff", "copy_pk", "copy_ak", "copy_fk", "all"]
        ),
        fk_modes=frozenset(["trusted", "untrusted", "disabled"]),
        migration_db_modes=frozenset(["preserve", "truncate", "drop"]),
        load_modes=frozenset(["truncate", "append"]),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_fasttransfer=True,
        supports_license=True,
        supports_no_progress=True,
        supports_quiet_ft=True,
        supports_log_dir=True,
    ),
    "0.6.29": VersionCapabilities(
        source_databases=frozenset(["oracle", "postgresql", "sqlserver", "netezza"]),
        target_databases=frozenset(["postgresql", "sqlserver", "mysql", "oracle"]),
        migration_db_types=frozenset(["sqlserver"]),
        tasks=frozenset(
            ["translate", "create", "transfer", "diff", "copy_pk", "copy_ak", "copy_fk", "all"]
        ),
        fk_modes=frozenset(["trusted", "untrusted", "disabled"]),
        migration_db_modes=frozenset(["preserve", "truncate", "drop"]),
        load_modes=frozenset(["truncate", "append"]),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_fasttransfer=True,
        supports_license=True,
        supports_no_progress=True,
        supports_quiet_ft=True,
        supports_log_dir=True,
    ),
    "0.6.30": VersionCapabilities(
        source_databases=frozenset(["oracle", "postgresql", "sqlserver", "netezza"]),
        target_databases=frozenset(["postgresql", "sqlserver", "mysql", "oracle"]),
        migration_db_types=frozenset(["sqlserver"]),
        tasks=frozenset(
            ["translate", "create", "transfer", "diff", "copy_pk", "copy_ak", "copy_fk", "all"]
        ),
        fk_modes=frozenset(["trusted", "untrusted", "disabled"]),
        migration_db_modes=frozenset(["preserve", "truncate", "drop"]),
        load_modes=frozenset(["truncate", "append"]),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_fasttransfer=True,
        supports_license=True,
        supports_no_progress=True,
        supports_quiet_ft=True,
        supports_log_dir=True,
        supports_project=True,
    ),
    "0.6.31": VersionCapabilities(
        source_databases=frozenset(["oracle", "postgresql", "sqlserver", "netezza"]),
        target_databases=frozenset(["postgresql", "sqlserver", "mysql", "oracle"]),
        migration_db_types=frozenset(["sqlserver"]),
        tasks=frozenset(
            ["translate", "create", "transfer", "diff", "copy_pk", "copy_ak", "copy_fk", "all"]
        ),
        fk_modes=frozenset(["trusted", "untrusted", "disabled"]),
        migration_db_modes=frozenset(["preserve", "truncate", "drop"]),
        load_modes=frozenset(["truncate", "append"]),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_fasttransfer=True,
        supports_license=True,
        supports_no_progress=True,
        supports_quiet_ft=True,
        supports_log_dir=True,
        supports_project=True,
    ),
    "0.6.32": VersionCapabilities(
        source_databases=frozenset(["oracle", "postgresql", "sqlserver", "netezza"]),
        target_databases=frozenset(["postgresql", "sqlserver", "mysql", "oracle"]),
        migration_db_types=frozenset(["sqlserver", "postgres"]),
        tasks=frozenset(
            ["translate", "create", "transfer", "diff", "copy_pk", "copy_ak", "copy_fk", "all"]
        ),
        fk_modes=frozenset(["trusted", "untrusted", "disabled"]),
        migration_db_modes=frozenset(["preserve", "truncate", "drop"]),
        load_modes=frozenset(["truncate", "append"]),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_fasttransfer=True,
        supports_license=True,
        supports_no_progress=True,
        supports_quiet_ft=True,
        supports_log_dir=True,
        supports_project=True,
        supports_postgres_migration_db=True,
    ),
}


def check_version_compatibility(
    params: dict,
    capabilities: VersionCapabilities,
    detected_version: Optional[object],
) -> list[str]:
    """Check for version-gated features and return warning strings.

    Args:
        params: The parameters dict for the command
        capabilities: Resolved capabilities for the detected version
        detected_version: The detected version, or None

    Returns:
        List of warning strings (empty if all OK)
    """
    warnings: list[str] = []

    # --project requires MigratorXpress 0.6.30+
    if params.get("project") and not capabilities.supports_project:
        ver_str = str(detected_version) if detected_version else "unknown"
        warnings.append(
            f"--project requires MigratorXpress 0.6.30+, "
            f"but detected version is {ver_str}"
        )

    # postgres tracking-DB backend requires MigratorXpress 0.6.32+
    if str(params.get("migration_db_type", "")).lower() == "postgres":
        if not capabilities.supports_postgres_migration_db:
            ver_str = str(detected_version) if detected_version else "unknown"
            warnings.append(
                f"PostgreSQL tracking DB (ds_type='postgres') requires "
                f"MigratorXpress 0.6.32+, but detected version is {ver_str}"
            )

    return warnings
