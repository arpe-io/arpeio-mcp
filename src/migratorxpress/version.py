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
    supports_upgrade_migdb: bool = False
    supports_optional_db_names: bool = False
    supports_drop_tables_bool_parsing: bool = False


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
    # 0.6.33: CI/security hardening (SHA-pinned actions, SBOM-in-zip). No CLI surface change.
    "0.6.33": VersionCapabilities(
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
    # 0.6.34: deferred-start licenses (license-file field, not a CLI flag), build-date
    # trial mechanism, Windows non-TTY stdout fix. No CLI surface change.
    "0.6.34": VersionCapabilities(
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
    # 0.7.0: per-run metadata snapshots (run_id added to the PKs of the sources /
    # sourcecolumns / sourceconstraints / targets / targetcolumns catalog tables).
    # New --upgrade_migdb flag upgrades an existing tracking DB in place and exits
    # (source/target args become optional in that mode). Pre-0.7.0 tracking DBs are
    # also auto-upgraded on the first regular run.
    "0.7.0": VersionCapabilities(
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
        supports_upgrade_migdb=True,
    ),
    # 0.7.1: bug fix only (diff step on multi-run tracking DBs for MSSQL / Netezza /
    # Oracle sources). No CLI surface change.
    "0.7.1": VersionCapabilities(
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
        supports_upgrade_migdb=True,
    ),
    # 0.7.2: --source_db_name (deprecated, ignored) and --target_db_name are no longer
    # required: the databases come from the auth entries (a --target_db_name that
    # differs from the target auth entry fails with exit 20). --drop_tables_if_exists
    # now parses true/false (before, any value including "false" meant True).
    # Pre-flight checks reject a target or tracking DB other than SQL Server /
    # PostgreSQL, Netezza -> PostgreSQL, and copy_fk with a PostgreSQL target (exit 10).
    "0.7.2": VersionCapabilities(
        source_databases=frozenset(["oracle", "postgresql", "sqlserver", "netezza"]),
        target_databases=frozenset(["postgresql", "sqlserver"]),
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
        supports_upgrade_migdb=True,
        supports_optional_db_names=True,
        supports_drop_tables_bool_parsing=True,
    ),
    # 0.7.3: copy_pk / copy_ak / copy_fk now work for PostgreSQL and SQL Server sources
    # (Netezza source + constraint task exits 10), runs with failed tables exit 40/41,
    # unknown auth entry exits 10. No CLI flag change.
    "0.7.3": VersionCapabilities(
        source_databases=frozenset(["oracle", "postgresql", "sqlserver", "netezza"]),
        target_databases=frozenset(["postgresql", "sqlserver"]),
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
        supports_upgrade_migdb=True,
        supports_optional_db_names=True,
        supports_drop_tables_bool_parsing=True,
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

    # --upgrade_migdb requires MigratorXpress 0.7.0+
    if params.get("upgrade_migdb") and not capabilities.supports_upgrade_migdb:
        ver_str = str(detected_version) if detected_version else "unknown"
        warnings.append(
            f"--upgrade_migdb requires MigratorXpress 0.7.0+, "
            f"but detected version is {ver_str}"
        )

    ver_str = str(detected_version) if detected_version else "unknown"
    # A migration run (not --upgrade_migdb) that names its source/target entries
    is_run = not params.get("upgrade_migdb") and bool(
        params.get("source_db_auth_id") or params.get("target_db_auth_id")
    )

    # --source_db_name / --target_db_name are required before 0.7.2
    if is_run and not capabilities.supports_optional_db_names:
        missing = [
            f"--{name}"
            for name in ("source_db_name", "target_db_name")
            if not params.get(name)
        ]
        if missing:
            warnings.append(
                f"{' and '.join(missing)} can only be omitted with MigratorXpress "
                f"0.7.2+ (earlier versions require them), but detected version "
                f"is {ver_str}"
            )

    # --source_db_name is deprecated and ignored from 0.7.2 on
    if params.get("source_db_name") and capabilities.supports_optional_db_names:
        warnings.append(
            "--source_db_name is deprecated and ignored since MigratorXpress 0.7.2: "
            "the source database is the one in the source auth entry"
        )

    # --drop_tables_if_exists false is parsed as True before 0.7.2 (type=bool);
    # the command builder never emits "false", so no warning is needed.

    return warnings
