"""
MigratorXpress command builder extending the base Arpe.io command builder.

This module provides the MigratorXpress-specific command building,
version detection, and helper functions.
"""

import logging
from typing import Any, Dict, List

from src.base.command_builder import BaseCommandBuilder, ArpeToolError
from src.base.version_detector import BaseVersionDetector

from .validators import MigrationParams
from .version import VERSION_REGISTRY, VersionCapabilities


logger = logging.getLogger(__name__)


class MigratorXpressError(ArpeToolError):
    """Exception for MigratorXpress operations."""
    pass


class CommandBuilder(BaseCommandBuilder):
    """Builds and executes MigratorXpress commands."""

    PRODUCT_NAME = "MigratorXpress"
    DOWNLOAD_URL = "https://arpe.io"
    DEFAULT_TIMEOUT = 3600
    SENSITIVE_FLAGS = {"--license"}

    def _create_version_detector(self) -> BaseVersionDetector:
        return BaseVersionDetector(
            binary_path=str(self.binary_path),
            version_registry=VERSION_REGISTRY,
            version_pattern=r"migratorxpress\s+(\d+\.\d+\.\d+)",
            product_name=self.PRODUCT_NAME,
        )

    def _get_version_capabilities(self, caps: VersionCapabilities) -> dict:
        return {
            "source_databases": sorted(caps.source_databases),
            "target_databases": sorted(caps.target_databases),
            "migration_db_types": sorted(caps.migration_db_types),
            "tasks": sorted(caps.tasks),
            "fk_modes": sorted(caps.fk_modes),
            "migration_db_modes": sorted(caps.migration_db_modes),
            "load_modes": sorted(caps.load_modes),
            "supports_no_banner": caps.supports_no_banner,
            "supports_version_flag": caps.supports_version_flag,
            "supports_fasttransfer": caps.supports_fasttransfer,
            "supports_license": caps.supports_license,
            "supports_no_progress": caps.supports_no_progress,
            "supports_quiet_ft": caps.supports_quiet_ft,
            "supports_log_dir": caps.supports_log_dir,
        }

    def build_command(self, params: MigrationParams) -> List[str]:
        """
        Build a MigratorXpress command from validated parameters.

        MigratorXpress is a single-command CLI -- no subcommands.

        Args:
            params: Validated migration parameters

        Returns:
            Command as list of strings (suitable for subprocess)
        """
        cmd = [str(self.binary_path)]

        # Auth file (required)
        cmd.extend(["-a", params.auth_file])

        # Required database identifiers
        cmd.extend(["--source_db_auth_id", params.source_db_auth_id])
        cmd.extend(["--source_db_name", params.source_db_name])
        cmd.extend(["--target_db_auth_id", params.target_db_auth_id])
        cmd.extend(["--target_db_name", params.target_db_name])
        cmd.extend(["--migration_db_auth_id", params.migration_db_auth_id])

        # Schema names
        if params.source_schema_name:
            cmd.extend(["--source_schema_name", params.source_schema_name])
        if params.target_schema_name:
            cmd.extend(["--target_schema_name", params.target_schema_name])

        # Task list (nargs='+')
        if params.task_list:
            cmd.append("--task_list")
            cmd.extend(params.task_list)

        # Resume
        if params.resume:
            cmd.extend(["-r", params.resume])

        # FastTransfer
        if params.fasttransfer_dir_path:
            cmd.extend(["--fasttransfer_dir_path", params.fasttransfer_dir_path])
        if params.fasttransfer_p is not None:
            cmd.extend(["-p", str(params.fasttransfer_p)])
        if params.ft_large_table_th is not None:
            cmd.extend(["--ft_large_table_th", str(params.ft_large_table_th)])

        # Parallelism
        if params.n_jobs is not None:
            cmd.extend(["--n_jobs", str(params.n_jobs)])

        # Index thresholds
        if params.cci_threshold is not None:
            cmd.extend(["--cci_threshold", str(params.cci_threshold)])
        if params.aci_threshold is not None:
            cmd.extend(["--aci_threshold", str(params.aci_threshold)])

        # Migration DB mode
        if params.migration_db_mode:
            cmd.extend(["--migration_db_mode", params.migration_db_mode.value])

        # String-boolean parameters
        if params.compute_nbrows is not None:
            cmd.extend(["--compute_nbrows", params.compute_nbrows])
        if params.drop_tables_if_exists is not None:
            cmd.extend(["--drop_tables_if_exists", params.drop_tables_if_exists])

        # Load mode
        if params.load_mode:
            cmd.extend(["--load_mode", params.load_mode.value])

        # Filtering
        if params.include_tables:
            cmd.extend(["-i", params.include_tables])
        if params.exclude_tables:
            cmd.extend(["-e", params.exclude_tables])
        if params.min_rows is not None:
            cmd.extend(["-min", str(params.min_rows)])
        if params.max_rows is not None:
            cmd.extend(["-max", str(params.max_rows)])

        # Oracle-specific lists (nargs='+')
        if params.forced_int_id_prefixes:
            cmd.append("--forced_int_id_prefixes")
            cmd.extend(params.forced_int_id_prefixes)
        if params.forced_int_id_suffixes:
            cmd.append("--forced_int_id_suffixes")
            cmd.extend(params.forced_int_id_suffixes)

        # Profiling
        if params.profiling_sample_pc is not None:
            cmd.extend(["--profiling_sample_pc", str(params.profiling_sample_pc)])
        if params.p_query is not None:
            cmd.extend(["--p_query", str(params.p_query)])
        if params.min_sample_pc_profile is not None:
            cmd.extend(["--min_sample_pc_profile", str(params.min_sample_pc_profile)])

        # Boolean flags
        if params.force:
            cmd.append("-f")
        if params.basic_diff:
            cmd.append("--basic_diff")
        if params.without_xid:
            cmd.append("--without_xid")

        # FK mode
        if params.fk_mode:
            cmd.extend(["--fk_mode", params.fk_mode.value])

        # Logging
        if params.log_level:
            cmd.extend(["--log_level", params.log_level.value])
        if params.log_dir:
            cmd.extend(["--log_dir", params.log_dir])

        # Display flags
        if params.no_banner:
            cmd.append("--no_banner")
        if params.no_progress:
            cmd.append("--no_progress")
        if params.quiet_ft:
            cmd.append("--quiet_ft")

        # License
        if params.license:
            cmd.extend(["--license", params.license])
        if params.license_file:
            cmd.extend(["--license_file", params.license_file])

        return cmd


def get_supported_capabilities() -> Dict[str, Any]:
    """
    Get supported source databases, target databases, migration database types,
    tasks, modes, and other capabilities.

    Returns:
        Dictionary with all supported capabilities
    """
    return {
        "Source Databases": [
            "Oracle (oracle)",
            "PostgreSQL (postgresql)",
            "SQL Server (sqlserver)",
            "Netezza (netezza)",
        ],
        "Target Databases": [
            "PostgreSQL (postgresql)",
            "SQL Server (sqlserver)",
            "MySQL (mysql) -- limited: schema creation only",
            "Oracle (oracle)",
        ],
        "Migration Database": [
            "SQL Server (sqlserver)",
        ],
        "Tasks": {
            "translate": "Translate source schema to target schema DDL",
            "create": "Create target tables from translated DDL",
            "transfer": "Transfer data from source to target",
            "diff": "Compare source and target row counts",
            "copy_pk": "Copy primary key constraints to target",
            "copy_ak": "Copy alternate key (unique) constraints to target",
            "copy_fk": "Copy foreign key constraints to target",
            "all": "Run all tasks in sequence (translate, create, transfer, diff, copy_pk, copy_ak, copy_fk)",
        },
        "Migration DB Modes": {
            "preserve": "Keep existing migration database data",
            "truncate": "Clear migration database before run",
            "drop": "Drop and recreate migration database",
        },
        "Load Modes": {
            "truncate": "Truncate target tables before loading",
            "append": "Append data to existing target tables",
        },
        "FK Modes": {
            "trusted": "Create foreign keys as trusted constraints",
            "untrusted": "Create foreign keys as untrusted constraints",
            "disabled": "Create foreign keys in disabled state",
        },
    }


def suggest_workflow(
    source_type: str,
    target_type: str,
    include_constraints: bool = True,
) -> Dict[str, Any]:
    """
    Suggest an ordered workflow of MigratorXpress tasks based on use case.

    Args:
        source_type: Source database type (e.g., 'oracle', 'postgresql')
        target_type: Target database type (e.g., 'postgresql', 'sqlserver')
        include_constraints: Whether to include constraint copy steps

    Returns:
        Dictionary with ordered workflow steps and example parameters
    """
    steps = []

    # Step 1: Translate
    steps.append(
        {
            "step": 1,
            "task": "translate",
            "description": "Translate source schema DDL to target-compatible DDL",
            "example": (
                "MigratorXpress -a auth.json "
                "--source_db_auth_id source_db --source_db_name mydb "
                "--target_db_auth_id target_db --target_db_name targetdb "
                "--migration_db_auth_id migration_db "
                "--task_list translate"
            ),
        }
    )

    # Step 2: Create
    steps.append(
        {
            "step": 2,
            "task": "create",
            "description": "Create target tables from translated DDL",
            "example": (
                "MigratorXpress -a auth.json "
                "--source_db_auth_id source_db --source_db_name mydb "
                "--target_db_auth_id target_db --target_db_name targetdb "
                "--migration_db_auth_id migration_db "
                "--task_list create"
            ),
        }
    )

    # Step 3: Transfer
    steps.append(
        {
            "step": 3,
            "task": "transfer",
            "description": "Transfer data from source to target tables",
            "example": (
                "MigratorXpress -a auth.json "
                "--source_db_auth_id source_db --source_db_name mydb "
                "--target_db_auth_id target_db --target_db_name targetdb "
                "--migration_db_auth_id migration_db "
                "--task_list transfer"
            ),
        }
    )

    # Step 4: Diff
    steps.append(
        {
            "step": 4,
            "task": "diff",
            "description": "Compare source and target row counts to verify transfer",
            "example": (
                "MigratorXpress -a auth.json "
                "--source_db_auth_id source_db --source_db_name mydb "
                "--target_db_auth_id target_db --target_db_name targetdb "
                "--migration_db_auth_id migration_db "
                "--task_list diff"
            ),
        }
    )

    # Step 5: Constraints (optional)
    if include_constraints:
        steps.append(
            {
                "step": 5,
                "task": "copy_pk + copy_ak + copy_fk",
                "description": "Copy primary keys, alternate keys, and foreign keys to target",
                "example": (
                    "MigratorXpress -a auth.json "
                    "--source_db_auth_id source_db --source_db_name mydb "
                    "--target_db_auth_id target_db --target_db_name targetdb "
                    "--migration_db_auth_id migration_db "
                    "--task_list copy_pk copy_ak copy_fk"
                ),
            }
        )

    # Alternative: all
    steps.append(
        {
            "step": "alt",
            "task": "all",
            "description": "Alternative: run all tasks in a single invocation",
            "example": (
                "MigratorXpress -a auth.json "
                "--source_db_auth_id source_db --source_db_name mydb "
                "--target_db_auth_id target_db --target_db_name targetdb "
                "--migration_db_auth_id migration_db "
                "--task_list all"
            ),
        }
    )

    return {
        "source_type": source_type,
        "target_type": target_type,
        "include_constraints": include_constraints,
        "steps": steps,
    }
