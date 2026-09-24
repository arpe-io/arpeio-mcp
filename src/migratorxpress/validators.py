"""
Input validation for MigratorXpress MCP Server.

This module provides Pydantic models and enums for validating
all MigratorXpress parameters and ensuring parameter compatibility.
"""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, model_validator


class TaskType(str, Enum):
    """MigratorXpress migration task types."""

    TRANSLATE = "translate"
    CREATE = "create"
    TRANSFER = "transfer"
    DIFF = "diff"
    COPY_PK = "copy_pk"
    COPY_AK = "copy_ak"
    COPY_FK = "copy_fk"
    ALL = "all"


class SourceDatabaseType(str, Enum):
    """Source database types supported by MigratorXpress."""

    ORACLE = "oracle"
    POSTGRESQL = "postgresql"
    SQLSERVER = "sqlserver"
    NETEZZA = "netezza"


class TargetDatabaseType(str, Enum):
    """Target database types supported by MigratorXpress."""

    POSTGRESQL = "postgresql"
    SQLSERVER = "sqlserver"
    MYSQL = "mysql"
    ORACLE = "oracle"


class MigrationDbMode(str, Enum):
    """Migration database modes."""

    PRESERVE = "preserve"
    TRUNCATE = "truncate"
    DROP = "drop"


class LoadMode(str, Enum):
    """Data load modes."""

    TRUNCATE = "truncate"
    APPEND = "append"


class FkMode(str, Enum):
    """Foreign key constraint modes."""

    TRUSTED = "trusted"
    UNTRUSTED = "untrusted"
    DISABLED = "disabled"


class LogLevel(str, Enum):
    """Log level for MigratorXpress output."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class MigrationParams(BaseModel):
    """Parameters for a MigratorXpress migration command.

    MigratorXpress is a single-command CLI -- all flags are at the top level.
    """

    # Required fields
    auth_file: str = Field(
        ..., description="Path to authentication/credentials JSON file"
    )
    migration_db_auth_id: str = Field(
        ..., description="Migration database credential ID"
    )

    # Source/target identifiers: required for a migration run, optional when
    # upgrade_migdb=True (0.7.0+ relaxes them for the maintenance command).
    source_db_auth_id: Optional[str] = Field(
        None, description="Source database credential ID"
    )
    source_db_name: Optional[str] = Field(None, description="Source database name")
    target_db_auth_id: Optional[str] = Field(
        None, description="Target database credential ID"
    )
    target_db_name: Optional[str] = Field(None, description="Target database name")

    # Schema
    source_schema_name: Optional[str] = Field(None, description="Source schema name")
    target_schema_name: Optional[str] = Field(None, description="Target schema name")

    # Tasks
    task_list: Optional[List[str]] = Field(
        None, description="List of tasks to run (nargs='+')"
    )
    resume: Optional[str] = Field(None, description="Resume a previous run by RUN_ID")

    # FastTransfer
    fasttransfer_dir_path: Optional[str] = Field(
        None, description="Path to FastTransfer binary directory"
    )
    fasttransfer_p: Optional[int] = Field(
        None, description="FastTransfer parallel degree"
    )
    ft_large_table_th: Optional[int] = Field(
        None, description="FastTransfer large table threshold"
    )

    # Parallelism
    n_jobs: Optional[int] = Field(None, description="Number of concurrent jobs")

    # Index thresholds
    cci_threshold: Optional[int] = Field(
        None, description="Clustered columnstore index threshold"
    )
    aci_threshold: Optional[int] = Field(
        None, description="Additional clustered index threshold"
    )

    # Migration DB
    migration_db_mode: Optional[MigrationDbMode] = Field(
        None, description="Migration database mode"
    )

    # String-boolean parameters
    compute_nbrows: Optional[str] = Field(
        None, description="Compute number of rows (true/false)"
    )
    drop_tables_if_exists: Optional[str] = Field(
        None, description="Drop target tables if they exist (true/false)"
    )

    # Load
    load_mode: Optional[LoadMode] = Field(None, description="Data load mode")

    # Filtering
    include_tables: Optional[str] = Field(None, description="Table include pattern")
    exclude_tables: Optional[str] = Field(None, description="Table exclude pattern")
    min_rows: Optional[int] = Field(
        None, description="Minimum row count for table inclusion"
    )
    max_rows: Optional[int] = Field(
        None, description="Maximum row count for table inclusion"
    )

    # Oracle-specific
    forced_int_id_prefixes: Optional[List[str]] = Field(
        None, description="Force integer ID for columns with these prefixes (nargs='+')"
    )
    forced_int_id_suffixes: Optional[List[str]] = Field(
        None, description="Force integer ID for columns with these suffixes (nargs='+')"
    )

    # Profiling
    profiling_sample_pc: Optional[float] = Field(
        None, description="Profiling sample percentage"
    )
    p_query: Optional[float] = Field(None, description="Profiling query parameter")
    min_sample_pc_profile: Optional[float] = Field(
        None, description="Minimum sample percentage for profiling"
    )

    # Boolean flags
    force: bool = Field(False, description="Force operation")
    basic_diff: bool = Field(False, description="Use basic diff mode")
    without_xid: bool = Field(False, description="Disable XID tracking")

    # FK
    fk_mode: Optional[FkMode] = Field(None, description="Foreign key constraint mode")

    # Logging
    log_level: Optional[LogLevel] = Field(None, description="Logging verbosity level")
    log_dir: Optional[str] = Field(None, description="Directory for log files")

    # Display
    no_banner: bool = Field(False, description="Suppress the startup banner")
    no_progress: bool = Field(False, description="Disable progress bar display")
    quiet_ft: bool = Field(False, description="Suppress FastTransfer output")

    # License
    license: Optional[str] = Field(
        None, description="License key (SENSITIVE -- will be masked in display)"
    )
    license_file: Optional[str] = Field(None, description="Path to license file")

    # Project tag (0.6.30+)
    project: Optional[str] = Field(
        None,
        description=(
            "Project tag attached to this run (0.6.30+). Stored on "
            "sourcetargetloads, eventlog, and sourcetargetdiff for later "
            "WHERE project='...' filtering. Letters, digits, underscore, "
            "hyphen; max 64 chars."
        ),
    )

    # Metadata DB upgrade (0.7.0+)
    upgrade_migdb: bool = Field(
        False,
        description=(
            "Upgrade the migration (tracking) DB metadata to the run_id schema "
            "introduced in 0.7.0, then exit (0.7.0+). Only auth_file and "
            "migration_db_auth_id are needed in this mode."
        ),
    )

    @model_validator(mode="after")
    def validate_required_for_migration_run(self):
        """Require source/target identifiers unless upgrade_migdb is set.

        With upgrade_migdb, reject run-only options (task_list, resume) that
        MigratorXpress would silently ignore since it exits after the upgrade.
        """
        src_tgt_fields = (
            "source_db_auth_id",
            "source_db_name",
            "target_db_auth_id",
            "target_db_name",
        )
        if self.upgrade_migdb:
            if self.task_list:
                raise ValueError(
                    "upgrade_migdb cannot be combined with task_list: "
                    "MigratorXpress upgrades the metadata DB and exits."
                )
            if self.resume:
                raise ValueError(
                    "upgrade_migdb cannot be combined with resume: "
                    "MigratorXpress upgrades the metadata DB and exits."
                )
        else:
            missing = [f for f in src_tgt_fields if not getattr(self, f)]
            if missing:
                raise ValueError(
                    f"Missing required field(s) for a migration run: "
                    f"{', '.join(missing)} (only optional with upgrade_migdb=true)"
                )
        return self

    @model_validator(mode="after")
    def validate_project_format(self):
        """Validate --project tag matches ^[A-Za-z0-9_-]{1,64}$ when provided."""
        if self.project is not None:
            import re

            if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.project):
                raise ValueError(
                    "'project' must contain only letters, digits, underscore, "
                    "or hyphen, and be 1-64 characters long."
                )
        return self

    @model_validator(mode="after")
    def validate_task_list_values(self):
        """Validate that all task_list values are valid and 'all' is not combined."""
        if self.task_list is not None:
            valid_tasks = {t.value for t in TaskType}
            for task in self.task_list:
                if task not in valid_tasks:
                    raise ValueError(
                        f"Invalid task '{task}'. Valid tasks: {sorted(valid_tasks)}"
                    )
            if "all" in self.task_list and len(self.task_list) > 1:
                raise ValueError("Task 'all' cannot be combined with other tasks.")
        return self

    @model_validator(mode="after")
    def validate_string_booleans(self):
        """Validate that string-boolean params are 'true' or 'false'."""
        for field_name in ("compute_nbrows", "drop_tables_if_exists"):
            value = getattr(self, field_name)
            if value is not None and value not in ("true", "false"):
                raise ValueError(
                    f"'{field_name}' must be 'true' or 'false', got '{value}'"
                )
        return self

    @model_validator(mode="after")
    def validate_license_mutual_exclusivity(self):
        """Validate that license and license_file are mutually exclusive."""
        if self.license is not None and self.license_file is not None:
            raise ValueError(
                "license and license_file are mutually exclusive. "
                "Provide one or the other, not both."
            )
        return self
