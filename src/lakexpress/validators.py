"""
Input validation for LakeXpress MCP Server.

This module provides Pydantic models and enums for validating
all LakeXpress parameters and ensuring parameter compatibility.
"""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, model_validator


class CommandType(str, Enum):
    """LakeXpress command types."""

    LOGDB_INIT = "logdb_init"
    LOGDB_DROP = "logdb_drop"
    LOGDB_TRUNCATE = "logdb_truncate"
    LOGDB_LOCKS = "logdb_locks"
    LOGDB_RELEASE_LOCKS = "logdb_release_locks"
    LXDB_INIT = "lxdb_init"
    LXDB_DROP = "lxdb_drop"
    LXDB_TRUNCATE = "lxdb_truncate"
    LXDB_LOCKS = "lxdb_locks"
    LXDB_RELEASE_LOCKS = "lxdb_release_locks"
    CONFIG_CREATE = "config_create"
    CONFIG_DELETE = "config_delete"
    CONFIG_LIST = "config_list"
    SYNC = "sync"
    SYNC_EXPORT = "sync_export"
    SYNC_PUBLISH = "sync_publish"
    RUN = "run"
    STATUS = "status"
    CLEANUP = "cleanup"


class SourceDatabaseType(str, Enum):
    """Source database types supported by LakeXpress."""

    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    ORACLE = "oracle"
    MYSQL = "mysql"
    MARIADB = "mariadb"
    SAPHANA = "saphana"


class LogDatabaseType(str, Enum):
    """Log database types supported by LakeXpress."""

    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MARIADB = "mariadb"
    SQLITE = "sqlite"
    DUCKDB = "duckdb"


class StorageBackend(str, Enum):
    """Storage backends for output files."""

    LOCAL = "local"
    S3 = "s3"
    S3_COMPATIBLE = "s3compatible"
    GCS = "gcs"
    AZURE_ADLS = "azure_adls"
    ONELAKE = "onelake"


class PublishTarget(str, Enum):
    """Publish targets for data lake publishing."""

    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    FABRIC = "fabric"
    BIGQUERY = "bigquery"
    MOTHERDUCK = "motherduck"
    GLUE = "glue"
    DUCKLAKE = "ducklake"


class PublishMethod(str, Enum):
    """Publish methods."""

    EXTERNAL = "external"
    INTERNAL = "internal"


class CompressionType(str, Enum):
    """Parquet compression types."""

    ZSTD = "Zstd"
    SNAPPY = "Snappy"
    GZIP = "Gzip"
    LZ4 = "Lz4"
    NONE = "None"


class ErrorAction(str, Enum):
    """Error action on failure."""

    FAIL = "fail"
    CONTINUE = "continue"
    SKIP = "skip"


class LogLevel(str, Enum):
    """Log level for LakeXpress output."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class CleanupStatus(str, Enum):
    """Status filter for cleanup command."""

    RUNNING = "running"
    FAILED = "failed"


class GlobalOptions(BaseModel):
    """Global options shared across most commands."""

    auth_file: str = Field(
        ..., description="Path to authentication/credentials JSON file"
    )
    log_db_auth_id: str = Field(
        ..., description="Credential ID for the log database connection"
    )
    log_level: Optional[LogLevel] = Field(None, description="Logging verbosity level")
    log_dir: Optional[str] = Field(None, description="Directory for log files")
    no_progress: bool = Field(False, description="Disable progress bar display")
    no_banner: bool = Field(False, description="Suppress the startup banner")
    license: Optional[str] = Field(None, description="Path to license file (0.3.0+)")


class LogdbInitParams(GlobalOptions):
    """Parameters for logdb init command."""

    pass


class LogdbDropParams(GlobalOptions):
    """Parameters for logdb drop command."""

    confirm: bool = Field(False, description="Confirm the drop operation")


class LogdbTruncateParams(GlobalOptions):
    """Parameters for logdb truncate command."""

    sync_id: Optional[str] = Field(
        None, description="Truncate only data for this sync_id"
    )
    confirm: bool = Field(False, description="Confirm the truncate operation")


class LogdbLocksParams(GlobalOptions):
    """Parameters for logdb locks command."""

    sync_id: Optional[str] = Field(None, description="Show locks for this sync_id only")


class LogdbReleaseLocksParams(GlobalOptions):
    """Parameters for logdb release-locks command."""

    max_age_hours: Optional[int] = Field(
        None, description="Release locks older than this many hours"
    )
    table_id: Optional[str] = Field(
        None, description="Release lock for a specific table ID"
    )
    confirm: bool = Field(False, description="Confirm the lock release operation")


class ConfigCreateParams(GlobalOptions):
    """Parameters for config create command."""

    # Source
    source_db_auth_id: str = Field(..., description="Source database credential ID")
    source_db_name: Optional[str] = Field(None, description="Source database name")
    source_schema_name: Optional[str] = Field(
        None, description="Comma-separated list of source schemas"
    )

    # Filtering
    include: Optional[str] = Field(
        None, description="Table include patterns (comma-separated, supports wildcards)"
    )
    exclude: Optional[str] = Field(
        None, description="Table exclude patterns (comma-separated, supports wildcards)"
    )
    min_rows: Optional[int] = Field(
        None, description="Minimum row count for table inclusion"
    )
    max_rows: Optional[int] = Field(
        None, description="Maximum row count for table inclusion"
    )

    # Incremental
    incremental_table: Optional[List[str]] = Field(
        None,
        description="Incremental table config: schema.table:column:type[:i|:e][@start][!strategy]",
    )
    incremental_safety_lag: Optional[int] = Field(
        None, description="Safety lag in seconds for incremental exports"
    )

    # Storage
    output_dir: Optional[str] = Field(
        None, description="Local output directory for exports"
    )
    target_storage_id: Optional[str] = Field(
        None, description="Target storage credential ID (for cloud storage)"
    )
    sub_path: Optional[str] = Field(
        None, description="Sub-path within storage location"
    )

    # FastBCP
    fastbcp_dir_path: Optional[str] = Field(
        None, description="Path to FastBCP binary directory"
    )
    fastbcp_p: Optional[int] = Field(None, description="FastBCP parallel degree")
    n_jobs: Optional[int] = Field(
        None, description="Number of concurrent table exports"
    )
    compression_type: Optional[CompressionType] = Field(
        None, description="Parquet compression type"
    )
    large_table_threshold: Optional[int] = Field(
        None, description="Row threshold for large table handling"
    )
    fastbcp_table_config: Optional[str] = Field(
        None,
        description="Per-table FastBCP config: 'table1:method:key:degree;table2:method:key:degree'",
    )

    # Publishing
    publish_target: Optional[str] = Field(
        None, description="Publish target credential ID"
    )
    publish_method: Optional[PublishMethod] = Field(
        None, description="Publish method (external tables or internal/loaded)"
    )
    publish_database_name: Optional[str] = Field(
        None, description="Target database name for publishing"
    )
    publish_schema_pattern: Optional[str] = Field(
        None, description="Schema naming pattern for publishing"
    )
    publish_table_pattern: Optional[str] = Field(
        None, description="Table naming pattern for publishing"
    )

    # Features
    no_views: bool = Field(
        False, description="Disable view creation when publishing external tables"
    )
    pk_constraints: bool = Field(
        False, description="Enable primary key constraints when publishing"
    )
    generate_metadata: bool = Field(False, description="Generate CDM metadata files")
    manifest_name: Optional[str] = Field(None, description="Manifest file name")

    # Other
    sync_id: Optional[str] = Field(
        None, description="Custom sync ID (1-64 chars, alphanumeric/underscore/hyphen)"
    )
    error_action: Optional[ErrorAction] = Field(
        None, description="Action on error: fail, continue, or skip"
    )
    env_name: Optional[str] = Field(
        None, description="Environment name for configuration isolation"
    )

    @model_validator(mode="after")
    def validate_output_destination(self):
        """Validate output_dir and target_storage_id are mutually exclusive."""
        if self.output_dir and self.target_storage_id:
            raise ValueError(
                "output_dir and target_storage_id are mutually exclusive. "
                "Use output_dir for local storage or target_storage_id for cloud storage."
            )
        if not self.output_dir and not self.target_storage_id:
            raise ValueError(
                "At least one of output_dir or target_storage_id must be provided."
            )
        return self

    @model_validator(mode="after")
    def validate_publish_method_requires_target(self):
        """Validate publish_method requires publish_target."""
        if self.publish_method and not self.publish_target:
            raise ValueError("publish_method requires publish_target to be set.")
        return self


class ConfigDeleteParams(GlobalOptions):
    """Parameters for config delete command."""

    sync_id: str = Field(..., description="The sync_id to delete")
    confirm: bool = Field(False, description="Confirm the delete operation")


class ConfigListParams(GlobalOptions):
    """Parameters for config list command."""

    env_name: Optional[str] = Field(None, description="Filter by environment name")


class SyncParams(BaseModel):
    """Parameters for sync command."""

    sync_id: Optional[str] = Field(None, description="The sync_id to execute")
    resume: bool = Field(False, description="Resume an incomplete run")
    run_id: Optional[str] = Field(
        None, description="Specific run_id to resume or continue"
    )
    auth_file: Optional[str] = Field(None, description="Override auth file")
    fastbcp_dir_path: Optional[str] = Field(
        None, description="Override FastBCP location"
    )
    log_level: Optional[LogLevel] = Field(None, description="Logging verbosity level")
    log_dir: Optional[str] = Field(None, description="Directory for log files")
    no_progress: bool = Field(False, description="Disable progress bar display")
    no_banner: bool = Field(False, description="Suppress the startup banner")
    quiet_fbcp: bool = Field(False, description="Suppress FastBCP output")
    license: Optional[str] = Field(None, description="Path to license file (0.3.0+)")
    env_name: Optional[str] = Field(
        None, description="Environment name for configuration isolation (0.3.0+)"
    )


class SyncExportParams(BaseModel):
    """Parameters for sync[export] command."""

    sync_id: Optional[str] = Field(None, description="The sync_id to execute")
    auth_file: Optional[str] = Field(None, description="Override auth file")
    fastbcp_dir_path: Optional[str] = Field(
        None, description="Override FastBCP location"
    )
    log_level: Optional[LogLevel] = Field(None, description="Logging verbosity level")
    log_dir: Optional[str] = Field(None, description="Directory for log files")
    no_progress: bool = Field(False, description="Disable progress bar display")
    no_banner: bool = Field(False, description="Suppress the startup banner")
    quiet_fbcp: bool = Field(False, description="Suppress FastBCP output")
    license: Optional[str] = Field(None, description="Path to license file (0.3.0+)")
    env_name: Optional[str] = Field(
        None, description="Environment name for configuration isolation (0.3.0+)"
    )


class SyncPublishParams(BaseModel):
    """Parameters for sync[publish] command."""

    sync_id: Optional[str] = Field(None, description="The sync_id to publish")
    run_id: Optional[str] = Field(None, description="Specific run_id to publish")
    auth_file: Optional[str] = Field(None, description="Override auth file")
    log_level: Optional[LogLevel] = Field(None, description="Logging verbosity level")
    log_dir: Optional[str] = Field(None, description="Directory for log files")
    no_progress: bool = Field(False, description="Disable progress bar display")
    no_banner: bool = Field(False, description="Suppress the startup banner")
    license: Optional[str] = Field(None, description="Path to license file (0.3.0+)")
    env_name: Optional[str] = Field(
        None, description="Environment name for configuration isolation (0.3.0+)"
    )


class RunParams(BaseModel):
    """Parameters for run command."""

    config: str = Field(..., description="Path to YAML configuration file")
    auth_file: Optional[str] = Field(
        None, description="Path to authentication JSON file (overrides YAML)"
    )
    log_db_auth_id: Optional[str] = Field(
        None, description="Log database credential ID (overrides YAML)"
    )
    log_level: Optional[LogLevel] = Field(None, description="Logging verbosity level")
    log_dir: Optional[str] = Field(None, description="Directory for log files")
    no_progress: bool = Field(False, description="Disable progress bar display")
    no_banner: bool = Field(False, description="Suppress the startup banner")
    license: Optional[str] = Field(None, description="Path to license file (0.3.0+)")
    env_name: Optional[str] = Field(
        None, description="Environment name for configuration isolation (0.3.0+)"
    )


class StatusParams(GlobalOptions):
    """Parameters for status command."""

    sync_id: Optional[str] = Field(
        None, description="Show runs for this sync configuration"
    )
    run_id: Optional[str] = Field(None, description="Show details for a specific run")
    verbose: bool = Field(False, description="Show detailed list of all runs")


class CleanupParams(GlobalOptions):
    """Parameters for cleanup command."""

    sync_id: str = Field(..., description="The sync_id to clean up")
    older_than: Optional[str] = Field(
        None,
        description="Only delete runs older than this duration (e.g., 7d, 24h, 30m)",
    )
    status: Optional[CleanupStatus] = Field(
        None, description="Only delete runs with this status"
    )
    dry_run: bool = Field(
        False, description="Preview what would be deleted without actually deleting"
    )


class LakeXpressRequest(BaseModel):
    """Top-level request model for LakeXpress commands."""

    command: CommandType = Field(..., description="The LakeXpress command to execute")

    # Command-specific parameters (exactly one should be provided)
    logdb_init: Optional[LogdbInitParams] = Field(
        None, description="Parameters for logdb init"
    )
    logdb_drop: Optional[LogdbDropParams] = Field(
        None, description="Parameters for logdb drop"
    )
    logdb_truncate: Optional[LogdbTruncateParams] = Field(
        None, description="Parameters for logdb truncate"
    )
    logdb_locks: Optional[LogdbLocksParams] = Field(
        None, description="Parameters for logdb locks"
    )
    logdb_release_locks: Optional[LogdbReleaseLocksParams] = Field(
        None, description="Parameters for logdb release-locks"
    )
    lxdb_init: Optional[LogdbInitParams] = Field(
        None, description="Parameters for lxdb init (0.3.0+)"
    )
    lxdb_drop: Optional[LogdbDropParams] = Field(
        None, description="Parameters for lxdb drop (0.3.0+)"
    )
    lxdb_truncate: Optional[LogdbTruncateParams] = Field(
        None, description="Parameters for lxdb truncate (0.3.0+)"
    )
    lxdb_locks: Optional[LogdbLocksParams] = Field(
        None, description="Parameters for lxdb locks (0.3.0+)"
    )
    lxdb_release_locks: Optional[LogdbReleaseLocksParams] = Field(
        None, description="Parameters for lxdb release-locks (0.3.0+)"
    )
    config_create: Optional[ConfigCreateParams] = Field(
        None, description="Parameters for config create"
    )
    config_delete: Optional[ConfigDeleteParams] = Field(
        None, description="Parameters for config delete"
    )
    config_list: Optional[ConfigListParams] = Field(
        None, description="Parameters for config list"
    )
    sync: Optional[SyncParams] = Field(None, description="Parameters for sync")
    sync_export: Optional[SyncExportParams] = Field(
        None, description="Parameters for sync[export]"
    )
    sync_publish: Optional[SyncPublishParams] = Field(
        None, description="Parameters for sync[publish]"
    )
    run: Optional[RunParams] = Field(None, description="Parameters for run")
    status: Optional[StatusParams] = Field(None, description="Parameters for status")
    cleanup: Optional[CleanupParams] = Field(None, description="Parameters for cleanup")

    @model_validator(mode="after")
    def validate_command_params(self):
        """Ensure the correct params are provided for the chosen command."""
        command_to_field = {
            CommandType.LOGDB_INIT: "logdb_init",
            CommandType.LOGDB_DROP: "logdb_drop",
            CommandType.LOGDB_TRUNCATE: "logdb_truncate",
            CommandType.LOGDB_LOCKS: "logdb_locks",
            CommandType.LOGDB_RELEASE_LOCKS: "logdb_release_locks",
            CommandType.LXDB_INIT: "lxdb_init",
            CommandType.LXDB_DROP: "lxdb_drop",
            CommandType.LXDB_TRUNCATE: "lxdb_truncate",
            CommandType.LXDB_LOCKS: "lxdb_locks",
            CommandType.LXDB_RELEASE_LOCKS: "lxdb_release_locks",
            CommandType.CONFIG_CREATE: "config_create",
            CommandType.CONFIG_DELETE: "config_delete",
            CommandType.CONFIG_LIST: "config_list",
            CommandType.SYNC: "sync",
            CommandType.SYNC_EXPORT: "sync_export",
            CommandType.SYNC_PUBLISH: "sync_publish",
            CommandType.RUN: "run",
            CommandType.STATUS: "status",
            CommandType.CLEANUP: "cleanup",
        }

        expected_field = command_to_field[self.command]
        params = getattr(self, expected_field)

        if params is None:
            raise ValueError(
                f"Command '{self.command.value}' requires '{expected_field}' parameters to be provided."
            )

        return self
