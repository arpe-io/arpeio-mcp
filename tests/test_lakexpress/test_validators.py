"""Tests for validators module."""

import pytest
from pydantic import ValidationError

from src.lakexpress.validators import (
    CommandType,
    SourceDatabaseType,
    LogDatabaseType,
    StorageBackend,
    PublishTarget,
    PublishMethod,
    CompressionType,
    ErrorAction,
    LogLevel,
    CleanupStatus,
    GlobalOptions,
    LogdbDropParams,
    LogdbTruncateParams,
    LogdbLocksParams,
    LogdbReleaseLocksParams,
    ConfigCreateParams,
    ConfigDeleteParams,
    SyncParams,
    SyncExportParams,
    SyncPublishParams,
    RunParams,
    StatusParams,
    CleanupParams,
    LakeXpressRequest,
)


class TestCommandType:
    """Tests for CommandType enum."""

    def test_all_14_command_types(self):
        """Test that there are exactly 19 command types."""
        assert len(CommandType) == 19

    def test_logdb_commands_exist(self):
        """Test logdb command types exist."""
        assert CommandType("logdb_init") == CommandType.LOGDB_INIT
        assert CommandType("logdb_drop") == CommandType.LOGDB_DROP
        assert CommandType("logdb_truncate") == CommandType.LOGDB_TRUNCATE
        assert CommandType("logdb_locks") == CommandType.LOGDB_LOCKS
        assert CommandType("logdb_release_locks") == CommandType.LOGDB_RELEASE_LOCKS

    def test_config_commands_exist(self):
        """Test config command types exist."""
        assert CommandType("config_create") == CommandType.CONFIG_CREATE
        assert CommandType("config_delete") == CommandType.CONFIG_DELETE
        assert CommandType("config_list") == CommandType.CONFIG_LIST

    def test_sync_commands_exist(self):
        """Test sync command types exist."""
        assert CommandType("sync") == CommandType.SYNC
        assert CommandType("sync_export") == CommandType.SYNC_EXPORT
        assert CommandType("sync_publish") == CommandType.SYNC_PUBLISH

    def test_other_commands_exist(self):
        """Test other command types exist."""
        assert CommandType("run") == CommandType.RUN
        assert CommandType("status") == CommandType.STATUS
        assert CommandType("cleanup") == CommandType.CLEANUP

    def test_invalid_command_type(self):
        """Test that invalid command type raises ValueError."""
        with pytest.raises(ValueError):
            CommandType("invalid")


class TestSourceDatabaseType:
    """Tests for SourceDatabaseType enum."""

    def test_all_6_source_types(self):
        """Test that there are exactly 6 source database types."""
        assert len(SourceDatabaseType) == 6

    def test_source_types_exist(self):
        """Test all source database types exist."""
        assert SourceDatabaseType("sqlserver") == SourceDatabaseType.SQLSERVER
        assert SourceDatabaseType("postgresql") == SourceDatabaseType.POSTGRESQL
        assert SourceDatabaseType("oracle") == SourceDatabaseType.ORACLE
        assert SourceDatabaseType("mysql") == SourceDatabaseType.MYSQL
        assert SourceDatabaseType("mariadb") == SourceDatabaseType.MARIADB
        assert SourceDatabaseType("saphana") == SourceDatabaseType.SAPHANA


class TestLogDatabaseType:
    """Tests for LogDatabaseType enum."""

    def test_all_6_log_types(self):
        """Test that there are exactly 6 log database types."""
        assert len(LogDatabaseType) == 6

    def test_log_types_exist(self):
        """Test all log database types exist."""
        assert LogDatabaseType("sqlserver") == LogDatabaseType.SQLSERVER
        assert LogDatabaseType("postgresql") == LogDatabaseType.POSTGRESQL
        assert LogDatabaseType("mysql") == LogDatabaseType.MYSQL
        assert LogDatabaseType("mariadb") == LogDatabaseType.MARIADB
        assert LogDatabaseType("sqlite") == LogDatabaseType.SQLITE
        assert LogDatabaseType("duckdb") == LogDatabaseType.DUCKDB


class TestOtherEnums:
    """Tests for other enum types."""

    def test_all_6_storage_backends(self):
        """Test all 6 storage backend values exist."""
        assert len(StorageBackend) == 6
        assert StorageBackend("local") == StorageBackend.LOCAL
        assert StorageBackend("s3") == StorageBackend.S3
        assert StorageBackend("s3compatible") == StorageBackend.S3_COMPATIBLE
        assert StorageBackend("gcs") == StorageBackend.GCS
        assert StorageBackend("azure_adls") == StorageBackend.AZURE_ADLS
        assert StorageBackend("onelake") == StorageBackend.ONELAKE

    def test_all_7_publish_targets(self):
        """Test all 7 publish target values exist."""
        assert len(PublishTarget) == 7
        assert PublishTarget("snowflake") == PublishTarget.SNOWFLAKE
        assert PublishTarget("databricks") == PublishTarget.DATABRICKS
        assert PublishTarget("fabric") == PublishTarget.FABRIC
        assert PublishTarget("bigquery") == PublishTarget.BIGQUERY
        assert PublishTarget("motherduck") == PublishTarget.MOTHERDUCK
        assert PublishTarget("glue") == PublishTarget.GLUE
        assert PublishTarget("ducklake") == PublishTarget.DUCKLAKE

    def test_all_2_publish_methods(self):
        """Test publish method values."""
        assert len(PublishMethod) == 2
        assert PublishMethod("external") == PublishMethod.EXTERNAL
        assert PublishMethod("internal") == PublishMethod.INTERNAL

    def test_all_5_compression_types(self):
        """Test all 5 compression type values exist."""
        assert len(CompressionType) == 5
        assert CompressionType("Zstd") == CompressionType.ZSTD
        assert CompressionType("Snappy") == CompressionType.SNAPPY
        assert CompressionType("Gzip") == CompressionType.GZIP
        assert CompressionType("Lz4") == CompressionType.LZ4
        assert CompressionType("None") == CompressionType.NONE

    def test_all_3_error_actions(self):
        """Test error action values."""
        assert len(ErrorAction) == 3
        assert ErrorAction("fail") == ErrorAction.FAIL
        assert ErrorAction("continue") == ErrorAction.CONTINUE
        assert ErrorAction("skip") == ErrorAction.SKIP

    def test_all_4_log_levels(self):
        """Test all 4 log level values exist."""
        assert len(LogLevel) == 4
        assert LogLevel("DEBUG") == LogLevel.DEBUG
        assert LogLevel("INFO") == LogLevel.INFO
        assert LogLevel("WARNING") == LogLevel.WARNING
        assert LogLevel("ERROR") == LogLevel.ERROR

    def test_all_2_cleanup_statuses(self):
        """Test cleanup status values."""
        assert len(CleanupStatus) == 2
        assert CleanupStatus("running") == CleanupStatus.RUNNING
        assert CleanupStatus("failed") == CleanupStatus.FAILED


class TestGlobalOptions:
    """Tests for GlobalOptions model."""

    def test_valid_global_options(self):
        """Test valid global options."""
        opts = GlobalOptions(auth_file="auth.json", log_db_auth_id="export_db")
        assert opts.auth_file == "auth.json"
        assert opts.log_db_auth_id == "export_db"
        assert opts.no_progress is False
        assert opts.no_banner is False

    def test_auth_file_required(self):
        """Test that auth_file is required."""
        with pytest.raises(ValidationError):
            GlobalOptions(log_db_auth_id="export_db")

    def test_log_db_auth_id_required(self):
        """Test that log_db_auth_id is required."""
        with pytest.raises(ValidationError):
            GlobalOptions(auth_file="auth.json")

    def test_global_options_with_all_fields(self):
        """Test global options with all optional fields."""
        opts = GlobalOptions(
            auth_file="auth.json",
            log_db_auth_id="export_db",
            log_level=LogLevel.DEBUG,
            log_dir="/tmp/logs",
            no_progress=True,
            no_banner=True,
        )
        assert opts.log_level == LogLevel.DEBUG
        assert opts.log_dir == "/tmp/logs"
        assert opts.no_progress is True
        assert opts.no_banner is True


class TestConfigCreateParams:
    """Tests for ConfigCreateParams model."""

    def test_valid_config_create_with_output_dir(self):
        """Test valid config create with output_dir."""
        params = ConfigCreateParams(
            auth_file="auth.json",
            log_db_auth_id="export_db",
            source_db_auth_id="source_db",
            output_dir="/tmp/exports",
        )
        assert params.source_db_auth_id == "source_db"
        assert params.output_dir == "/tmp/exports"

    def test_valid_config_create_with_target_storage(self):
        """Test valid config create with target_storage_id."""
        params = ConfigCreateParams(
            auth_file="auth.json",
            log_db_auth_id="export_db",
            source_db_auth_id="source_db",
            target_storage_id="s3_storage",
        )
        assert params.target_storage_id == "s3_storage"

    def test_source_db_auth_id_required(self):
        """Test that source_db_auth_id is required."""
        with pytest.raises(ValidationError):
            ConfigCreateParams(
                auth_file="auth.json",
                log_db_auth_id="export_db",
                output_dir="/tmp/exports",
            )

    def test_output_dir_and_target_storage_mutually_exclusive(self):
        """Test output_dir and target_storage_id cannot both be set."""
        with pytest.raises(ValidationError) as exc_info:
            ConfigCreateParams(
                auth_file="auth.json",
                log_db_auth_id="export_db",
                source_db_auth_id="source_db",
                output_dir="/tmp/exports",
                target_storage_id="s3_storage",
            )
        errors = exc_info.value.errors()
        assert any("mutually exclusive" in str(e).lower() for e in errors)

    def test_at_least_one_output_destination_required(self):
        """Test that at least one of output_dir or target_storage_id must be set."""
        with pytest.raises(ValidationError) as exc_info:
            ConfigCreateParams(
                auth_file="auth.json",
                log_db_auth_id="export_db",
                source_db_auth_id="source_db",
            )
        errors = exc_info.value.errors()
        assert any(
            "output_dir" in str(e) or "target_storage_id" in str(e) for e in errors
        )

    def test_publish_method_requires_publish_target(self):
        """Test that publish_method requires publish_target."""
        with pytest.raises(ValidationError) as exc_info:
            ConfigCreateParams(
                auth_file="auth.json",
                log_db_auth_id="export_db",
                source_db_auth_id="source_db",
                output_dir="/tmp/exports",
                publish_method=PublishMethod.EXTERNAL,
            )
        errors = exc_info.value.errors()
        assert any("publish_target" in str(e) for e in errors)

    def test_publish_method_with_publish_target_valid(self):
        """Test that publish_method with publish_target is valid."""
        params = ConfigCreateParams(
            auth_file="auth.json",
            log_db_auth_id="export_db",
            source_db_auth_id="source_db",
            output_dir="/tmp/exports",
            publish_target="snowflake_cred",
            publish_method=PublishMethod.EXTERNAL,
        )
        assert params.publish_method == PublishMethod.EXTERNAL

    def test_config_create_with_all_params(self):
        """Test config create with many optional params."""
        params = ConfigCreateParams(
            auth_file="auth.json",
            log_db_auth_id="export_db",
            source_db_auth_id="source_db",
            source_db_name="mydb",
            source_schema_name="sales,hr",
            include="orders*,customers*",
            exclude="*_tmp",
            min_rows=100,
            max_rows=1000000,
            output_dir="/tmp/exports",
            fastbcp_dir_path="/opt/fastbcp",
            fastbcp_p=4,
            n_jobs=2,
            compression_type=CompressionType.ZSTD,
            no_views=True,
            pk_constraints=True,
            generate_metadata=True,
            error_action=ErrorAction.CONTINUE,
            env_name="production",
        )
        assert params.n_jobs == 2
        assert params.compression_type == CompressionType.ZSTD
        assert params.no_views is True
        assert params.error_action == ErrorAction.CONTINUE


class TestLogdbParams:
    """Tests for logdb parameter models."""

    def test_logdb_drop_confirm_default(self):
        """Test logdb drop confirm defaults to False."""
        params = LogdbDropParams(auth_file="auth.json", log_db_auth_id="db")
        assert params.confirm is False

    def test_logdb_drop_confirm_true(self):
        """Test logdb drop with confirm=True."""
        params = LogdbDropParams(
            auth_file="auth.json", log_db_auth_id="db", confirm=True
        )
        assert params.confirm is True

    def test_logdb_truncate_optional_sync_id(self):
        """Test logdb truncate with optional sync_id."""
        params = LogdbTruncateParams(
            auth_file="auth.json", log_db_auth_id="db", sync_id="my-sync"
        )
        assert params.sync_id == "my-sync"

    def test_logdb_locks_optional_sync_id(self):
        """Test logdb locks with optional sync_id."""
        params = LogdbLocksParams(auth_file="auth.json", log_db_auth_id="db")
        assert params.sync_id is None

    def test_logdb_release_locks_params(self):
        """Test logdb release-locks with all params."""
        params = LogdbReleaseLocksParams(
            auth_file="auth.json",
            log_db_auth_id="db",
            max_age_hours=24,
            table_id="table-123",
            confirm=True,
        )
        assert params.max_age_hours == 24
        assert params.table_id == "table-123"
        assert params.confirm is True


class TestSyncParams:
    """Tests for sync parameter models."""

    def test_sync_defaults(self):
        """Test sync params with defaults."""
        params = SyncParams()
        assert params.sync_id is None
        assert params.resume is False
        assert params.run_id is None

    def test_sync_with_resume(self):
        """Test sync with resume flag."""
        params = SyncParams(sync_id="my-sync", resume=True)
        assert params.sync_id == "my-sync"
        assert params.resume is True

    def test_sync_export_params(self):
        """Test sync[export] params."""
        params = SyncExportParams(sync_id="my-sync", auth_file="auth.json")
        assert params.sync_id == "my-sync"

    def test_sync_publish_params(self):
        """Test sync[publish] params."""
        params = SyncPublishParams(sync_id="my-sync", run_id="run-001")
        assert params.run_id == "run-001"


class TestRunParams:
    """Tests for RunParams model."""

    def test_config_required(self):
        """Test that config file path is required."""
        with pytest.raises(ValidationError):
            RunParams()

    def test_valid_run_params(self):
        """Test valid run params."""
        params = RunParams(config="config.yaml")
        assert params.config == "config.yaml"
        assert params.auth_file is None

    def test_run_with_overrides(self):
        """Test run params with auth and log_db overrides."""
        params = RunParams(
            config="config.yaml",
            auth_file="auth.json",
            log_db_auth_id="export_db",
        )
        assert params.auth_file == "auth.json"
        assert params.log_db_auth_id == "export_db"


class TestStatusParams:
    """Tests for StatusParams model."""

    def test_status_required_fields(self):
        """Test status requires auth_file and log_db_auth_id."""
        with pytest.raises(ValidationError):
            StatusParams()

    def test_status_with_optional_fields(self):
        """Test status with optional sync_id and run_id."""
        params = StatusParams(
            auth_file="auth.json",
            log_db_auth_id="db",
            sync_id="my-sync",
            run_id="run-001",
            verbose=True,
        )
        assert params.sync_id == "my-sync"
        assert params.run_id == "run-001"
        assert params.verbose is True


class TestCleanupParams:
    """Tests for CleanupParams model."""

    def test_cleanup_sync_id_required(self):
        """Test cleanup requires sync_id."""
        with pytest.raises(ValidationError):
            CleanupParams(auth_file="auth.json", log_db_auth_id="db")

    def test_cleanup_with_all_params(self):
        """Test cleanup with all optional params."""
        params = CleanupParams(
            auth_file="auth.json",
            log_db_auth_id="db",
            sync_id="my-sync",
            older_than="7d",
            status=CleanupStatus.FAILED,
            dry_run=True,
        )
        assert params.older_than == "7d"
        assert params.status == CleanupStatus.FAILED
        assert params.dry_run is True


class TestConfigDeleteParams:
    """Tests for ConfigDeleteParams model."""

    def test_config_delete_sync_id_required(self):
        """Test config delete requires sync_id."""
        with pytest.raises(ValidationError):
            ConfigDeleteParams(auth_file="auth.json", log_db_auth_id="db")

    def test_config_delete_valid(self):
        """Test valid config delete."""
        params = ConfigDeleteParams(
            auth_file="auth.json",
            log_db_auth_id="db",
            sync_id="my-sync",
            confirm=True,
        )
        assert params.sync_id == "my-sync"
        assert params.confirm is True


class TestLakeXpressRequest:
    """Tests for LakeXpressRequest model."""

    def test_valid_logdb_init_request(self):
        """Test valid logdb init request."""
        request = LakeXpressRequest(
            command=CommandType.LOGDB_INIT,
            logdb_init={"auth_file": "auth.json", "log_db_auth_id": "db"},
        )
        assert request.command == CommandType.LOGDB_INIT
        assert request.logdb_init is not None

    def test_valid_config_create_request(self):
        """Test valid config create request."""
        request = LakeXpressRequest(
            command=CommandType.CONFIG_CREATE,
            config_create={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "source_db_auth_id": "source",
                "output_dir": "/tmp/exports",
            },
        )
        assert request.command == CommandType.CONFIG_CREATE
        assert request.config_create is not None

    def test_valid_sync_request(self):
        """Test valid sync request."""
        request = LakeXpressRequest(
            command=CommandType.SYNC,
            sync={"sync_id": "my-sync"},
        )
        assert request.command == CommandType.SYNC
        assert request.sync.sync_id == "my-sync"

    def test_valid_run_request(self):
        """Test valid run request."""
        request = LakeXpressRequest(
            command=CommandType.RUN,
            run={"config": "config.yaml"},
        )
        assert request.command == CommandType.RUN
        assert request.run.config == "config.yaml"

    def test_valid_cleanup_request(self):
        """Test valid cleanup request."""
        request = LakeXpressRequest(
            command=CommandType.CLEANUP,
            cleanup={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "sync_id": "my-sync",
                "dry_run": True,
            },
        )
        assert request.cleanup.dry_run is True

    def test_missing_params_for_command(self):
        """Test that missing params for the selected command raises error."""
        with pytest.raises(ValidationError) as exc_info:
            LakeXpressRequest(
                command=CommandType.LOGDB_INIT,
            )
        errors = exc_info.value.errors()
        assert any("logdb_init" in str(e) for e in errors)

    def test_wrong_params_for_command(self):
        """Test that providing wrong params for command raises error."""
        with pytest.raises(ValidationError) as exc_info:
            LakeXpressRequest(
                command=CommandType.LOGDB_INIT,
                sync={"sync_id": "my-sync"},
            )
        errors = exc_info.value.errors()
        assert any("logdb_init" in str(e) for e in errors)

    def test_valid_status_request(self):
        """Test valid status request."""
        request = LakeXpressRequest(
            command=CommandType.STATUS,
            status={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "sync_id": "my-sync",
                "verbose": True,
            },
        )
        assert request.status.verbose is True
