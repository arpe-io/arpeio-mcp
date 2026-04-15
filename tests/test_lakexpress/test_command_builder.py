"""Tests for LakeXpress command builder."""

from pathlib import Path
from unittest.mock import Mock, patch
import subprocess

import pytest

from src.lakexpress.command_builder import (
    CommandBuilder,
    LakeXpressError,
    get_supported_capabilities,
    suggest_workflow,
)
from src.base.command_builder import ArpeToolError
from src.lakexpress.validators import (
    LakeXpressRequest,
    CommandType,
)
from src.base.version_detector import ToolVersion


@pytest.fixture
def mock_binary(tmp_path):
    """Create a mock LakeXpress binary."""
    binary = tmp_path / "LakeXpress"
    binary.write_text("#!/bin/bash\necho 'mock binary'")
    binary.chmod(0o755)
    return str(binary)


@pytest.fixture
def command_builder(mock_binary):
    """Create a CommandBuilder with mock binary."""
    with patch("src.lakexpress.command_builder.BaseVersionDetector") as MockDetector:
        mock_detector = MockDetector.return_value
        mock_detector.detect.return_value = ToolVersion(parts=(0, 2, 8))
        mock_detector.capabilities = Mock()
        mock_detector.capabilities.source_databases = frozenset(
            ["sqlserver", "postgresql", "oracle", "mysql", "mariadb"]
        )
        mock_detector.capabilities.log_databases = frozenset(
            ["sqlserver", "postgresql", "mysql", "mariadb", "sqlite", "duckdb"]
        )
        mock_detector.capabilities.storage_backends = frozenset(
            ["local", "s3", "s3compatible", "gcs", "azure_adls", "onelake"]
        )
        mock_detector.capabilities.publish_targets = frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
            ]
        )
        mock_detector.capabilities.compression_types = frozenset(
            ["Zstd", "Snappy", "Gzip", "Lz4", "None"]
        )
        mock_detector.capabilities.commands = frozenset(
            ["logdb_init", "config_create", "sync", "status", "cleanup"]
        )
        mock_detector.capabilities.supports_no_banner = True
        mock_detector.capabilities.supports_version_flag = True
        mock_detector.capabilities.supports_incremental = True
        mock_detector.capabilities.supports_cleanup = True
        builder = CommandBuilder(mock_binary)
    return builder


class TestCommandBuilder:
    """Tests for CommandBuilder class."""

    def test_init_with_valid_binary(self, mock_binary):
        """Test initialization with valid binary."""
        with patch("src.lakexpress.command_builder.BaseVersionDetector"):
            builder = CommandBuilder(mock_binary)
        assert builder.binary_path == Path(mock_binary)

    def test_init_with_nonexistent_binary(self):
        """Test initialization with nonexistent binary enters preview-only mode."""
        builder = CommandBuilder("/nonexistent/path/LakeXpress")
        assert builder._preview_only is True

    def test_init_with_non_executable_binary(self, tmp_path):
        """Test initialization with non-executable binary enters preview-only mode."""
        binary = tmp_path / "LakeXpress"
        binary.write_text("not executable")
        binary.chmod(0o644)

        builder = CommandBuilder(str(binary))
        assert builder._preview_only is True

    def test_build_logdb_init(self, command_builder):
        """Legacy logdb_init request routes to the 0.4.0+ lxdb init subcommand."""
        request = LakeXpressRequest(
            command=CommandType.LOGDB_INIT,
            logdb_init={"auth_file": "auth.json", "log_db_auth_id": "export_db"},
        )
        command = command_builder.build_command(request)

        assert command[0] == str(command_builder.binary_path)
        assert command[1] == "lxdb"
        assert command[2] == "init"
        assert "-a" in command
        idx = command.index("-a")
        assert command[idx + 1] == "auth.json"
        assert "--lxdb_auth_id" in command
        idx = command.index("--lxdb_auth_id")
        assert command[idx + 1] == "export_db"

    def test_build_logdb_drop_with_confirm(self, command_builder):
        """Test building logdb drop command with confirm."""
        request = LakeXpressRequest(
            command=CommandType.LOGDB_DROP,
            logdb_drop={
                "auth_file": "auth.json",
                "log_db_auth_id": "export_db",
                "confirm": True,
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "lxdb"
        assert command[2] == "drop"
        assert "--confirm" in command

    def test_build_logdb_truncate_with_sync_id(self, command_builder):
        """Test building logdb truncate with sync_id."""
        request = LakeXpressRequest(
            command=CommandType.LOGDB_TRUNCATE,
            logdb_truncate={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "sync_id": "my-sync",
                "confirm": True,
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "lxdb"
        assert command[2] == "truncate"
        assert "--sync_id" in command
        idx = command.index("--sync_id")
        assert command[idx + 1] == "my-sync"
        assert "--confirm" in command

    def test_build_logdb_release_locks(self, command_builder):
        """Test building logdb release-locks command."""
        request = LakeXpressRequest(
            command=CommandType.LOGDB_RELEASE_LOCKS,
            logdb_release_locks={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "max_age_hours": 24,
                "table_id": "table-123",
                "confirm": True,
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "lxdb"
        assert command[2] == "release-locks"
        assert "--max_age_hours" in command
        idx = command.index("--max_age_hours")
        assert command[idx + 1] == "24"
        assert "--table_id" in command
        assert "--confirm" in command

    def test_build_config_create_basic(self, command_builder):
        """Test building basic config create command."""
        request = LakeXpressRequest(
            command=CommandType.CONFIG_CREATE,
            config_create={
                "auth_file": "auth.json",
                "log_db_auth_id": "export_db",
                "source_db_auth_id": "source_db",
                "output_dir": "/tmp/exports",
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "config"
        assert command[2] == "create"
        assert "--source_db_auth_id" in command
        idx = command.index("--source_db_auth_id")
        assert command[idx + 1] == "source_db"
        assert "--output_dir" in command
        idx = command.index("--output_dir")
        assert command[idx + 1] == "/tmp/exports"

    def test_build_config_create_all_params(self, command_builder):
        """Test building config create with many parameters."""
        request = LakeXpressRequest(
            command=CommandType.CONFIG_CREATE,
            config_create={
                "auth_file": "auth.json",
                "log_db_auth_id": "export_db",
                "source_db_auth_id": "source_db",
                "source_db_name": "mydb",
                "source_schema_name": "sales,hr",
                "include": "orders*,customers*",
                "exclude": "*_tmp",
                "min_rows": 100,
                "max_rows": 1000000,
                "output_dir": "/tmp/exports",
                "fastbcp_dir_path": "/opt/fastbcp",
                "fastbcp_p": 4,
                "n_jobs": 2,
                "compression_type": "Zstd",
                "large_table_threshold": 500000,
                "publish_target": "snowflake_cred",
                "publish_method": "external",
                "publish_database_name": "analytics",
                "publish_schema_pattern": "{schema}",
                "publish_table_pattern": "{table}",
                "no_views": True,
                "pk_constraints": True,
                "generate_metadata": True,
                "sync_id": "my-sync",
                "error_action": "continue",
                "env_name": "production",
            },
        )
        command = command_builder.build_command(request)

        assert "--source_db_name" in command
        assert "--source_schema_name" in command
        assert "-i" in command
        assert "-e" in command
        assert "--min_rows" in command
        assert "--max_rows" in command
        assert "--fastbcp_dir_path" in command
        assert "-p" in command
        assert "--n_jobs" in command
        assert "--compression_type" in command
        assert "--large_table_threshold" in command
        assert "--publish_target" in command
        assert "--publish_method" in command
        assert "--publish_database_name" in command
        assert "--publish_schema_pattern" in command
        assert "--publish_table_pattern" in command
        assert "--no_views" in command
        assert "--pk_constraints" in command
        assert "--generate_metadata" in command
        assert "--sync_id" in command
        assert "--error_action" in command
        assert "--env_name" in command

    def test_build_config_create_incremental(self, command_builder):
        """Test building config create with incremental tables."""
        request = LakeXpressRequest(
            command=CommandType.CONFIG_CREATE,
            config_create={
                "auth_file": "auth.json",
                "log_db_auth_id": "export_db",
                "source_db_auth_id": "source_db",
                "output_dir": "/tmp/exports",
                "incremental_table": [
                    "sales.orders:updated_at:datetime",
                    "sales.items:id:int",
                ],
                "incremental_safety_lag": 300,
            },
        )
        command = command_builder.build_command(request)

        # Each incremental_table gets its own --incremental_table flag
        inc_count = command.count("--incremental_table")
        assert inc_count == 2
        assert "--incremental_safety_lag" in command
        idx = command.index("--incremental_safety_lag")
        assert command[idx + 1] == "300"

    def test_build_config_delete(self, command_builder):
        """Test building config delete command."""
        request = LakeXpressRequest(
            command=CommandType.CONFIG_DELETE,
            config_delete={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "sync_id": "my-sync",
                "confirm": True,
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "config"
        assert command[2] == "delete"
        assert "--sync_id" in command
        assert "--confirm" in command

    def test_build_config_list(self, command_builder):
        """Test building config list command."""
        request = LakeXpressRequest(
            command=CommandType.CONFIG_LIST,
            config_list={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "env_name": "production",
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "config"
        assert command[2] == "list"
        assert "--env_name" in command
        idx = command.index("--env_name")
        assert command[idx + 1] == "production"

    def test_build_sync(self, command_builder):
        """Test building sync command."""
        request = LakeXpressRequest(
            command=CommandType.SYNC,
            sync={"sync_id": "my-sync"},
        )
        command = command_builder.build_command(request)

        assert command[1] == "sync"
        assert "--sync_id" in command
        idx = command.index("--sync_id")
        assert command[idx + 1] == "my-sync"

    def test_build_sync_with_resume(self, command_builder):
        """Test building sync command with resume flag."""
        request = LakeXpressRequest(
            command=CommandType.SYNC,
            sync={
                "sync_id": "my-sync",
                "resume": True,
                "run_id": "run-001",
                "auth_file": "auth.json",
            },
        )
        command = command_builder.build_command(request)

        assert "--resume" in command
        assert "--run_id" in command
        assert "-a" in command

    def test_build_sync_with_quiet_fbcp(self, command_builder):
        """Test building sync command with quiet_fbcp flag."""
        request = LakeXpressRequest(
            command=CommandType.SYNC,
            sync={"sync_id": "my-sync", "quiet_fbcp": True},
        )
        command = command_builder.build_command(request)

        assert "--quiet_fbcp" in command

    def test_build_sync_export(self, command_builder):
        """Test building sync[export] command."""
        request = LakeXpressRequest(
            command=CommandType.SYNC_EXPORT,
            sync_export={
                "sync_id": "my-sync",
                "fastbcp_dir_path": "/opt/fastbcp",
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "sync[export]"
        assert "--sync_id" in command
        assert "--fastbcp_dir_path" in command

    def test_build_sync_export_with_quiet_fbcp(self, command_builder):
        """Test building sync[export] command with quiet_fbcp flag."""
        request = LakeXpressRequest(
            command=CommandType.SYNC_EXPORT,
            sync_export={"sync_id": "my-sync", "quiet_fbcp": True},
        )
        command = command_builder.build_command(request)

        assert command[1] == "sync[export]"
        assert "--quiet_fbcp" in command

    def test_build_sync_publish(self, command_builder):
        """Test building sync[publish] command."""
        request = LakeXpressRequest(
            command=CommandType.SYNC_PUBLISH,
            sync_publish={
                "sync_id": "my-sync",
                "run_id": "run-001",
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "sync[publish]"
        assert "--sync_id" in command
        assert "--run_id" in command

    def test_build_run(self, command_builder):
        """Test building run command."""
        request = LakeXpressRequest(
            command=CommandType.RUN,
            run={
                "config": "config.yaml",
                "auth_file": "auth.json",
                "log_db_auth_id": "export_db",
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "run"
        assert "-c" in command
        idx = command.index("-c")
        assert command[idx + 1] == "config.yaml"
        assert "-a" in command
        assert "--lxdb_auth_id" in command

    def test_build_status(self, command_builder):
        """Test building status command."""
        request = LakeXpressRequest(
            command=CommandType.STATUS,
            status={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "sync_id": "my-sync",
                "verbose": True,
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "status"
        assert "--sync_id" in command
        assert "--verbose" in command

    def test_build_cleanup(self, command_builder):
        """Test building cleanup command."""
        request = LakeXpressRequest(
            command=CommandType.CLEANUP,
            cleanup={
                "auth_file": "auth.json",
                "log_db_auth_id": "db",
                "sync_id": "my-sync",
                "older_than": "7d",
                "status": "failed",
                "dry_run": True,
            },
        )
        command = command_builder.build_command(request)

        assert command[1] == "cleanup"
        assert "--sync_id" in command
        assert "--older-than" in command
        idx = command.index("--older-than")
        assert command[idx + 1] == "7d"
        assert "--status" in command
        idx = command.index("--status")
        assert command[idx + 1] == "failed"
        assert "--dry-run" in command

    def test_build_global_options(self, command_builder):
        """Test global options are appended correctly."""
        request = LakeXpressRequest(
            command=CommandType.LOGDB_INIT,
            logdb_init={
                "auth_file": "auth.json",
                "log_db_auth_id": "export_db",
                "log_level": "DEBUG",
                "log_dir": "/tmp/logs",
                "no_progress": True,
                "no_banner": True,
            },
        )
        command = command_builder.build_command(request)

        assert "--log_level" in command
        idx = command.index("--log_level")
        assert command[idx + 1] == "DEBUG"
        assert "--log_dir" in command
        idx = command.index("--log_dir")
        assert command[idx + 1] == "/tmp/logs"
        assert "--no_progress" in command
        assert "--no_banner" in command

    def test_format_command_display(self, command_builder):
        """Test formatting command for display."""
        request = LakeXpressRequest(
            command=CommandType.LOGDB_INIT,
            logdb_init={"auth_file": "auth.json", "log_db_auth_id": "export_db"},
        )
        command = command_builder.build_command(request)
        display = command_builder.format_command_display(command)

        assert "-a auth.json" in display
        assert "--lxdb_auth_id export_db" in display
        assert " \\\n  " in display

    def test_get_version_method(self, command_builder):
        """Test get_version returns structured info."""
        info = command_builder.get_version()

        assert "version" in info
        assert "detected" in info
        assert "binary_path" in info
        assert "capabilities" in info
        assert "source_databases" in info["capabilities"]
        assert "log_databases" in info["capabilities"]
        assert "storage_backends" in info["capabilities"]
        assert "publish_targets" in info["capabilities"]
        assert "compression_types" in info["capabilities"]
        assert "supports_no_banner" in info["capabilities"]
        assert "supports_incremental" in info["capabilities"]
        assert "supports_cleanup" in info["capabilities"]

    def test_version_detector_property(self, command_builder):
        """Test version_detector property is accessible."""
        assert command_builder.version_detector is not None

    @patch("subprocess.run")
    def test_execute_command_success(self, mock_run, command_builder):
        """Test successful command execution."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Sync completed successfully"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        command = [str(command_builder.binary_path), "--help"]
        return_code, stdout, stderr = command_builder.execute_command(
            command, timeout=10
        )

        assert return_code == 0
        assert "success" in stdout.lower()
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_execute_command_failure(self, mock_run, command_builder):
        """Test failed command execution."""
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Connection failed"
        mock_run.return_value = mock_result

        command = [str(command_builder.binary_path), "--help"]
        return_code, stdout, stderr = command_builder.execute_command(
            command, timeout=10
        )

        assert return_code == 1
        assert "failed" in stderr.lower()

    @patch("subprocess.run")
    def test_execute_command_timeout(self, mock_run, command_builder):
        """Test command execution timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=1)

        command = [str(command_builder.binary_path), "--help"]
        with pytest.raises(ArpeToolError) as exc_info:
            command_builder.execute_command(command, timeout=1)

        assert "timed out" in str(exc_info.value).lower()

    @patch("subprocess.run")
    def test_execute_command_with_logging(self, mock_run, command_builder, tmp_path):
        """Test command execution with log saving."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Success"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        log_dir = tmp_path / "logs"
        command = [str(command_builder.binary_path), "--help"]
        command_builder.execute_command(command, timeout=10, log_dir=log_dir)

        assert log_dir.exists()
        log_files = list(log_dir.glob("lakexpress_*.log"))
        assert len(log_files) == 1

        log_content = log_files[0].read_text()
        assert "LakeXpress Execution Log" in log_content
        assert "Return Code: 0" in log_content


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_get_supported_capabilities(self):
        """Test getting supported capabilities."""
        caps = get_supported_capabilities()

        assert isinstance(caps, dict)
        assert "Source Databases" in caps
        assert "Log Databases" in caps
        assert "Storage Backends" in caps
        assert "Publishing Targets" in caps
        assert "Compression Types" in caps
        assert "Commands" in caps

        assert len(caps["Source Databases"]) == 7  # 0.4.0 adds Teradata
        assert len(caps["Log Databases"]) == 6
        assert len(caps["Storage Backends"]) == 6
        assert len(caps["Publishing Targets"]) == 8  # 0.4.0 adds Redshift
        assert len(caps["Compression Types"]) == 5
        assert any("teradata" in db.lower() for db in caps["Source Databases"])
        assert any("redshift" in pt.lower() for pt in caps["Publishing Targets"])

    def test_get_supported_capabilities_includes_saphana(self):
        """Test that SAP HANA appears in get_supported_capabilities output."""
        caps = get_supported_capabilities()
        assert any("saphana" in db.lower() for db in caps["Source Databases"])

    def test_suggest_workflow_local_no_publish(self):
        """Test workflow suggestion for local export without publishing."""
        workflow = suggest_workflow("postgresql", "local")

        assert workflow["source_type"] == "postgresql"
        assert workflow["destination"] == "local"
        assert workflow["publish_target"] is None
        assert len(workflow["steps"]) >= 3

        # Should have lxdb init (0.4.0+), config create, sync[export], status
        commands = [s["command"] for s in workflow["steps"]]
        assert "lxdb init" in commands
        assert "config create" in commands

    def test_suggest_workflow_s3_with_publish(self):
        """Test workflow suggestion for S3 export with publishing."""
        workflow = suggest_workflow("sqlserver", "s3", "snowflake")

        assert workflow["publish_target"] == "snowflake"
        assert len(workflow["steps"]) >= 4

        # Should include sync (full) option
        commands = [s["command"] for s in workflow["steps"]]
        assert "sync" in commands

    def test_suggest_workflow_examples_contain_source(self):
        """Test that workflow examples reference the source type."""
        workflow = suggest_workflow("oracle", "azure_adls", "databricks")

        # Config create step should mention storage
        config_step = next(
            s for s in workflow["steps"] if s["command"] == "config create"
        )
        assert (
            "azure_adls" in config_step["example"]
            or "storage" in config_step["description"]
        )
