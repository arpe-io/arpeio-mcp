"""Tests for --upgrade_migdb (MigratorXpress 0.7.0+) and the 0.7.x registry entries."""

import jsonschema
import pytest
from pydantic import ValidationError

from src.base.version_detector import ToolVersion
from src.migratorxpress.command_builder import CommandBuilder
from src.migratorxpress.tools import PREVIEW_OUTPUT_SCHEMA, create_tools
from src.migratorxpress.validators import MigrationParams
from src.migratorxpress.version import VERSION_REGISTRY, check_version_compatibility


def _upgrade_params(**overrides):
    base = {
        "auth_file": "auth.json",
        "migration_db_auth_id": "migration_db",
        "upgrade_migdb": True,
    }
    base.update(overrides)
    return base


def _run_params(**overrides):
    base = {
        "auth_file": "auth.json",
        "source_db_auth_id": "source_db",
        "source_db_name": "mydb",
        "target_db_auth_id": "target_db",
        "target_db_name": "targetdb",
        "migration_db_auth_id": "migration_db",
    }
    base.update(overrides)
    return base


class TestRegistry07:
    def test_070_and_071_registered(self):
        assert "0.7.0" in VERSION_REGISTRY
        assert "0.7.1" in VERSION_REGISTRY

    def test_upgrade_migdb_capability_boundary(self):
        assert not VERSION_REGISTRY["0.6.34"].supports_upgrade_migdb
        assert VERSION_REGISTRY["0.7.0"].supports_upgrade_migdb
        assert VERSION_REGISTRY["0.7.1"].supports_upgrade_migdb

    def test_07x_keeps_06x_surface(self):
        """0.7.x removed nothing: every 0.6.34 capability is still present."""
        old = VERSION_REGISTRY["0.6.34"]
        for ver in ("0.7.0", "0.7.1"):
            new = VERSION_REGISTRY[ver]
            assert new.source_databases == old.source_databases
            assert new.target_databases == old.target_databases
            assert new.migration_db_types == old.migration_db_types
            assert new.tasks == old.tasks
            assert new.fk_modes == old.fk_modes
            assert new.migration_db_modes == old.migration_db_modes
            assert new.load_modes == old.load_modes
            assert new.supports_project and new.supports_postgres_migration_db


class TestUpgradeMigdbValidation:
    def test_upgrade_without_source_target_ok(self):
        params = MigrationParams(**_upgrade_params())
        assert params.upgrade_migdb is True
        assert params.source_db_auth_id is None
        assert params.target_db_name is None

    def test_run_without_source_target_rejected(self):
        with pytest.raises(ValidationError, match="source_db_auth_id"):
            MigrationParams(
                auth_file="auth.json", migration_db_auth_id="mig"
            )

    def test_run_missing_single_field_rejected(self):
        p = _run_params()
        del p["target_db_name"]
        with pytest.raises(ValidationError, match="target_db_name"):
            MigrationParams(**p)

    def test_upgrade_with_task_list_rejected(self):
        with pytest.raises(ValidationError, match="task_list"):
            MigrationParams(**_upgrade_params(task_list=["translate"]))

    def test_upgrade_with_resume_rejected(self):
        with pytest.raises(ValidationError, match="resume"):
            MigrationParams(**_upgrade_params(resume="run-123"))

    def test_upgrade_with_source_target_still_ok(self):
        params = MigrationParams(**_run_params(upgrade_migdb=True))
        assert params.upgrade_migdb is True


class TestUpgradeMigdbCommand:
    def _builder(self):
        return CommandBuilder("/nonexistent/MigratorXpress")  # preview-only mode

    def test_upgrade_command_minimal(self):
        cmd = self._builder().build_command(MigrationParams(**_upgrade_params()))
        assert cmd[1:] == [
            "-a", "auth.json",
            "--migration_db_auth_id", "migration_db",
            "--upgrade_migdb",
        ]
        assert "--source_db_auth_id" not in cmd
        assert "--target_db_name" not in cmd

    def test_upgrade_command_with_license_and_log(self):
        cmd = self._builder().build_command(
            MigrationParams(**_upgrade_params(license_file="lic.lic", log_level="DEBUG"))
        )
        assert "--upgrade_migdb" in cmd
        assert cmd[cmd.index("--license_file") + 1] == "lic.lic"
        assert cmd[cmd.index("--log_level") + 1] == "DEBUG"

    def test_regular_run_has_no_upgrade_flag(self):
        cmd = self._builder().build_command(MigrationParams(**_run_params()))
        assert "--upgrade_migdb" not in cmd
        assert cmd[cmd.index("--source_db_auth_id") + 1] == "source_db"
        assert cmd[cmd.index("--target_db_name") + 1] == "targetdb"


class TestUpgradeMigdbVersionGate:
    def test_upgrade_on_0634_produces_warning(self):
        caps = VERSION_REGISTRY["0.6.34"]
        version = ToolVersion(parts=(0, 6, 34))
        warnings = check_version_compatibility({"upgrade_migdb": True}, caps, version)
        assert len(warnings) == 1
        assert "--upgrade_migdb" in warnings[0]
        assert "0.7.0" in warnings[0]
        assert "0.6.34" in warnings[0]

    def test_upgrade_on_070_no_warning(self):
        caps = VERSION_REGISTRY["0.7.0"]
        version = ToolVersion(parts=(0, 7, 0))
        assert check_version_compatibility({"upgrade_migdb": True}, caps, version) == []

    def test_upgrade_false_no_warning_on_old(self):
        caps = VERSION_REGISTRY["0.6.24"]
        version = ToolVersion(parts=(0, 6, 24))
        assert check_version_compatibility({"upgrade_migdb": False}, caps, version) == []


def _handler():
    builder = CommandBuilder("/nonexistent/MigratorXpress")
    return create_tools(
        builder, {"timeout": 60, "log_dir": "./logs", "path": "/nonexistent/MigratorXpress"}
    )


class TestUpgradeMigdbTool:
    def test_schema_exposes_upgrade_migdb_and_project(self):
        tools, _ = _handler()
        preview = {t.name: t for t in tools}["migratorxpress_preview_command"]
        props = preview.inputSchema["properties"]
        assert props["upgrade_migdb"]["type"] == "boolean"
        assert "project" in props
        assert preview.inputSchema["required"] == ["auth_file", "migration_db_auth_id"]

    @pytest.mark.asyncio
    async def test_preview_upgrade_command(self):
        _, handler = _handler()
        content, structured = await handler(
            "migratorxpress_preview_command",
            {"auth_file": "/tmp/auth.json", "migration_db_auth_id": "mig", "upgrade_migdb": True},
        )
        assert structured["status"] == "ok"
        assert "--upgrade_migdb" in structured["command"]
        assert "--source_db_auth_id" not in structured["command"]
        assert "run_id" in structured["explanation"]
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_preview_run_missing_source_is_error_with_tip(self):
        _, handler = _handler()
        _content, structured = await handler(
            "migratorxpress_preview_command",
            {"auth_file": "/tmp/auth.json", "migration_db_auth_id": "mig"},
        )
        assert structured["status"] == "error"
        assert any("source_db_auth_id" in e["message"] for e in structured["errors"])
        assert any("validate_auth_file" in t for t in structured["tips"])
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)
