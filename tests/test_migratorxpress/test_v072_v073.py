"""Tests for the MigratorXpress 0.7.2 / 0.7.3 registry entries and CLI changes.

0.7.2: --source_db_name (deprecated, ignored) and --target_db_name are optional,
--drop_tables_if_exists parses true/false, targets are SQL Server / PostgreSQL
only, copy_fk is rejected for a PostgreSQL target.
0.7.3: constraint copy from PostgreSQL / SQL Server sources, Netezza source with a
constraint task rejected. No CLI flag change.
"""

import jsonschema
import pytest

from src.base.version_detector import ToolVersion
from src.migratorxpress.command_builder import (
    CommandBuilder,
    get_supported_capabilities,
    suggest_workflow,
)
from src.migratorxpress.tools import PREVIEW_OUTPUT_SCHEMA, create_tools
from src.migratorxpress.validators import MigrationParams
from src.migratorxpress.version import VERSION_REGISTRY, check_version_compatibility


def _run_params(**overrides):
    base = {
        "auth_file": "auth.json",
        "source_db_auth_id": "source_db",
        "target_db_auth_id": "target_db",
        "migration_db_auth_id": "migration_db",
    }
    base.update(overrides)
    return base


class TestRegistry072073:
    def test_registered(self):
        assert "0.7.2" in VERSION_REGISTRY
        assert "0.7.3" in VERSION_REGISTRY

    def test_optional_db_names_boundary(self):
        assert not VERSION_REGISTRY["0.7.1"].supports_optional_db_names
        assert VERSION_REGISTRY["0.7.2"].supports_optional_db_names
        assert VERSION_REGISTRY["0.7.3"].supports_optional_db_names

    def test_drop_tables_bool_parsing_boundary(self):
        assert not VERSION_REGISTRY["0.7.1"].supports_drop_tables_bool_parsing
        assert VERSION_REGISTRY["0.7.2"].supports_drop_tables_bool_parsing
        assert VERSION_REGISTRY["0.7.3"].supports_drop_tables_bool_parsing

    def test_targets_restricted_to_sqlserver_and_postgresql(self):
        assert "mysql" in VERSION_REGISTRY["0.7.1"].target_databases
        for ver in ("0.7.2", "0.7.3"):
            assert VERSION_REGISTRY[ver].target_databases == {"postgresql", "sqlserver"}

    def test_rest_of_surface_unchanged(self):
        old = VERSION_REGISTRY["0.7.1"]
        for ver in ("0.7.2", "0.7.3"):
            new = VERSION_REGISTRY[ver]
            assert new.source_databases == old.source_databases
            assert new.migration_db_types == old.migration_db_types
            assert new.tasks == old.tasks
            assert new.fk_modes == old.fk_modes
            assert new.migration_db_modes == old.migration_db_modes
            assert new.load_modes == old.load_modes
            assert new.supports_upgrade_migdb and new.supports_project

    def test_073_is_latest(self):
        builder = CommandBuilder("/nonexistent/MigratorXpress")
        assert builder.version_detector.capabilities == VERSION_REGISTRY["0.7.3"]


class TestOptionalDbNames:
    def test_run_without_db_names_validates(self):
        params = MigrationParams(**_run_params())
        assert params.source_db_name is None
        assert params.target_db_name is None

    def test_command_omits_db_names(self):
        cmd = CommandBuilder("/nonexistent/MigratorXpress").build_command(
            MigrationParams(**_run_params(task_list=["translate"]))
        )
        assert "--source_db_name" not in cmd
        assert "--target_db_name" not in cmd
        assert cmd[cmd.index("--source_db_auth_id") + 1] == "source_db"
        assert cmd[cmd.index("--target_db_auth_id") + 1] == "target_db"

    def test_command_keeps_target_db_name_when_given(self):
        cmd = CommandBuilder("/nonexistent/MigratorXpress").build_command(
            MigrationParams(**_run_params(target_db_name="tgtdb"))
        )
        assert cmd[cmd.index("--target_db_name") + 1] == "tgtdb"

    def test_missing_names_warn_on_071(self):
        warnings = check_version_compatibility(
            _run_params(), VERSION_REGISTRY["0.7.1"], ToolVersion(parts=(0, 7, 1))
        )
        assert len(warnings) == 1
        assert "--source_db_name and --target_db_name" in warnings[0]
        assert "0.7.2" in warnings[0]
        assert "0.7.1" in warnings[0]

    def test_missing_target_name_only_warns_on_071(self):
        warnings = check_version_compatibility(
            _run_params(source_db_name="src"),
            VERSION_REGISTRY["0.7.1"],
            ToolVersion(parts=(0, 7, 1)),
        )
        assert len(warnings) == 1
        assert "--target_db_name" in warnings[0]
        assert "--source_db_name" not in warnings[0]

    def test_missing_names_no_warning_on_072(self):
        assert check_version_compatibility(
            _run_params(), VERSION_REGISTRY["0.7.2"], ToolVersion(parts=(0, 7, 2))
        ) == []

    def test_names_given_on_071_no_warning(self):
        assert check_version_compatibility(
            _run_params(source_db_name="s", target_db_name="t"),
            VERSION_REGISTRY["0.7.1"],
            ToolVersion(parts=(0, 7, 1)),
        ) == []

    def test_upgrade_migdb_on_071_no_name_warning(self):
        assert check_version_compatibility(
            {"auth_file": "a.json", "migration_db_auth_id": "m", "upgrade_migdb": True},
            VERSION_REGISTRY["0.7.1"],
            ToolVersion(parts=(0, 7, 1)),
        ) == []

    def test_source_db_name_deprecated_on_073(self):
        warnings = check_version_compatibility(
            _run_params(source_db_name="src", target_db_name="tgt"),
            VERSION_REGISTRY["0.7.3"],
            ToolVersion(parts=(0, 7, 3)),
        )
        assert len(warnings) == 1
        assert "--source_db_name is deprecated" in warnings[0]


class TestDropTablesIfExistsFalse:
    """'false' is never emitted, so pre-0.7.2 binaries cannot misread it."""

    def test_false_omitted_from_command(self, tmp_path):
        binary = tmp_path / "MigratorXpress"
        binary.write_text("#!/bin/sh\n")
        binary.chmod(0o755)
        command = CommandBuilder(str(binary)).build_command(
            MigrationParams(**_run_params(drop_tables_if_exists="false"))
        )
        assert "--drop_tables_if_exists" not in command

    def test_false_on_071_no_warning(self):
        assert check_version_compatibility(
            {"drop_tables_if_exists": "false"},
            VERSION_REGISTRY["0.7.1"],
            ToolVersion(parts=(0, 7, 1)),
        ) == []


class TestWorkflowPairs:
    def test_postgresql_target_drops_copy_fk_and_all(self):
        wf = suggest_workflow("oracle", "postgresql", include_constraints=True)
        tasks = [s["task"] for s in wf["steps"]]
        assert "copy_pk + copy_ak" in tasks
        assert not any("copy_fk" in t for t in tasks)
        assert "all" not in tasks
        assert "translate create transfer diff copy_pk copy_ak" in tasks
        assert any("COPY_FK_UNSUPPORTED_TARGET" in n for n in wf["notes"])

    def test_sqlserver_target_keeps_all(self):
        wf = suggest_workflow("oracle", "sqlserver", include_constraints=True)
        tasks = [s["task"] for s in wf["steps"]]
        assert "copy_pk + copy_ak + copy_fk" in tasks
        assert "all" in tasks

    def test_netezza_source_skips_constraints(self):
        wf = suggest_workflow("netezza", "sqlserver", include_constraints=True)
        tasks = [s["task"] for s in wf["steps"]]
        assert not any("copy_" in t for t in tasks)
        assert "all" not in tasks
        assert any("CONSTRAINT_COPY_UNSUPPORTED_SOURCE" in n for n in wf["notes"])

    def test_netezza_to_postgresql_unsupported_note(self):
        wf = suggest_workflow("netezza", "postgresql")
        assert any("UNSUPPORTED_SOURCE_TARGET_PAIR" in n for n in wf["notes"])

    def test_mysql_target_note(self):
        wf = suggest_workflow("oracle", "mysql")
        assert any("UNSUPPORTED_TARGET_DB_TYPE" in n for n in wf["notes"])

    def test_pg_mssql_source_constraints_need_073(self):
        wf = suggest_workflow("postgresql", "sqlserver", include_constraints=True)
        assert any("0.7.3" in n for n in wf["notes"])

    def test_capabilities_mark_mysql_oracle_targets_legacy(self):
        targets = get_supported_capabilities()["Target Databases"]
        assert any("mysql" in t and "before 0.7.2" in t for t in targets)
        assert any("oracle" in t and "before 0.7.2" in t for t in targets)


def _handler():
    builder = CommandBuilder("/nonexistent/MigratorXpress")
    return create_tools(
        builder, {"timeout": 60, "log_dir": "./logs", "path": "/nonexistent/MigratorXpress"}
    )


class TestPreviewTool:
    @pytest.mark.asyncio
    async def test_preview_without_db_names(self):
        _, handler = _handler()
        _content, structured = await handler(
            "migratorxpress_preview_command",
            {
                "auth_file": "/tmp/auth.json",
                "source_db_auth_id": "src",
                "target_db_auth_id": "tgt",
                "migration_db_auth_id": "mig",
                "task_list": ["translate", "create"],
            },
        )
        assert structured["status"] == "ok"
        assert "--source_db_name" not in structured["command"]
        assert "auth entry 'src'" in structured["explanation"]
        # Latest known capabilities (0.7.3) apply in preview-only mode: no warning
        assert structured["warnings"] == []
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_workflow_notes_rendered(self):
        _, handler = _handler()
        result = await handler(
            "migratorxpress_info",
            {"action": "workflow", "source_type": "oracle", "target_type": "postgresql"},
        )
        content = result[0] if isinstance(result, tuple) else result
        text = "\n".join(c.text for c in content)
        assert "## Notes:" in text
        assert "COPY_FK_UNSUPPORTED_TARGET" in text
