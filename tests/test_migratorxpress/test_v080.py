"""Tests for the MigratorXpress 0.8.0 - 0.8.2 registry entries and CLI changes.

0.8.0: a licence file is required (also for the trial), FastTransfer bundled in
engine/, new --with_xid (the Oracle / Netezza incremental skip is off by default),
--without_xid deprecated and ignored.
0.8.1: security release. 0.8.2: diff fixes (exit code 40 / 41 for tables that
could not be compared). No CLI flag change in either.
"""

from dataclasses import replace

import jsonschema
import pytest

from src.base.version_detector import ToolVersion
from src.migratorxpress.command_builder import CommandBuilder, suggest_workflow
from src.migratorxpress.tools import PREVIEW_OUTPUT_SCHEMA, create_tools
from src.migratorxpress.validators import MigrationParams
from src.migratorxpress.version import VERSION_REGISTRY, check_version_compatibility

RELEASED = ["0.8.0", "0.8.1", "0.8.2"]


def _run_params(**overrides):
    base = {
        "auth_file": "auth.json",
        "source_db_auth_id": "source_db",
        "target_db_auth_id": "target_db",
        "migration_db_auth_id": "migration_db",
    }
    base.update(overrides)
    return base


class TestRegistry080To082:
    @pytest.mark.parametrize("version", RELEASED)
    def test_registered_with_new_flags(self, version):
        caps = VERSION_REGISTRY[version]
        assert caps.supports_with_xid is True
        assert caps.requires_license is True

    @pytest.mark.parametrize("version", RELEASED)
    def test_same_databases_and_tasks_as_073(self, version):
        """Only the two 0.8.0 flags differ from 0.7.3."""
        assert (
            replace(
                VERSION_REGISTRY[version], supports_with_xid=False, requires_license=False
            )
            == VERSION_REGISTRY["0.7.3"]
        )

    def test_073_has_neither_flag(self):
        assert VERSION_REGISTRY["0.7.3"].supports_with_xid is False
        assert VERSION_REGISTRY["0.7.3"].requires_license is False

    def test_082_is_latest(self):
        latest = max(VERSION_REGISTRY, key=lambda v: tuple(int(x) for x in v.split(".")))
        assert latest == "0.8.2"
        builder = CommandBuilder("/nonexistent/MigratorXpress")
        assert builder.version_detector.capabilities == VERSION_REGISTRY["0.8.2"]


class TestWithXid:
    def test_with_xid_emitted(self):
        cmd = CommandBuilder("/nonexistent/MigratorXpress").build_command(
            MigrationParams(**_run_params(with_xid=True))
        )
        assert "--with_xid" in cmd
        assert "--without_xid" not in cmd

    def test_not_emitted_by_default(self):
        cmd = CommandBuilder("/nonexistent/MigratorXpress").build_command(
            MigrationParams(**_run_params())
        )
        assert "--with_xid" not in cmd
        assert "--without_xid" not in cmd

    def test_without_xid_still_emitted(self):
        """--without_xid is what disables the skip on a pre-0.8.0 binary."""
        cmd = CommandBuilder("/nonexistent/MigratorXpress").build_command(
            MigrationParams(**_run_params(without_xid=True))
        )
        assert "--without_xid" in cmd

    def test_with_xid_warns_before_080(self):
        warnings = check_version_compatibility(
            _run_params(with_xid=True),
            VERSION_REGISTRY["0.7.3"],
            ToolVersion(parts=(0, 7, 3)),
        )
        assert len(warnings) == 1
        assert "--with_xid requires MigratorXpress 0.8.0+" in warnings[0]
        assert "0.7.3" in warnings[0]

    @pytest.mark.parametrize("version", RELEASED)
    def test_with_xid_no_warning_from_080(self, version):
        warnings = check_version_compatibility(
            _run_params(with_xid=True),
            VERSION_REGISTRY[version],
            ToolVersion.parse(version),
        )
        assert warnings == []

    def test_with_xid_ignored_with_drop_tables(self):
        warnings = check_version_compatibility(
            _run_params(with_xid=True, drop_tables_if_exists="true"),
            VERSION_REGISTRY["0.8.2"],
            ToolVersion(parts=(0, 8, 2)),
        )
        assert len(warnings) == 1
        assert "--with_xid is ignored with --drop_tables_if_exists" in warnings[0]

    def test_with_xid_kept_with_drop_tables_false(self):
        warnings = check_version_compatibility(
            _run_params(with_xid=True, drop_tables_if_exists="false"),
            VERSION_REGISTRY["0.8.2"],
            ToolVersion(parts=(0, 8, 2)),
        )
        assert warnings == []

    def test_without_xid_deprecated_from_080(self):
        warnings = check_version_compatibility(
            _run_params(without_xid=True),
            VERSION_REGISTRY["0.8.0"],
            ToolVersion(parts=(0, 8, 0)),
        )
        assert len(warnings) == 1
        assert "--without_xid is deprecated and ignored" in warnings[0]

    def test_without_xid_no_warning_on_073(self):
        warnings = check_version_compatibility(
            _run_params(without_xid=True),
            VERSION_REGISTRY["0.7.3"],
            ToolVersion(parts=(0, 7, 3)),
        )
        assert warnings == []


class TestWorkflowNotes:
    def test_license_note_always_present(self):
        notes = " ".join(suggest_workflow("postgresql", "sqlserver")["notes"])
        assert "0.8.0+ needs a license file" in notes
        assert "MIGRATORXPRESS_LICENSE" in notes
        assert "engine/" in notes

    @pytest.mark.parametrize("source", ["oracle", "netezza"])
    def test_with_xid_note_for_oracle_and_netezza(self, source):
        notes = " ".join(suggest_workflow(source, "sqlserver")["notes"])
        assert "with_xid=true" in notes

    @pytest.mark.parametrize("source", ["postgresql", "sqlserver"])
    def test_no_with_xid_note_for_other_sources(self, source):
        notes = " ".join(suggest_workflow(source, "sqlserver")["notes"])
        assert "with_xid" not in notes


def _handler():
    builder = CommandBuilder("/nonexistent/MigratorXpress")
    return create_tools(
        builder, {"timeout": 60, "log_dir": "./logs", "path": "/nonexistent/MigratorXpress"}
    )


class TestTools:
    def test_schema_declares_with_xid(self):
        tools, _ = _handler()
        preview = next(t for t in tools if t.name == "migratorxpress_preview_command")
        props = preview.inputSchema["properties"]
        assert props["with_xid"]["type"] == "boolean"
        assert "0.8.0" in props["with_xid"]["description"]
        assert "Deprecated" in props["without_xid"]["description"]
        assert "MIGRATORXPRESS_LICENSE" in props["license_file"]["description"]

    @pytest.mark.asyncio
    async def test_preview_with_xid(self):
        _, handler = _handler()
        _content, structured = await handler(
            "migratorxpress_preview_command",
            {
                "auth_file": "/tmp/auth.json",
                "source_db_auth_id": "src",
                "target_db_auth_id": "tgt",
                "migration_db_auth_id": "mig",
                "task_list": ["transfer"],
                "with_xid": True,
            },
        )
        assert structured["status"] == "ok"
        assert "--with_xid" in structured["command"]
        # Latest known capabilities (0.8.2) apply in preview-only mode: no warning
        assert structured["warnings"] == []
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_preview_without_xid_warns_on_latest(self):
        _, handler = _handler()
        _content, structured = await handler(
            "migratorxpress_preview_command",
            {
                "auth_file": "/tmp/auth.json",
                "source_db_auth_id": "src",
                "target_db_auth_id": "tgt",
                "migration_db_auth_id": "mig",
                "task_list": ["transfer"],
                "without_xid": True,
            },
        )
        assert structured["status"] == "ok"
        assert "--without_xid" in structured["command"]
        assert any("deprecated and ignored" in w for w in structured["warnings"])

    @pytest.mark.asyncio
    async def test_version_action_reports_new_flags(self):
        _, handler = _handler()
        content = await handler("migratorxpress_info", {"action": "version"})
        text = content[0].text
        assert "with_xid (incremental skip is opt-in): Yes" in text
        assert "License Required: Yes" in text
