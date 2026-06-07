"""Structured-output contract tests for MigratorXpress preview/execute tools."""

import jsonschema
import pytest

from src.migratorxpress.command_builder import CommandBuilder
from src.migratorxpress.tools import (
    create_tools,
    PREVIEW_OUTPUT_SCHEMA,
    EXECUTE_OUTPUT_SCHEMA,
)


def _handler():
    builder = CommandBuilder("/nonexistent/MigratorXpress")  # preview-only mode
    _tools, handler = create_tools(
        builder, {"timeout": 60, "log_dir": "./logs", "path": "/nonexistent/MigratorXpress"}
    )
    return _tools, handler


def test_preview_and_execute_declare_output_schema():
    tools, _ = _handler()
    by_name = {t.name: t for t in tools}
    assert by_name["migratorxpress_preview_command"].outputSchema is not None
    assert by_name["migratorxpress_execute_command"].outputSchema is not None


@pytest.mark.asyncio
async def test_preview_success_returns_structured_command():
    _, handler = _handler()
    content, structured = await handler("migratorxpress_preview_command", {
        "auth_file": "/tmp/auth.json",
        "source_db_auth_id": "src",
        "source_db_name": "srcdb",
        "target_db_auth_id": "tgt",
        "target_db_name": "tgtdb",
        "migration_db_auth_id": "mig",
        "task_list": ["translate", "create", "transfer"],
    })
    assert structured["status"] == "ok"
    assert isinstance(structured["command"], list)
    assert structured["command"], "argv should be non-empty"
    assert structured["command_string"]
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)
    # The text block is still present and human-readable
    assert content[0].text.startswith("# MigratorXpress Command Preview")


@pytest.mark.asyncio
async def test_preview_validation_error_still_conforms():
    _, handler = _handler()
    content, structured = await handler("migratorxpress_preview_command", {
        "auth_file": "/tmp/auth.json",
        "source_db_auth_id": "src",
        "source_db_name": "srcdb",
        "target_db_auth_id": "tgt",
        "target_db_name": "tgtdb",
        "migration_db_auth_id": "mig",
        "task_list": ["not_a_real_task"],
    })
    assert structured["status"] == "error"
    assert structured["errors"]
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)


@pytest.mark.asyncio
async def test_execute_blocked_without_binary_conforms():
    _, handler = _handler()
    _content, structured = await handler("migratorxpress_execute_command", {
        "command": "MigratorXpress --auth-file /tmp/auth.json", "confirmation": True,
    })
    assert structured["status"] == "error"
    jsonschema.validate(structured, EXECUTE_OUTPUT_SCHEMA)
