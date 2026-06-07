"""Structured-output contract tests for LakeXpress preview/execute tools."""

import jsonschema
import pytest

from src.lakexpress.command_builder import CommandBuilder
from src.lakexpress.tools import create_tools, PREVIEW_OUTPUT_SCHEMA, EXECUTE_OUTPUT_SCHEMA


def _handler():
    builder = CommandBuilder("/nonexistent/LakeXpress")  # preview-only mode
    _tools, handler = create_tools(
        builder,
        {"timeout": 60, "log_dir": "./logs", "path": "/nonexistent/LakeXpress", "fastbcp_dir_path": ""},
    )
    return _tools, handler


def test_preview_and_execute_declare_output_schema():
    tools, _ = _handler()
    by_name = {t.name: t for t in tools}
    assert by_name["lakexpress_preview_command"].outputSchema is not None
    assert by_name["lakexpress_execute_command"].outputSchema is not None


@pytest.mark.asyncio
async def test_preview_success_returns_structured_command():
    _, handler = _handler()
    content, structured = await handler("lakexpress_preview_command", {
        "command": "status",
        "status": {"auth_file": "/tmp/auth.json", "log_db_auth_id": "logdb"},
    })
    assert structured["status"] == "ok"
    assert isinstance(structured["command"], list)
    assert structured["command"], "argv should be non-empty"
    assert structured["command_string"]
    assert structured["command_type"] == "status"
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)
    # The text block is still present and human-readable
    assert content[0].text.startswith("# LakeXpress Command Preview")


@pytest.mark.asyncio
async def test_preview_validation_error_still_conforms():
    _, handler = _handler()
    content, structured = await handler("lakexpress_preview_command", {
        "command": "status",
        # Missing required params (auth_file, log_db_auth_id) for the status command
        "status": {},
    })
    assert structured["status"] == "error"
    assert structured["errors"]
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)


@pytest.mark.asyncio
async def test_execute_blocked_without_binary_conforms():
    _, handler = _handler()
    _content, structured = await handler("lakexpress_execute_command", {
        "command": "LakeXpress status", "confirmation": True,
    })
    assert structured["status"] == "error"
    jsonschema.validate(structured, EXECUTE_OUTPUT_SCHEMA)
