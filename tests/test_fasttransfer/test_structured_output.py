"""Structured-output contract tests for FastTransfer preview/execute tools."""

import jsonschema
import pytest

from src.fasttransfer.command_builder import CommandBuilder
from src.fasttransfer.tools import create_tools, PREVIEW_OUTPUT_SCHEMA, EXECUTE_OUTPUT_SCHEMA


def _handler():
    builder = CommandBuilder("/nonexistent/FastTransfer")  # preview-only mode
    _tools, handler = create_tools(builder, {"timeout": 60, "log_dir": None, "path": "/nonexistent/FastTransfer"})
    return _tools, handler


def test_preview_and_execute_declare_output_schema():
    tools, _ = _handler()
    by_name = {t.name: t for t in tools}
    assert by_name["fasttransfer_preview_transfer"].outputSchema is not None
    assert by_name["fasttransfer_execute_transfer"].outputSchema is not None


@pytest.mark.asyncio
async def test_preview_success_returns_structured_command():
    _, handler = _handler()
    content, structured = await handler("fasttransfer_preview_transfer", {
        "source": {"type": "pgsql", "database": "srcdb", "table": "orders",
                   "user": "u", "password": "secret123"},
        "target": {"type": "pgsql", "database": "dstdb", "table": "orders",
                   "user": "u2", "password": "secret456"},
    })
    assert structured["status"] == "ok"
    assert isinstance(structured["command"], list)
    assert structured["command"], "argv should be non-empty"
    assert structured["command_string"]
    assert "secret123" not in structured["command_display"] or "****" in structured["command_display"]
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)
    # The text block is still present and human-readable
    assert content[0].text.startswith("# FastTransfer Command Preview")


@pytest.mark.asyncio
async def test_preview_validation_error_still_conforms():
    _, handler = _handler()
    content, structured = await handler("fasttransfer_preview_transfer", {
        "source": {"type": "not_a_real_db", "database": "x"},
        "target": {"type": "pgsql", "database": "y"},
    })
    assert structured["status"] == "error"
    assert structured["errors"]
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)


@pytest.mark.asyncio
async def test_execute_blocked_without_binary_conforms():
    _, handler = _handler()
    _content, structured = await handler("fasttransfer_execute_transfer", {
        "command": "FastTransfer -t pgsql", "confirmation": True,
    })
    assert structured["status"] == "error"
    jsonschema.validate(structured, EXECUTE_OUTPUT_SCHEMA)
