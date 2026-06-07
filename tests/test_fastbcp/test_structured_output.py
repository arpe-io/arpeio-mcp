"""Structured-output contract tests for FastBCP preview/execute tools."""

import jsonschema
import pytest

from src.fastbcp.command_builder import CommandBuilder
from src.fastbcp.tools import create_tools, PREVIEW_OUTPUT_SCHEMA, EXECUTE_OUTPUT_SCHEMA


def _handler():
    builder = CommandBuilder("/nonexistent/FastBCP")  # preview-only mode
    _tools, handler = create_tools(builder, {"timeout": 60, "log_dir": None, "path": "/nonexistent/FastBCP"})
    return _tools, handler


def test_preview_and_execute_declare_output_schema():
    tools, _ = _handler()
    by_name = {t.name: t for t in tools}
    assert by_name["fastbcp_preview_export"].outputSchema is not None
    assert by_name["fastbcp_execute_export"].outputSchema is not None


@pytest.mark.asyncio
async def test_preview_success_returns_structured_command():
    _, handler = _handler()
    content, structured = await handler("fastbcp_preview_export", {
        "source": {"type": "pgsql", "database": "mydb", "table": "orders",
                   "user": "u", "password": "secret123"},
        "output": {"format": "parquet", "directory": "/tmp/out"},
    })
    assert structured["status"] == "ok"
    assert isinstance(structured["command"], list)
    assert structured["command"], "argv should be non-empty"
    assert structured["command_string"]
    assert "password" not in structured["command_display"].lower() or "****" in structured["command_display"]
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)
    # The text block is still present and human-readable
    assert content[0].text.startswith("# FastBCP Command Preview")


@pytest.mark.asyncio
async def test_preview_validation_error_still_conforms():
    _, handler = _handler()
    content, structured = await handler("fastbcp_preview_export", {
        "source": {"type": "not_a_real_db", "database": "x"},
        "output": {"format": "parquet"},
    })
    assert structured["status"] == "error"
    assert structured["errors"]
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)


@pytest.mark.asyncio
async def test_execute_blocked_without_binary_conforms():
    _, handler = _handler()
    _content, structured = await handler("fastbcp_execute_export", {
        "command": "FastBCP -t pgsql", "confirmation": True,
    })
    assert structured["status"] == "error"
    jsonschema.validate(structured, EXECUTE_OUTPUT_SCHEMA)
