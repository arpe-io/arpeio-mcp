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


def _tool(tools, name):
    return next(t for t in tools if t.name == name)


def test_input_schema_lists_new_source_types():
    tools, _ = _handler()
    preview = _tool(tools, "fastbcp_preview_export")
    enum = preview.inputSchema["properties"]["source"]["properties"]["type"]["enum"]
    assert {"adbc_db2", "backup_mssql", "backup_rman"} <= set(enum)
    assert "hanaexport" not in enum
    info = _tool(tools, "fastbcp_info")
    assert "adbc_db2" in info.inputSchema["properties"]["source_type"]["enum"]


@pytest.mark.asyncio
async def test_preview_backup_auto_suggests_physical():
    _, handler = _handler()
    content, structured = await handler("fastbcp_preview_export", {
        "source": {"type": "backup_mssql", "server": "/var/opt/mssql/backup",
                   "database": "tpch_*.bak", "schema": "dbo", "table": "orders"},
        "output": {"format": "parquet", "directory": "/export/orders"},
    })
    assert structured["status"] == "ok", structured
    assert structured["auto_parallelism"] == "Physical"
    assert "--sourceuser" not in structured["command"]
    assert "backup files 'tpch_*.bak' in /var/opt/mssql/backup" in structured["explanation"]
    jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)


@pytest.mark.asyncio
async def test_preview_backup_query_not_auto_physical():
    _, handler = _handler()
    _content, structured = await handler("fastbcp_preview_export", {
        "source": {"type": "backup_rman", "server": "D:\\Backups\\RMAN", "database": "full_*;ctl_*",
                   "query": "SELECT ID, NAME, CREATED FROM HRPDB.HR.EMPLOYEES"},
        "output": {"format": "parquet", "directory": "D:\\export"},
    })
    assert structured["status"] == "ok", structured
    assert "auto_parallelism" not in structured
    assert structured["command"][structured["command"].index("--method") + 1] == "None"


@pytest.mark.asyncio
async def test_preview_backup_rejects_ntile():
    _, handler = _handler()
    _content, structured = await handler("fastbcp_preview_export", {
        "source": {"type": "backup_rman", "server": "/b", "database": "full_*",
                   "schema": "SALES", "table": "ORDERS"},
        "output": {"format": "parquet", "directory": "/o"},
        "options": {"method": "Ntile", "distribute_key_column": "ID", "degree": 4},
    })
    assert structured["status"] == "error"
    assert "Ntile method is not available with connection type backup_rman" in structured["errors"][0]["message"]


@pytest.mark.asyncio
async def test_preview_lowercase_method_canonical():
    _, handler = _handler()
    _content, structured = await handler("fastbcp_preview_export", {
        "source": {"type": "adbc_db2", "server": "db2-host", "database": "SAMPLE",
                   "schema": "S", "table": "T", "user": "u", "password": "p"},
        "output": {"format": "parquet", "directory": "/o"},
        "options": {"method": "dbpartition", "degree": 4},
    })
    assert structured["status"] == "ok", structured
    assert structured["command"][structured["command"].index("--method") + 1] == "DBPartition"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_type,expected",
    [
        ("adbc_db2", ["ArpeDb2", "50000", "DBPartition", "trusted authentication is refused"]),
        ("backup_mssql", ["Pages", "private key", "FASTBCP_BACKUP_RESTORE_POINT"]),
        ("backup_rman", ["Pages", "l0/*;l1/**", "No credentials"]),
    ],
)
async def test_workflow_tips_for_new_types(source_type, expected):
    _, handler = _handler()
    content = await handler("fastbcp_info", {
        "action": "workflow", "source_type": source_type, "output_format": "parquet",
    })
    text = content[0].text
    for fragment in expected:
        assert fragment in text, fragment


@pytest.mark.asyncio
async def test_formats_notes_parquet_only_types():
    _, handler = _handler()
    content = await handler("fastbcp_info", {"action": "formats"})
    text = content[0].text
    assert "backup_mssql" in text and "adbc_db2" in text
    assert "All source databases support all output formats" not in text
