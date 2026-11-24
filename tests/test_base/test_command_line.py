"""The command built by a preview tool reaches the binary unchanged (issue #4)."""

import importlib

import pytest

from src.base.command_line import command_input_schema, join_command, parse_command


ARGV = [
    r"D:\FastTransfer\FastTransfer.exe",
    "--sourceserver", r"host\SS2025",
    "--sourcedatabase", r"D:\MSSQL\BACKUPS",
    "--query", "SELECT a, b FROM t WHERE c = 'x' AND d = \"y\"",
    "--sourceconnectstring", r"adbc.arpemsbackup.path=D:\MSSQL\BACKUPS;adbc.arpemsbackup.file='a.bak;b.bak'",
    "--empty", "",
    "--unicode", "café $HOME `id` *.bak",
]


def test_join_then_parse_gives_back_the_argv():
    assert parse_command(join_command(ARGV)) == ARGV


def test_parse_accepts_the_argv_list():
    assert parse_command(list(ARGV)) == ARGV


@pytest.mark.parametrize("value", [None, 42, {"a": 1}, ["ok", 3]])
def test_parse_rejects_other_values(value):
    with pytest.raises(ValueError):
        parse_command(value)


@pytest.mark.parametrize("value", ["", "   ", []])
def test_parse_rejects_an_empty_command(value):
    with pytest.raises(ValueError):
        parse_command(value)


def test_parse_rejects_an_unclosed_quote():
    with pytest.raises(ValueError):
        parse_command("FastTransfer --query 'SELECT 1")


def test_input_schema_takes_a_list_or_a_string():
    schema = command_input_schema("x_preview")
    assert {"type": "array", "items": {"type": "string"}} in schema["anyOf"]
    assert {"type": "string"} in schema["anyOf"]
    assert "x_preview" in schema["description"]


# Each product: (package, binary name, execute tool name, extra config)
PRODUCTS = [
    ("fasttransfer", "FastTransfer", "fasttransfer_execute_transfer", {}),
    ("fastbcp", "FastBCP", "fastbcp_execute_export", {}),
    ("lakexpress", "LakeXpress", "lakexpress_execute_command", {"log_dir": "./logs", "fastbcp_dir_path": ""}),
    ("migratorxpress", "MigratorXpress", "migratorxpress_execute_command", {"log_dir": "./logs"}),
]


def _executing_handler(package, binary, extra):
    """A handler whose builder runs nothing and records the argv it would run."""
    builder_module = importlib.import_module(f"src.{package}.command_builder")
    tools_module = importlib.import_module(f"src.{package}.tools")
    builder = builder_module.CommandBuilder(f"/nonexistent/{binary}")
    builder._preview_only = False
    calls = []

    def fake_execute(command, timeout=None, log_dir=None):
        calls.append(command)
        return 0, "done", ""

    builder.execute_command = fake_execute
    config = {"timeout": 60, "log_dir": None, "path": f"/nonexistent/{binary}", **extra}
    tools, handler = tools_module.create_tools(builder, config)
    return tools, handler, calls


@pytest.mark.parametrize("package,binary,tool,extra", PRODUCTS)
@pytest.mark.parametrize("form", ["list", "string"])
@pytest.mark.asyncio
async def test_execute_runs_the_previewed_argv(package, binary, tool, extra, form):
    _tools, handler, calls = _executing_handler(package, binary, extra)
    command = ARGV if form == "list" else join_command(ARGV)
    _content, structured = await handler(tool, {"command": command, "confirmation": True})
    assert structured["status"] == "ok"
    assert calls == [ARGV]


@pytest.mark.parametrize("package,binary,tool,extra", PRODUCTS)
def test_execute_tool_declares_list_or_string(package, binary, tool, extra):
    tools, _handler, _calls = _executing_handler(package, binary, extra)
    schema = {t.name: t for t in tools}[tool].inputSchema["properties"]["command"]
    assert schema["anyOf"] == command_input_schema("x")["anyOf"]


@pytest.mark.parametrize("package,binary,tool,extra", PRODUCTS)
@pytest.mark.asyncio
async def test_execute_reports_an_unparsable_command(package, binary, tool, extra):
    _tools, handler, calls = _executing_handler(package, binary, extra)
    _content, structured = await handler(tool, {"command": "x --query 'SELECT 1", "confirmation": True})
    assert structured["status"] == "error"
    assert calls == []


@pytest.mark.asyncio
async def test_fasttransfer_preview_string_runs_unchanged():
    """End to end: the backup source of FastTransfer 1.1 takes a Windows directory in `server`."""
    _tools, handler, calls = _executing_handler("fasttransfer", "FastTransfer", {})
    _content, preview = await handler("fasttransfer_preview_transfer", {
        "source": {"type": "mssql", "server": r"host\SS2025", "database": "src", "trusted_auth": True,
                   "query": "SELECT a, b FROM dbo.t WHERE c = 'x'"},
        "target": {"type": "pgsql", "server": "localhost:5432", "database": "dst", "schema": "public",
                   "table": "t", "user": "u", "password": "p w"},
        "options": {"method": "None"},
    })
    assert preview["status"] == "ok", preview
    _content, result = await handler(
        "fasttransfer_execute_transfer", {"command": preview["command_string"], "confirmation": True}
    )
    assert result["status"] == "ok"
    assert calls == [preview["command"]]
    assert r"host\SS2025" in calls[0]
    assert "SELECT a, b FROM dbo.t WHERE c = 'x'" in calls[0]
