"""FastTransfer 0.18: DBPartition and Timepartition parallel methods,
FASTTRANSFER_DATEFORMAT, docs served at /latest/."""

import jsonschema
import pytest
from pydantic import ValidationError

from src.doc_search.cache import best_version_for
from src.doc_search.doc_versions import (
    DOC_URL_VERSION_ALIASES,
    DOC_URLS,
    FASTTRANSFER_DOC_VERSIONS,
)
from src.fasttransfer.command_builder import CommandBuilder
from src.fasttransfer.tools import PREVIEW_OUTPUT_SCHEMA, create_tools
from src.fasttransfer.validators import (
    DB_PARTITION_SOURCES,
    ParallelismMethod,
    TransferOptions,
    TransferRequest,
    normalize_timepartition_key,
)
from src.fasttransfer.version import VERSION_REGISTRY
from src.base.version_detector import ToolVersion


TARGET = {
    "type": "pgcopy",
    "server": "localhost:5432",
    "database": "tpch",
    "schema": "public",
    "table": "lineitem",
    "user": "postgres",
    "password": "pw",
}


def _source(source_type="mssql", **extra):
    src = {
        "type": source_type,
        "server": "localhost",
        "database": "tpch",
        "user": "u",
        "password": "p",
    }
    src.update(extra)
    return src


def _handler(builder=None):
    builder = builder or CommandBuilder("/nonexistent/FastTransfer")  # preview-only
    return create_tools(
        builder, {"timeout": 60, "log_dir": None, "path": "/nonexistent/FastTransfer"}
    )


def _text(result):
    content = result[0] if isinstance(result, tuple) else result
    return "\n".join(c.text for c in content)


class TestEnum:
    def test_new_methods_in_enum(self):
        assert ParallelismMethod("DBPartition") is ParallelismMethod.DB_PARTITION
        assert ParallelismMethod("Timepartition") is ParallelismMethod.TIMEPARTITION

    def test_enum_matches_latest_registry(self):
        """Every method the latest binary accepts is in the schema enum, and vice versa."""
        latest = VERSION_REGISTRY["0.18.1.0"]
        assert {m.value for m in ParallelismMethod} == set(latest.parallelism_methods)


class TestDBPartition:
    @pytest.mark.parametrize(
        "source_type",
        ["mssql", "msoledbsql", "pgsql", "pgcopy", "oraodp", "mysql", "teradata"],
    )
    def test_documented_sources_accepted(self, source_type):
        request = TransferRequest(
            source=_source(source_type, schema="dbo", table="lineitem_part"),
            target=TARGET,
            options={"method": "DBPartition", "degree": 8},
        )
        assert request.options.method == ParallelismMethod.DB_PARTITION

    def test_documented_source_set(self):
        assert DB_PARTITION_SOURCES == {
            "mssql", "msoledbsql", "pgsql", "pgcopy", "oraodp", "mysql", "teradata"
        }

    @pytest.mark.parametrize(
        "source_type",
        ["odbc", "oledb", "clickhouse", "hana", "duckdb", "duckdbstream", "nzsql", "nzoledb"],
    )
    def test_other_sources_rejected(self, source_type):
        with pytest.raises(ValidationError, match="DBPartition"):
            TransferRequest(
                source=_source(source_type, schema="s", table="t"),
                target=TARGET,
                options={"method": "DBPartition"},
            )

    def test_requires_schema(self):
        with pytest.raises(ValidationError, match="'schema' and 'table'"):
            TransferRequest(
                source=_source("pgsql", table="t"),
                target=TARGET,
                options={"method": "DBPartition"},
            )

    def test_query_rejected(self):
        with pytest.raises(ValidationError, match="'schema' and 'table'"):
            TransferRequest(
                source=_source("pgsql", schema="public", query="SELECT * FROM t"),
                target=TARGET,
                options={"method": "DBPartition"},
            )

    def test_no_distribute_key_needed(self):
        opts = TransferOptions(method="DBPartition")
        assert opts.distribute_key_column is None

    def test_command(self):
        builder = CommandBuilder("/nonexistent/FastTransfer")
        request = TransferRequest(
            source=_source("mssql", schema="dbo", table="lineitem_part"),
            target=TARGET,
            options={"method": "DBPartition", "degree": 8, "load_mode": "Truncate"},
        )
        cmd = builder.build_command(request)
        i = cmd.index("--method")
        assert cmd[i + 1] == "DBPartition"
        assert "--distributekeycolumn" not in cmd
        assert cmd[cmd.index("--sourceschema") + 1] == "dbo"
        assert cmd[cmd.index("--sourcetable") + 1] == "lineitem_part"


class TestTimepartition:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("(l_shipdate,year)", "(l_shipdate,year)"),
            ("(l_shipdate,year,month)", "(l_shipdate,year,month)"),
            ("(l_shipdate,year,month,day)", "(l_shipdate,year,month,day)"),
            ("(l_shipdate,year,week)", "(l_shipdate,year,week)"),
            ("( l_shipdate , Year, MONTH )", "(l_shipdate,year,month)"),
            ("(OrderDate,year,month)", "(OrderDate,year,month)"),
        ],
    )
    def test_valid_tuples(self, value, expected):
        assert normalize_timepartition_key(value) == expected
        opts = TransferOptions(method="Timepartition", distribute_key_column=value)
        assert opts.distribute_key_column == expected

    @pytest.mark.parametrize(
        "value",
        [
            "l_shipdate",
            "(l_shipdate)",
            "(l_shipdate,month)",
            "(l_shipdate,year,day)",
            "(l_shipdate,year,week,day)",
            "(l_shipdate,year,month,day,hour)",
            "(,year)",
            "l_shipdate,year,month",
        ],
    )
    def test_invalid_tuples(self, value):
        with pytest.raises(ValidationError, match="tuple"):
            TransferOptions(method="Timepartition", distribute_key_column=value)

    def test_requires_distribute_key(self):
        with pytest.raises(ValidationError, match="requires distribute_key_column"):
            TransferOptions(method="Timepartition")

    def test_tuple_not_enforced_for_other_methods(self):
        opts = TransferOptions(method="Ntile", distribute_key_column="l_orderkey")
        assert opts.distribute_key_column == "l_orderkey"

    @pytest.mark.parametrize("source_type", ["mssql", "pgsql", "oraodp", "clickhouse", "duckdb"])
    def test_query_source_accepted(self, source_type):
        request = TransferRequest(
            source=_source(source_type, query="SELECT * FROM dbo.lineitem WHERE l_shipmode = 'AIR'"),
            target=TARGET,
            options={"method": "Timepartition", "distribute_key_column": "(l_shipdate,year,month)"},
        )
        assert request.options.method == ParallelismMethod.TIMEPARTITION

    def test_command_passes_tuple_through(self):
        builder = CommandBuilder("/nonexistent/FastTransfer")
        request = TransferRequest(
            source=_source("mssql", query="SELECT * FROM dbo.lineitem"),
            target=TARGET,
            options={
                "method": "Timepartition",
                "distribute_key_column": "(l_shipdate, year, month)",
                "degree": 8,
            },
        )
        cmd = builder.build_command(request)
        assert cmd[cmd.index("--method") + 1] == "Timepartition"
        assert cmd[cmd.index("--distributekeycolumn") + 1] == "(l_shipdate,year,month)"


class TestPreviewTool:
    @pytest.mark.asyncio
    async def test_timepartition_preview_mentions_dateformat(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("mssql", query="SELECT * FROM dbo.lineitem"),
            "target": TARGET,
            "options": {"method": "Timepartition", "distribute_key_column": "(l_shipdate,year,month)"},
        })
        assert structured["status"] == "ok"
        assert "FASTTRANSFER_DATEFORMAT" in structured["explanation"]
        # Latest known capabilities (0.18.1) apply in preview-only mode: no warning
        assert structured["warnings"] == []
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_dbpartition_bad_source_is_validation_error(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("odbc", schema="dbo", table="t"),
            "target": TARGET,
            "options": {"method": "DBPartition"},
        })
        assert structured["status"] == "error"
        assert any("DBPartition" in e["message"] for e in structured["errors"])
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["DBPartition", "Timepartition"])
    async def test_warning_on_017_binary(self, method):
        builder = CommandBuilder("/nonexistent/FastTransfer")
        detector = builder.version_detector
        # Simulate a detected 0.17.1 binary
        detector._detected_version = ToolVersion.parse("0.17.1.0")
        detector._detection_done = True
        assert detector.capabilities is VERSION_REGISTRY["0.17.1.0"]
        _, handler = _handler(builder)
        options = {"method": method}
        source = _source("mssql", schema="dbo", table="lineitem_part")
        if method == "Timepartition":
            options["distribute_key_column"] = "(l_shipdate,year)"
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": source, "target": TARGET, "options": options,
        })
        assert structured["status"] == "ok"
        assert len(structured["warnings"]) == 1
        assert "0.18.0.0" in structured["warnings"][0]
        assert "0.17.1.0" in structured["warnings"][0]

    def test_schema_descriptions(self):
        tools, _ = _handler()
        preview = {t.name: t for t in tools}["fasttransfer_preview_transfer"]
        props = preview.inputSchema["properties"]["options"]["properties"]
        assert "DBPartition" in props["method"]["enum"]
        assert "Timepartition" in props["method"]["enum"]
        assert "DBPartition" in props["method"]["description"]
        assert "(order_date,year,month)" in props["method"]["description"]
        assert "Timepartition" in props["distribute_key_column"]["description"]


class TestWorkflowTips:
    @pytest.mark.asyncio
    async def test_teradata_tip(self):
        _, handler = _handler()
        text = _text(await handler(
            "fasttransfer_info",
            {"action": "workflow", "source_type": "teradata", "target_type": "pgcopy"},
        ))
        assert "DBPartition" in text and "PPI" in text

    @pytest.mark.asyncio
    async def test_partition_and_dateformat_tips(self):
        _, handler = _handler()
        text = _text(await handler(
            "fasttransfer_info",
            {"action": "workflow", "source_type": "pgsql", "target_type": "pgcopy"},
        ))
        assert "Timepartition" in text
        assert "FASTTRANSFER_DATEFORMAT" in text
        assert "DBPartition" in text

    @pytest.mark.asyncio
    async def test_msbulk_sorted_hint_tip(self):
        _, handler = _handler()
        text = _text(await handler(
            "fasttransfer_info",
            {"action": "workflow", "source_type": "pgsql", "target_type": "msbulk"},
        ))
        assert "FASTTRANSFER_SORTEDINPUT" in text
        assert "rowstore clustered index" in text


class TestDocVersions:
    def test_018_listed(self):
        assert FASTTRANSFER_DOC_VERSIONS[-1] == "0.18"
        assert "0.17" in FASTTRANSFER_DOC_VERSIONS

    def test_018_served_at_latest(self):
        aliases = DOC_URL_VERSION_ALIASES["fasttransfer"]
        assert aliases == {"0.18": "latest"}
        url = DOC_URLS["fasttransfer"].format(version=aliases.get("0.18", "0.18"))
        assert url == "https://fasttransfer-docs.arpe.io/latest/sitemap"

    def test_017_keeps_versioned_path(self):
        version = DOC_URL_VERSION_ALIASES["fasttransfer"].get("0.17", "0.17")
        assert DOC_URLS["fasttransfer"].format(version=version) == (
            "https://fasttransfer-docs.arpe.io/0.17/sitemap"
        )

    @pytest.mark.parametrize(
        "detected,expected",
        [("0.18.1.0", "0.18"), ("0.18.0.0", "0.18"), ("0.17.1.0", "0.17"), (None, "0.18")],
    )
    def test_detected_version_maps_to_doc_version(self, detected, expected):
        assert best_version_for("fasttransfer", detected, FASTTRANSFER_DOC_VERSIONS) == expected
