"""FastTransfer 1.0: ADBC connection types (adbc_*), the Physical parallel
method, deprecated engine-specific methods, docs 1.0 served at /latest/."""

import jsonschema
import pytest
from pydantic import ValidationError

from src.base.version_detector import ToolVersion
from src.doc_search.cache import best_version_for
from src.doc_search.doc_versions import (
    DOC_URL_VERSION_ALIASES,
    DOC_URLS,
    FASTTRANSFER_DOC_VERSIONS,
)
from src.fasttransfer.command_builder import (
    CommandBuilder,
    get_supported_combinations,
    suggest_parallelism_method,
)
from src.fasttransfer.tools import PREVIEW_OUTPUT_SCHEMA, create_tools
from src.fasttransfer.validators import (
    ADBC_TYPES,
    DB_PARTITION_SOURCES,
    PHYSICAL_STRATEGIES,
    ConnectionConfig,
    ParallelismMethod,
    SourceConnectionType,
    TargetConnectionType,
    TransferRequest,
    native_source_type,
)
from src.fasttransfer.version import VERSION_REGISTRY, check_version_compatibility
from src.instructions import INSTRUCTIONS as SERVER_INSTRUCTIONS

ADBC = [
    "adbc_mssql",
    "adbc_pgsql",
    "adbc_oracle",
    "adbc_netezza",
    "adbc_db2",
    "adbc_teradata",
]

TARGET = {
    "type": "msbulk",
    "server": "sqlhost",
    "database": "dwh",
    "schema": "dbo",
    "table": "orders",
    "user": "u",
    "password": "p",
}


def _source(source_type="pgsql", **extra):
    src = {
        "type": source_type,
        "server": "dbhost",
        "database": "tpch",
        "user": "u",
        "password": "p",
    }
    src.update(extra)
    return src


def _target(target_type="msbulk", **extra):
    tgt = dict(TARGET, type=target_type)
    tgt.update(extra)
    return tgt


def _builder(version=None):
    """Preview-only builder; ``version`` simulates a detected binary."""
    builder = CommandBuilder("/nonexistent/FastTransfer")
    if version:
        detector = builder.version_detector
        detector._detected_version = ToolVersion.parse(version)
        detector._detection_done = True
        assert detector.capabilities is VERSION_REGISTRY[version]
    return builder


def _handler(version=None):
    return create_tools(
        _builder(version),
        {"timeout": 60, "log_dir": None, "path": "/nonexistent/FastTransfer"},
    )


def _text(result):
    content = result[0] if isinstance(result, tuple) else result
    return "\n".join(c.text for c in content)


def _errors(exc_info):
    return " | ".join(str(e["msg"]) for e in exc_info.value.errors())


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #


class TestRegistry:
    def test_100_is_registered(self):
        """1.0.0.0 was the latest known version until 1.1.0.0 (see test_v110)."""
        assert "1.0.0.0" in VERSION_REGISTRY

    def test_100_adds_adbc_types_on_both_sides(self):
        caps = VERSION_REGISTRY["1.0.0.0"]
        assert set(ADBC) <= caps.source_types
        assert set(ADBC) <= caps.target_types
        old = VERSION_REGISTRY["0.18.2.0"]
        assert not (set(ADBC) & old.source_types)
        assert not (set(ADBC) & old.target_types)

    def test_100_adds_physical_and_keeps_deprecated_names(self):
        caps = VERSION_REGISTRY["1.0.0.0"]
        old = VERSION_REGISTRY["0.18.2.0"]
        assert caps.parallelism_methods - old.parallelism_methods == {"Physical"}
        assert {"Ctid", "Rowid", "Physloc", "NZDataSlice"} <= caps.parallelism_methods

    def test_100_requires_license(self):
        assert VERSION_REGISTRY["1.0.0.0"].requires_license is True

    def test_enums_cover_the_100_registry(self):
        """The schema enums hold what the 1.0.0 CLI accepts (1.1 adds the backup sources, see test_v110)."""
        caps = VERSION_REGISTRY["1.0.0.0"]
        assert {t.value for t in SourceConnectionType} - set(caps.source_types) == {
            "backup_mssql",
            "backup_rman",
        }
        assert set(caps.source_types) <= {t.value for t in SourceConnectionType}
        assert {t.value for t in TargetConnectionType} == set(caps.target_types)
        assert {m.value for m in ParallelismMethod} == set(caps.parallelism_methods)

    def test_100_source_types_use_nzbulk(self):
        caps = VERSION_REGISTRY["1.0.0.0"]
        assert "nzbulk" in caps.source_types
        assert "nzcopy" not in caps.source_types


# --------------------------------------------------------------------------- #
# ADBC connection types
# --------------------------------------------------------------------------- #


class TestAdbcTypes:
    def test_adbc_type_set(self):
        assert ADBC_TYPES == frozenset(ADBC)

    @pytest.mark.parametrize(
        "adbc,native",
        [
            ("adbc_mssql", "mssql"),
            ("adbc_pgsql", "pgsql"),
            ("adbc_oracle", "oraodp"),
            ("adbc_netezza", "nzsql"),
            ("adbc_teradata", "teradata"),
            ("adbc_db2", "adbc_db2"),  # no native counterpart
            ("mysql", "mysql"),
        ],
    )
    def test_native_source_type(self, adbc, native):
        assert native_source_type(adbc) == native

    @pytest.mark.parametrize("source_type", ADBC)
    @pytest.mark.parametrize("target_type", ADBC)
    def test_any_adbc_pair_accepted(self, source_type, target_type):
        request = TransferRequest(
            source=_source(source_type, schema="S", table="T"),
            target=_target(target_type),
        )
        assert request.source.type == source_type
        assert request.target.type == target_type

    @pytest.mark.parametrize("target_type", ["msbulk", "pgcopy", "orabulk", "duckdb"])
    def test_adbc_source_with_native_target_rejected(self, target_type):
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source=_source("adbc_db2", schema="S", table="T"),
                target=_target(target_type),
            )
        assert "cannot be mixed" in _errors(exc)
        assert "adbc_db2" in _errors(exc)

    @pytest.mark.parametrize("source_type", ["mssql", "pgsql", "duckdbstream", "odbc"])
    def test_native_source_with_adbc_target_rejected(self, source_type):
        extra = {"dsn": "MyDsn", "server": None} if source_type == "odbc" else {}
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source=_source(source_type, schema="S", table="T", **extra),
                target=_target("adbc_pgsql"),
            )
        assert "cannot be mixed" in _errors(exc)

    def test_work_tables_refused_with_adbc_target(self):
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source=_source("adbc_pgsql", schema="public", table="orders"),
                target=_target("adbc_mssql"),
                options={"use_work_tables": True},
            )
        assert "use_work_tables" in _errors(exc)

    def test_work_tables_false_accepted_with_adbc_target(self):
        request = TransferRequest(
            source=_source("adbc_pgsql", schema="public", table="orders"),
            target=_target("adbc_mssql"),
            options={"use_work_tables": False},
        )
        assert request.options.use_work_tables is False

    def test_netezza_refuses_trusted_auth(self):
        with pytest.raises(ValidationError) as exc:
            ConnectionConfig(
                type="adbc_netezza", server="nz", database="db", trusted_auth=True
            )
        assert "adbc_netezza" in _errors(exc)
        assert "trusted" in _errors(exc).lower()

    @pytest.mark.parametrize(
        "adbc_type", ["adbc_mssql", "adbc_pgsql", "adbc_oracle", "adbc_db2", "adbc_teradata"]
    )
    def test_other_adbc_types_accept_trusted_auth(self, adbc_type):
        config = ConnectionConfig(
            type=adbc_type, server="h", database="db", trusted_auth=True
        )
        assert config.trusted_auth is True

    def test_adbc_command(self):
        request = TransferRequest(
            source=_source("adbc_db2", server="dbhost:50000", schema="TPCH", table="ORDERS"),
            target=_target("adbc_mssql", server="localhost\\SS2025"),
            options={"method": "RangeId", "distribute_key_column": "o_orderkey", "degree": 4},
        )
        cmd = _builder().build_command(request)
        assert cmd[cmd.index("--sourceconnectiontype") + 1] == "adbc_db2"
        assert cmd[cmd.index("--targetconnectiontype") + 1] == "adbc_mssql"
        assert cmd[cmd.index("--sourceserver") + 1] == "dbhost:50000"
        assert cmd[cmd.index("--method") + 1] == "RangeId"

    def test_adbc_connect_string_is_masked(self):
        request = TransferRequest(
            source={
                "type": "adbc_pgsql",
                "database": "tpch",
                "schema": "public",
                "table": "orders",
                "connect_string": "uri=postgresql://u:secret@pghost:5432/tpch",
            },
            target=_target("adbc_mssql"),
        )
        builder = _builder()
        display = builder.format_command_display(builder.build_command(request), mask=True)
        assert "secret" not in display


# --------------------------------------------------------------------------- #
# Physical and the engine-specific methods
# --------------------------------------------------------------------------- #


class TestPhysical:
    def test_strategies(self):
        # 1.1 adds Pages for the backup sources (see test_v110)
        engines = {k: v for k, v in PHYSICAL_STRATEGIES.items() if v != "Pages"}
        assert engines == {
            "pgsql": "Ctid",
            "pgcopy": "Ctid",
            "oraodp": "Rowid",
            "mssql": "Physloc",
            "msoledbsql": "Physloc",
            "nzsql": "NZDataSlice",
            "nzbulk": "NZDataSlice",
            "nzoledb": "NZDataSlice",
        }

    @pytest.mark.parametrize(
        "source_type",
        ["pgsql", "pgcopy", "oraodp", "mssql", "msoledbsql", "nzsql", "nzbulk", "nzoledb"],
    )
    def test_native_sources_accepted(self, source_type):
        request = TransferRequest(
            source=_source(source_type, schema="s", table="t"),
            target=TARGET,
            options={"method": "Physical"},
        )
        assert request.options.method is ParallelismMethod.PHYSICAL

    @pytest.mark.parametrize(
        "source_type", ["adbc_pgsql", "adbc_oracle", "adbc_mssql", "adbc_netezza"]
    )
    def test_adbc_sources_accepted(self, source_type):
        request = TransferRequest(
            source=_source(source_type, schema="s", table="t"),
            target=_target("adbc_pgsql"),
            options={"method": "Physical"},
        )
        assert request.options.method is ParallelismMethod.PHYSICAL

    @pytest.mark.parametrize(
        "source_type", ["mysql", "teradata", "hana", "clickhouse", "duckdb"]
    )
    def test_other_native_sources_rejected(self, source_type):
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source=_source(source_type, schema="s", table="t"),
                target=TARGET,
                options={"method": "Physical"},
            )
        assert "Physical" in _errors(exc)
        assert source_type in _errors(exc)

    @pytest.mark.parametrize("source_type", ["adbc_db2", "adbc_teradata"])
    def test_other_adbc_sources_rejected(self, source_type):
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source=_source(source_type, schema="s", table="t"),
                target=_target("adbc_mssql"),
                options={"method": "Physical"},
            )
        assert "Physical" in _errors(exc)

    def test_odbc_rejected_with_hint(self):
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source={"type": "odbc", "dsn": "MsDsn", "database": "db", "schema": "dbo", "table": "t"},
                target=TARGET,
                options={"method": "Physical"},
            )
        assert "cannot identify" in _errors(exc)

    @pytest.mark.parametrize(
        "method,source_type",
        [
            ("Physical", "pgsql"),
            ("Physical", "oraodp"),
            ("Physical", "mssql"),
            ("Ctid", "pgsql"),
            ("Rowid", "oraodp"),
            ("Physloc", "mssql"),
        ],
    )
    def test_schema_and_table_required(self, method, source_type):
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source=_source(source_type, query="select * from t"),
                target=TARGET,
                options={"method": method},
            )
        assert "'schema' and 'table'" in _errors(exc)

    @pytest.mark.parametrize("method", ["Physical", "NZDataSlice"])
    @pytest.mark.parametrize("source_type", ["nzsql", "nzbulk", "nzoledb"])
    def test_netezza_needs_no_schema_or_table(self, method, source_type):
        request = TransferRequest(
            source=_source(source_type, query="select * from orders"),
            target=TARGET,
            options={"method": method},
        )
        assert request.source.table is None

    @pytest.mark.parametrize("method", ["Physical", "Ctid"])
    def test_query_can_filter_the_table(self, method):
        request = TransferRequest(
            source=_source(
                "pgsql",
                schema="public",
                table="orders",
                query="select * from public.orders where o_orderdate >= '1995-01-01'",
            ),
            target=TARGET,
            options={"method": method},
        )
        cmd = _builder().build_command(request)
        assert cmd[cmd.index("--sourcetable") + 1] == "orders"
        assert "1995-01-01" in cmd[cmd.index("--query") + 1]

    @pytest.mark.parametrize("method", ["None", "Ntile", "RangeId", "DBPartition"])
    def test_table_and_query_still_exclusive_for_other_methods(self, method):
        options = {"method": method}
        if method in ("Ntile", "RangeId"):
            options["distribute_key_column"] = "o_orderkey"
        with pytest.raises(ValidationError):
            TransferRequest(
                source=_source("pgsql", schema="public", table="orders", query="select 1"),
                target=TARGET,
                options=options,
            )

    @pytest.mark.parametrize(
        "method,source_type,target_type",
        [
            ("Ctid", "adbc_pgsql", "adbc_mssql"),
            ("Rowid", "adbc_oracle", "adbc_mssql"),
            ("Physloc", "adbc_mssql", "adbc_pgsql"),
            ("NZDataSlice", "adbc_netezza", "adbc_pgsql"),
            ("NZDataSlice", "nzbulk", "msbulk"),
            ("Physloc", "msoledbsql", "msbulk"),
        ],
    )
    def test_deprecated_names_follow_the_10_source_lists(self, method, source_type, target_type):
        request = TransferRequest(
            source=_source(source_type, schema="s", table="t"),
            target=_target(target_type),
            options={"method": method},
        )
        assert request.options.method.value == method

    def test_physloc_still_accepted_through_odbc(self):
        request = TransferRequest(
            source={"type": "odbc", "dsn": "MsDsn", "database": "db", "schema": "dbo", "table": "t"},
            target=TARGET,
            options={"method": "Physloc"},
        )
        assert request.options.method is ParallelismMethod.PHYSLOC

    def test_physloc_rejected_through_oledb(self):
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source={
                    "type": "oledb", "provider": "MSOLEDBSQL", "database": "db",
                    "schema": "dbo", "table": "t", "user": "u", "password": "p",
                },
                target=TARGET,
                options={"method": "Physloc"},
            )
        assert "Physloc" in _errors(exc)

    def test_physical_needs_no_distribute_key(self):
        request = TransferRequest(
            source=_source("pgsql", schema="public", table="orders"),
            target=TARGET,
            options={"method": "Physical"},
        )
        cmd = _builder().build_command(request)
        assert cmd[cmd.index("--method") + 1] == "Physical"
        assert "--distributekeycolumn" not in cmd


class TestDBPartitionSources:
    def test_source_set(self):
        assert DB_PARTITION_SOURCES == frozenset(
            ["mssql", "msoledbsql", "pgsql", "pgcopy", "oraodp", "mysql", "teradata", "adbc_db2"]
        )

    @pytest.mark.parametrize(
        "source_type",
        ["adbc_mssql", "adbc_pgsql", "adbc_oracle", "adbc_teradata", "adbc_db2"],
    )
    def test_adbc_sources_accepted(self, source_type):
        request = TransferRequest(
            source=_source(source_type, schema="S", table="T"),
            target=_target("adbc_mssql"),
            options={"method": "DBPartition"},
        )
        assert request.options.method is ParallelismMethod.DB_PARTITION

    def test_adbc_netezza_rejected(self):
        with pytest.raises(ValidationError) as exc:
            TransferRequest(
                source=_source("adbc_netezza", schema="S", table="T"),
                target=_target("adbc_mssql"),
                options={"method": "DBPartition"},
            )
        assert "DBPartition" in _errors(exc)


# --------------------------------------------------------------------------- #
# Command builder
# --------------------------------------------------------------------------- #


class TestWorkTablesFlag:
    def _command(self, **options):
        request = TransferRequest(
            source=_source("pgsql", schema="public", table="orders"),
            target=TARGET,
            options=options,
        )
        return _builder().build_command(request)

    def test_true_emits_the_switch(self):
        assert "--useworktables" in self._command(use_work_tables=True)

    def test_false_does_not_emit_the_switch(self):
        assert "--useworktables" not in self._command(use_work_tables=False)

    def test_unset_does_not_emit_the_switch(self):
        assert "--useworktables" not in self._command()


class TestSuggestion:
    @pytest.mark.parametrize(
        "source_type,engine",
        [
            ("pgsql", "Ctid"),
            ("pgcopy", "Ctid"),
            ("adbc_pgsql", "Ctid"),
            ("oraodp", "Rowid"),
            ("adbc_oracle", "Rowid"),
            ("nzsql", "NZDataSlice"),
            ("nzbulk", "NZDataSlice"),
            ("adbc_netezza", "NZDataSlice"),
            ("mssql", "Physloc"),
            ("adbc_mssql", "Physloc"),
        ],
    )
    def test_physical_on_10(self, source_type, engine):
        suggestion = suggest_parallelism_method(source_type, False, False, "large")
        assert suggestion["method"] == "Physical"
        assert engine in suggestion["explanation"]

    @pytest.mark.parametrize(
        "source_type,engine",
        [("pgsql", "Ctid"), ("oraodp", "Rowid"), ("nzsql", "NZDataSlice"), ("mssql", "Physloc")],
    )
    def test_engine_name_before_10(self, source_type, engine):
        suggestion = suggest_parallelism_method(
            source_type, False, False, "large", physical_supported=False
        )
        assert suggestion["method"] == engine

    @pytest.mark.parametrize("source_type", ["msoledbsql", "nzbulk", "nzoledb"])
    def test_types_accepted_for_the_physical_split_from_10_only(self, source_type):
        suggestion = suggest_parallelism_method(
            source_type, False, False, "large", physical_supported=False
        )
        assert suggestion["method"] == "DataDriven"

    @pytest.mark.parametrize("source_type", ["odbc", "oledb"])
    @pytest.mark.parametrize("physical_supported", [True, False])
    def test_generic_drivers_never_get_a_physical_method(self, source_type, physical_supported):
        suggestion = suggest_parallelism_method(
            source_type, False, False, "large", physical_supported=physical_supported
        )
        assert suggestion["method"] in ("DataDriven", "Ntile")

    @pytest.mark.parametrize("source_type", ["adbc_db2", "adbc_teradata", "teradata", "mysql"])
    def test_engines_without_physical(self, source_type):
        suggestion = suggest_parallelism_method(source_type, False, False, "large")
        assert suggestion["method"] == "DataDriven"

    def test_small_table_stays_single_stream(self):
        assert suggest_parallelism_method("adbc_pgsql", False, False, "small")["method"] == "None"


class TestCombinations:
    def test_adbc_entry(self):
        combos = get_supported_combinations()
        adbc_keys = [k for k in combos if k.startswith("ADBC")]
        assert len(adbc_keys) == 1
        targets = " ".join(combos[adbc_keys[0]])
        for adbc_type in ADBC:
            assert adbc_type in adbc_keys[0]
            assert adbc_type in targets


# --------------------------------------------------------------------------- #
# Version compatibility warnings
# --------------------------------------------------------------------------- #


def _warnings(params, version):
    return check_version_compatibility(
        params, VERSION_REGISTRY[version], ToolVersion.parse(version)
    )


class TestVersionWarnings:
    @pytest.mark.parametrize("version", ["0.17.1.0", "0.18.2.0"])
    def test_adbc_types_need_10(self, version):
        warnings = _warnings(
            {"source": {"type": "adbc_pgsql"}, "target": {"type": "adbc_mssql"}}, version
        )
        text = " ".join(warnings)
        assert "adbc_pgsql" in text and "adbc_mssql" in text
        assert "1.0.0.0" in text and version in text

    def test_adbc_types_no_warning_on_10(self):
        assert _warnings(
            {"source": {"type": "adbc_pgsql"}, "target": {"type": "adbc_mssql"}}, "1.0.0.0"
        ) == []

    def test_physical_needs_10(self):
        warnings = _warnings(
            {"source": {"type": "pgsql", "schema": "s", "table": "t"},
             "options": {"method": "Physical"}},
            "0.18.2.0",
        )
        assert len(warnings) == 1
        assert "Physical" in warnings[0] and "1.0.0.0" in warnings[0]
        assert "Ctid" in warnings[0]

    @pytest.mark.parametrize("method", ["Ctid", "Rowid", "Physloc", "NZDataSlice"])
    def test_engine_names_deprecated_on_10(self, method):
        warnings = _warnings(
            {"source": {"type": "pgsql", "schema": "s", "table": "t"},
             "options": {"method": method}},
            "1.0.0.0",
        )
        assert len(warnings) == 1
        assert "deprecated" in warnings[0] and "Physical" in warnings[0]

    def test_physloc_through_odbc_is_not_told_to_use_physical(self):
        """Physical refuses odbc: the deprecated Physloc is the only way there."""
        assert _warnings(
            {"source": {"type": "odbc", "schema": "dbo", "table": "t"},
             "options": {"method": "Physloc"}},
            "1.0.0.0",
        ) == []

    @pytest.mark.parametrize("source_type", ["nzbulk", "nzoledb"])
    def test_nzdataslice_on_other_netezza_types_needs_10(self, source_type):
        warnings = _warnings(
            {"source": {"type": source_type, "schema": "admin", "table": "t"},
             "options": {"method": "NZDataSlice"}},
            "0.18.2.0",
        )
        assert len(warnings) == 1
        assert source_type in warnings[0] and "nzsql" in warnings[0]

    def test_adbc_type_sets_agree(self):
        from src.fasttransfer import version

        assert version.ADBC_TYPES == ADBC_TYPES

    def test_physical_no_warning_on_10(self):
        assert _warnings(
            {"source": {"type": "pgsql", "schema": "s", "table": "t"},
             "options": {"method": "Physical"}},
            "1.0.0.0",
        ) == []

    @pytest.mark.parametrize("target_type", ["msbulk", "pgcopy"])
    def test_work_tables_lose_data_before_10(self, target_type):
        warnings = _warnings(
            {"source": {"type": "pgsql"}, "target": {"type": target_type},
             "options": {"use_work_tables": True}},
            "0.18.2.0",
        )
        assert len(warnings) == 1
        assert "use_work_tables" in warnings[0]
        assert "exit" in warnings[0].lower()

    @pytest.mark.parametrize(
        "target_type,version,flag",
        [
            ("msbulk", "1.0.0.0", True),
            ("orabulk", "0.18.2.0", True),
            ("msbulk", "0.18.2.0", False),
        ],
    )
    def test_work_tables_no_warning_otherwise(self, target_type, version, flag):
        assert _warnings(
            {"source": {"type": "pgsql"}, "target": {"type": target_type},
             "options": {"use_work_tables": flag}},
            version,
        ) == []

    def test_nzdataslice_without_table_needs_10(self):
        warnings = _warnings(
            {"source": {"type": "nzsql", "query": "select * from t"},
             "options": {"method": "NZDataSlice"}},
            "0.18.2.0",
        )
        assert len(warnings) == 1
        assert "NZDataSlice" in warnings[0] and "schema" in warnings[0]

    def test_nzdataslice_with_table_ok_before_10(self):
        assert _warnings(
            {"source": {"type": "nzsql", "schema": "admin", "table": "t"},
             "options": {"method": "NZDataSlice"}},
            "0.18.2.0",
        ) == []

    def test_msoledbsql_physloc_needs_10(self):
        warnings = _warnings(
            {"source": {"type": "msoledbsql", "schema": "dbo", "table": "t"},
             "options": {"method": "Physloc"}},
            "0.18.2.0",
        )
        assert len(warnings) == 1
        assert "msoledbsql" in warnings[0] and "1.0.0.0" in warnings[0]


# --------------------------------------------------------------------------- #
# Tools
# --------------------------------------------------------------------------- #


class TestPreviewTool:
    @pytest.mark.asyncio
    async def test_adbc_preview(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("adbc_db2", schema="TPCH", table="ORDERS"),
            "target": _target("adbc_mssql"),
            "options": {"method": "RangeId", "distribute_key_column": "o_orderkey"},
        })
        assert structured["status"] == "ok"
        assert structured["warnings"] == []
        assert "adbc_db2" in structured["command"]
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_mixed_pair_is_a_validation_error(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("adbc_db2", schema="TPCH", table="ORDERS"),
            "target": TARGET,
        })
        assert structured["status"] == "error"
        assert any("cannot be mixed" in e["message"] for e in structured["errors"])
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_auto_suggests_physical_on_10(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("pgsql", schema="public", table="orders"),
            "target": TARGET,
        })
        assert structured["status"] == "ok"
        assert structured["auto_parallelism"] == "Physical"
        assert "Physical" in structured["command"]
        assert "Ctid" in structured["explanation"]

    @pytest.mark.asyncio
    async def test_auto_suggests_engine_name_on_018(self):
        _, handler = _handler("0.18.2.0")
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("pgsql", schema="public", table="orders"),
            "target": TARGET,
        })
        assert structured["status"] == "ok"
        assert structured["auto_parallelism"] == "Ctid"
        assert structured["warnings"] == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("source_type", ["pgsql", "oraodp", "mssql"])
    async def test_no_auto_physical_without_schema_and_table(self, source_type):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source(source_type, query="select * from orders"),
            "target": TARGET,
        })
        assert structured["status"] == "ok"
        assert "auto_parallelism" not in structured
        assert structured["command"][structured["command"].index("--method") + 1] == "None"

    @pytest.mark.asyncio
    async def test_netezza_query_auto_physical_on_10_only(self):
        args = {
            "source": _source("nzsql", query="select * from orders"),
            "target": TARGET,
        }
        _, handler = _handler()
        _, structured = await handler("fasttransfer_preview_transfer", dict(args, source=dict(args["source"])))
        assert structured["auto_parallelism"] == "Physical"
        _, handler = _handler("0.18.2.0")
        _, structured = await handler("fasttransfer_preview_transfer", dict(args, source=dict(args["source"])))
        assert "auto_parallelism" not in structured

    @pytest.mark.asyncio
    async def test_physical_warns_on_018_binary(self):
        _, handler = _handler("0.18.2.0")
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("pgsql", schema="public", table="orders"),
            "target": TARGET,
            "options": {"method": "Physical"},
        })
        assert structured["status"] == "ok"
        assert any("1.0.0.0" in w for w in structured["warnings"])

    @pytest.mark.asyncio
    async def test_explanation_names_table_and_filter(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("pgsql", schema="public", table="orders", query="select * from public.orders where 1=1"),
            "target": TARGET,
            "options": {"method": "Physical"},
        })
        assert structured["status"] == "ok"
        assert "public.orders" in structured["explanation"]
        assert "filter" in structured["explanation"].lower()

    def test_schema_enums_and_descriptions(self):
        tools, _ = _handler()
        preview = {t.name: t for t in tools}["fasttransfer_preview_transfer"]
        props = preview.inputSchema["properties"]
        for adbc_type in ADBC:
            assert adbc_type in props["source"]["properties"]["type"]["enum"]
            assert adbc_type in props["target"]["properties"]["type"]["enum"]
        assert "adbc_" in props["source"]["properties"]["type"]["description"]
        assert "adbc_" in props["target"]["properties"]["type"]["description"]
        options = props["options"]["properties"]
        assert "Physical" in options["method"]["enum"]
        assert "deprecated" in options["method"]["description"]
        assert "1.0" in options["use_work_tables"]["description"]
        assert "ADBC" in options["license_path"]["description"]


class TestInfoTool:
    @pytest.mark.asyncio
    async def test_parallelism_recommends_physical(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "parallelism", "source_type": "oraodp",
            "has_numeric_key": False, "table_size_estimate": "large",
        }))
        assert "`Physical`" in text

    @pytest.mark.asyncio
    async def test_parallelism_recommends_engine_name_on_018(self):
        _, handler = _handler("0.18.2.0")
        text = _text(await handler("fasttransfer_info", {
            "action": "parallelism", "source_type": "oraodp",
            "has_numeric_key": False, "table_size_estimate": "large",
        }))
        assert "`Rowid`" in text

    @pytest.mark.asyncio
    async def test_workflow_adbc_tips(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "adbc_db2", "target_type": "adbc_mssql",
        }))
        assert "both ends" in text
        assert "ADBC DRIVER" in text
        assert "stored case" in text

    @pytest.mark.asyncio
    async def test_workflow_flags_a_mixed_pair(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "adbc_db2", "target_type": "msbulk",
        }))
        assert "cannot be mixed" in text

    @pytest.mark.asyncio
    async def test_workflow_native_tip_uses_physical(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "pgsql", "target_type": "msbulk",
        }))
        assert "`Physical`" in text

    @pytest.mark.asyncio
    async def test_workflow_work_tables_tip_is_gated(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "pgsql", "target_type": "msbulk",
        }))
        assert "use_work_tables" in text
        assert "1.0" in text

    @pytest.mark.asyncio
    async def test_workflow_pgcopy_known_limitation(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "mssql", "target_type": "pgcopy",
        }))
        assert "bytea" in text

    @pytest.mark.asyncio
    async def test_combinations_list_adbc(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {"action": "combinations"}))
        assert "adbc_db2" in text
        assert "both ends" in text

    @pytest.mark.asyncio
    async def test_version_lists_adbc_and_physical(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {"action": "version"}))
        assert "`adbc_db2`" in text
        assert "`Physical`" in text


# --------------------------------------------------------------------------- #
# Docs and instructions
# --------------------------------------------------------------------------- #


class TestDocVersions:
    def test_10_is_a_doc_version(self):
        assert "1.0" in FASTTRANSFER_DOC_VERSIONS
        assert "0.18" in FASTTRANSFER_DOC_VERSIONS

    def test_10_served_at_latest_and_018_at_its_own_path(self):
        aliases = DOC_URL_VERSION_ALIASES["fasttransfer"]
        assert aliases == {"1.0": "latest"}
        assert DOC_URLS["fasttransfer"].format(version=aliases.get("1.0", "1.0")) == (
            "https://fasttransfer-docs.arpe.io/latest/sitemap"
        )
        assert DOC_URLS["fasttransfer"].format(version=aliases.get("0.18", "0.18")) == (
            "https://fasttransfer-docs.arpe.io/0.18/sitemap"
        )

    @pytest.mark.parametrize(
        "detected,expected",
        [("1.0.0.0", "1.0"), ("0.18.2.0", "0.18"), ("0.17.1.0", "0.17"), (None, "1.1")],
    )
    def test_detected_version_maps_to_doc_version(self, detected, expected):
        assert best_version_for("fasttransfer", detected, FASTTRANSFER_DOC_VERSIONS) == expected


class TestInstructions:
    def test_physical_covers_fasttransfer(self):
        assert "FastTransfer 1.0+" in SERVER_INSTRUCTIONS
        assert "FastTransfer keep the engine-specific names" not in " ".join(
            SERVER_INSTRUCTIONS.split()
        )

    def test_fasttransfer_10_notes(self):
        assert "## FastTransfer 1.0 notes" in SERVER_INSTRUCTIONS
        section = SERVER_INSTRUCTIONS.split("## FastTransfer 1.0 notes")[1]
        for needle in ("adbc_", "both ends", "use_work_tables", "pgcopy"):
            assert needle in section

    def test_adbc_drivers_are_not_described_as_fastbcp_only(self):
        flat = " ".join(SERVER_INSTRUCTIONS.split())
        assert "behind FastBCP's `adbc_*` connection types" not in flat
        assert "connection types of FastBCP and FastTransfer" in flat

    def test_adbc_is_not_the_default_for_a_native_limitation(self):
        section = " ".join(SERVER_INSTRUCTIONS.split("## FastTransfer 1.0 notes")[1].split())
        assert "Otherwise keep the native types" in section
        assert "known to hit" in section
