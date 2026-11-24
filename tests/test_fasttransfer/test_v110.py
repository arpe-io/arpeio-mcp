"""FastTransfer 1.1: load mode Create and --inferdatatype, the backup sources
(backup_mssql, backup_rman), SQL Server server names, docs 1.1 served at /1.1/."""

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
    BACKUP_METHODS,
    BACKUP_QUERY_FORMS,
    BACKUP_TYPES,
    CREATE_LOAD_MODES,
    PHYSICAL_STRATEGIES,
    ConnectionConfig,
    InferDataType,
    LoadMode,
    ParallelismMethod,
    SourceConnectionType,
    TargetConnectionType,
    TransferOptions,
    TransferRequest,
    check_mssql_server,
    is_host_colon_port,
    is_mssql_server_type,
)
from src.fasttransfer.version import (
    BACKUP_TYPES as VERSION_BACKUP_TYPES,
    VERSION_REGISTRY,
    check_version_compatibility,
)
from src.instructions import INSTRUCTIONS as SERVER_INSTRUCTIONS

BACKUPS = ["backup_mssql", "backup_rman"]
ADBC = sorted(ADBC_TYPES)

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


def _backup(source_type="backup_mssql", **extra):
    """A backup source: a directory, file patterns, no login."""
    src = {
        "type": source_type,
        "server": "/backup/mssql" if source_type == "backup_mssql" else "/backup/rman",
        "database": "tpch_full.bak;tpch_diff_*.bak" if source_type == "backup_mssql" else "l0;l1/**",
    }
    if "query" not in extra:
        src.update(schema="dbo", table="orders")
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


def _command(request):
    return _builder().build_command(request)


def _value(command, flag):
    return command[command.index(flag) + 1]


def _warnings(params, version):
    return check_version_compatibility(
        params, VERSION_REGISTRY[version], ToolVersion.parse(version)
    )


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #


class TestRegistry:
    def test_110_is_latest(self):
        latest = max(VERSION_REGISTRY, key=lambda v: tuple(int(x) for x in v.split(".")))
        assert latest == "1.1.0.0"

    def test_110_adds_the_backup_sources_only(self):
        caps = VERSION_REGISTRY["1.1.0.0"]
        old = VERSION_REGISTRY["1.0.0.0"]
        assert caps.source_types - old.source_types == set(BACKUPS)
        assert caps.target_types == old.target_types
        assert caps.parallelism_methods == old.parallelism_methods

    def test_backup_types_are_not_targets(self):
        assert not (set(BACKUPS) & VERSION_REGISTRY["1.1.0.0"].target_types)
        assert not (set(BACKUPS) & {t.value for t in TargetConnectionType})

    def test_110_flags(self):
        caps = VERSION_REGISTRY["1.1.0.0"]
        old = VERSION_REGISTRY["1.0.0.0"]
        assert caps.supports_create_load_mode and not old.supports_create_load_mode
        assert caps.supports_mssql_host_colon_port and not old.supports_mssql_host_colon_port
        assert caps.requires_license is True

    def test_enums_match_latest_registry(self):
        """The schema enums are exactly what the 1.1.0 CLI accepts."""
        caps = VERSION_REGISTRY["1.1.0.0"]
        assert {t.value for t in SourceConnectionType} == set(caps.source_types)
        assert {t.value for t in TargetConnectionType} == set(caps.target_types)
        assert {m.value for m in ParallelismMethod} == set(caps.parallelism_methods)

    def test_backup_type_sets_agree(self):
        assert BACKUP_TYPES == VERSION_BACKUP_TYPES == set(BACKUPS)

    def test_capabilities_report_the_110_flags(self):
        info = _builder("1.1.0.0").get_version()
        assert info["capabilities"]["supports_create_load_mode"] is True
        assert info["capabilities"]["supports_mssql_host_colon_port"] is True
        assert "backup_rman" in info["capabilities"]["source_types"]


# --------------------------------------------------------------------------- #
# Load mode Create and --inferdatatype
# --------------------------------------------------------------------------- #


class TestCreateLoadMode:
    def test_load_modes(self):
        """The four values of the 1.1.0 parser."""
        assert {m.value for m in LoadMode} == {"Append", "Truncate", "Create", "Replace"}
        assert CREATE_LOAD_MODES == {"Create", "Replace"}

    def test_infer_data_type_values(self):
        assert {m.value for m in InferDataType} == {"allvarchar", "fast", "best"}

    def test_default_is_still_append_without_inference(self):
        options = TransferOptions()
        assert options.load_mode is LoadMode.APPEND
        assert options.infer_data_type is None

    @pytest.mark.parametrize("mode", ["Create", "Replace"])
    def test_command_carries_the_load_mode(self, mode):
        request = TransferRequest(
            source=_source(schema="public", table="orders"),
            target=TARGET,
            options={"load_mode": mode},
        )
        command = _command(request)
        assert _value(command, "--loadmode") == mode
        assert "--inferdatatype" not in command

    @pytest.mark.parametrize("infer", ["allvarchar", "fast", "best"])
    def test_command_carries_inferdatatype(self, infer):
        request = TransferRequest(
            source=_source("oraodp", schema="SALES", table="ORDERS"),
            target=TARGET,
            options={"load_mode": "Create", "infer_data_type": infer},
        )
        command = _command(request)
        assert _value(command, "--loadmode") == "Create"
        assert _value(command, "--inferdatatype") == infer

    def test_unknown_infer_data_type_rejected(self):
        with pytest.raises(ValidationError):
            TransferOptions(load_mode="Create", infer_data_type="exact")

    def test_unknown_load_mode_rejected(self):
        with pytest.raises(ValidationError):
            TransferOptions(load_mode="Drop")

    def test_infer_data_type_with_another_load_mode_is_not_an_error(self):
        """FastTransfer ignores it with a warning: the preview warns too."""
        request = TransferRequest(
            source=_source(schema="public", table="orders"),
            target=TARGET,
            options={"load_mode": "Truncate", "infer_data_type": "best"},
        )
        assert _value(_command(request), "--inferdatatype") == "best"

    def test_create_with_an_adbc_pair(self):
        request = TransferRequest(
            source=_source("adbc_oracle", schema="SALES", table="ORDERS"),
            target=_target("adbc_pgsql"),
            options={"load_mode": "Create"},
        )
        assert _value(_command(request), "--loadmode") == "Create"


# --------------------------------------------------------------------------- #
# Backup sources
# --------------------------------------------------------------------------- #


class TestBackupSources:
    def test_methods_and_strategies(self):
        assert BACKUP_METHODS == ("None", "Physical")
        for backup in BACKUPS:
            assert PHYSICAL_STRATEGIES[backup] == "Pages"

    @pytest.mark.parametrize("source_type", BACKUPS)
    @pytest.mark.parametrize("target_type", ADBC)
    def test_any_adbc_target_accepted(self, source_type, target_type):
        request = TransferRequest(source=_backup(source_type), target=_target(target_type))
        assert request.source.type == source_type

    @pytest.mark.parametrize("source_type", BACKUPS)
    @pytest.mark.parametrize("target_type", ["msbulk", "pgcopy", "orabulk", "duckdb"])
    def test_native_target_rejected(self, source_type, target_type):
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(source=_backup(source_type), target=_target(target_type))
        message = _errors(exc_info)
        assert "the target must be an ADBC type" in message
        assert target_type in message

    @pytest.mark.parametrize("backup", BACKUPS)
    def test_backup_type_rejected_as_a_target(self, backup):
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(
                source=_source("adbc_mssql", schema="dbo", table="orders"),
                target=_target(backup),
            )
        assert "source connection type only" in _errors(exc_info)

    @pytest.mark.parametrize("source_type", BACKUPS)
    def test_no_login_needed(self, source_type):
        config = ConnectionConfig(**_backup(source_type))
        assert config.user is None and config.password is None

    def test_other_types_still_need_a_login(self):
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(type="adbc_mssql", server="h", database="d")
        assert "user/password" in _errors(exc_info)

    @pytest.mark.parametrize("source_type", BACKUPS)
    @pytest.mark.parametrize("method", ["None", "Physical"])
    def test_none_and_physical_accepted(self, source_type, method):
        request = TransferRequest(
            source=_backup(source_type),
            target=_target("adbc_pgsql"),
            options={"method": method},
        )
        assert _value(_command(request), "--method") == method

    @pytest.mark.parametrize("source_type", BACKUPS)
    @pytest.mark.parametrize(
        "method,key",
        [
            ("Ntile", "o_orderkey"),
            ("RangeId", "o_orderkey"),
            ("Random", "o_orderkey"),
            ("DataDriven", "o_orderdate"),
            ("Timepartition", "(o_orderdate,year)"),
            ("DBPartition", None),
            ("Ctid", None),
            ("Rowid", None),
            ("Physloc", None),
            ("NZDataSlice", None),
        ],
    )
    def test_other_methods_rejected(self, source_type, method, key):
        options = {"method": method}
        if key:
            options["distribute_key_column"] = key
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(source=_backup(source_type), target=_target("adbc_pgsql"), options=options)
        message = _errors(exc_info)
        assert "a backup has no SQL engine to split it" in message
        assert "None or Physical" in message

    @pytest.mark.parametrize("source_type", BACKUPS)
    def test_physical_needs_no_schema_and_table(self, source_type):
        """Pages is split by the driver: FastTransfer asks schema and table for Ctid, Rowid and Physloc only."""
        request = TransferRequest(
            source=_backup(source_type, query="SELECT * FROM dbo.orders"),
            target=_target("adbc_pgsql"),
            options={"method": "Physical"},
        )
        assert request.source.table is None

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM orders",
            "select * from dbo.orders",
            "SELECT o_orderkey, o_totalprice FROM dbo.orders",
            "SELECT [o_orderkey], [o_totalprice] FROM [dbo].[orders];",
            "SELECT TOP 100 * FROM dbo.orders",
            "SELECT TOP 100 o_orderkey FROM orders",
        ],
    )
    def test_mssql_query_forms_accepted(self, query):
        request = TransferRequest(
            source=_backup("backup_mssql", query=query), target=_target("adbc_pgsql")
        )
        assert _value(_command(request), "--query") == query

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM ORDERS",
            "SELECT * FROM TPCH.ORDERS",
            'SELECT O_ORDERKEY, "O_TOTALPRICE" FROM TPCH.ORDERS',
        ],
    )
    def test_rman_query_forms_accepted(self, query):
        request = TransferRequest(
            source=_backup("backup_rman", query=query), target=_target("adbc_mssql")
        )
        assert _value(_command(request), "--query") == query

    @pytest.mark.parametrize("source_type", BACKUPS)
    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM dbo.orders WHERE o_orderkey < 10",
            "SELECT * FROM dbo.orders o JOIN dbo.lineitem l ON o.o_orderkey = l.l_orderkey",
            "SELECT count(*) FROM dbo.orders",
            "SELECT o_orderkey AS k FROM dbo.orders",
            "SELECT * FROM dbo.orders ORDER BY o_orderkey",
            "SELECT * FROM (SELECT * FROM dbo.orders) t",
        ],
    )
    def test_other_sql_rejected(self, source_type, query):
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(source=_backup(source_type, query=query), target=_target("adbc_pgsql"))
        message = _errors(exc_info)
        assert "no WHERE, no join" in message
        assert BACKUP_QUERY_FORMS[source_type][1] in message

    def test_top_is_for_backup_mssql_only(self):
        with pytest.raises(ValidationError):
            TransferRequest(
                source=_backup("backup_rman", query="SELECT TOP 10 * FROM TPCH.ORDERS"),
                target=_target("adbc_mssql"),
            )

    @pytest.mark.parametrize("source_type", BACKUPS)
    def test_table_and_query_are_exclusive(self, source_type):
        """A backup query cannot filter: no table + query, even with Physical."""
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(
                source=_backup(source_type, schema="dbo", table="orders", query="SELECT * FROM dbo.orders"),
                target=_target("adbc_pgsql"),
                options={"method": "Physical"},
            )
        assert "only one of" in _errors(exc_info)

    def test_work_tables_refused(self):
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(
                source=_backup(), target=_target("adbc_mssql"), options={"use_work_tables": True}
            )
        assert "use_work_tables" in _errors(exc_info)

    def test_command(self):
        request = TransferRequest(
            source=_backup("backup_mssql", server="D:\\MSSQLBACKUP"),
            target=_target("adbc_pgsql", server="localhost:5432", schema="public"),
            options={"method": "Physical", "degree": 8, "load_mode": "Truncate"},
        )
        command = _command(request)
        assert _value(command, "--sourceconnectiontype") == "backup_mssql"
        assert _value(command, "--sourceserver") == "D:\\MSSQLBACKUP"
        assert _value(command, "--sourcedatabase") == "tpch_full.bak;tpch_diff_*.bak"
        assert _value(command, "--sourceschema") == "dbo"
        assert _value(command, "--sourcetable") == "orders"
        assert _value(command, "--targetconnectiontype") == "adbc_pgsql"
        assert _value(command, "--method") == "Physical"
        assert _value(command, "--degree") == "8"
        assert "--sourceuser" not in command and "--sourcetrusted" not in command

    def test_encrypted_backup_key_file_and_masked_password(self):
        builder = _builder()
        request = TransferRequest(
            source=_backup("backup_mssql", user="/keys/backup_cert.pvk", password="KeyPass!"),
            target=_target("adbc_pgsql"),
        )
        command = builder.build_command(request)
        assert _value(command, "--sourceuser") == "/keys/backup_cert.pvk"
        assert _value(command, "--sourcepassword") == "KeyPass!"
        assert "KeyPass!" not in builder.format_command_display(command, mask=True)

    def test_connect_string_replaces_the_discrete_parameters(self):
        """FastTransfer's parser refuses --sourceconnectstring next to -i, -u, -x (unlike FastBCP)."""
        connect = "adbc.arpemsbackup.path=/backup/mssql;adbc.arpemsbackup.file=tpch_full.bak"
        request = TransferRequest(
            source={"type": "backup_mssql", "database": "", "connect_string": connect,
                    "schema": "dbo", "table": "orders"},
            target=_target("adbc_pgsql"),
        )
        command = _command(request)
        assert _value(command, "--sourceconnectstring") == connect
        assert "--sourceserver" not in command and "--sourcedatabase" not in command
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(type="backup_mssql", server="/backup/mssql", database="x.bak",
                             connect_string=connect)
        assert "connect_string cannot be used with" in _errors(exc_info)


# --------------------------------------------------------------------------- #
# SQL Server server names
# --------------------------------------------------------------------------- #


class TestMssqlServerNames:
    @pytest.mark.parametrize(
        "server",
        ["sqlhost", "sqlhost:1433", "sqlhost,1433", "sqlhost\\SS2025", "sqlhost\\SS2025,1433",
         "10.0.0.5:1433", "[::1]:1433", "::1"],
    )
    def test_accepted_forms(self, server):
        assert check_mssql_server(server) is None
        for conn_type in ("mssql", "msoledbsql", "adbc_mssql"):
            assert ConnectionConfig(type=conn_type, server=server, database="d", user="u").server == server

    def test_instance_with_colon_port_refused(self):
        error = check_mssql_server("sqlhost\\SS2025:1433")
        assert "written with a comma" in error
        assert "'sqlhost\\SS2025,1433'" in error

    @pytest.mark.parametrize(
        "server,instead",
        [
            ("tcp:sqlhost,1433", "sqlhost,1433"),
            ("TCP:sqlhost", "sqlhost"),
            ("tcp:sqlhost\\SS2025:1433", "sqlhost\\SS2025,1433"),
            ("tcp:sqlhost:1433", "sqlhost:1433"),
        ],
    )
    def test_tcp_prefix_refused(self, server, instead):
        error = check_mssql_server(server)
        assert "tcp: prefix is not accepted" in error
        assert f"'{instead}'" in error

    @pytest.mark.parametrize("conn_type", ["mssql", "msbulk", "msoledbsql", "adbc_mssql"])
    @pytest.mark.parametrize("server", ["tcp:sqlhost,1433", "sqlhost\\SS2025:1433"])
    def test_validator_refuses_them(self, conn_type, server):
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(type=conn_type, server=server, database="d", user="u")
        message = _errors(exc_info)
        assert conn_type in message and "Accepted forms" in message

    @pytest.mark.parametrize("conn_type", ["pgsql", "oraodp", "mysql", "odbc", "adbc_pgsql"])
    def test_other_engines_are_left_alone(self, conn_type):
        assert not is_mssql_server_type(conn_type)
        config = ConnectionConfig(type=conn_type, server="tcp:dbhost", database="d", user="u")
        assert config.server == "tcp:dbhost"

    def test_oledb_counts_with_a_sql_server_provider_only(self):
        assert is_mssql_server_type("oledb", "MSOLEDBSQL")
        assert is_mssql_server_type("oledb", "sqlncli11")
        assert not is_mssql_server_type("oledb", "NZOLEDB")
        assert not is_mssql_server_type("oledb", None)
        with pytest.raises(ValidationError):
            ConnectionConfig(type="oledb", provider="MSOLEDBSQL", server="tcp:sqlhost",
                             database="d", user="u")
        ConnectionConfig(type="oledb", provider="NZOLEDB", server="tcp:nzhost", database="d", user="u")

    def test_host_colon_port(self):
        assert is_host_colon_port("sqlhost:1433")
        assert is_host_colon_port("[::1]:1433")
        assert not is_host_colon_port("sqlhost,1433")
        assert not is_host_colon_port("sqlhost\\SS2025")
        assert not is_host_colon_port("sqlhost")


# --------------------------------------------------------------------------- #
# Suggestions and combinations
# --------------------------------------------------------------------------- #


class TestSuggestion:
    @pytest.mark.parametrize("source_type", BACKUPS)
    @pytest.mark.parametrize("has_key", [True, False])
    def test_backup_gets_physical(self, source_type, has_key):
        suggestion = suggest_parallelism_method(source_type, has_key, False, "large")
        assert suggestion["method"] == "Physical"
        assert "Pages" in suggestion["explanation"]

    @pytest.mark.parametrize("source_type", BACKUPS)
    def test_small_backup_table_stays_single_stream(self, source_type):
        assert suggest_parallelism_method(source_type, False, False, "small")["method"] == "None"


class TestCombinations:
    def test_backup_entry(self):
        combos = get_supported_combinations()
        keys = [k for k in combos if k.startswith("Backup")]
        assert len(keys) == 1
        assert "backup_mssql" in keys[0] and "backup_rman" in keys[0]
        targets = " ".join(combos[keys[0]])
        for adbc_type in ADBC:
            assert adbc_type in targets


# --------------------------------------------------------------------------- #
# Version compatibility warnings
# --------------------------------------------------------------------------- #


class TestVersionWarnings:
    @pytest.mark.parametrize("version", ["0.18.2.0", "1.0.0.0"])
    @pytest.mark.parametrize("backup", BACKUPS)
    def test_backup_sources_need_11(self, version, backup):
        warnings = _warnings({"source": {"type": backup}, "target": {"type": "adbc_pgsql"}}, version)
        text = " ".join(warnings)
        assert backup in text and "1.1.0.0" in text and version in text

    @pytest.mark.parametrize("backup", BACKUPS)
    def test_backup_sources_no_warning_on_11(self, backup):
        assert _warnings(
            {"source": {"type": backup}, "target": {"type": "adbc_pgsql"},
             "options": {"method": "Physical"}},
            "1.1.0.0",
        ) == []

    @pytest.mark.parametrize("version", ["0.18.2.0", "1.0.0.0"])
    def test_create_needs_11(self, version):
        warnings = _warnings({"options": {"load_mode": "Create"}}, version)
        assert len(warnings) == 1
        assert "'Create'" in warnings[0] and "1.1.0.0" in warnings[0] and version in warnings[0]

    @pytest.mark.parametrize("version", ["0.18.2.0", "1.0.0.0"])
    def test_replace_behaves_like_append_before_11(self, version):
        warnings = _warnings({"options": {"load_mode": "Replace"}}, version)
        assert len(warnings) == 1
        assert "behaves like 'Append'" in warnings[0] and version in warnings[0]

    @pytest.mark.parametrize("mode", ["Create", "Replace", "Append", "Truncate"])
    def test_load_modes_no_warning_on_11(self, mode):
        assert _warnings({"options": {"load_mode": mode}}, "1.1.0.0") == []

    @pytest.mark.parametrize("mode", ["Append", "Truncate"])
    def test_old_load_modes_no_warning_before_11(self, mode):
        assert _warnings({"options": {"load_mode": mode}}, "1.0.0.0") == []

    def test_infer_data_type_needs_11(self):
        warnings = _warnings({"options": {"load_mode": "Create", "infer_data_type": "best"}}, "1.0.0.0")
        text = " ".join(warnings)
        assert len(warnings) == 2
        assert "infer_data_type" in text and "1.1.0.0" in text

    @pytest.mark.parametrize("mode", ["Append", "Truncate", None])
    def test_infer_data_type_ignored_without_create(self, mode):
        options = {"infer_data_type": "best"}
        if mode:
            options["load_mode"] = mode
        warnings = _warnings({"options": options}, "1.1.0.0")
        assert len(warnings) == 1
        assert "only used with load mode 'Create'" in warnings[0]

    @pytest.mark.parametrize("mode", ["Create", "Replace"])
    @pytest.mark.parametrize("infer", ["fast", "allvarchar", "best"])
    def test_infer_data_type_with_create_is_fine(self, mode, infer):
        assert _warnings(
            {"source": {"type": "oraodp"}, "options": {"load_mode": mode, "infer_data_type": infer}},
            "1.1.0.0",
        ) == []

    @pytest.mark.parametrize("source_type", ["odbc", "oledb"])
    def test_allvarchar_needs_a_known_engine(self, source_type):
        warnings = _warnings(
            {"source": {"type": source_type},
             "options": {"load_mode": "Create", "infer_data_type": "allvarchar"}},
            "1.1.0.0",
        )
        assert len(warnings) == 1
        assert "engine is known" in warnings[0] and source_type in warnings[0]

    @pytest.mark.parametrize("infer", ["allvarchar", "best"])
    def test_rewriting_modes_with_a_physical_query(self, infer):
        params = {
            "source": {"type": "oraodp", "schema": "S", "table": "T", "query": "select * from S.T where 1=1"},
            "options": {"load_mode": "Create", "infer_data_type": infer, "method": "Physical"},
        }
        warnings = _warnings(params, "1.1.0.0")
        assert len(warnings) == 1 and "rewrites the source query" in warnings[0]
        params["options"]["infer_data_type"] = "fast"
        assert _warnings(params, "1.1.0.0") == []

    @pytest.mark.parametrize(
        "side,conn_type",
        [("source", "mssql"), ("source", "msoledbsql"), ("target", "msbulk")],
    )
    def test_host_colon_port_needs_11_on_the_native_types(self, side, conn_type):
        params = {side: {"type": conn_type, "server": "sqlhost:1433"}}
        warnings = _warnings(params, "1.0.0.0")
        assert len(warnings) == 1
        assert "host,port" in warnings[0] and side in warnings[0] and "1.1.0.0" in warnings[0]
        assert _warnings(params, "1.1.0.0") == []

    def test_host_colon_port_with_a_sql_server_oledb_provider(self):
        params = {"source": {"type": "oledb", "provider": "MSOLEDBSQL", "server": "sqlhost:1433"}}
        assert len(_warnings(params, "1.0.0.0")) == 1
        params["source"]["provider"] = "NZOLEDB"
        assert _warnings(params, "1.0.0.0") == []

    @pytest.mark.parametrize(
        "conn",
        [
            {"type": "mssql", "server": "sqlhost,1433"},
            {"type": "mssql", "server": "sqlhost\\SS2025"},
            {"type": "pgsql", "server": "pghost:5432"},
            {"type": "adbc_mssql", "server": "sqlhost:1433"},
        ],
    )
    def test_host_colon_port_no_warning_otherwise(self, conn):
        warnings = _warnings({"source": conn}, "1.0.0.0")
        assert not any("host,port" in w for w in warnings)


# --------------------------------------------------------------------------- #
# Tools
# --------------------------------------------------------------------------- #


class TestPreviewTool:
    @pytest.mark.asyncio
    async def test_create_preview(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("oraodp", schema="SALES", table="ORDERS"),
            "target": TARGET,
            "options": {"method": "None", "load_mode": "Create", "infer_data_type": "best"},
        })
        assert structured["status"] == "ok"
        assert structured["warnings"] == []
        command = structured["command"]
        assert _value(command, "--loadmode") == "Create"
        assert _value(command, "--inferdatatype") == "best"
        assert "CREATE" in structured["explanation"]
        assert "TRUNCATED" in structured["explanation"]
        assert "best" in structured["explanation"]
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_create_warns_on_10_binary(self):
        _, handler = _handler("1.0.0.0")
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("pgsql", schema="public", table="orders"),
            "target": TARGET,
            "options": {"method": "None", "load_mode": "Create"},
        })
        assert structured["status"] == "ok"
        assert any("1.1.0.0" in w and "Create" in w for w in structured["warnings"])

    @pytest.mark.asyncio
    async def test_infer_data_type_without_create_warns(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("pgsql", schema="public", table="orders"),
            "target": TARGET,
            "options": {"method": "None", "infer_data_type": "allvarchar"},
        })
        assert structured["status"] == "ok"
        assert any("ignores it" in w for w in structured["warnings"])

    @pytest.mark.asyncio
    async def test_backup_preview_auto_suggests_physical(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _backup("backup_mssql"),
            "target": _target("adbc_pgsql"),
        })
        assert structured["status"] == "ok"
        assert structured["warnings"] == []
        assert structured["auto_parallelism"] == "Physical"
        assert "Pages" in structured["explanation"]
        assert "without restoring" in structured["explanation"]
        assert "tpch_full.bak" in structured["explanation"]
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_backup_preview_warns_on_10_binary(self):
        _, handler = _handler("1.0.0.0")
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _backup("backup_rman", schema="TPCH", table="ORDERS"),
            "target": _target("adbc_mssql"),
            "options": {"method": "None"},
        })
        assert structured["status"] == "ok"
        assert any("backup_rman" in w and "1.1.0.0" in w for w in structured["warnings"])

    @pytest.mark.asyncio
    async def test_backup_to_native_target_is_a_validation_error(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _backup("backup_mssql"),
            "target": TARGET,
        })
        assert structured["status"] == "error"
        assert any("ADBC type" in e["message"] for e in structured["errors"])
        jsonschema.validate(structured, PREVIEW_OUTPUT_SCHEMA)

    @pytest.mark.asyncio
    async def test_tcp_prefix_is_a_validation_error(self):
        _, handler = _handler()
        content, structured = await handler("fasttransfer_preview_transfer", {
            "source": _source("mssql", server="tcp:sqlhost,1433", schema="dbo", table="orders"),
            "target": _target("pgcopy"),
        })
        assert structured["status"] == "error"
        assert any("tcp: prefix" in e["message"] for e in structured["errors"])

    def test_schema_enums_and_descriptions(self):
        tools, _ = _handler()
        preview = {t.name: t for t in tools}["fasttransfer_preview_transfer"]
        props = preview.inputSchema["properties"]
        source = props["source"]["properties"]
        target = props["target"]["properties"]
        options = props["options"]["properties"]
        for backup in BACKUPS:
            assert backup in source["type"]["enum"]
            assert backup not in target["type"]["enum"]
        assert "backup_mssql" in source["type"]["description"]
        assert "directory of the backup files" in source["server"]["description"]
        assert "separated by ';'" in source["database"]["description"]
        assert "private key file" in source["user"]["description"]
        assert "no WHERE" in source["query"]["description"]
        assert "adbc.arpemsbackup" in source["connect_string"]["description"]
        assert "tcp:" in source["server"]["description"]
        assert "tcp:" in target["server"]["description"]
        assert "Create" in target["table"]["description"]
        assert options["load_mode"]["enum"] == ["Append", "Truncate", "Create", "Replace"]
        assert "alias of Create" in options["load_mode"]["description"]
        assert sorted(options["infer_data_type"]["enum"]) == ["allvarchar", "best", "fast"]
        for variable in (
            "FASTTRANSFER_INFER_DEFAULT_STRING_MAXSIZE",
            "FASTTRANSFER_INFER_STRING_MARGIN",
            "FASTTRANSFER_INFER_SAMPLE_PERCENT",
        ):
            assert variable in options["infer_data_type"]["description"]
        assert "Pages" in options["method"]["description"]

    def test_info_tool_lists_the_backup_source_types(self):
        tools, _ = _handler()
        info = {t.name: t for t in tools}["fasttransfer_info"]
        assert "backup_rman" in info.inputSchema["properties"]["source_type"]["enum"]
        assert "backup_rman" not in info.inputSchema["properties"]["target_type"]["enum"]


class TestInfoTool:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("backup", BACKUPS)
    async def test_parallelism_recommends_physical_for_a_backup(self, backup):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "parallelism", "source_type": backup,
            "has_numeric_key": True, "table_size_estimate": "large",
        }))
        assert "`Physical`" in text
        assert "distribute_key_column" not in text

    @pytest.mark.asyncio
    async def test_workflow_backup_mssql(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "backup_mssql", "target_type": "adbc_pgsql",
        }))
        assert "### Backup source" in text
        assert "private key file" in text
        assert "TOP n" in text
        for variable in (
            "FASTTRANSFER_BACKUP_RESTORE_MODE",
            "FASTTRANSFER_BACKUP_RESTORE_POINT",
            "FASTTRANSFER_INDEX_DIR",
        ):
            assert variable in text
        assert "refused**" not in text
        assert "Timepartition" not in text

    @pytest.mark.asyncio
    async def test_workflow_backup_rman(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "backup_rman", "target_type": "adbc_mssql",
        }))
        assert "l0;l1/**" in text
        assert "private key file" not in text
        assert "TOP n" not in text

    @pytest.mark.asyncio
    async def test_workflow_flags_a_backup_to_a_native_target(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "backup_mssql", "target_type": "msbulk",
        }))
        assert "This pair is refused" in text
        assert "adbc_*" in text

    @pytest.mark.asyncio
    async def test_workflow_load_mode_section(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "oraodp", "target_type": "msbulk",
        }))
        assert "### Load mode" in text
        assert "`Create`" in text and "infer_data_type" in text
        assert "strict mode" not in text
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "oraodp", "target_type": "mysqlbulk",
        }))
        assert "strict mode" in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pair", [("adbc_netezza", "adbc_pgsql"), ("adbc_db2", "adbc_mssql")])
    async def test_workflow_adbc_sslmode(self, pair):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": pair[0], "target_type": pair[1],
        }))
        assert "FASTTRANSFER_ADBC_SSLMODE" in text

    @pytest.mark.asyncio
    async def test_workflow_pgcopy_fix(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {
            "action": "workflow", "source_type": "mssql", "target_type": "pgcopy",
        }))
        assert "fixed in 1.1" in text

    @pytest.mark.asyncio
    async def test_combinations_list_backups_and_create(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {"action": "combinations"}))
        assert "backup_mssql" in text and "backup_rman" in text
        assert "`Create`" in text

    @pytest.mark.asyncio
    async def test_version_lists_the_backup_sources(self):
        _, handler = _handler()
        text = _text(await handler("fasttransfer_info", {"action": "version"}))
        assert "`backup_mssql`" in text and "`backup_rman`" in text


# --------------------------------------------------------------------------- #
# Docs and instructions
# --------------------------------------------------------------------------- #


class TestDocVersions:
    def test_11_is_the_latest_doc_version(self):
        assert FASTTRANSFER_DOC_VERSIONS[-1] == "1.1"
        assert "1.0" in FASTTRANSFER_DOC_VERSIONS

    def test_11_served_at_latest_and_10_at_its_own_path(self):
        """1.1 is the "latest" version of the site; 1.0 moved to /1.0/."""
        aliases = DOC_URL_VERSION_ALIASES["fasttransfer"]
        assert aliases == {"1.1": "latest"}
        assert DOC_URLS["fasttransfer"].format(version=aliases.get("1.1", "1.1")) == (
            "https://fasttransfer-docs.arpe.io/latest/sitemap"
        )
        assert DOC_URLS["fasttransfer"].format(version=aliases.get("1.0", "1.0")) == (
            "https://fasttransfer-docs.arpe.io/1.0/sitemap"
        )

    @pytest.mark.parametrize(
        "detected,expected",
        [("1.1.0.0", "1.1"), ("1.0.0.0", "1.0"), ("0.18.2.0", "0.18"), (None, "1.1")],
    )
    def test_detected_version_maps_to_doc_version(self, detected, expected):
        assert best_version_for("fasttransfer", detected, FASTTRANSFER_DOC_VERSIONS) == expected


class TestInstructions:
    def test_fasttransfer_11_notes(self):
        assert "## FastTransfer 1.1 notes" in SERVER_INSTRUCTIONS
        section = " ".join(
            SERVER_INSTRUCTIONS.split("## FastTransfer 1.1 notes")[1].split("\n## ")[0].split()
        )
        for needle in (
            "Create",
            "`Replace` is an alias of `Create`",
            "infer_data_type",
            "backup_mssql",
            "backup_rman",
            "FASTTRANSFER_ADBC_SSLMODE",
            "FASTTRANSFER_INDEX_DIR",
            "FASTTRANSFER_BACKUP_RESTORE_MODE",
            "FASTTRANSFER_BACKUP_RESTORE_POINT",
            "FASTTRANSFER_INFER_STRING_MARGIN",
            "FASTTRANSFER_INFER_DEFAULT_STRING_MAXSIZE",
            "FASTTRANSFER_INFER_SAMPLE_PERCENT",
            "tcp:",
            "host\\INSTANCE:port",
        ):
            assert needle in section, needle

    def test_bundled_driver_versions_of_110(self):
        section = " ".join(SERVER_INSTRUCTIONS.split("## FastTransfer 1.1 notes")[1].split())
        for driver in (
            "ArpeMSSQL 0.8.4",
            "ArpePGSQL 0.5.6",
            "ArpeOracle 0.4.8",
            "ArpeNetezza 0.3.12",
            "ArpeDb2 0.4.3",
            "ArpeTeradata 0.3.8",
            "ArpeMSBackup 0.4.1",
            "ArpeOraBackup 0.3.1",
        ):
            assert driver in section, driver

    def test_10_notes_keep_the_100_driver_versions(self):
        section = " ".join(
            SERVER_INSTRUCTIONS.split("## FastTransfer 1.0 notes")[1]
            .split("## FastTransfer 1.1 notes")[0]
            .split()
        )
        assert "ArpeMSSQL 0.8.2" in section and "in 1.0.0" in section

    def test_physical_pages_covers_fasttransfer(self):
        flat = " ".join(SERVER_INSTRUCTIONS.split())
        assert "in FastBCP to Pages" not in flat
        assert "FastTransfer 1.1+" in flat
