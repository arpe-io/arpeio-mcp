"""
Input validation for FastTransfer MCP Server.

This module provides Pydantic models and enums for validating
all FastTransfer parameters and ensuring parameter compatibility.
"""

import re
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


# Bounds of --degree in the FastTransfer parser (BoundedValueArgument, inclusive).
DEGREE_MIN = -32
DEGREE_MAX = 1024

class SourceConnectionType(str, Enum):
    """Source database connection types supported by FastTransfer."""

    CLICKHOUSE = "clickhouse"
    DUCKDB = "duckdb"
    DUCKDB_STREAM = "duckdbstream"
    HANA = "hana"
    MSSQL = "mssql"
    MSOLEDBSQL = "msoledbsql"
    MYSQL = "mysql"
    NETEZZA_OLEDB = "nzoledb"
    NETEZZA_SQL = "nzsql"
    NETEZZA_BULK = "nzbulk"
    ODBC = "odbc"
    OLEDB = "oledb"
    ORACLE_ODP = "oraodp"
    POSTGRES_COPY = "pgcopy"
    POSTGRES = "pgsql"
    TERADATA = "teradata"
    # ADBC connection types (FastTransfer 1.0+): the bundled Arpe ADBC drivers.
    # Only with an adbc_* target.
    ADBC_MSSQL = "adbc_mssql"  # ArpeMSSQL
    ADBC_PGSQL = "adbc_pgsql"  # ArpePGSQL
    ADBC_ORACLE = "adbc_oracle"  # ArpeOracle
    ADBC_NETEZZA = "adbc_netezza"  # ArpeNetezza
    ADBC_DB2 = "adbc_db2"  # ArpeDb2, Db2 for LUW (no native counterpart)
    ADBC_TERADATA = "adbc_teradata"  # ArpeTeradata
    # Backup files read without restoring them (FastTransfer 1.1+), through
    # the bundled backup drivers. Source only, and only with an adbc_* target.
    BACKUP_MSSQL = "backup_mssql"  # SQL Server .bak files (ArpeMSBackup)
    BACKUP_RMAN = "backup_rman"  # Oracle RMAN backup sets (ArpeOraBackup)


class TargetConnectionType(str, Enum):
    """Target database connection types supported by FastTransfer."""

    CLICKHOUSE_BULK = "clickhousebulk"
    DUCKDB = "duckdb"
    HANA_BULK = "hanabulk"
    MSSQL_BULK = "msbulk"
    MYSQL_BULK = "mysqlbulk"
    NETEZZA_BULK = "nzbulk"
    ORACLE_BULK = "orabulk"
    ORACLE_DIRECT = "oradirect"
    POSTGRES_COPY = "pgcopy"
    POSTGRES = "pgsql"
    TERADATA = "teradata"
    # ADBC connection types (FastTransfer 1.0+). Only with an adbc_* source.
    ADBC_MSSQL = "adbc_mssql"
    ADBC_PGSQL = "adbc_pgsql"
    ADBC_ORACLE = "adbc_oracle"
    ADBC_NETEZZA = "adbc_netezza"
    ADBC_DB2 = "adbc_db2"
    ADBC_TERADATA = "adbc_teradata"


class ParallelismMethod(str, Enum):
    """Parallelism methods for data distribution."""

    CTID = "Ctid"  # PostgreSQL-specific (deprecated in 1.0, use Physical)
    DATA_DRIVEN = "DataDriven"  # Distribute by distinct key values
    NTILE = "Ntile"  # Even distribution
    NZ_DATA_SLICE = "NZDataSlice"  # Netezza-specific (deprecated in 1.0)
    NONE = "None"  # No parallelism
    PHYSLOC = "Physloc"  # SQL Server physical location (deprecated in 1.0)
    RANDOM = "Random"  # Random distribution using modulo
    RANGE_ID = "RangeId"  # Numeric range distribution
    ROWID = "Rowid"  # Oracle-specific (deprecated in 1.0)
    DB_PARTITION = "DBPartition"  # Native table partitions (FastTransfer 0.18+)
    TIMEPARTITION = "Timepartition"  # Time parts of a date column (FastTransfer 0.18+)
    PHYSICAL = "Physical"  # Physical row location, resolved per engine (1.0+)


# Each ADBC connection type with a native counterpart maps to the native type
# of the same engine: FastTransfer picks its SQL (catalog queries, parallel
# planning) by engine, so the parallel methods follow the native source.
ADBC_NATIVE_TYPES = {
    "adbc_mssql": "mssql",
    "adbc_pgsql": "pgsql",
    "adbc_oracle": "oraodp",
    "adbc_netezza": "nzsql",
    "adbc_teradata": "teradata",
}

# Every ADBC connection type (FastTransfer 1.0+); adbc_db2 has no native twin.
# ADBC moves Arrow batches from one driver to the other: both ends must be adbc_*.
ADBC_TYPES = frozenset([*ADBC_NATIVE_TYPES, "adbc_db2"])

# ADBC types whose driver has no integrated / Kerberos logon.
NO_TRUSTED_AUTH_TYPES = frozenset(["adbc_netezza"])

# Backup source types (FastTransfer 1.1+): --sourceserver is the directory of
# the backup files and --sourcedatabase their names or patterns. The drivers
# return Arrow batches, which only an adbc_* target loads.
BACKUP_TYPES = frozenset(["backup_mssql", "backup_rman"])

# Parallel methods of a backup source: a backup has no SQL engine to split it.
BACKUP_METHODS = ("None", "Physical")

# --query accepted by the backup source types: a column list of one table, no
# WHERE, no join. backup_mssql also takes TOP n.
_IDENT = r'(?:\[[^\]]+\]|"[^"]+"|[A-Za-z_][\w$#@]*)'


def _backup_query_re(top: bool, max_qualifiers: int) -> "re.Pattern[str]":
    top_clause = r"(?:TOP\s+(?:\d+|\(\s*\d+\s*\))\s+)?" if top else ""
    return re.compile(
        rf"^\s*SELECT\s+{top_clause}(?:\*|{_IDENT}(?:\s*,\s*{_IDENT})*)\s+FROM\s+"
        rf"{_IDENT}(?:\s*\.\s*{_IDENT}){{0,{max_qualifiers}}}\s*;?\s*$",
        re.IGNORECASE | re.DOTALL,
    )


# The documented form is [schema.]table for both types. One more qualifier is
# let through for backup_rman (container.owner.table, which the same driver
# takes in FastBCP): FastTransfer's documentation does not rule it out.
BACKUP_QUERY_FORMS = {
    "backup_mssql": (
        _backup_query_re(top=True, max_qualifiers=1),
        "SELECT [TOP n] * | col1, col2, ... FROM [schema.]table",
    ),
    "backup_rman": (
        _backup_query_re(top=False, max_qualifiers=2),
        "SELECT * | col1, col2, ... FROM [schema.]table",
    ),
}

# Connection types whose --sourceserver / --targetserver names a SQL Server
# (FastTransfer 1.1 checks its syntax before any connection); oledb counts
# with a SQL Server provider only.
MSSQL_SERVER_TYPES = frozenset(["mssql", "msbulk", "msoledbsql", "adbc_mssql"])
MSSQL_OLEDB_PROVIDER_PREFIXES = ("MSOLEDBSQL", "SQLOLEDB", "SQLNCLI")
_INSTANCE_COLON_PORT = re.compile(r"^(?P<server>[^:,\\]+\\[^:,\\]+):(?P<port>\d{1,5})$")
_HOST_COLON_PORT = re.compile(r"^(?:\[[^\]]+\]|[^:,\\\[\]\s]+):\d{1,5}$")
_MSSQL_SERVER_FORMS = "host, host:port, host,port, host\\INSTANCE or host\\INSTANCE,port"


def is_mssql_server_type(connection_type: str, provider: Optional[str] = None) -> bool:
    """Whether the server of this connection type is a SQL Server name."""
    connection_type = (connection_type or "").lower()
    if connection_type in MSSQL_SERVER_TYPES:
        return True
    return connection_type == "oledb" and (provider or "").strip().upper().startswith(
        MSSQL_OLEDB_PROVIDER_PREFIXES
    )


def check_mssql_server(server: Optional[str]) -> Optional[str]:
    """Why FastTransfer 1.1 refuses this SQL Server name, or None when it is accepted."""
    name = (server or "").strip()
    if name[:4].lower() == "tcp:":
        rest = name[4:].strip()
        match = _INSTANCE_COLON_PORT.match(rest)
        instead = f"{match.group('server')},{match.group('port')}" if match else rest
        return (
            f"the tcp: prefix is not accepted (the connection is TCP anyway): use "
            f"'{instead}'. Accepted forms: {_MSSQL_SERVER_FORMS}"
        )
    match = _INSTANCE_COLON_PORT.match(name)
    if match:
        return (
            f"a port after a named instance is written with a comma: use "
            f"'{match.group('server')},{match.group('port')}'. "
            f"Accepted forms: {_MSSQL_SERVER_FORMS}"
        )
    return None


def is_host_colon_port(server: Optional[str]) -> bool:
    """host:port (a name, an IPv4 address or a bracketed IPv6 address)."""
    return bool(_HOST_COLON_PORT.match((server or "").strip()))


# Connection types per Physical strategy (FastTransfer 1.0+; Pages for the
# backup sources, 1.1+). Physical is resolved from the source connection type;
# other sources are refused.
PHYSICAL_STRATEGIES = {
    "pgsql": "Ctid",
    "pgcopy": "Ctid",
    "oraodp": "Rowid",
    "mssql": "Physloc",
    "msoledbsql": "Physloc",
    "nzsql": "NZDataSlice",
    "nzbulk": "NZDataSlice",
    "nzoledb": "NZDataSlice",
    "backup_mssql": "Pages",
    "backup_rman": "Pages",
}

# Methods that split a table on its physical row location: Physical and the
# engine-specific names it replaces. A query given with them filters the table.
PHYSICAL_METHODS = frozenset(["Physical", "Ctid", "Rowid", "Physloc", "NZDataSlice"])

# Source types whose native partitions DBPartition can read (FastTransfer 0.18;
# adbc_db2: Db2 for LUW range partitioning, 1.0+). ADBC types follow their
# native counterpart. FastTransfer falls back to None (single stream) on any
# other source.
DB_PARTITION_SOURCES = frozenset(
    ["mssql", "msoledbsql", "pgsql", "pgcopy", "oraodp", "mysql", "teradata", "adbc_db2"]
)


def native_source_type(source_type: str) -> str:
    """Return the native connection type behind an ADBC type (or the type itself)."""
    source_type = source_type.lower()
    return ADBC_NATIVE_TYPES.get(source_type, source_type)


# Timepartition --distributekeycolumn tuple: (date_column,<time parts>), where the
# time parts are year | year,month | year,month,day | year,week.
TIMEPARTITION_GRANULARITIES = (
    ("year",),
    ("year", "month"),
    ("year", "month", "day"),
    ("year", "week"),
)
_TIMEPARTITION_TUPLE = re.compile(r"^\(\s*(?P<body>[^()]+?)\s*\)$")


def normalize_timepartition_key(value: str) -> str:
    """Validate a Timepartition tuple and return it in the documented form.

    Accepts e.g. ``"(order_date, year, month)"`` and returns
    ``"(order_date,year,month)"``. Raises ValueError on any other shape.
    """
    match = _TIMEPARTITION_TUPLE.match(value.strip())
    parts = [p.strip() for p in match.group("body").split(",")] if match else []
    column, granularity = (parts[0], tuple(p.lower() for p in parts[1:])) if parts else ("", ())
    if not column or granularity not in TIMEPARTITION_GRANULARITIES:
        raise ValueError(
            "Method 'Timepartition' needs distribute_key_column as a tuple of a "
            "date column and time parts: '(date_column,year)', "
            "'(date_column,year,month)', '(date_column,year,month,day)' or "
            f"'(date_column,year,week)', not '{value}'"
        )
    return "(" + ",".join((column,) + granularity) + ")"


class LoadMode(str, Enum):
    """Load mode for target table."""

    APPEND = "Append"  # Add to existing data
    TRUNCATE = "Truncate"  # Clear before loading
    # FastTransfer 1.1+: create the target table from the source when it does
    # not exist, then load it; truncate and load it when it exists.
    CREATE = "Create"
    # Alias of Create from 1.1 (it behaved like Append before).
    REPLACE = "Replace"


# Load modes that create the target table when it is missing (FastTransfer 1.1+)
CREATE_LOAD_MODES = frozenset(["Create", "Replace"])


class InferDataType(str, Enum):
    """How load mode Create derives the column types (FastTransfer 1.1+)."""

    FAST = "fast"  # From the source metadata, no row is read (default)
    ALLVARCHAR = "allvarchar"  # Every column as text
    BEST = "best"  # Profiling scan of the source (Oracle and DuckDB sources)


class MapMethod(str, Enum):
    """Column mapping method."""

    POSITION = "Position"  # Map by position
    NAME = "Name"  # Map by name (case-insensitive)


class LogLevel(str, Enum):
    """Log level for FastTransfer output."""

    ERROR = "error"
    WARNING = "warning"
    INFORMATION = "information"
    DEBUG = "debug"
    VERBOSE = "verbose"


class ConnectionConfig(BaseModel):
    """Database connection configuration."""

    type: str = Field(..., description="Connection type (source or target)")
    server: Optional[str] = Field(
        None, description="Server address (host:port or host\\instance)"
    )
    database: str = Field(..., description="Database name")
    schema: Optional[str] = Field(None, description="Schema name")
    table: Optional[str] = Field(
        None, description="Table name (optional if query provided)"
    )
    query: Optional[str] = Field(None, description="SQL query (alternative to table)")
    file_input: Optional[str] = Field(
        None, description="File path for data input (alternative to table/query)"
    )
    user: Optional[str] = Field(None, description="Username for authentication")
    password: Optional[str] = Field(None, description="Password for authentication")
    trusted_auth: bool = Field(
        False, description="Use trusted authentication (Windows)"
    )
    connect_string: Optional[str] = Field(
        None, description="Full connection string (alternative)"
    )
    dsn: Optional[str] = Field(None, description="ODBC DSN name")
    provider: Optional[str] = Field(None, description="OleDB provider name")

    @model_validator(mode="after")
    def validate_adbc_rules(self):
        """Connection rules of the ADBC connection types."""
        if self.type.lower() in NO_TRUSTED_AUTH_TYPES and self.trusted_auth:
            raise ValueError(
                f"{self.type} does not support trusted authentication: "
                f"give user and password (or a connect_string)"
            )
        return self

    @model_validator(mode="after")
    def validate_mssql_server(self):
        """SQL Server server names FastTransfer 1.1 refuses before any connection."""
        if (
            self.server
            and not self.connect_string
            and is_mssql_server_type(self.type, self.provider)
        ):
            error = check_mssql_server(self.server)
            if error:
                raise ValueError(f"server '{self.server}' ({self.type}): {error}")
        return self

    @model_validator(mode="after")
    def validate_mutual_exclusivity(self):
        """Validate mutually exclusive connection parameters."""
        # connect_string excludes individual connection params
        if self.connect_string:
            conflicts = []
            if self.dsn:
                conflicts.append("dsn")
            if self.provider:
                conflicts.append("provider")
            if self.server:
                conflicts.append("server")
            if self.user:
                conflicts.append("user")
            if self.password:
                conflicts.append("password")
            if self.trusted_auth:
                conflicts.append("trusted_auth")
            if conflicts:
                raise ValueError(
                    f"connect_string cannot be used with: {', '.join(conflicts)}"
                )

        # dsn excludes provider and server
        if self.dsn:
            conflicts = []
            if self.provider:
                conflicts.append("provider")
            if self.server:
                conflicts.append("server")
            if conflicts:
                raise ValueError(f"dsn cannot be used with: {', '.join(conflicts)}")

        # trusted_auth excludes user and password
        if self.trusted_auth:
            conflicts = []
            if self.user:
                conflicts.append("user")
            if self.password:
                conflicts.append("password")
            if conflicts:
                raise ValueError(
                    f"trusted_auth cannot be used with: {', '.join(conflicts)}"
                )

        # file_input excludes query
        if self.file_input and self.query:
            raise ValueError("file_input cannot be used with query")

        return self

    @model_validator(mode="after")
    def validate_authentication(self):
        """Ensure either credentials or trusted auth is provided."""
        # Backup files have no database login (user / password are the private
        # key of an encrypted backup_mssql backup)
        if self.type.lower() in BACKUP_TYPES:
            return self
        if not self.trusted_auth and not self.connect_string and not self.dsn:
            if not self.user:
                raise ValueError(
                    "Either user/password, trusted_auth, connect_string, or dsn must be provided"
                )
        return self


class TransferOptions(BaseModel):
    """Options for data transfer execution."""

    method: ParallelismMethod = Field(
        ParallelismMethod.NONE, description="Parallelism method"
    )
    distribute_key_column: Optional[str] = Field(
        None, description="Column for data distribution (required for some methods)"
    )
    degree: int = Field(
        -2,
        description="Parallelism degree: 0=auto, >0=fixed, <0=CPU adaptive (e.g., -2=half CPUs)",
    )
    load_mode: LoadMode = Field(
        LoadMode.APPEND,
        description="Load mode: Append, Truncate, Create (1.1+) or Replace (alias of Create)",
    )
    infer_data_type: Optional[InferDataType] = Field(
        None,
        description="With load mode Create, how the column types are derived: fast, allvarchar or best",
    )
    batch_size: Optional[int] = Field(
        None, ge=1, description="Batch size for bulk copy operations"
    )
    map_method: MapMethod = Field(
        MapMethod.POSITION, description="Column mapping method: Position or Name"
    )
    run_id: Optional[str] = Field(
        None, description="Run ID for logging and tracking purposes"
    )
    data_driven_query: Optional[str] = Field(
        None, description="Custom SQL query for DataDriven parallelism method"
    )
    use_work_tables: Optional[bool] = Field(
        None, description="Use intermediate work tables for CCI"
    )
    settings_file: Optional[str] = Field(
        None, description="Path to custom settings JSON file"
    )
    log_level: Optional[LogLevel] = Field(None, description="Override log level")
    no_banner: bool = Field(False, description="Suppress the FastTransfer banner")
    license_path: Optional[str] = Field(None, description="Path or URL to license file")

    @field_validator("degree")
    @classmethod
    def validate_degree(cls, v):
        """Validate parallelism degree: the FastTransfer parser bounds --degree to -32..1024."""
        if DEGREE_MIN <= v <= DEGREE_MAX:
            return v
        raise ValueError(
            f"Degree must be between {DEGREE_MIN} and {DEGREE_MAX}: 0 (all cores), "
            f"1 to {DEGREE_MAX} (fixed), or {DEGREE_MIN} to -1 (cores divided by the absolute value)"
        )

    @model_validator(mode="after")
    def validate_distribute_key_requirements(self):
        """Validate distribute key column requirements."""
        methods_requiring_key = {
            ParallelismMethod.DATA_DRIVEN,
            ParallelismMethod.RANDOM,
            ParallelismMethod.RANGE_ID,
            ParallelismMethod.NTILE,
            ParallelismMethod.TIMEPARTITION,
        }

        if self.method in methods_requiring_key and not self.distribute_key_column:
            hint = (
                ", a tuple such as '(order_date,year,month)'"
                if self.method == ParallelismMethod.TIMEPARTITION
                else ""
            )
            raise ValueError(
                f"Method '{self.method.value}' requires distribute_key_column{hint}"
            )

        if self.method == ParallelismMethod.TIMEPARTITION:
            self.distribute_key_column = normalize_timepartition_key(
                self.distribute_key_column
            )

        return self

    @model_validator(mode="after")
    def validate_data_driven_query(self):
        """Validate data_driven_query is only used with DataDriven method."""
        if self.data_driven_query and self.method != ParallelismMethod.DATA_DRIVEN:
            raise ValueError(
                "data_driven_query can only be used with the DataDriven method"
            )
        return self


class TransferRequest(BaseModel):
    """Complete transfer request with source, target, and options."""

    source: ConnectionConfig = Field(..., description="Source database configuration")
    target: ConnectionConfig = Field(..., description="Target database configuration")
    options: TransferOptions = Field(
        default_factory=TransferOptions, description="Transfer execution options"
    )

    @model_validator(mode="after")
    def validate_source_table_or_query(self):
        """Ensure source has either table, query, or file_input."""
        has_table = bool(self.source.table)
        has_query = bool(self.source.query)
        has_file_input = bool(self.source.file_input)
        count = sum([has_table, has_query, has_file_input])

        if count == 0:
            raise ValueError(
                "Source must specify either 'table', 'query', or 'file_input'"
            )
        # The physical-location methods split the table itself: a query given
        # with the table filters it (FastTransfer sends both). Not on a backup
        # source, whose query cannot filter.
        table_with_filter = (
            has_table
            and has_query
            and not has_file_input
            and self.options.method.value in PHYSICAL_METHODS
            and self.source.type.lower() not in BACKUP_TYPES
        )
        if count > 1 and not table_with_filter:
            raise ValueError(
                "Source must specify only one of 'table', 'query', or 'file_input' "
                "(a 'query' can be added to 'table' only with the Physical method, "
                "to filter the table)"
            )
        return self

    @model_validator(mode="after")
    def validate_adbc_pairing(self):
        """ADBC connection types work only as a pair (FastTransfer 1.0+)."""
        source_type = self.source.type.lower()
        target_type = self.target.type.lower()
        source_is_adbc = source_type in ADBC_TYPES
        target_is_adbc = target_type in ADBC_TYPES
        if target_type in BACKUP_TYPES:
            raise ValueError(
                f"{self.target.type} is a source connection type only: a backup "
                f"cannot be a target"
            )
        # Backup sources (FastTransfer 1.1+) return Arrow batches, which only
        # an ADBC target loads
        if source_type in BACKUP_TYPES:
            if not target_is_adbc:
                raise ValueError(
                    f"Source type {self.source.type} reads a backup, which returns "
                    f"Arrow batches: the target must be an ADBC type (adbc_mssql, "
                    f"adbc_pgsql, adbc_oracle, adbc_netezza, adbc_db2 or "
                    f"adbc_teradata), not {self.target.type}"
                )
        elif source_is_adbc != target_is_adbc:
            raise ValueError(
                f"ADBC connection types (adbc_*) cannot be mixed with other types: "
                f"source is {self.source.type}, target is {self.target.type}. "
                f"Use adbc_* on both the source and the target, or on neither"
            )
        if target_is_adbc and self.options.use_work_tables:
            raise ValueError(
                f"use_work_tables is not available with an ADBC target "
                f"({self.target.type}): the parallel streams load the target table directly"
            )
        return self

    @model_validator(mode="after")
    def validate_backup_source(self):
        """Backup sources (FastTransfer 1.1+): methods None or Physical, restricted query."""
        source_type = self.source.type.lower()
        if source_type not in BACKUP_TYPES:
            return self
        method = self.options.method.value
        if method not in BACKUP_METHODS:
            raise ValueError(
                f"{method} method is not available with source type "
                f"{self.source.type}: a backup has no SQL engine to split it ==> "
                f"use method None or Physical"
            )
        if self.source.query:
            pattern, form = BACKUP_QUERY_FORMS[source_type]
            if not pattern.match(self.source.query):
                raise ValueError(
                    f"With {self.source.type}, 'query' is limited to {form} "
                    f"(no WHERE, no join, no other SQL); give 'schema' and 'table' "
                    f"to read the whole table"
                )
        return self

    @model_validator(mode="after")
    def validate_target_requires_table(self):
        """Ensure target has schema and table specified.

        FastTransfer stops with 'Target Table is mandatory' / 'Target Schema is
        mandatory' before connecting when --targettable / --targetschema is missing.
        """
        if not self.target.table:
            raise ValueError("Target must specify 'table'")
        if not self.target.schema:
            raise ValueError(
                "Target must specify 'schema': FastTransfer requires --targetschema "
                "(for example 'dbo' on SQL Server, 'public' on PostgreSQL, 'main' on DuckDB)"
            )
        return self

    @model_validator(mode="after")
    def validate_method_compatibility(self):
        """Validate parallelism method compatibility with source database."""
        method = self.options.method
        # ADBC types use the parallel planning of their native counterpart
        source_type = native_source_type(self.source.type)

        # Ctid is PostgreSQL-specific
        if method == ParallelismMethod.CTID and source_type not in [
            "pgsql",
            "pgcopy",
        ]:
            raise ValueError(
                f"Method 'Ctid' only works with PostgreSQL sources, not '{self.source.type}'"
            )

        # Rowid is Oracle-specific
        if method == ParallelismMethod.ROWID and source_type not in [
            "oraodp",
        ]:
            raise ValueError(
                f"Method 'Rowid' only works with Oracle sources, not '{self.source.type}'"
            )

        # NZDataSlice is Netezza-specific
        if method == ParallelismMethod.NZ_DATA_SLICE and source_type not in [
            "nzoledb",
            "nzsql",
            "nzbulk",
        ]:
            raise ValueError(
                f"Method 'NZDataSlice' only works with Netezza sources, not '{self.source.type}'"
            )

        # Physloc is SQL Server-specific. odbc is accepted with this deprecated
        # name only: Physical cannot tell the engine behind an ODBC driver.
        if method == ParallelismMethod.PHYSLOC and source_type not in [
            "mssql",
            "odbc",
            "msoledbsql",
        ]:
            raise ValueError(
                f"Method 'Physloc' only works with SQL Server sources (mssql, msoledbsql, "
                f"adbc_mssql, or odbc to SQL Server), not '{self.source.type}'"
            )

        # Physical resolves to Ctid / Rowid / Physloc / NZDataSlice from the
        # source connection type (FastTransfer 1.0+); other sources are refused.
        if method == ParallelismMethod.PHYSICAL and source_type not in PHYSICAL_STRATEGIES:
            if source_type in ("odbc", "oledb"):
                raise ValueError(
                    f"Method 'Physical' cannot identify the source engine behind "
                    f"connection type '{self.source.type}': use a native connection type "
                    f"(pgsql, pgcopy, oraodp, mssql, msoledbsql, nzsql, nzbulk, nzoledb) "
                    f"or its adbc_* counterpart"
                )
            raise ValueError(
                f"Method 'Physical' is not supported for source connection type "
                f"'{self.source.type}': supported sources are PostgreSQL, Oracle, "
                f"SQL Server, Netezza, and the SQL Server and Oracle RMAN backups"
            )

        # Ctid, Rowid and Physloc split the table itself: schema and table are
        # mandatory, even with a query (which then filters the table). Netezza
        # (NZDataSlice) needs neither.
        strategy = (
            PHYSICAL_STRATEGIES.get(source_type)
            if method == ParallelismMethod.PHYSICAL
            else method.value
        )
        if strategy in ("Ctid", "Rowid", "Physloc") and (
            not self.source.schema or not self.source.table
        ):
            label = (
                f"Physical ({strategy})"
                if method == ParallelismMethod.PHYSICAL
                else method.value
            )
            raise ValueError(
                f"Method '{label}' requires both 'schema' and 'table' "
                f"(FastTransfer splits the table on its physical row location; "
                f"a 'query' can be added to filter that table)"
            )

        # DBPartition reads the native partitions of a table (FastTransfer 0.18+).
        # FastTransfer would silently fall back to None on other sources.
        if method == ParallelismMethod.DB_PARTITION:
            if source_type not in DB_PARTITION_SOURCES:
                raise ValueError(
                    f"Method 'DBPartition' only works with SQL Server (mssql, msoledbsql), "
                    f"PostgreSQL (pgsql, pgcopy), Oracle (oraodp), MySQL (mysql), "
                    f"Teradata (teradata), their adbc_* counterparts and Db2 for LUW "
                    f"(adbc_db2) sources, not '{self.source.type}' "
                    f"(FastTransfer would fall back to a single stream)"
                )
            if not self.source.schema or not self.source.table:
                raise ValueError(
                    "Method 'DBPartition' requires both 'schema' and 'table' "
                    "(it reads the table's partitions; 'query' and 'file_input' "
                    "are not allowed)"
                )

        return self

    @model_validator(mode="after")
    def validate_random_requires_numeric_key(self):
        """Random method requires numeric distribute key column."""
        if (
            self.options.method == ParallelismMethod.RANDOM
            and self.options.distribute_key_column
        ):
            # Note: We can't validate if column is numeric without database access
            pass
        return self


class ConnectionValidationRequest(BaseModel):
    """Request to validate a database connection."""

    connection: ConnectionConfig = Field(..., description="Connection to validate")
    side: str = Field(..., description="Connection side: 'source' or 'target'")

    @field_validator("side")
    @classmethod
    def validate_side(cls, v):
        """Ensure side is either source or target."""
        if v not in ["source", "target"]:
            raise ValueError("Side must be 'source' or 'target'")
        return v


class ParallelismSuggestionRequest(BaseModel):
    """Request for parallelism method suggestion."""

    source_type: str = Field(..., description="Source database type")
    has_numeric_key: bool = Field(
        ..., description="Whether table has a numeric key column"
    )
    has_identity_column: bool = Field(
        False, description="Whether table has an identity/auto-increment column"
    )
    table_size_estimate: str = Field(
        ..., description="Table size: 'small', 'medium', or 'large'"
    )

    @field_validator("table_size_estimate")
    @classmethod
    def validate_table_size(cls, v):
        """Validate table size estimate."""
        if v not in ["small", "medium", "large"]:
            raise ValueError(
                "Table size estimate must be 'small', 'medium', or 'large'"
            )
        return v
