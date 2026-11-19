"""
Input validation for FastTransfer MCP Server.

This module provides Pydantic models and enums for validating
all FastTransfer parameters and ensuring parameter compatibility.
"""

import re
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


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

# Native connection types per Physical strategy (FastTransfer 1.0+). Physical
# is resolved from the source connection type; other sources are refused.
PHYSICAL_STRATEGIES = {
    "pgsql": "Ctid",
    "pgcopy": "Ctid",
    "oraodp": "Rowid",
    "mssql": "Physloc",
    "msoledbsql": "Physloc",
    "nzsql": "NZDataSlice",
    "nzbulk": "NZDataSlice",
    "nzoledb": "NZDataSlice",
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
    FATAL = "fatal"


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
        LoadMode.APPEND, description="Load mode: Append or Truncate"
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
        """Validate parallelism degree."""
        if v == 0 or (v > 0 and v < 1024) or v < 0:
            return v
        raise ValueError(
            "Degree must be 0 (auto), 0 < n < 1024 (fixed), or < 0 (CPU adaptive)"
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
        # with the table filters it (FastTransfer sends both).
        table_with_filter = (
            has_table
            and has_query
            and not has_file_input
            and self.options.method.value in PHYSICAL_METHODS
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
        source_is_adbc = self.source.type.lower() in ADBC_TYPES
        target_is_adbc = self.target.type.lower() in ADBC_TYPES
        if source_is_adbc != target_is_adbc:
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
    def validate_target_requires_table(self):
        """Ensure target has table specified."""
        if not self.target.table:
            raise ValueError("Target must specify 'table'")
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
                f"SQL Server and Netezza"
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
