"""
Registry of the Arpe ADBC drivers.

The drivers are native ADBC libraries (not CLIs): there is no command to
build or run, so this module only holds the reference data the MCP serves
(commercial name, load name, current version, connection URI, FastBCP
connection type). Versions follow https://adbc-drivers-docs.arpe.io/versioning;
bump them here when a driver is released.
"""

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

DOCS_BASE_URL = "https://adbc-drivers-docs.arpe.io"
INSTALL_SCRIPT_URL = "https://raw.githubusercontent.com/arpe-io/adbc-drivers/main/install.sh"
INSTALL_PS1_URL = "https://raw.githubusercontent.com/arpe-io/adbc-drivers/main/install.ps1"


@dataclass(frozen=True)
class AdbcDriver:
    """One driver of the Arpe ADBC family."""

    name: str  # commercial name
    database: str
    also_covers: str
    load_name: str  # name passed to the ADBC driver manager
    version: str  # latest published version
    protocol: str
    authentication: str
    uri_example: str
    docs_slug: str  # /drivers/<docs_slug>/ on the docs site
    fastbcp_type: Optional[str] = None  # FastBCP --connectiontype reading through it
    fastbcp_min_version: Optional[str] = None
    status: str = "published"

    @property
    def option_prefix(self) -> str:
        return f"adbc.{self.load_name}."

    @property
    def docs_url(self) -> str:
        return f"{DOCS_BASE_URL}/drivers/{self.docs_slug}/"

    @property
    def whats_new_url(self) -> str:
        return f"{DOCS_BASE_URL}/drivers/{self.docs_slug}/whats-new"

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["option_prefix"] = self.option_prefix
        data["docs_url"] = self.docs_url
        data["whats_new_url"] = self.whats_new_url
        return data


# Keyed by load name, in the order of the docs site.
ADBC_DRIVERS: Dict[str, AdbcDriver] = {
    "arpemssql": AdbcDriver(
        name="ArpeMSSQL",
        database="Microsoft SQL Server",
        also_covers="Azure SQL, Microsoft Fabric Warehouse",
        load_name="arpemssql",
        version="0.7.6",
        protocol="Native MS-TDS 7.4 and 8.0 (strict encryption) over TCP/TLS; no ODBC, no SQL Server client libraries",
        authentication="SQL logins, Microsoft Entra ID, Windows SSPI, Kerberos on Linux",
        uri_example="sqlserver://sa:<pw>@host:1433/?database=db&encrypt=true",
        docs_slug="arpemssql",
        fastbcp_type="adbc_mssql",
        fastbcp_min_version="1.0",
    ),
    "arpepgsql": AdbcDriver(
        name="ArpePGSQL",
        database="PostgreSQL",
        also_covers="PostgreSQL-compatible services",
        load_name="arpepgsql",
        version="0.4.7",
        protocol="Native Frontend/Backend (FEBE) v3 over TCP/TLS; no libpq",
        authentication="SCRAM-SHA-256 (with channel binding), MD5, cleartext, Windows SSPI",
        uri_example="postgresql://user:<pw>@host:5432/mydb?sslmode=require",
        docs_slug="arpepgsql",
        fastbcp_type="adbc_pgsql",
        fastbcp_min_version="1.1",
    ),
    "arpeoracle": AdbcDriver(
        name="ArpeOracle",
        database="Oracle",
        also_covers="Oracle Cloud Autonomous Database",
        load_name="arpeoracle",
        version="0.4.2",
        protocol="Native TNS + TTC over TCP/TLS; no OCI, no Instant Client, no ODBC",
        authentication="O5LOGON password, proxy (CONNECT THROUGH), OCI IAM token, OAuth2 / Microsoft Entra ID bearer token",
        uri_example="oracle://scott:tiger@dbhost:2484/orclpdb1?ssl_mode=verify-full",
        docs_slug="arpeoracle",
        fastbcp_type="adbc_oracle",
        fastbcp_min_version="1.2",
    ),
    "arpenz": AdbcDriver(
        name="ArpeNetezza",
        database="IBM Netezza",
        also_covers="IBM Cloud Netezza Performance Server",
        load_name="arpenz",
        version="0.3.7",
        protocol="Native Netezza protocol (HSV2 handshake + DBOS binary results) over TCP/TLS; no ODBC, no nzpy",
        authentication="Password (cleartext / MD5 / SHA-256) over TLS",
        uri_example="netezza://admin:<pw>@nzhost:5480/SYSTEM?sslmode=verify-full",
        docs_slug="arpenetezza",
        fastbcp_type="adbc_netezza",
        fastbcp_min_version="1.3",
    ),
    "arpeteradata": AdbcDriver(
        name="ArpeTeradata",
        database="Teradata",
        also_covers="Teradata cloud environments",
        load_name="arpeteradata",
        version="0.3.5",
        protocol=(
            "Native Teradata parcel protocol; no ODBC, no teradatasql. Logon over TLS needs "
            "no Teradata software; servers without TLS are reached through an installed TTU (CLIv2)"
        ),
        authentication="TD2, LDAP (legacy port only), JWT, Kerberos (KRB5 / TDNEGO)",
        uri_example="teradata://alice:<pw>@td.example.com/SALES?sslmode=verify-full",
        docs_slug="arpeteradata",
        fastbcp_type="adbc_teradata",
        fastbcp_min_version="1.4",
    ),
}

# Announced on the docs site, not downloadable yet.
UPCOMING_DRIVERS: List[Dict[str, str]] = [
    {"name": "ArpeDb2", "database": "IBM Db2", "load_name": "arpedb2", "status": "coming soon"},
]

# Driver lookup accepts the load name, the commercial name, the docs slug or
# the FastBCP connection type, case-insensitively.
_ALIASES: Dict[str, str] = {}
for _key, _drv in ADBC_DRIVERS.items():
    for _alias in (_key, _drv.name, _drv.docs_slug, _drv.fastbcp_type):
        if _alias:
            _ALIASES[_alias.lower()] = _key


def find_driver(name: str) -> Optional[AdbcDriver]:
    """Return the driver matching a load name, commercial name, docs slug or FastBCP type."""
    key = _ALIASES.get((name or "").strip().lower())
    return ADBC_DRIVERS.get(key) if key else None


def driver_for_fastbcp_type(source_type: str) -> Optional[AdbcDriver]:
    """Return the driver behind a FastBCP ``adbc_*`` connection type, if any."""
    for drv in ADBC_DRIVERS.values():
        if drv.fastbcp_type == source_type:
            return drv
    return None


def install_commands(load_name: str) -> Dict[str, str]:
    """One-line installers for a driver (the licence path is a placeholder)."""
    return {
        "linux": f"curl -fsSL {INSTALL_SCRIPT_URL} | sh -s -- {load_name} --license /path/to/your.lic",
        "windows": (
            f"& ([scriptblock]::Create((irm {INSTALL_PS1_URL}))) "
            f"{load_name} -License C:\\path\\to\\your.lic"
        ),
    }


LICENCE_SOURCES = [
    "arpeio.adbc.license (ADBC database option, licence blob inline)",
    "arpeio.adbc.license_file (ADBC database option, path to a .lic file)",
    "ARPEIO_ADBC_LICENCE (environment variable, licence blob inline)",
    "ARPEIO_ADBC_LICENCE_FILE (environment variable, path to a .lic file)",
    "arpeio_adbc.lic next to the driver library (written by the installer with --license)",
]


def get_supported_capabilities() -> Dict[str, Any]:
    """Driver family summary, served as an MCP resource and by ``adbc_info``."""
    return {
        "docs": DOCS_BASE_URL,
        "drivers": [drv.to_dict() for drv in ADBC_DRIVERS.values()],
        "upcoming": UPCOMING_DRIVERS,
        "platforms": ["Linux x64", "Windows x64"],
        "licence_sources": LICENCE_SOURCES,
        "install": {
            "linux": f"curl -fsSL {INSTALL_SCRIPT_URL} | sh -s -- <load_name> --license /path/to/your.lic",
            "list_versions": f"curl -fsSL {INSTALL_SCRIPT_URL} | sh -s -- --list",
        },
    }
