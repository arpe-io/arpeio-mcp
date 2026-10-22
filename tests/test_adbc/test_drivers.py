"""Tests for the Arpe ADBC drivers registry, the adbc_info tool and their release notes."""

import pytest
from rank_bm25 import BM25Okapi

from src.adbc.drivers import (
    ADBC_DRIVERS,
    driver_for_fastbcp_type,
    find_driver,
    get_supported_capabilities,
)
from src.adbc.tools import create_tools
from src.base.release_notes_handler import build_release_notes_tool
from src.doc_search.index import SearchEngine
from src.fastbcp.validators import ADBC_NATIVE_TYPES
from src.fastbcp.version import ADBC_MIN_VERSIONS


class TestRegistry:
    def test_commercial_names_and_latest_versions(self):
        assert {d.name: d.version for d in ADBC_DRIVERS.values()} == {
            "ArpeMSSQL": "0.7.6",
            "ArpePGSQL": "0.4.7",
            "ArpeOracle": "0.4.2",
            "ArpeNetezza": "0.3.7",
            "ArpeTeradata": "0.3.5",
        }

    def test_netezza_load_name_differs_from_docs_slug(self):
        drv = ADBC_DRIVERS["arpenz"]
        assert drv.option_prefix == "adbc.arpenz."
        assert drv.docs_url.endswith("/drivers/arpenetezza/")

    def test_every_fastbcp_adbc_type_has_a_driver(self):
        """FastBCP's adbc_* types and the driver registry must stay in step."""
        for source_type in ADBC_NATIVE_TYPES:
            drv = driver_for_fastbcp_type(source_type)
            assert drv is not None, source_type
            assert ADBC_MIN_VERSIONS[source_type].startswith(drv.fastbcp_min_version + ".")

    @pytest.mark.parametrize("alias", ["arpeteradata", "ArpeTeradata", "adbc_teradata", " ARPETERADATA "])
    def test_find_driver_aliases(self, alias):
        assert find_driver(alias).name == "ArpeTeradata"

    def test_find_driver_netezza_aliases(self):
        assert find_driver("arpenetezza") is find_driver("arpenz") is ADBC_DRIVERS["arpenz"]

    def test_find_driver_unknown(self):
        assert find_driver("arpedb2") is None
        assert find_driver("") is None

    def test_capabilities(self):
        caps = get_supported_capabilities()
        assert [d["load_name"] for d in caps["drivers"]] == list(ADBC_DRIVERS)
        assert caps["upcoming"][0]["name"] == "ArpeDb2"


class TestAdbcInfoTool:
    def setup_method(self):
        tools, self.handler = create_tools()
        self.tool = tools[0]

    def test_tool_is_read_only(self):
        assert self.tool.name == "adbc_info"
        assert self.tool.annotations.readOnlyHint is True

    @pytest.mark.asyncio
    async def test_list(self):
        content, structured = await self.handler("adbc_info", {})
        text = content[0].text
        for drv in ADBC_DRIVERS.values():
            assert drv.name in text and drv.version in text
        assert structured["status"] == "ok"
        assert len(structured["drivers"]) == len(ADBC_DRIVERS)

    @pytest.mark.asyncio
    async def test_detail(self):
        content, structured = await self.handler("adbc_info", {"driver": "adbc_teradata"})
        text = content[0].text
        assert "ArpeTeradata" in text and "v0.3.5" in text
        assert 'driver="arpeteradata"' in text
        assert "sh -s -- arpeteradata --license" in structured["install"]["linux"]
        assert structured["drivers"][0]["fastbcp_type"] == "adbc_teradata"

    @pytest.mark.asyncio
    async def test_upcoming_driver(self):
        content, structured = await self.handler("adbc_info", {"driver": "ArpeDb2"})
        assert structured["status"] == "error"
        assert "not published yet" in content[0].text

    @pytest.mark.asyncio
    async def test_unknown_driver(self):
        _, structured = await self.handler("adbc_info", {"driver": "mysql"})
        assert structured["status"] == "error"

    @pytest.mark.asyncio
    async def test_other_tool_not_handled(self):
        assert await self.handler("fastbcp_info", {}) is None


WHATS_NEW = "https://adbc-drivers-docs.arpe.io/drivers/arpeteradata/whats-new"


@pytest.fixture
def engine_with_adbc_docs():
    chunks = [
        {"text": "Feature-oriented release highlights.", "url": WHATS_NEW,
         "source": "adbc / What's new in ArpeTeradata"},
        {"text": "teradata:// URIs and ADO.NET connection strings.", "url": WHATS_NEW,
         "source": "adbc / What's new in ArpeTeradata / Connection URIs and strings — October 2026 (v0.3.5)"},
        {"text": "New options logmech, logdata and kerberos_spn.", "url": WHATS_NEW,
         "source": "adbc / What's new in ArpeTeradata / LDAP, JWT and Kerberos logons — October 2026 (v0.3.4)"},
        {"text": "The Teradata parcel protocol.", "url": WHATS_NEW,
         "source": "adbc / What's new in ArpeTeradata / The native Teradata engine — foundational (v0.1.0)"},
        {"text": "Netezza type names in field metadata.",
         "url": "https://adbc-drivers-docs.arpe.io/drivers/arpenetezza/whats-new",
         "source": "adbc / What's new in ArpeNetezza / Netezza type names — October 2026 (v0.3.7)"},
        {"text": "Connection options.", "url": "https://adbc-drivers-docs.arpe.io/drivers/arpeteradata/connection",
         "source": "adbc / ArpeTeradata — Connection guide"},
    ]
    engine = SearchEngine()
    engine._indexes["adbc"] = (chunks, BM25Okapi([c["text"].lower().split() for c in chunks]))
    engine._ready = True
    return engine


class TestDriverReleaseNotes:
    def test_whole_history_without_version(self, engine_with_adbc_docs):
        chunks = engine_with_adbc_docs.get_release_notes("arpeteradata")
        assert len(chunks) == 4
        assert all(c["url"] == WHATS_NEW for c in chunks)

    def test_exact_version(self, engine_with_adbc_docs):
        chunks = engine_with_adbc_docs.get_release_notes("arpeteradata", "0.3.5")
        assert [c["text"] for c in chunks] == ["teradata:// URIs and ADO.NET connection strings."]

    def test_version_prefix(self, engine_with_adbc_docs):
        assert len(engine_with_adbc_docs.get_release_notes("arpeteradata", "0.3")) == 2
        assert engine_with_adbc_docs.get_release_notes("arpeteradata", "0.3.1") == []

    def test_load_name_maps_to_docs_slug(self, engine_with_adbc_docs):
        chunks = engine_with_adbc_docs.get_release_notes("arpenz", "v0.3.7")
        assert len(chunks) == 1 and "Netezza" in chunks[0]["text"]

    def test_no_adbc_index(self):
        assert SearchEngine().get_release_notes("arpemssql") == []

    @pytest.mark.asyncio
    async def test_tool_uses_commercial_name(self, engine_with_adbc_docs):
        _, handler = build_release_notes_tool("arpeteradata", engine_with_adbc_docs)
        content, structured = await handler({"version": "0.3.4"})
        assert content[0].text.startswith("# ArpeTeradata Release Notes")
        assert structured["count"] == 1
