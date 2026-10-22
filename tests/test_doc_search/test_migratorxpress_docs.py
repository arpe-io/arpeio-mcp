"""MigratorXpress docs: migratorxpress-docs.arpe.io version mapping, crawl scope, release notes."""

import subprocess
from types import SimpleNamespace

import httpx
import pytest
from rank_bm25 import BM25Okapi

import src.doc_search.index as index_module
from src.base.release_notes_handler import build_release_notes_tool
from src.doc_search.cache import best_version_for
from src.doc_search.crawler import crawl_doc_site
from src.doc_search.doc_versions import (
    DOC_URL_SCOPES,
    DOC_URL_VERSION_ALIASES,
    DOC_URLS,
    MIGRATORXPRESS_DOC_VERSIONS,
    RELEASE_NOTES_UNPUBLISHED,
    RELEASE_NOTES_URLS,
)
from src.doc_search.index import SearchEngine

SITE = "https://migratorxpress-docs.arpe.io"


def _scope_for(version: str) -> str:
    url_version = DOC_URL_VERSION_ALIASES["migratorxpress"].get(version, version)
    return DOC_URL_SCOPES["migratorxpress"].format(version=url_version)


class TestVersionMapping:
    def test_doc_versions_are_minor_releases(self):
        assert MIGRATORXPRESS_DOC_VERSIONS == ["0.6", "0.7"]

    def test_root_sitemap(self):
        assert DOC_URLS["migratorxpress"] == f"{SITE}/sitemap.xml"

    def test_no_reference_to_old_site(self):
        urls = [DOC_URLS["migratorxpress"], DOC_URL_SCOPES["migratorxpress"],
                RELEASE_NOTES_UNPUBLISHED["migratorxpress"]]
        assert not any("github.io" in u for u in urls)
        assert "migratorxpress" not in RELEASE_NOTES_URLS

    @pytest.mark.parametrize(
        "detected,expected",
        [
            ("0.7.3", "0.7"),
            ("0.7.0.0", "0.7"),
            ("0.6.34", "0.6"),
            ("0.6.27", "0.6"),
            (None, "0.7"),
        ],
    )
    def test_detected_version_maps_to_doc_version(self, detected, expected):
        assert best_version_for("migratorxpress", detected, MIGRATORXPRESS_DOC_VERSIONS) == expected

    def test_07_served_at_latest(self):
        assert _scope_for("0.7") == f"{SITE}/latest/"

    def test_06_keeps_versioned_path(self):
        assert _scope_for("0.6") == f"{SITE}/0.6/"


SITEMAP_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>{SITE}/</loc></url>
<url><loc>{SITE}/0.6/</loc></url>
<url><loc>{SITE}/0.6/compatibility</loc></url>
<url><loc>{SITE}/latest/</loc></url>
<url><loc>{SITE}/latest/reference/cli-reference</loc></url>
</urlset>"""


def _page(title: str) -> str:
    return (f"<html><body><article><h1>{title}</h1><h2>Section</h2>"
            f"<p>Content of the {title} page, long enough to keep.</p></article></body></html>")


def _mock_client(fetched: list) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        fetched.append(url)
        if url == f"{SITE}/sitemap.xml":
            return httpx.Response(200, text=SITEMAP_XML, headers={"content-type": "application/xml"})
        return httpx.Response(200, text=_page(url), headers={"content-type": "text/html"})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class TestCrawlScope:
    @pytest.mark.asyncio
    async def test_latest_scope_crawls_only_07_pages(self):
        fetched: list = []
        async with _mock_client(fetched) as client:
            chunks = await crawl_doc_site(
                DOC_URLS["migratorxpress"], "migratorxpress", client, _scope_for("0.7"))
        pages = fetched[1:]
        assert pages == [f"{SITE}/latest/", f"{SITE}/latest/reference/cli-reference"]
        assert {c["url"] for c in chunks} == set(pages)

    @pytest.mark.asyncio
    async def test_06_scope_crawls_only_06_pages(self):
        fetched: list = []
        async with _mock_client(fetched) as client:
            await crawl_doc_site(
                DOC_URLS["migratorxpress"], "migratorxpress", client, _scope_for("0.6"))
        assert fetched[1:] == [f"{SITE}/0.6/", f"{SITE}/0.6/compatibility"]

    @pytest.mark.asyncio
    async def test_no_prefix_crawls_everything(self):
        fetched: list = []
        async with _mock_client(fetched) as client:
            await crawl_doc_site(DOC_URLS["migratorxpress"], "migratorxpress", client)
        assert len(fetched) == 6  # sitemap + 5 pages


class TestInitializeTargets:
    """initialize() sends the root sitemap and the detected version's prefix to the crawler."""

    async def _crawl_targets(self, monkeypatch, tmp_path, binary_output):
        monkeypatch.setattr(index_module, "CACHE_DIR", tmp_path)
        monkeypatch.setattr(index_module, "is_cache_fresh", lambda *a, **k: False)
        monkeypatch.setattr(
            subprocess, "run",
            lambda *a, **k: SimpleNamespace(stdout=binary_output, stderr=""),
        )
        engine = SearchEngine()
        captured = []

        async def fake_crawl(sources):
            captured.extend(sources)

        monkeypatch.setattr(engine, "_crawl_sources", fake_crawl)
        version_info = {"migratorxpress": {"path": "/fake/MigratorXpress"}} if binary_output else None
        await engine.initialize(version_info)
        return {s[0]: s for s in captured}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "binary_output,doc_version,prefix",
        [
            ("MigratorXpress version 0.7.3", "0.7", f"{SITE}/latest/"),
            ("MigratorXpress version 0.6.30", "0.6", f"{SITE}/0.6/"),
            (None, "0.7", f"{SITE}/latest/"),
        ],
    )
    async def test_migratorxpress_target(self, monkeypatch, tmp_path, binary_output, doc_version, prefix):
        targets = await self._crawl_targets(monkeypatch, tmp_path, binary_output)
        assert targets["migratorxpress"] == ("migratorxpress", f"{SITE}/sitemap.xml", doc_version, prefix)

    @pytest.mark.asyncio
    async def test_other_products_unscoped(self, monkeypatch, tmp_path):
        targets = await self._crawl_targets(monkeypatch, tmp_path, None)
        assert targets["fastbcp"] == ("fastbcp", "https://fastbcp-docs.arpe.io/latest/sitemap", "1.4", None)
        assert targets["adbc"][1] == "https://adbc-drivers-docs.arpe.io/sitemap.xml"
        assert targets["adbc"][3] is None
        assert targets["blog"][3] is None


def _seed(engine: SearchEngine, chunks):
    engine._indexes["migratorxpress"] = (chunks, BM25Okapi([c["text"].lower().split() for c in chunks]))
    engine._ready = True


WHATS_NEW = {
    "text": "Per-run metadata snapshots (0.7.0) ... PostgreSQL target (0.7.2) ...",
    "url": f"{SITE}/latest/",
    "source": "migratorxpress / MigratorXpress / What's new in 0.7",
}
OTHER = {
    "text": "MigratorXpress delegates all data movement to FastTransfer.",
    "url": f"{SITE}/latest/",
    "source": "migratorxpress / MigratorXpress / Data transfer with FastTransfer",
}


class TestReleaseNotes:
    def test_whats_new_section_returned(self):
        engine = SearchEngine()
        _seed(engine, [OTHER, WHATS_NEW])
        assert engine.get_release_notes("migratorxpress") == [WHATS_NEW]

    @pytest.mark.parametrize("version", ["0.7", "0.7.2", "v0.7.3"])
    def test_whats_new_matches_07_versions(self, version):
        engine = SearchEngine()
        _seed(engine, [OTHER, WHATS_NEW])
        assert engine.get_release_notes("migratorxpress", version) == [WHATS_NEW]

    @pytest.mark.parametrize("version", ["0.6", "0.6.30", "0.70"])
    def test_whats_new_not_returned_for_other_versions(self, version):
        engine = SearchEngine()
        _seed(engine, [OTHER, WHATS_NEW])
        assert engine.get_release_notes("migratorxpress", version) == []

    @pytest.mark.asyncio
    async def test_tool_returns_whats_new_with_note(self):
        engine = SearchEngine()
        _seed(engine, [OTHER, WHATS_NEW])
        _, handler = build_release_notes_tool("migratorxpress", engine)
        content, structured = await handler({})
        text = content[0].text
        assert "no published release notes" in text
        assert "Per-run metadata snapshots" in text
        assert structured["count"] == 1
        assert structured["release_notes_published"] is False
        assert structured["docs_url"] == f"{SITE}/latest/"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("args", [{}, {"version": "0.6.30"}])
    async def test_tool_says_unpublished_when_nothing_indexed(self, args):
        engine = SearchEngine()
        _seed(engine, [OTHER])
        _, handler = build_release_notes_tool("migratorxpress", engine)
        content, structured = await handler(args)
        text = content[0].text
        assert "MigratorXpress has no published release notes" in text
        assert f"{SITE}/latest/" in text
        assert "search_docs" not in text
        assert structured["status"] == "ok"
        assert structured["count"] == 0
        assert structured["release_notes_published"] is False
        assert structured["docs_url"] == f"{SITE}/latest/"

    @pytest.mark.asyncio
    async def test_other_products_unchanged(self):
        _, handler = build_release_notes_tool("fastbcp", SearchEngine())
        _, structured = await handler({})
        assert "release_notes_published" not in structured
