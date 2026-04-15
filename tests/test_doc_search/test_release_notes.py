"""Tests for the per-product release-notes helper and tool handler."""

import pytest
from rank_bm25 import BM25Okapi

from src.base.release_notes_handler import build_release_notes_tool
from src.doc_search.index import SearchEngine


def _seed_chunks(engine: SearchEngine, product: str, chunks):
    """Inject a prebuilt index into the engine without crawling."""
    corpus = [[w.lower() for w in c["text"].split()] for c in chunks]
    engine._indexes[product] = (chunks, BM25Okapi(corpus or [[""]]))
    engine._ready = True


@pytest.fixture
def engine_with_fastbcp_notes():
    engine = SearchEngine()
    _seed_chunks(
        engine,
        "fastbcp",
        [
            {
                "text": "Improved Parquet schema precision using GetSchemaTable.",
                "url": "https://fastbcp-docs.arpe.io/latest/release-notes/release-notes-0.31",
                "source": "fastbcp",
            },
            {
                "text": "Adds Timepartition parallelism method and GCS storage.",
                "url": "https://fastbcp-docs.arpe.io/latest/release-notes/release-notes-0.30",
                "source": "fastbcp",
            },
            {
                "text": "Unrelated CLI reference page.",
                "url": "https://fastbcp-docs.arpe.io/latest/documentation/cli/overview",
                "source": "fastbcp",
            },
        ],
    )
    return engine


class TestGetReleaseNotes:
    def test_returns_only_release_notes_chunks(self, engine_with_fastbcp_notes):
        chunks = engine_with_fastbcp_notes.get_release_notes("fastbcp")
        assert chunks, "expected at least one release-notes chunk"
        assert all("release-notes" in c["url"] for c in chunks)

    def test_filter_by_version(self, engine_with_fastbcp_notes):
        chunks = engine_with_fastbcp_notes.get_release_notes("fastbcp", "0.31")
        assert len(chunks) == 1
        assert "GetSchemaTable" in chunks[0]["text"]

    def test_unknown_product_returns_empty(self):
        engine = SearchEngine()
        assert engine.get_release_notes("fastbcp") == []

    def test_newest_version_when_version_omitted(self, engine_with_fastbcp_notes):
        chunks = engine_with_fastbcp_notes.get_release_notes("fastbcp")
        assert len(chunks) == 1
        assert "0.31" in chunks[0]["url"]


class TestReleaseNotesTool:
    @pytest.mark.asyncio
    async def test_tool_returns_text(self, engine_with_fastbcp_notes):
        tool, handler = build_release_notes_tool("fastbcp", engine_with_fastbcp_notes)
        assert tool.name == "fastbcp_release_notes"
        assert tool.annotations.readOnlyHint is True
        result = await handler({"version": "0.31"})
        assert result
        assert "GetSchemaTable" in result[0].text

    @pytest.mark.asyncio
    async def test_tool_handles_missing_version(self, engine_with_fastbcp_notes):
        _, handler = build_release_notes_tool("fastbcp", engine_with_fastbcp_notes)
        result = await handler({"version": "9.9"})
        assert "No cached release notes" in result[0].text

    @pytest.mark.asyncio
    async def test_tool_handles_no_release_notes_indexed(self):
        engine = SearchEngine()
        _seed_chunks(
            engine,
            "lakexpress",
            [
                {
                    "text": "Some other page.",
                    "url": "https://lakexpress-docs.arpe.io/latest/cli",
                    "source": "lakexpress",
                }
            ],
        )
        _, handler = build_release_notes_tool("lakexpress", engine)
        result = await handler({})
        assert "No release-notes chunks" in result[0].text
