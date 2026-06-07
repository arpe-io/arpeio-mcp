"""Tests for doc_search tools module."""

import pytest

from src.doc_search.index import SearchEngine
from src.doc_search.tools import create_tools


class TestCreateTools:
    def test_returns_tools_and_handler(self):
        engine = SearchEngine()
        tools, handler = create_tools(engine)
        assert len(tools) == 1
        assert tools[0].name == "search_docs"
        assert callable(handler)

    def test_tool_has_required_fields(self):
        engine = SearchEngine()
        tools, _ = create_tools(engine)
        tool = tools[0]
        assert tool.description
        assert tool.inputSchema
        assert "question" in tool.inputSchema["properties"]
        assert "question" in tool.inputSchema["required"]


class TestSearchDocsHandler:
    def _make_engine_with_data(self):
        engine = SearchEngine()
        # Need >=3 docs with the query term in a minority for BM25 idf to be positive.
        chunks = [
            {"id": "1", "text": "Ntile parallelism distributes data evenly across threads", "source": "fastbcp / Parallelism", "url": "http://ex.com/1"},
            {"id": "2", "text": "Parquet is the recommended format for analytics workloads", "source": "fastbcp / Formats", "url": "http://ex.com/2"},
            {"id": "3", "text": "Connection strings configure database authentication", "source": "fastbcp / Connections", "url": "http://ex.com/3"},
        ]
        engine._build_index("test", chunks)
        return engine

    @pytest.mark.asyncio
    async def test_search_returns_results(self):
        engine = self._make_engine_with_data()
        _, handler = create_tools(engine)
        content, structured = await handler("search_docs", {"question": "Ntile parallelism"})
        assert len(content) == 1
        text = content[0].text
        assert "Ntile" in text
        assert "Documentation Search" in text
        # structured content carries the results as records
        assert structured["status"] == "ok"
        assert structured["count"] >= 1
        assert any("Ntile" in r["text"] for r in structured["results"])

    @pytest.mark.asyncio
    async def test_search_no_results(self):
        engine = self._make_engine_with_data()
        _, handler = create_tools(engine)
        content, structured = await handler("search_docs", {"question": "xyznonexistent123"})
        text = content[0].text
        assert "No results found" in text
        assert structured["status"] == "ok"
        assert structured["count"] == 0
        assert structured["results"] == []

    @pytest.mark.asyncio
    async def test_search_empty_question(self):
        engine = self._make_engine_with_data()
        _, handler = create_tools(engine)
        content, structured = await handler("search_docs", {"question": ""})
        text = content[0].text
        assert "Error" in text
        assert structured["status"] == "error"

    @pytest.mark.asyncio
    async def test_search_not_ready(self):
        engine = SearchEngine()  # No data loaded
        _, handler = create_tools(engine)
        content, structured = await handler("search_docs", {"question": "parallelism"})
        text = content[0].text
        assert "still loading" in text
        assert structured["index_ready"] is False

    @pytest.mark.asyncio
    async def test_handler_returns_none_for_unknown_tool(self):
        engine = self._make_engine_with_data()
        _, handler = create_tools(engine)
        result = await handler("unknown_tool", {"question": "test"})
        assert result is None

    @pytest.mark.asyncio
    async def test_top_k_parameter(self):
        engine = SearchEngine()
        chunks = [
            {"id": str(i), "text": f"Document about parallelism method number {i}", "source": "test", "url": f"http://ex.com/{i}"}
            for i in range(10)
        ]
        engine._build_index("test", chunks)
        _, handler = create_tools(engine)
        content, structured = await handler("search_docs", {"question": "parallelism", "top_k": 3})
        text = content[0].text
        # Count result headers
        assert text.count("## [") <= 3
        assert structured["count"] <= 3
