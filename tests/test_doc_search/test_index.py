"""Tests for doc_search index module."""

import pytest

from src.doc_search.index import SearchEngine, _tokenize


class TestTokenize:
    def test_basic(self):
        tokens = _tokenize("hello world")
        assert tokens == ["hello", "world"]

    def test_lowercases(self):
        tokens = _tokenize("Hello WORLD")
        assert tokens == ["hello", "world"]

    def test_splits_hyphens(self):
        tokens = _tokenize("parallel-degree")
        assert "parallel" in tokens
        assert "degree" in tokens

    def test_strips_cli_flag_dashes(self):
        tokens = _tokenize("--paralleldegree")
        assert "paralleldegree" in tokens

    def test_strips_punctuation(self):
        tokens = _tokenize("hello, world!")
        assert "hello" in tokens
        assert "world" in tokens

    def test_filters_short_tokens(self):
        tokens = _tokenize("a b cd ef")
        assert "a" not in tokens
        assert "b" not in tokens
        assert "cd" in tokens
        assert "ef" in tokens

    def test_underscores_split(self):
        tokens = _tokenize("distribute_key_column")
        assert "distribute" in tokens
        assert "key" in tokens
        assert "column" in tokens

    def test_empty_string(self):
        assert _tokenize("") == []


class TestSearchEngine:
    def _make_engine_with_data(self):
        """Create a SearchEngine with test data loaded."""
        engine = SearchEngine()
        chunks = [
            {"id": "1", "text": "FastBCP Ntile parallelism method for even distribution", "source": "fastbcp", "url": "http://ex.com/1"},
            {"id": "2", "text": "DataDriven method uses distinct values for partitioning", "source": "fastbcp", "url": "http://ex.com/2"},
            {"id": "3", "text": "PhysLoc is SQL Server specific no key needed", "source": "fastbcp", "url": "http://ex.com/3"},
            {"id": "4", "text": "PostgreSQL export with pgcopy connection type", "source": "fastbcp", "url": "http://ex.com/4"},
            {"id": "5", "text": "Parquet compression options Snappy Gzip Zstd Lz4", "source": "fastbcp", "url": "http://ex.com/5"},
            {"id": "6", "text": "FastTransfer database to database transfer between systems", "source": "fasttransfer", "url": "http://ex.com/6"},
            {"id": "7", "text": "LakeXpress pipeline from Oracle to Snowflake as Parquet", "source": "lakexpress", "url": "http://ex.com/7"},
        ]
        engine._build_index("test", chunks)
        return engine

    def test_ready_when_index_built(self):
        engine = self._make_engine_with_data()
        assert engine.ready is True

    def test_not_ready_when_empty(self):
        engine = SearchEngine()
        assert engine.ready is False

    def test_search_returns_results(self):
        engine = self._make_engine_with_data()
        results = engine.search("Ntile parallelism")
        assert len(results) > 0
        assert results[0]["id"] == "1"  # Most relevant

    def test_search_relevance_ranking(self):
        engine = self._make_engine_with_data()
        results = engine.search("parquet compression")
        assert len(results) > 0
        # The parquet compression chunk should rank first
        assert results[0]["id"] == "5"

    def test_search_cross_source(self):
        engine = self._make_engine_with_data()
        results = engine.search("Snowflake pipeline")
        assert len(results) > 0
        assert any("lakexpress" in r.get("source", "") for r in results)

    def test_search_top_k_limits_results(self):
        engine = self._make_engine_with_data()
        results = engine.search("method", top_k=2)
        assert len(results) <= 2

    def test_search_empty_query(self):
        engine = self._make_engine_with_data()
        results = engine.search("")
        assert results == []

    def test_search_no_match(self):
        engine = self._make_engine_with_data()
        results = engine.search("xyznonexistentterm123")
        assert results == []

    def test_results_have_score(self):
        engine = self._make_engine_with_data()
        results = engine.search("parallelism")
        assert len(results) > 0
        assert "score" in results[0]
        assert results[0]["score"] > 0

    def test_results_sorted_by_score(self):
        engine = self._make_engine_with_data()
        results = engine.search("method")
        scores = [r["score"] for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_build_index_empty_chunks(self):
        engine = SearchEngine()
        engine._build_index("empty", [])
        assert "empty" not in engine._indexes

    def test_multiple_indexes(self):
        engine = SearchEngine()
        # BM25 IDF needs n>=3 docs for a term in 1 doc to score > 0
        chunks1 = [
            {"id": "a1", "text": "FastBCP export tool for databases and files", "source": "fastbcp", "url": "http://ex.com/a1"},
            {"id": "a2", "text": "Configuration and settings for exports", "source": "fastbcp", "url": "http://ex.com/a2"},
            {"id": "a3", "text": "Parallelism methods for extraction", "source": "fastbcp", "url": "http://ex.com/a3"},
        ]
        chunks2 = [
            {"id": "b1", "text": "FastTransfer transfer tool for databases and tables", "source": "fasttransfer", "url": "http://ex.com/b1"},
            {"id": "b2", "text": "Connection setup and validation steps", "source": "fasttransfer", "url": "http://ex.com/b2"},
            {"id": "b3", "text": "Load modes and mapping options", "source": "fasttransfer", "url": "http://ex.com/b3"},
        ]
        engine._build_index("source1", chunks1)
        engine._build_index("source2", chunks2)
        results = engine.search("databases")
        assert len(results) >= 2
