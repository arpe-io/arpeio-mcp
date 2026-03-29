"""Tests for doc_search cache module."""

import gzip
import json
import time
from pathlib import Path

import pytest

from src.doc_search.cache import (
    best_version_for,
    get_cache_path,
    is_cache_fresh,
    load_cache,
    save_cache,
)


class TestGetCachePath:
    def test_versioned_path(self):
        path = get_cache_path("fastbcp", "0.30")
        assert path.name == "fastbcp_0.30.json.gz"
        assert str(path).endswith(".cache/arpeio-mcp/fastbcp_0.30.json.gz")

    def test_unversioned_path(self):
        path = get_cache_path("blog")
        assert path.name == "blog.json.gz"

    def test_versioned_vs_unversioned(self):
        versioned = get_cache_path("fastbcp", "0.30")
        unversioned = get_cache_path("fastbcp")
        assert versioned != unversioned


class TestSaveAndLoadCache:
    def test_roundtrip(self, tmp_path):
        path = tmp_path / "test.json.gz"
        data = [
            {"id": "test-1", "text": "hello world", "source": "test", "url": "http://example.com"},
            {"id": "test-2", "text": "foo bar", "source": "test", "url": "http://example.com/2"},
        ]
        save_cache(path, data)
        assert path.exists()

        loaded = load_cache(path)
        assert loaded == data

    def test_load_missing_file(self, tmp_path):
        path = tmp_path / "missing.json.gz"
        assert load_cache(path) is None

    def test_load_corrupt_file(self, tmp_path):
        path = tmp_path / "corrupt.json.gz"
        path.write_bytes(b"not valid gzip data")
        assert load_cache(path) is None

    def test_load_non_list_json(self, tmp_path):
        path = tmp_path / "notlist.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump({"key": "value"}, f)
        assert load_cache(path) is None

    def test_save_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "nested" / "dir" / "test.json.gz"
        save_cache(path, [{"id": "1", "text": "test"}])
        assert path.exists()

    def test_empty_list_roundtrip(self, tmp_path):
        path = tmp_path / "empty.json.gz"
        save_cache(path, [])
        loaded = load_cache(path)
        assert loaded == []


class TestIsCacheFresh:
    def test_missing_file_not_fresh(self, tmp_path):
        path = tmp_path / "missing.json.gz"
        assert is_cache_fresh(path, ttl_days=7) is False

    def test_fresh_file(self, tmp_path):
        path = tmp_path / "fresh.json.gz"
        save_cache(path, [{"id": "1", "text": "test"}])
        assert is_cache_fresh(path, ttl_days=7) is True

    def test_stale_file(self, tmp_path):
        path = tmp_path / "stale.json.gz"
        save_cache(path, [{"id": "1", "text": "test"}])
        # Set mtime to 10 days ago
        old_time = time.time() - (10 * 86400)
        import os
        os.utime(path, (old_time, old_time))
        assert is_cache_fresh(path, ttl_days=7) is False


class TestBestVersionFor:
    def test_exact_match(self):
        result = best_version_for("fastbcp", "0.30.1.0", ["0.28", "0.29", "0.30"])
        assert result == "0.30"

    def test_prefix_match(self):
        result = best_version_for("fastbcp", "0.29.5.0", ["0.28", "0.29", "0.30"])
        assert result == "0.29"

    def test_no_match_falls_back_to_latest(self):
        result = best_version_for("fastbcp", "0.99.0.0", ["0.28", "0.29", "0.30"])
        assert result == "0.30"

    def test_no_detected_version_returns_latest(self):
        result = best_version_for("fastbcp", None, ["0.28", "0.29", "0.30"])
        assert result == "0.30"

    def test_empty_versions_returns_latest_string(self):
        result = best_version_for("fastbcp", "0.30.0.0", [])
        assert result == "latest"
