"""Tests for doc_search crawler module."""

import pytest

from src.doc_search.crawler import chunk_html, _slugify


class TestSlugify:
    def test_simple(self):
        assert _slugify("Hello World") == "hello-world"

    def test_special_chars(self):
        assert _slugify("What's New?") == "whats-new"

    def test_long_text_truncated(self):
        result = _slugify("a" * 200)
        assert len(result) <= 80

    def test_empty(self):
        assert _slugify("") == ""


class TestChunkHtml:
    def test_basic_chunking_by_h2(self):
        html = """
        <html><body>
        <article>
            <h1>Page Title</h1>
            <p>Introduction text.</p>
            <h2>Section One</h2>
            <p>Content of section one with enough text to pass the minimum length threshold for chunks.</p>
            <h2>Section Two</h2>
            <p>Content of section two with enough text to pass the minimum length threshold for chunks.</p>
        </article>
        </body></html>
        """
        chunks = chunk_html(html, "https://example.com/page", "test")
        assert len(chunks) >= 2
        # Check chunk structure
        for chunk in chunks:
            assert "id" in chunk
            assert "text" in chunk
            assert "source" in chunk
            assert "url" in chunk
            assert chunk["url"] == "https://example.com/page"

    def test_h3_also_splits(self):
        html = """
        <html><body>
        <article>
            <h1>Title</h1>
            <h2>Main Section</h2>
            <p>Some introductory text that is long enough to be a valid chunk for our system.</p>
            <h3>Subsection</h3>
            <p>Subsection content that is also long enough to be a valid chunk for our system.</p>
        </article>
        </body></html>
        """
        chunks = chunk_html(html, "https://example.com/page", "test")
        assert len(chunks) >= 2

    def test_no_headings_produces_single_chunk(self):
        html = """
        <html><body>
        <article>
            <p>This is a full page of text content without any headings that should produce a single chunk.</p>
        </article>
        </body></html>
        """
        chunks = chunk_html(html, "https://example.com/simple", "test")
        assert len(chunks) == 1

    def test_chunk_id_format(self):
        html = """
        <html><body>
        <article>
            <h1>Page Title</h1>
            <h2>Installation Guide</h2>
            <p>Install instructions with enough text to pass the minimum chunk size threshold.</p>
        </article>
        </body></html>
        """
        chunks = chunk_html(html, "https://example.com/docs/install", "fastbcp")
        assert len(chunks) >= 1
        assert chunks[0]["id"].startswith("fastbcp-")

    def test_source_format(self):
        html = """
        <html><body>
        <article>
            <h1>Parallelism</h1>
            <h2>Ntile Method</h2>
            <p>The Ntile method distributes data evenly across parallel threads for maximum throughput.</p>
        </article>
        </body></html>
        """
        chunks = chunk_html(html, "https://example.com/parallel", "fastbcp")
        assert len(chunks) >= 1
        assert "fastbcp" in chunks[0]["source"]

    def test_very_short_text_skipped(self):
        html = """
        <html><body>
        <article>
            <h2>Short</h2>
            <p>tiny</p>
            <h2>Long Enough</h2>
            <p>This section has enough content to be included as a valid search result chunk.</p>
        </article>
        </body></html>
        """
        chunks = chunk_html(html, "https://example.com/page", "test")
        # The "tiny" chunk should be skipped
        assert all(len(c["text"]) > 20 for c in chunks)

    def test_nav_footer_excluded(self):
        html = """
        <html><body>
        <nav><a href="/">Home</a></nav>
        <article>
            <h1>Content</h1>
            <p>Main content area with sufficient text to be a valid chunk for our system.</p>
        </article>
        <footer>Footer text</footer>
        </body></html>
        """
        chunks = chunk_html(html, "https://example.com/page", "test")
        for chunk in chunks:
            assert "Footer text" not in chunk["text"]

    def test_empty_html(self):
        chunks = chunk_html("", "https://example.com", "test")
        assert chunks == []

    def test_code_blocks_included(self):
        html = """
        <html><body>
        <article>
            <h2>Example Command</h2>
            <p>Run the following command to export data from the database:</p>
            <pre><code>FastBCP --source pgsql --database mydb --table users</code></pre>
        </article>
        </body></html>
        """
        chunks = chunk_html(html, "https://example.com/example", "test")
        assert len(chunks) >= 1
        assert "FastBCP" in chunks[0]["text"]
