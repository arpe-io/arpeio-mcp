"""
BM25 search index for documentation chunks.

Builds and manages BM25 indexes across multiple documentation sources,
with cache-aware initialization and background refresh.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple

import httpx
from rank_bm25 import BM25Okapi

from src.adbc.drivers import ADBC_DRIVERS

from .cache import (
    CACHE_DIR,
    best_version_for,
    get_cache_path,
    is_cache_fresh,
    load_cache,
    save_cache,
)
from .crawler import crawl_blog, crawl_doc_site
from .doc_versions import (
    BLOG_TTL_DAYS,
    BLOG_URL,
    DOC_TTL_DAYS,
    DOC_URLS,
    RELEASE_NOTES_UNPUBLISHED,
    VERSION_LISTS,
    DOC_URL_SCOPES,
    DOC_URL_VERSION_ALIASES,
)

logger = logging.getLogger(__name__)


def _tokenize(text: str) -> List[str]:
    """Tokenize text for BM25 indexing.

    Lowercases, splits on whitespace/hyphens/underscores, strips punctuation.
    Handles CLI flags like --paralleldegree -> ['paralleldegree'].
    """
    text = text.lower()
    # Replace hyphens and underscores with spaces (splits CLI flags)
    text = re.sub(r"[-_]+", " ", text)
    # Remove leading dashes from flags
    text = re.sub(r"\b-+", "", text)
    # Split on whitespace and filter
    tokens = re.split(r"\s+", text)
    # Strip non-alphanumeric from edges
    tokens = [re.sub(r"^[^\w]+|[^\w]+$", "", t) for t in tokens]
    return [t for t in tokens if len(t) >= 2]


class SearchEngine:
    """BM25 search engine over documentation from multiple sources.

    Manages per-source indexes that can be loaded from cache or built
    from crawled documentation. Supports background initialization.
    """

    def __init__(self):
        self._indexes: Dict[str, Tuple[List[dict], BM25Okapi]] = {}
        self._loading = False
        self._ready = False
        self._load_errors: List[str] = []

    @property
    def ready(self) -> bool:
        """Whether at least one index is loaded and searchable."""
        return bool(self._indexes)

    @property
    def fully_loaded(self) -> bool:
        """Whether all indexes have finished loading."""
        return self._ready

    def _build_index(self, source: str, chunks: List[dict]) -> None:
        """Build a BM25 index from chunks and store it.

        Args:
            source: Source name (e.g., 'fastbcp', 'blog').
            chunks: List of chunk dicts with 'text' field.
        """
        if not chunks:
            logger.warning(f"No chunks for {source}, skipping index build")
            return

        corpus = [_tokenize(chunk["text"]) for chunk in chunks]
        bm25 = BM25Okapi(corpus)
        self._indexes[source] = (chunks, bm25)
        logger.info(f"Built BM25 index for {source}: {len(chunks)} chunks")

    def search(self, question: str, top_k: int = 5) -> List[dict]:
        """Search across ALL loaded indexes.

        Args:
            question: Search query string.
            top_k: Maximum number of results to return.

        Returns:
            Top-k chunks sorted by BM25 score, each with an added 'score' field.
        """
        if not self._indexes:
            return []

        query_tokens = _tokenize(question)
        if not query_tokens:
            return []

        all_results: List[Tuple[float, dict]] = []

        for source, (chunks, bm25) in self._indexes.items():
            scores = bm25.get_scores(query_tokens)
            for i, score in enumerate(scores):
                if score > 0:
                    result = dict(chunks[i])
                    result["score"] = float(score)
                    all_results.append((score, result))

        # Sort by score descending
        all_results.sort(key=lambda x: x[0], reverse=True)

        return [r[1] for r in all_results[:top_k]]

    def get_release_notes(
        self, product: str, version: Optional[str] = None
    ) -> List[dict]:
        """Return cached release-notes chunks for a product, optionally filtered by version.

        Filters the indexed chunks for `product` down to those whose URL path
        contains "release-notes". If `version` is given, further filters to
        URLs matching "release-notes-{version}". Returns results in the original
        crawl order (typically heading order on the page), not by BM25 score.

        Args:
            product: One of "fastbcp", "fasttransfer", "lakexpress", "migratorxpress",
                or an ADBC driver load name ("arpemssql", "arpeteradata", ...).
            version: Optional version string like "0.31" or "0.4". If None, the
                most recent version available in the index is used.

        Returns:
            List of chunk dicts (may be empty if nothing is indexed yet).
        """
        if product in ADBC_DRIVERS:
            return self._get_adbc_whats_new(product, version)
        if product in RELEASE_NOTES_UNPUBLISHED:
            return self._get_whats_new_sections(product, version)

        if product not in self._indexes:
            return []

        chunks, _ = self._indexes[product]
        release_chunks = [
            c for c in chunks if "release-notes" in c.get("url", "").lower()
        ]
        if not release_chunks:
            return []

        if version:
            needle = f"release-notes-{version}".lower()
            filtered = [c for c in release_chunks if needle in c.get("url", "").lower()]
            return filtered

        # No explicit version: return chunks for the highest version seen
        def _extract_version(url: str) -> tuple:
            m = re.search(r"release-notes[-/](\d+(?:\.\d+)+)", url.lower())
            if not m:
                return ()
            return tuple(int(x) for x in m.group(1).split("."))

        versioned = [(_extract_version(c.get("url", "")), c) for c in release_chunks]
        versioned = [(v, c) for v, c in versioned if v]
        if not versioned:
            return release_chunks
        max_version = max(v for v, _ in versioned)
        return [c for v, c in versioned if v == max_version]

    def _get_adbc_whats_new(self, load_name: str, version: Optional[str]) -> List[dict]:
        """Return a driver's "What's new" chunks from the shared ADBC docs index.

        The page has one h2 section per release, headed "<topic> — <month> (vX.Y.Z)".
        Without `version` the whole page is returned (it is the release history,
        newest first); with it, the sections whose tag starts with that version.
        """
        if "adbc" not in self._indexes:
            return []
        chunks, _ = self._indexes["adbc"]
        marker = f"/drivers/{ADBC_DRIVERS[load_name].docs_slug}/whats-new"
        page = [c for c in chunks if marker in c.get("url", "").lower()]
        if not version:
            return page
        tag = re.compile(r"\(v" + re.escape(version.lstrip("vV")) + r"(?:[.)])")
        return [c for c in page if tag.search(c.get("source", ""))]

    def _get_whats_new_sections(self, product: str, version: Optional[str]) -> List[dict]:
        """Return the "What's new in X.Y" sections of a product without release-notes pages.

        MigratorXpress publishes no release notes; its 0.7 intro page has a
        "What's new in 0.7" section listing the 0.7.x changes. With `version`,
        only a section whose X.Y matches it ('0.7', or a patch such as '0.7.2')
        is returned.
        """
        if product not in self._indexes:
            return []
        chunks, _ = self._indexes[product]
        heading = re.compile(r"what[\u2019']s new in v?(\d+(?:\.\d+)*)", re.IGNORECASE)
        wanted = version.lstrip("vV") if version else None
        sections = []
        for c in chunks:
            m = heading.search(c.get("source", ""))
            if not m:
                continue
            section_version = m.group(1)
            if wanted and not (
                wanted == section_version
                or wanted.startswith(section_version + ".")
                or section_version.startswith(wanted + ".")
            ):
                continue
            sections.append(c)
        return sections

    async def initialize(self, version_info: Optional[Dict] = None) -> None:
        """Initialize all indexes from cache or by crawling.

        Loads cached indexes immediately, triggers background crawl for
        stale or missing caches.

        Args:
            version_info: Dict mapping product names to their config dicts
                (each with a 'path' key for the binary). Used to detect
                installed versions.
        """
        if self._loading:
            return
        self._loading = True

        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)

            # Detect installed versions
            detected_versions: Dict[str, Optional[str]] = {}
            for product in DOC_URLS:
                detected_versions[product] = None
                if version_info and product in version_info:
                    try:
                        from src.base.version_detector import BaseVersionDetector
                        # Try to get version from existing command builder
                        config = version_info[product]
                        binary_path = config.get("path", "")
                        if binary_path:
                            # Use a lightweight version check
                            import subprocess
                            result = subprocess.run(
                                [binary_path, "--version", "--nobanner"],
                                capture_output=True,
                                text=True,
                                timeout=5,
                            )
                            # Extract version number
                            import re as _re
                            match = _re.search(r"(\d+\.\d+(?:\.\d+)*)", result.stdout + result.stderr)
                            if match:
                                detected_versions[product] = match.group(1)
                    except Exception:
                        pass

            # Load or refresh each doc source
            # (product, sitemap url, version, url prefix or None)
            needs_crawl: List[Tuple[str, str, str, Optional[str]]] = []

            for product, url_template in DOC_URLS.items():
                known_versions = VERSION_LISTS.get(product, [])
                version = best_version_for(product, detected_versions.get(product), known_versions)

                # Build URL
                url_version = DOC_URL_VERSION_ALIASES.get(product, {}).get(version, version)
                if "{version}" in url_template:
                    url = url_template.format(version=url_version)
                else:
                    url = url_template
                scope = DOC_URL_SCOPES.get(product)
                url_prefix = scope.format(version=url_version) if scope else None

                cache_path = get_cache_path(product, version)

                if is_cache_fresh(cache_path, DOC_TTL_DAYS):
                    # Load from cache
                    cached = load_cache(cache_path)
                    if cached:
                        self._build_index(product, cached)
                        continue

                # Need to crawl
                needs_crawl.append((product, url, version, url_prefix))

            # Blog
            blog_cache_path = get_cache_path("blog")
            if is_cache_fresh(blog_cache_path, BLOG_TTL_DAYS):
                cached = load_cache(blog_cache_path)
                if cached:
                    self._build_index("blog", cached)
            else:
                needs_crawl.append(("blog", BLOG_URL, "", None))

            # Crawl missing/stale sources
            if needs_crawl:
                await self._crawl_sources(needs_crawl)

        except Exception as e:
            logger.error(f"Error during search engine initialization: {e}")
            self._load_errors.append(str(e))
        finally:
            self._ready = True
            self._loading = False

    async def _crawl_sources(
        self, sources: List[Tuple[str, str, str, Optional[str]]]
    ) -> None:
        """Crawl documentation sources and build indexes.

        Args:
            sources: List of (product, url, version, url_prefix) tuples to crawl;
                url_prefix (or None) limits the crawl to one version's pages.
        """
        async with httpx.AsyncClient() as client:
            for product, url, version, url_prefix in sources:
                try:
                    if product == "blog":
                        chunks = await crawl_blog(url, client)
                    else:
                        chunks = await crawl_doc_site(url, product, client, url_prefix)

                    if chunks:
                        self._build_index(product, chunks)
                        # Save to cache
                        cache_path = get_cache_path(product, version if version else None)
                        save_cache(cache_path, chunks)
                    else:
                        logger.warning(f"No content crawled for {product}")

                except Exception as e:
                    logger.error(f"Failed to crawl {product}: {e}")
                    self._load_errors.append(f"{product}: {e}")
