"""
Documentation site crawler and HTML chunker.

Fetches pages from Docusaurus doc sites, GitHub Pages, and the blog,
strips HTML to plain text, and splits content into searchable chunks.
"""

import logging
import re
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Timeout for individual HTTP requests
REQUEST_TIMEOUT = 30.0


def _slugify(text: str) -> str:
    """Convert a heading to a URL-safe slug."""
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[\s_]+", "-", slug).strip("-")
    return slug[:80]  # cap length


def _extract_main_content(soup: BeautifulSoup) -> BeautifulSoup:
    """Extract the main content area, skipping nav/header/footer."""
    # Docusaurus uses <article> or <main> for content
    for selector in ("article", "main", '[role="main"]', ".markdown"):
        content = soup.find(selector)
        if content:
            return content

    # Fall back to body
    body = soup.find("body")
    if body:
        # Remove nav, header, footer, sidebar elements
        for tag in body.find_all(["nav", "header", "footer", "aside"]):
            tag.decompose()
        return body

    return soup


def chunk_html(html: str, url: str, source_name: str) -> List[dict]:
    """Parse HTML and split into chunks by h2/h3 headings.

    Args:
        html: Raw HTML content.
        url: Source URL for attribution.
        source_name: Product/source name (e.g., 'fastbcp', 'blog').

    Returns:
        List of chunk dicts with keys: id, text, source, url.
    """
    soup = BeautifulSoup(html, "html.parser")
    content = _extract_main_content(soup)

    # Get page title from h1 or <title>
    h1 = content.find("h1")
    page_title = h1.get_text(strip=True) if h1 else ""
    if not page_title:
        title_tag = soup.find("title")
        page_title = title_tag.get_text(strip=True) if title_tag else ""

    # Extract URL slug for chunk IDs
    path = urlparse(url).path.strip("/")
    page_slug = _slugify(path.split("/")[-1]) if path else "index"

    chunks = []
    current_heading = page_title or page_slug
    current_text_parts: list[str] = []

    def _flush():
        text = "\n".join(current_text_parts).strip()
        if text and len(text) > 20:  # Skip very short chunks
            heading_slug = _slugify(current_heading)
            chunk_id = f"{source_name}-{page_slug}-{heading_slug}"
            section = f"{page_title} / {current_heading}" if current_heading != page_title else page_title
            chunks.append({
                "id": chunk_id,
                "text": text,
                "source": f"{source_name} / {section}",
                "url": url,
            })

    for element in content.descendants:
        if element.name in ("h2", "h3"):
            _flush()
            current_heading = element.get_text(strip=True)
            current_text_parts = []
        elif element.name in ("p", "li", "td", "th", "pre", "code", "blockquote", "dt", "dd"):
            text = element.get_text(separator=" ", strip=True)
            if text:
                current_text_parts.append(text)

    _flush()

    # If no headings were found, create a single chunk from the whole page
    if not chunks:
        full_text = content.get_text(separator="\n", strip=True)
        if full_text and len(full_text) > 20:
            chunks.append({
                "id": f"{source_name}-{page_slug}",
                "text": full_text,
                "source": f"{source_name} / {page_title or page_slug}",
                "url": url,
            })

    return chunks


async def crawl_sitemap_page(sitemap_url: str, client: httpx.AsyncClient) -> List[str]:
    """Fetch a Docusaurus /sitemap page and extract all page links.

    Docusaurus sitemap pages are HTML pages with links, not XML sitemaps.
    Also tries sitemap.xml as a fallback.

    Args:
        sitemap_url: URL of the sitemap page.
        client: httpx async client.

    Returns:
        List of page URLs found.
    """
    urls = []
    try:
        resp = await client.get(sitemap_url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()

        content_type = resp.headers.get("content-type", "")
        text = resp.text

        if "xml" in content_type or text.strip().startswith("<?xml"):
            # XML sitemap
            soup = BeautifulSoup(text, "html.parser")
            for loc in soup.find_all("loc"):
                url = loc.get_text(strip=True)
                if url:
                    urls.append(url)
        else:
            # HTML sitemap page
            soup = BeautifulSoup(text, "html.parser")
            base_url = f"{urlparse(sitemap_url).scheme}://{urlparse(sitemap_url).netloc}"
            for a in soup.find_all("a", href=True):
                href = a["href"]
                full_url = urljoin(base_url, href)
                # Only include same-domain links
                if urlparse(full_url).netloc == urlparse(sitemap_url).netloc:
                    urls.append(full_url)

    except (httpx.HTTPError, Exception) as e:
        logger.warning(f"Failed to fetch sitemap {sitemap_url}: {e}")

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)

    logger.info(f"Sitemap {sitemap_url}: found {len(unique)} URLs")
    return unique


async def fetch_and_chunk(url: str, source_name: str, client: httpx.AsyncClient) -> List[dict]:
    """Fetch a single page and chunk its content.

    Args:
        url: Page URL.
        source_name: Product name for chunk attribution.
        client: httpx async client.

    Returns:
        List of chunk dicts.
    """
    try:
        resp = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
        return chunk_html(resp.text, url, source_name)
    except (httpx.HTTPError, Exception) as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return []


async def crawl_doc_site(
    sitemap_url: str,
    source_name: str,
    client: httpx.AsyncClient,
) -> List[dict]:
    """Crawl a complete documentation site via its sitemap.

    Args:
        sitemap_url: URL to the sitemap page.
        source_name: Product name (e.g., 'fastbcp').
        client: httpx async client.

    Returns:
        List of all chunks from the site.
    """
    page_urls = await crawl_sitemap_page(sitemap_url, client)
    if not page_urls:
        logger.warning(f"No URLs found for {source_name} at {sitemap_url}")
        return []

    all_chunks = []
    for url in page_urls:
        chunks = await fetch_and_chunk(url, source_name, client)
        all_chunks.extend(chunks)

    logger.info(f"Crawled {source_name}: {len(page_urls)} pages, {len(all_chunks)} chunks")
    return all_chunks


async def crawl_blog(blog_url: str, client: httpx.AsyncClient) -> List[dict]:
    """Crawl the arpe.io blog.

    Fetches the blog index to discover post URLs, then fetches and chunks each post.

    Args:
        blog_url: Blog index URL.
        client: httpx async client.

    Returns:
        List of blog post chunks.
    """
    try:
        resp = await client.get(blog_url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except (httpx.HTTPError, Exception) as e:
        logger.warning(f"Failed to fetch blog index {blog_url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    base_url = f"{urlparse(blog_url).scheme}://{urlparse(blog_url).netloc}"

    # Find blog post links
    post_urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        # Blog posts typically have longer paths than the index
        if (parsed.netloc == urlparse(blog_url).netloc
                and len(parsed.path.strip("/").split("/")) >= 2
                and not parsed.path.endswith((".css", ".js", ".png", ".jpg", ".svg"))):
            post_urls.add(full_url)

    all_chunks = []
    for url in sorted(post_urls):
        chunks = await fetch_and_chunk(url, "blog", client)
        all_chunks.extend(chunks)

    logger.info(f"Crawled blog: {len(post_urls)} posts, {len(all_chunks)} chunks")
    return all_chunks
