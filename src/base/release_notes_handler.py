"""Shared handler for per-product `*_release_notes` MCP tools.

Exposes a single `build_release_notes_tool` factory that returns a
`(Tool, handler)` pair wired to a `SearchEngine` and a product key.
"""

from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from mcp.types import Tool, ToolAnnotations, TextContent

from .structured import respond
from ..adbc.drivers import ADBC_DRIVERS
from ..doc_search.doc_versions import RELEASE_NOTES_UNPUBLISHED
from ..doc_search.index import SearchEngine

# "fastbcp" | "fasttransfer" | "lakexpress" | "migratorxpress", or an ADBC
# driver load name ("arpemssql", "arpeteradata", ...)
ProductKey = str

_PRODUCT_DISPLAY_NAMES: Dict[ProductKey, str] = {
    "fastbcp": "FastBCP",
    "fasttransfer": "FastTransfer",
    "lakexpress": "LakeXpress",
    "migratorxpress": "MigratorXpress",
    **{key: drv.name for key, drv in ADBC_DRIVERS.items()},
}


def build_release_notes_tool(
    product: ProductKey,
    search_engine: SearchEngine,
) -> Tuple[Tool, Callable[[Dict[str, Any]], Awaitable[Tuple[List[TextContent], Dict[str, Any]]]]]:
    """Build a `{product}_release_notes` Tool and its async handler.

    The tool returns text chunks from the indexed release-notes pages for the
    given product. It is read-only and idempotent.
    """
    display = _PRODUCT_DISPLAY_NAMES[product]
    tool_name = f"{product}_release_notes"

    tool = Tool(
        name=tool_name,
        description=(
            f"Return release-notes chunks for {display}. "
            "Pass `version` to filter to a specific release (e.g. '0.31', '0.4'); "
            "omit it to get the newest indexed version. Read-only; queries the local "
            "docs cache populated by the same crawler that powers `search_docs`."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "version": {
                    "type": "string",
                    "description": (
                        "Optional version string such as '0.31' or '0.4.0'. "
                        "When omitted, the newest indexed release-notes page is returned."
                    ),
                },
            },
            "required": [],
        },
    )

    # Products without release-notes pages (MigratorXpress): say so instead of
    # pointing at pages that do not exist.
    unpublished_docs_url: Optional[str] = RELEASE_NOTES_UNPUBLISHED.get(product)
    extra: Dict[str, Any] = (
        {"release_notes_published": False, "docs_url": unpublished_docs_url}
        if unpublished_docs_url else {}
    )

    async def handler(arguments: Dict[str, Any]) -> Tuple[List[TextContent], Dict[str, Any]]:
        version: Optional[str] = arguments.get("version") or None
        chunks = search_engine.get_release_notes(product, version)

        if not chunks:
            if not search_engine.ready:
                msg = (
                    f"# {display} Release Notes\n\n"
                    "Documentation index is still loading. Try again in a moment."
                )
            elif unpublished_docs_url:
                scope = f" for version {version!r}" if version else ""
                msg = (
                    f"# {display} Release Notes\n\n"
                    f"{display} has no published release notes{scope}. "
                    f"See the documentation: {unpublished_docs_url}"
                )
            elif version:
                msg = (
                    f"# {display} Release Notes\n\n"
                    f"No cached release notes found for version {version!r}. "
                    f"Call `search_docs` with a query like 'release notes {version}' "
                    "to locate the page, or check the docs site directly."
                )
            else:
                msg = (
                    f"# {display} Release Notes\n\n"
                    "No release-notes chunks are indexed yet. Call `search_docs` "
                    "once to trigger a crawl, then retry."
                )
            return respond(msg, {
                "status": "ok",
                "tool": "arpe_release_notes",
                "product": product,
                "version": version,
                "count": 0,
                "chunks": [],
                "index_ready": bool(search_engine.ready),
                **extra,
            })

        parts: List[str] = [f"# {display} Release Notes"]
        if version:
            parts.append(f"*Filtered to version {version}*")
        if unpublished_docs_url:
            parts.append(
                f"*{display} has no published release notes; this is the \"What's new\" "
                f"summary from the documentation ({unpublished_docs_url}).*"
            )
        parts.append("")
        seen_urls = set()
        structured_chunks: List[Dict[str, str]] = []
        for chunk in chunks:
            url = chunk.get("url", "")
            if url and url not in seen_urls:
                parts.append(f"Source: {url}")
                seen_urls.add(url)
                parts.append("")
            text = chunk.get("text", "").strip()
            if text:
                parts.append(text)
                parts.append("")
                parts.append("---")
                parts.append("")
            structured_chunks.append({"url": url, "text": text})

        return respond("\n".join(parts), {
            "status": "ok",
            "tool": "arpe_release_notes",
            "product": product,
            "version": version,
            "count": len(structured_chunks),
            "chunks": structured_chunks,
            "index_ready": bool(search_engine.ready),
            **extra,
        })

    return tool, handler
