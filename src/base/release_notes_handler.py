"""Shared handler for per-product `*_release_notes` MCP tools.

Exposes a single `build_release_notes_tool` factory that returns a
`(Tool, handler)` pair wired to a `SearchEngine` and a product key.
"""

from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from mcp.types import Tool, ToolAnnotations, TextContent

from ..doc_search.index import SearchEngine

ProductKey = str  # "fastbcp" | "fasttransfer" | "lakexpress" | "migratorxpress"

_PRODUCT_DISPLAY_NAMES: Dict[ProductKey, str] = {
    "fastbcp": "FastBCP",
    "fasttransfer": "FastTransfer",
    "lakexpress": "LakeXpress",
    "migratorxpress": "MigratorXpress",
}


def build_release_notes_tool(
    product: ProductKey,
    search_engine: SearchEngine,
) -> Tuple[Tool, Callable[[Dict[str, Any]], Awaitable[List[TextContent]]]]:
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

    async def handler(arguments: Dict[str, Any]) -> List[TextContent]:
        version: Optional[str] = arguments.get("version") or None
        chunks = search_engine.get_release_notes(product, version)

        if not chunks:
            if not search_engine.ready:
                msg = (
                    f"# {display} Release Notes\n\n"
                    "Documentation index is still loading. Try again in a moment."
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
            return [TextContent(type="text", text=msg)]

        parts: List[str] = [f"# {display} Release Notes"]
        if version:
            parts.append(f"*Filtered to version {version}*")
        parts.append("")
        seen_urls = set()
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

        return [TextContent(type="text", text="\n".join(parts))]

    return tool, handler
