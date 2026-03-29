"""
MCP tool definition and handler for documentation search.
"""

import logging
from typing import Any, Callable, Dict, List, Tuple

from mcp.types import Tool, ToolAnnotations, TextContent

from .index import SearchEngine

logger = logging.getLogger(__name__)


def create_tools(search_engine: SearchEngine) -> Tuple[List[Tool], Callable]:
    """Create the search_docs MCP tool and its handler.

    Args:
        search_engine: Initialized SearchEngine instance.

    Returns:
        Tuple of (tools_list, handle_call_async_function).
    """
    tools = [
        Tool(
            name="search_docs",
            description=(
                "Search arpe.io documentation and blog for answers about CLI parameters, "
                "use cases, parallelism strategies, integrations, and best practices. "
                "Call this when unsure about a parameter value, need an example command, "
                "or want the recommended approach for a specific source/target/format combination. "
                "Searches across all product documentation (FastBCP, FastTransfer, LakeXpress, MigratorXpress) "
                "and the arpe.io blog simultaneously. Does not execute anything."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Search query — a question or keywords about arpe.io tools, parameters, or use cases (e.g., 'how to use Ntile parallelism', 'parquet compression options', 'Oracle to Snowflake pipeline').",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Maximum number of results to return (1-10).",
                        "default": 5,
                        "minimum": 1,
                        "maximum": 10,
                    },
                },
                "required": ["question"],
            },
        ),
    ]

    async def handle_call(name: str, arguments: Dict[str, Any]):
        """Route tool calls to the search handler."""
        if name != "search_docs":
            return None
        return await handle_search_docs(arguments)

    async def handle_search_docs(arguments: Dict[str, Any]) -> List[TextContent]:
        """Handle search_docs tool calls."""
        question = arguments.get("question", "")
        top_k = min(max(arguments.get("top_k", 5), 1), 10)

        if not question.strip():
            return [TextContent(
                type="text",
                text="Error: Please provide a search question or keywords.",
            )]

        results = search_engine.search(question, top_k=top_k)

        if not results:
            parts = ["# Documentation Search Results", ""]
            if not search_engine.ready:
                parts.append(
                    "Documentation indexes are still loading. "
                    "Please try again in a moment."
                )
            else:
                parts.append(
                    f"No results found for: \"{question}\"\n\n"
                    "Try different keywords or check the arpe.io documentation directly."
                )
            return [TextContent(type="text", text="\n".join(parts))]

        # Format results
        parts = [f"# Documentation Search: \"{question}\"", ""]

        if not search_engine.fully_loaded:
            parts.append("*Note: Some documentation sources are still loading.*")
            parts.append("")

        for i, result in enumerate(results, 1):
            source = result.get("source", "Unknown")
            url = result.get("url", "")
            text = result.get("text", "")

            # Truncate long chunks
            if len(text) > 800:
                text = text[:800] + "..."

            parts.append(f"## [{i}] {source}")
            if url:
                parts.append(f"*{url}*")
            parts.append("")
            parts.append(text)
            parts.append("")
            parts.append("---")
            parts.append("")

        return [TextContent(type="text", text="\n".join(parts))]

    return tools, handle_call
