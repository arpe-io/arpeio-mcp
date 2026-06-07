"""Helpers for building MCP tool results that carry structured content.

The low-level MCP ``Server.call_tool`` handler accepts three return shapes:

* ``list[ContentBlock]``                -> unstructured content only
* ``dict``                              -> structured content only
* ``(list[ContentBlock], dict)`` tuple  -> both

When a tool declares an ``outputSchema``, **every** return path for that tool
must produce conforming structured content -- including validation-error and
execution-error paths, and the server's global exception handler. To make that
robust, :func:`make_output_schema` builds a schema whose only required field is
a ``status`` discriminator with ``additionalProperties`` allowed, so any
``{"status": "ok"|"error", ...}`` payload validates while the declared
``properties`` still document the useful fields for clients.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple, Union

from mcp.types import TextContent


def make_output_schema(properties: Dict[str, Any]) -> Dict[str, Any]:
    """Build a permissive tool ``outputSchema`` with a ``status`` discriminator.

    Args:
        properties: JSON-Schema property definitions for the tool's structured
            output (documentation for clients). ``status`` is added automatically.

    Returns:
        A JSON Schema object requiring only ``status`` and allowing additional
        properties, so success and error payloads both validate.
    """
    props: Dict[str, Any] = {
        "status": {
            "type": "string",
            "enum": ["ok", "error"],
            "description": (
                "'ok' when the call succeeded; 'error' when the parameters "
                "failed validation or execution failed."
            ),
        },
    }
    props.update(properties)
    return {
        "type": "object",
        "properties": props,
        "required": ["status"],
        "additionalProperties": True,
    }


def respond(
    text: Union[str, List[str]],
    structured: Dict[str, Any],
) -> Tuple[List[TextContent], Dict[str, Any]]:
    """Return an MCP tool result carrying both human text and structured content.

    Args:
        text: Markdown to show the user. A list is joined with newlines.
        structured: The machine-readable payload (must include a ``status`` key).

    Returns:
        A ``(content, structured)`` tuple suitable for returning from a
        ``call_tool`` handler.
    """
    if isinstance(text, list):
        text = "\n".join(text)
    return ([TextContent(type="text", text=text)], structured)
