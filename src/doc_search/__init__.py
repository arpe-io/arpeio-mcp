"""
Documentation search module for arpeio-mcp.

Provides BM25-based search over arpe.io documentation sites and blog,
with local caching and background refresh.
"""

from .index import SearchEngine
from .tools import create_tools

__all__ = ["SearchEngine", "create_tools"]
