"""
Cache management for documentation search indexes.

Handles reading/writing gzipped JSON cache files with version-keyed
naming and TTL-based freshness checks.
"""

import gzip
import json
import logging
import os
import time
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".cache" / "arpeio-mcp"


def get_cache_path(source_name: str, version: Optional[str] = None) -> Path:
    """Get the cache file path for a documentation source.

    Args:
        source_name: Product name (e.g., 'fastbcp') or 'blog'.
        version: Version string for version-keyed caches. None for TTL-only (blog).

    Returns:
        Path to the gzipped JSON cache file.
    """
    if version:
        return CACHE_DIR / f"{source_name}_{version}.json.gz"
    return CACHE_DIR / f"{source_name}.json.gz"


def load_cache(path: Path) -> Optional[List[dict]]:
    """Load cached chunks from a gzipped JSON file.

    Args:
        path: Path to the cache file.

    Returns:
        List of chunk dicts, or None if the file is missing or corrupt.
    """
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        logger.warning(f"Cache file {path} does not contain a list")
        return None
    except (gzip.BadGzipFile, json.JSONDecodeError, OSError) as e:
        logger.warning(f"Failed to load cache {path}: {e}")
        return None


def save_cache(path: Path, data: List[dict]) -> None:
    """Save chunks to a gzipped JSON cache file atomically.

    Writes to a temporary file first, then renames to avoid
    partial writes on crash.

    Args:
        path: Target cache file path.
        data: List of chunk dicts to save.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    try:
        with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_path, path)
        logger.info(f"Saved cache: {path} ({len(data)} chunks)")
    except OSError as e:
        logger.error(f"Failed to save cache {path}: {e}")
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def is_cache_fresh(path: Path, ttl_days: int) -> bool:
    """Check if a cache file exists and is within its TTL.

    Args:
        path: Path to the cache file.
        ttl_days: Maximum age in days.

    Returns:
        True if the file exists and was modified within ttl_days.
    """
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < (ttl_days * 86400)


def best_version_for(product: str, detected_version: Optional[str], known_versions: List[str]) -> str:
    """Find the best matching documentation version for a detected CLI version.

    Uses the latest known doc version whose major.minor prefix matches.
    Falls back to the latest known version if no match.

    Args:
        product: Product name.
        detected_version: Version string from CLI binary (e.g., '0.30.1.0').
        known_versions: List of documented version strings (e.g., ['0.28', '0.29', '0.30']).

    Returns:
        The best matching version string from known_versions.
    """
    if not known_versions:
        return "latest"

    if detected_version:
        # Try matching major.minor prefix
        parts = detected_version.split(".")
        for prefix_len in (2, 1):
            prefix = ".".join(parts[:prefix_len])
            for v in reversed(known_versions):
                if v.startswith(prefix) or prefix.startswith(v):
                    return v

    # Fall back to latest
    return known_versions[-1]
