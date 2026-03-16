"""Generic version detection for Arpe.io tools."""

import logging
import re
import subprocess
from dataclasses import dataclass
from functools import total_ordering
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


@total_ordering
@dataclass(frozen=True)
class ToolVersion:
    """Represents a tool version number (X.Y.Z.W or X.Y.Z)."""

    parts: Tuple[int, ...]

    @classmethod
    def parse(cls, version_string: str) -> "ToolVersion":
        match = re.search(r"(\d+(?:\.\d+)+)", version_string.strip())
        if not match:
            raise ValueError(f"Cannot parse version from: {version_string!r}")
        parts = tuple(int(x) for x in match.group(1).split("."))
        return cls(parts=parts)

    def __str__(self) -> str:
        return ".".join(str(p) for p in self.parts)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ToolVersion):
            return NotImplemented
        return self.parts == other.parts

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ToolVersion):
            return NotImplemented
        return self.parts < other.parts

    def __hash__(self) -> int:
        return hash(self.parts)


class BaseVersionDetector:
    """Generic version detector for Arpe.io tools."""

    def __init__(self, binary_path: str, version_registry: dict, version_pattern: str, product_name: str):
        self._binary_path = binary_path
        self._registry = version_registry
        self._version_pattern = version_pattern
        self._product_name = product_name
        self._detected_version: Optional[ToolVersion] = None
        self._detection_done = False

        # Pre-sort registry
        self._sorted_versions: List[Tuple[ToolVersion, object]] = sorted(
            [(ToolVersion.parse(k), v) for k, v in version_registry.items()],
            key=lambda x: x[0],
        )

    def detect(self, timeout: int = 10) -> Optional[ToolVersion]:
        if self._detection_done:
            return self._detected_version

        self._detection_done = True

        try:
            result = subprocess.run(
                [self._binary_path, "--version", "--nobanner"],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            output = (result.stdout + result.stderr).strip()
            match = re.search(self._version_pattern, output)
            if match:
                version_str = match.group(0) if not match.groups() else match.group(0)
                self._detected_version = ToolVersion.parse(version_str)
                logger.info(f"Detected {self._product_name} version: {self._detected_version}")
            else:
                logger.warning(f"Could not parse {self._product_name} version from output: {output!r}")
        except subprocess.TimeoutExpired:
            logger.warning(f"{self._product_name} version detection timed out")
        except FileNotFoundError:
            logger.warning(f"Binary not found at: {self._binary_path}")
        except Exception as e:
            logger.warning(f"{self._product_name} version detection failed: {e}")

        return self._detected_version

    @property
    def capabilities(self):
        if not self._detection_done:
            self.detect()

        if not self._sorted_versions:
            return None

        if self._detected_version is None:
            return self._sorted_versions[-1][1]

        best = None
        for ver, caps in self._sorted_versions:
            if ver <= self._detected_version:
                best = caps
            else:
                break

        return best if best is not None else self._sorted_versions[-1][1]
