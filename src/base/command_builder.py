"""Abstract base command builder for Arpe.io tools."""

import os
import subprocess
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

if TYPE_CHECKING:
    from .version_detector import BaseVersionDetector
from datetime import datetime


logger = logging.getLogger(__name__)


class ArpeToolError(Exception):
    """Base exception for Arpe.io tool operations."""
    pass


class BaseCommandBuilder(ABC):
    """Abstract base class for Arpe.io tool command builders."""

    # Subclasses should set these
    PRODUCT_NAME: str = "ArpeTool"
    DOWNLOAD_URL: str = "https://arpe.io"
    DEFAULT_TIMEOUT: int = 1800
    SENSITIVE_FLAGS: Set[str] = set()

    def __init__(self, binary_path: str):
        self.binary_path = Path(binary_path)
        self._preview_only = False
        self._validate_binary()

        if self._preview_only:
            self._version_detector = self._create_version_detector()
            self._version_detector._detection_done = True
            logger.info(f"{self.PRODUCT_NAME}: Running in command builder mode (binary not configured)")
        else:
            self._version_detector = self._create_version_detector()
            detected = self._version_detector.detect()
            if detected:
                logger.info(f"{self.PRODUCT_NAME} version {detected} detected")
            else:
                logger.warning(f"Could not detect {self.PRODUCT_NAME} version")

    @property
    def version_detector(self):
        return self._version_detector

    @property
    def preview_only(self) -> bool:
        return self._preview_only

    def _validate_binary(self) -> None:
        if not self.binary_path.exists():
            logger.warning(f"{self.PRODUCT_NAME} binary not found at: {self.binary_path}. Set {self.PRODUCT_NAME.upper().replace(' ', '')}_PATH to enable execution. Download from {self.DOWNLOAD_URL}")
            self._preview_only = True
            return
        if not self.binary_path.is_file():
            logger.warning(f"{self.PRODUCT_NAME} path is not a file: {self.binary_path}. Set {self.PRODUCT_NAME.upper().replace(' ', '')}_PATH to enable execution. Download from {self.DOWNLOAD_URL}")
            self._preview_only = True
            return
        if not os.access(self.binary_path, os.X_OK):
            logger.warning(f"{self.PRODUCT_NAME} binary is not executable: {self.binary_path}. Set {self.PRODUCT_NAME.upper().replace(' ', '')}_PATH to enable execution. Download from {self.DOWNLOAD_URL}")
            self._preview_only = True
            return

    @abstractmethod
    def _create_version_detector(self) -> "BaseVersionDetector": ...

    @abstractmethod
    def build_command(self, request) -> List[str]: ...

    @abstractmethod
    def _get_version_capabilities(self, caps) -> dict: ...

    def get_version(self) -> Dict[str, Any]:
        if self._preview_only:
            caps = self._version_detector.capabilities
            return {
                "preview_only": True,
                "binary_path": str(self.binary_path),
                "message": f"Set binary path to enable execution. Download from {self.DOWNLOAD_URL}",
                "version": None,
                "detected": False,
                "capabilities": self._get_version_capabilities(caps),
            }

        detected = self._version_detector.detect()
        caps = self._version_detector.capabilities
        return {
            "preview_only": False,
            "version": str(detected) if detected else None,
            "detected": detected is not None,
            "binary_path": str(self.binary_path),
            "capabilities": self._get_version_capabilities(caps),
        }

    def mask_password(self, command: List[str]) -> List[str]:
        masked = []
        mask_next = False
        for part in command:
            if mask_next:
                masked.append("******")
                mask_next = False
            else:
                if part in self.SENSITIVE_FLAGS:
                    mask_next = True
                masked.append(part)
        return masked

    def format_command_display(self, command: List[str], mask: bool = True, os_type: str = "linux") -> str:
        display_cmd = self.mask_password(command) if mask else command

        # Adjust binary path for Windows
        if os_type == "windows":
            binary = display_cmd[0].replace("/", "\\")
            if not binary.endswith(".exe"):
                binary += ".exe"
            formatted_parts = [binary]
        else:
            formatted_parts = [display_cmd[0]]

        i = 1
        while i < len(display_cmd):
            if i < len(display_cmd) - 1 and display_cmd[i].startswith("-") and not display_cmd[i + 1].startswith("-"):
                param = display_cmd[i]
                value = display_cmd[i + 1]
                if " " in value:
                    formatted_parts.append(f'{param} "{value}"')
                else:
                    formatted_parts.append(f"{param} {value}")
                i += 2
            else:
                formatted_parts.append(display_cmd[i])
                i += 1

        # Windows uses ^ for line continuation, Linux uses \
        if os_type == "windows":
            return " ^\n  ".join(formatted_parts)
        return " \\\n  ".join(formatted_parts)

    def execute_command(self, command: List[str], timeout: Optional[int] = None, log_dir: Optional[Path] = None) -> Tuple[int, str, str]:
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT

        if self._preview_only:
            raise ArpeToolError(
                f"Execution requires the {self.PRODUCT_NAME} binary. "
                f"Set the binary path or download from {self.DOWNLOAD_URL}"
            )

        start_time = datetime.now()
        masked_cmd = self.mask_password(command)
        logger.info(f"Executing {self.PRODUCT_NAME} command: {' '.join(masked_cmd)}")

        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"{self.PRODUCT_NAME} completed in {duration:.2f}s with return code {result.returncode}")

            if log_dir:
                self._save_execution_log(log_dir, command, result.returncode, result.stdout, result.stderr, duration)

            return result.returncode, result.stdout, result.stderr

        except subprocess.TimeoutExpired as e:
            logger.error(f"{self.PRODUCT_NAME} execution timed out after {timeout}s")
            raise ArpeToolError(f"Execution timed out after {timeout} seconds") from e
        except Exception as e:
            logger.error(f"{self.PRODUCT_NAME} execution failed: {e}")
            raise ArpeToolError(f"Execution failed: {e}") from e

    def _save_execution_log(self, log_dir: Path, command: List[str], return_code: int, stdout: str, stderr: str, duration: float) -> None:
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            product_lower = self.PRODUCT_NAME.lower().replace(" ", "")
            log_file = log_dir / f"{product_lower}_{timestamp}.log"
            masked_cmd = self.mask_password(command)

            with open(log_file, "w") as f:
                f.write(f"{self.PRODUCT_NAME} Execution Log\n")
                f.write(f"{'=' * 80}\n\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Duration: {duration:.2f} seconds\n")
                f.write(f"Return Code: {return_code}\n\n")
                f.write(f"Command:\n{' '.join(masked_cmd)}\n\n")
                f.write(f"{'=' * 80}\n")
                f.write(f"STDOUT:\n{stdout}\n\n")
                f.write(f"{'=' * 80}\n")
                f.write(f"STDERR:\n{stderr}\n")

            logger.info(f"Execution log saved to: {log_file}")
        except Exception as e:
            logger.warning(f"Failed to save execution log: {e}")
