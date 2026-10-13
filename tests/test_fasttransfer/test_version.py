"""Tests for version detection and capabilities registry."""

import subprocess
from unittest.mock import patch, Mock

import pytest

from src.fasttransfer.version import (
    VERSION_REGISTRY,
    VersionCapabilities,
    check_version_compatibility,
)
from src.base.version_detector import BaseVersionDetector as VersionDetector, ToolVersion


class TestToolVersion:
    """Tests for ToolVersion dataclass."""

    def test_parse_full_version_string(self):
        """Test parsing a full 'FastTransfer Version X.Y.Z.W' string."""
        v = ToolVersion.parse("FastTransfer Version 0.16.0.0")
        assert v.parts[0] == 0
        assert v.parts[1] == 16
        assert v.parts[2] == 0
        assert v.parts[3] == 0

    def test_parse_numeric_only(self):
        """Test parsing a bare version number."""
        v = ToolVersion.parse("0.16.0.0")
        assert v == ToolVersion(parts=(0, 16, 0, 0))

    def test_parse_with_whitespace(self):
        """Test parsing a version string with leading/trailing whitespace."""
        v = ToolVersion.parse("  FastTransfer Version 1.2.3.4  ")
        assert v == ToolVersion(parts=(1, 2, 3, 4))

    def test_parse_invalid_string(self):
        """Test that an unparseable string raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse version"):
            ToolVersion.parse("no version here")

    def test_parse_incomplete_version(self):
        """Test that a string with no version number raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse version"):
            ToolVersion.parse("no_digits_here")

    def test_str_representation(self):
        """Test string representation."""
        v = ToolVersion(parts=(0, 16, 0, 0))
        assert str(v) == "0.16.0.0"

    def test_equality(self):
        """Test equality comparison."""
        a = ToolVersion(parts=(0, 16, 0, 0))
        b = ToolVersion(parts=(0, 16, 0, 0))
        assert a == b

    def test_inequality(self):
        """Test inequality comparison."""
        a = ToolVersion(parts=(0, 16, 0, 0))
        b = ToolVersion(parts=(0, 17, 0, 0))
        assert a != b

    def test_less_than(self):
        """Test less-than comparison."""
        a = ToolVersion(parts=(0, 15, 0, 0))
        b = ToolVersion(parts=(0, 16, 0, 0))
        assert a < b

    def test_greater_than(self):
        """Test greater-than comparison (via total_ordering)."""
        a = ToolVersion(parts=(0, 16, 0, 0))
        b = ToolVersion(parts=(0, 15, 9, 9))
        assert a > b

    def test_comparison_across_fields(self):
        """Test comparison across major/minor/patch/build."""
        versions = [
            ToolVersion(parts=(0, 15, 0, 0)),
            ToolVersion(parts=(0, 16, 0, 0)),
            ToolVersion(parts=(0, 16, 0, 1)),
            ToolVersion(parts=(0, 16, 1, 0)),
            ToolVersion(parts=(1, 0, 0, 0)),
        ]
        for i in range(len(versions) - 1):
            assert versions[i] < versions[i + 1]


class TestVersionDetector:
    """Tests for VersionDetector class."""

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_success(self, mock_run):
        """Test successful version detection."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 0.16.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        version = detector.detect()

        assert version == ToolVersion(parts=(0, 16, 0, 0))
        mock_run.assert_called_once_with(
            ["/fake/binary", "--version", "--nobanner"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_failure_no_match(self, mock_run):
        """Test detection when output doesn't match version pattern."""
        mock_result = Mock()
        mock_result.stdout = "Unknown output"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        version = detector.detect()

        assert version is None

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_timeout(self, mock_run):
        """Test detection handles timeout gracefully."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=10)

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        version = detector.detect()

        assert version is None

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_binary_not_found(self, mock_run):
        """Test detection handles missing binary gracefully."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        version = detector.detect()

        assert version is None

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_caching(self, mock_run):
        """Test that second call returns cached result without re-running subprocess."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 0.16.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        v1 = detector.detect()
        v2 = detector.detect()

        assert v1 == v2
        assert mock_run.call_count == 1

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_known_version(self, mock_run):
        """Test capabilities resolution for a known version."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 0.16.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        detector.detect()
        caps = detector.capabilities

        assert "oraodp" in caps.source_types
        assert "pgsql" in caps.target_types
        assert caps.supports_nobanner is True
        assert caps.supports_version_flag is True

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_newer_unknown_version(self, mock_run):
        """Test capabilities falls back to latest known for newer unknown version."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 1.0.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        detector.detect()
        caps = detector.capabilities

        # Should get the latest known capabilities (0.17.1.0)
        assert caps is VERSION_REGISTRY["0.17.1.0"]

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_undetected_version(self, mock_run):
        """Test capabilities falls back to latest known when detection fails."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        detector.detect()
        caps = detector.capabilities

        # Should fall back to latest known
        assert caps is VERSION_REGISTRY["0.17.1.0"]

    def test_registry_016_source_completeness(self):
        """Test that 0.16.0.0 registry has all expected source types."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        expected = {
            "clickhouse",
            "duckdb",
            "duckdbstream",
            "hana",
            "mssql",
            "msoledbsql",
            "mysql",
            "nzoledb",
            "nzsql",
            "nzcopy",
            "odbc",
            "oledb",
            "oraodp",
            "pgcopy",
            "pgsql",
            "teradata",
        }
        assert caps.source_types == expected

    def test_registry_016_target_completeness(self):
        """Test that 0.16.0.0 registry has all expected target types."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        expected = {
            "clickhousebulk",
            "duckdb",
            "hanabulk",
            "msbulk",
            "mysqlbulk",
            "nzbulk",
            "orabulk",
            "oradirect",
            "pgcopy",
            "pgsql",
            "teradata",
        }
        assert caps.target_types == expected

    def test_registry_016_method_completeness(self):
        """Test that 0.16.0.0 registry has all expected parallelism methods."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        expected = {
            "Ctid",
            "DataDriven",
            "Ntile",
            "NZDataSlice",
            "None",
            "Physloc",
            "Random",
            "RangeId",
            "Rowid",
        }
        assert caps.parallelism_methods == expected


    def test_registry_0171_present(self):
        """0.17.1.0 is registered and is the latest known version."""
        assert "0.17.1.0" in VERSION_REGISTRY
        latest = max(VERSION_REGISTRY, key=lambda v: tuple(int(x) for x in v.split(".")))
        assert latest == "0.17.1.0"

    def test_registry_0171_same_cli_surface_as_0170(self):
        """0.17.1 is a bug-fix release: no CLI types/methods/flags changed."""
        assert VERSION_REGISTRY["0.17.1.0"] == VERSION_REGISTRY["0.17.0.0"]

    def test_registry_0171_has_no_adbc_types(self):
        """ADBC connection types are not part of FastTransfer 0.17.1."""
        caps = VERSION_REGISTRY["0.17.1.0"]
        assert not any(t.startswith("adbc") for t in caps.source_types)
        assert not any(t.startswith("adbc") for t in caps.target_types)

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_0171_exact_match(self, mock_run):
        """Detecting 'FastTransfer Version 0.17.1.0' resolves to its registry entry."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 0.17.1.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"FastTransfer\s+Version\s+(\d+\.\d+\.\d+\.\d+)",
            "FastTransfer",
        )
        version = detector.detect()
        assert str(version) == "0.17.1.0"
        assert detector.capabilities is VERSION_REGISTRY["0.17.1.0"]


class TestCheckVersionCompatibility:
    """Tests for check_version_compatibility function."""

    def test_basic_params_no_warnings(self):
        """Basic params produce no warnings."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        version = ToolVersion(parts=(0, 16, 0, 0))
        warnings = check_version_compatibility(
            {"source": {"type": "pgsql"}}, caps, version
        )
        assert warnings == []

    def test_empty_params_no_warnings(self):
        """Empty params produce no warnings."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        version = ToolVersion(parts=(0, 16, 0, 0))
        warnings = check_version_compatibility({}, caps, version)
        assert warnings == []

    def test_none_version_no_warnings(self):
        """None detected version with basic params produces no warnings."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        warnings = check_version_compatibility(
            {"source": {"type": "pgsql"}}, caps, None
        )
        assert warnings == []
