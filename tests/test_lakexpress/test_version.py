"""Tests for version detection and capabilities registry."""

import subprocess
from unittest.mock import patch, Mock

import pytest

from src.lakexpress.version import (
    VERSION_REGISTRY,
    VersionCapabilities,
    check_version_compatibility,
)
from src.base.version_detector import BaseVersionDetector as VersionDetector, ToolVersion


class TestToolVersion:
    """Tests for ToolVersion dataclass."""

    def test_parse_full_version_string(self):
        """Test parsing a full 'LakeXpress X.Y.Z' string."""
        v = ToolVersion.parse("LakeXpress 0.2.8")
        assert v.parts[0] == 0
        assert v.parts[1] == 2
        assert v.parts[2] == 8

    def test_parse_numeric_only(self):
        """Test parsing a bare version number."""
        v = ToolVersion.parse("0.2.8")
        assert v == ToolVersion(parts=(0, 2, 8))

    def test_parse_with_whitespace(self):
        """Test parsing a version string with leading/trailing whitespace."""
        v = ToolVersion.parse("  LakeXpress 1.2.3  ")
        assert v == ToolVersion(parts=(1, 2, 3))

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
        v = ToolVersion(parts=(0, 2, 8))
        assert str(v) == "0.2.8"

    def test_equality(self):
        """Test equality comparison."""
        a = ToolVersion(parts=(0, 2, 8))
        b = ToolVersion(parts=(0, 2, 8))
        assert a == b

    def test_inequality(self):
        """Test inequality comparison."""
        a = ToolVersion(parts=(0, 2, 8))
        b = ToolVersion(parts=(0, 3, 0))
        assert a != b

    def test_less_than(self):
        """Test less-than comparison."""
        a = ToolVersion(parts=(0, 2, 7))
        b = ToolVersion(parts=(0, 2, 8))
        assert a < b

    def test_greater_than(self):
        """Test greater-than comparison (via total_ordering)."""
        a = ToolVersion(parts=(0, 2, 8))
        b = ToolVersion(parts=(0, 2, 7))
        assert a > b

    def test_comparison_across_fields(self):
        """Test comparison across major/minor/patch."""
        versions = [
            ToolVersion(parts=(0, 1, 0)),
            ToolVersion(parts=(0, 2, 0)),
            ToolVersion(parts=(0, 2, 8)),
            ToolVersion(parts=(0, 3, 0)),
            ToolVersion(parts=(1, 0, 0)),
        ]
        for i in range(len(versions) - 1):
            assert versions[i] < versions[i + 1]


class TestVersionDetector:
    """Tests for VersionDetector class."""

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_success(self, mock_run):
        """Test successful version detection."""
        mock_result = Mock()
        mock_result.stdout = "LakeXpress 0.2.8\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"LakeXpress\s+(\d+\.\d+\.\d+)",
            "LakeXpress",
        )
        version = detector.detect()

        assert version == ToolVersion(parts=(0, 2, 8))
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
            r"LakeXpress\s+(\d+\.\d+\.\d+)",
            "LakeXpress",
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
            r"LakeXpress\s+(\d+\.\d+\.\d+)",
            "LakeXpress",
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
            r"LakeXpress\s+(\d+\.\d+\.\d+)",
            "LakeXpress",
        )
        version = detector.detect()

        assert version is None

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_caching(self, mock_run):
        """Test that second call returns cached result without re-running subprocess."""
        mock_result = Mock()
        mock_result.stdout = "LakeXpress 0.2.8\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"LakeXpress\s+(\d+\.\d+\.\d+)",
            "LakeXpress",
        )
        v1 = detector.detect()
        v2 = detector.detect()

        assert v1 == v2
        assert mock_run.call_count == 1

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_known_version(self, mock_run):
        """Test capabilities resolution for a known version."""
        mock_result = Mock()
        mock_result.stdout = "LakeXpress 0.2.8\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"LakeXpress\s+(\d+\.\d+\.\d+)",
            "LakeXpress",
        )
        detector.detect()
        caps = detector.capabilities

        assert "postgresql" in caps.source_databases
        assert "sqlite" in caps.log_databases
        assert "s3" in caps.storage_backends
        assert "snowflake" in caps.publish_targets
        assert "Zstd" in caps.compression_types
        assert caps.supports_no_banner is True
        assert caps.supports_version_flag is True
        assert caps.supports_incremental is True
        assert caps.supports_cleanup is True

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_newer_unknown_version(self, mock_run):
        """Test capabilities falls back to latest known for newer unknown version."""
        mock_result = Mock()
        mock_result.stdout = "LakeXpress 1.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"LakeXpress\s+(\d+\.\d+\.\d+)",
            "LakeXpress",
        )
        detector.detect()
        caps = detector.capabilities

        # Should get the latest known capabilities
        assert caps == VERSION_REGISTRY["0.4.0"]

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_undetected_version(self, mock_run):
        """Test capabilities falls back to latest known when detection fails."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"LakeXpress\s+(\d+\.\d+\.\d+)",
            "LakeXpress",
        )
        detector.detect()
        caps = detector.capabilities

        # Should fall back to latest known
        assert caps == VERSION_REGISTRY["0.4.0"]

    def test_registry_028_source_completeness(self):
        """Test that 0.2.8 registry has all expected source databases."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {
            "sqlserver",
            "postgresql",
            "oracle",
            "mysql",
            "mariadb",
        }
        assert caps.source_databases == expected

    def test_registry_028_log_completeness(self):
        """Test that 0.2.8 registry has all expected log databases."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {
            "sqlserver",
            "postgresql",
            "mysql",
            "mariadb",
            "sqlite",
            "duckdb",
        }
        assert caps.log_databases == expected

    def test_registry_028_storage_completeness(self):
        """Test that 0.2.8 registry has all expected storage backends."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {
            "local",
            "s3",
            "s3compatible",
            "gcs",
            "azure_adls",
            "onelake",
        }
        assert caps.storage_backends == expected

    def test_registry_028_publish_completeness(self):
        """Test that 0.2.8 registry has all expected publish targets."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {
            "snowflake",
            "databricks",
            "fabric",
            "bigquery",
            "motherduck",
            "glue",
            "ducklake",
        }
        assert caps.publish_targets == expected

    def test_registry_028_compression_completeness(self):
        """Test that 0.2.8 registry has all expected compression types."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {"Zstd", "Snappy", "Gzip", "Lz4", "None"}
        assert caps.compression_types == expected

    def test_registry_028_command_completeness(self):
        """Test that 0.2.8 registry has all 14 commands."""
        caps = VERSION_REGISTRY["0.2.8"]
        assert len(caps.commands) == 14
        expected = {
            "logdb_init",
            "logdb_drop",
            "logdb_truncate",
            "logdb_locks",
            "logdb_release_locks",
            "config_create",
            "config_delete",
            "config_list",
            "sync",
            "sync_export",
            "sync_publish",
            "run",
            "status",
            "cleanup",
        }
        assert caps.commands == expected

    def test_registry_029_exists(self):
        """Test that 0.2.9 registry entry exists and includes saphana."""
        assert "0.2.9" in VERSION_REGISTRY
        caps = VERSION_REGISTRY["0.2.9"]
        assert "saphana" in caps.source_databases

    def test_registry_029_source_completeness(self):
        """Test that 0.2.9 has all expected source databases including saphana."""
        caps = VERSION_REGISTRY["0.2.9"]
        expected = {
            "sqlserver",
            "postgresql",
            "oracle",
            "mysql",
            "mariadb",
            "saphana",
        }
        assert caps.source_databases == expected

    def test_supports_quiet_fbcp_029(self):
        """Test that supports_quiet_fbcp is True in 0.2.9."""
        caps = VERSION_REGISTRY["0.2.9"]
        assert caps.supports_quiet_fbcp is True

    def test_supports_quiet_fbcp_028(self):
        """Test that supports_quiet_fbcp is False in 0.2.8."""
        caps = VERSION_REGISTRY["0.2.8"]
        assert caps.supports_quiet_fbcp is False


class TestCheckVersionCompatibility:
    """Tests for check_version_compatibility function."""

    def test_quiet_fbcp_on_029_no_warning(self):
        """quiet_fbcp=True on 0.2.9 produces no warnings."""
        caps = VERSION_REGISTRY["0.2.9"]
        version = ToolVersion(parts=(0, 2, 9))
        warnings = check_version_compatibility(
            "sync", {"quiet_fbcp": True}, caps, version
        )
        assert warnings == []

    def test_quiet_fbcp_on_028_produces_warning(self):
        """quiet_fbcp=True on 0.2.8 produces a warning mentioning 0.2.9."""
        caps = VERSION_REGISTRY["0.2.8"]
        version = ToolVersion(parts=(0, 2, 8))
        warnings = check_version_compatibility(
            "sync", {"quiet_fbcp": True}, caps, version
        )
        assert len(warnings) == 1
        assert "0.2.9" in warnings[0]
        assert "quiet_fbcp" in warnings[0]

    def test_quiet_fbcp_on_irrelevant_command_no_warning(self):
        """quiet_fbcp=True on config_create (irrelevant) produces no warnings."""
        caps = VERSION_REGISTRY["0.2.8"]
        version = ToolVersion(parts=(0, 2, 8))
        warnings = check_version_compatibility(
            "config_create", {"quiet_fbcp": True}, caps, version
        )
        assert warnings == []

    def test_quiet_fbcp_false_on_028_no_warning(self):
        """quiet_fbcp=False on 0.2.8 produces no warnings."""
        caps = VERSION_REGISTRY["0.2.8"]
        version = ToolVersion(parts=(0, 2, 8))
        warnings = check_version_compatibility(
            "sync", {"quiet_fbcp": False}, caps, version
        )
        assert warnings == []

    def test_quiet_fbcp_sync_export_on_028_produces_warning(self):
        """quiet_fbcp=True for sync_export on 0.2.8 produces a warning."""
        caps = VERSION_REGISTRY["0.2.8"]
        version = ToolVersion(parts=(0, 2, 8))
        warnings = check_version_compatibility(
            "sync_export", {"quiet_fbcp": True}, caps, version
        )
        assert len(warnings) == 1
        assert "0.2.9" in warnings[0]

    def test_basic_params_no_warnings(self):
        """Basic params without version-gated features produce no warnings."""
        caps = VERSION_REGISTRY["0.2.8"]
        version = ToolVersion(parts=(0, 2, 8))
        warnings = check_version_compatibility(
            "sync", {"sync_id": "my_sync"}, caps, version
        )
        assert warnings == []

    def test_sync_on_040_missing_all_required_args_warns(self):
        """sync on 0.4.0 missing -a, --lxdb_auth_id, and --sync_id warns about all three."""
        caps = VERSION_REGISTRY["0.4.0"]
        version = ToolVersion(parts=(0, 4, 0))
        warnings = check_version_compatibility("sync", {}, caps, version)
        assert len(warnings) == 1
        msg = warnings[0]
        assert "--sync_id" in msg
        assert "--lxdb_auth_id" in msg
        assert "auth_file" in msg
        assert "0.4.0" in msg or "0, 4, 0" in msg

    def test_sync_on_043_all_required_args_provided_no_warning(self):
        """sync on 0.4.3 with all three required args produces no sync-registry warning."""
        caps = VERSION_REGISTRY["0.4.3"]
        version = ToolVersion(parts=(0, 4, 3))
        warnings = check_version_compatibility(
            "sync",
            {
                "sync_id": "my_sync",
                "log_db_auth_id": "tracking_db",
                "auth_file": "/tmp/auth.json",
            },
            caps,
            version,
        )
        assert warnings == []

    def test_sync_on_030_missing_args_no_warning(self):
        """sync on 0.3.0 (pre-removal) does NOT warn about missing sync registry args."""
        caps = VERSION_REGISTRY["0.3.0"]
        version = ToolVersion(parts=(0, 3, 0))
        warnings = check_version_compatibility("sync", {}, caps, version)
        # 0.3.0 still had the local sync registry, so all three are optional.
        assert warnings == []

    def test_sync_export_on_042_missing_lxdb_auth_id_warns(self):
        """sync_export on 0.4.2 missing only --lxdb_auth_id warns and names the missing flag."""
        caps = VERSION_REGISTRY["0.4.2"]
        version = ToolVersion(parts=(0, 4, 2))
        warnings = check_version_compatibility(
            "sync_export",
            {"sync_id": "my_sync", "auth_file": "/tmp/auth.json"},
            caps,
            version,
        )
        assert len(warnings) == 1
        assert "--lxdb_auth_id" in warnings[0]
        assert "--sync_id" not in warnings[0]
        assert "auth_file" not in warnings[0]
