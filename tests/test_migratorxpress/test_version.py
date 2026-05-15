"""Tests for version detection and capabilities registry."""

import subprocess
from unittest.mock import patch, Mock

import pytest

from src.migratorxpress.version import (
    VERSION_REGISTRY,
    VersionCapabilities,
    check_version_compatibility,
)
from src.base.version_detector import BaseVersionDetector as VersionDetector, ToolVersion


class TestToolVersion:
    """Tests for ToolVersion dataclass."""

    def test_parse_full_version_string(self):
        """Test parsing a full 'migratorxpress X.Y.Z' string."""
        v = ToolVersion.parse("migratorxpress 0.6.24")
        assert v.parts[0] == 0
        assert v.parts[1] == 6
        assert v.parts[2] == 24

    def test_parse_numeric_only(self):
        """Test parsing a bare version number."""
        v = ToolVersion.parse("0.6.24")
        assert v == ToolVersion(parts=(0, 6, 24))

    def test_parse_with_whitespace(self):
        """Test parsing a version string with leading/trailing whitespace."""
        v = ToolVersion.parse("  migratorxpress 1.2.3  ")
        assert v == ToolVersion(parts=(1, 2, 3))

    def test_parse_case_insensitive(self):
        """Test parsing is case-insensitive for the prefix."""
        v = ToolVersion.parse("MigratorXpress 0.6.24")
        assert v == ToolVersion(parts=(0, 6, 24))

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
        v = ToolVersion(parts=(0, 6, 24))
        assert str(v) == "0.6.24"

    def test_equality(self):
        """Test equality comparison."""
        a = ToolVersion(parts=(0, 6, 24))
        b = ToolVersion(parts=(0, 6, 24))
        assert a == b

    def test_inequality(self):
        """Test inequality comparison."""
        a = ToolVersion(parts=(0, 6, 24))
        b = ToolVersion(parts=(0, 7, 0))
        assert a != b

    def test_less_than(self):
        """Test less-than comparison."""
        a = ToolVersion(parts=(0, 6, 23))
        b = ToolVersion(parts=(0, 6, 24))
        assert a < b

    def test_greater_than(self):
        """Test greater-than comparison (via total_ordering)."""
        a = ToolVersion(parts=(0, 6, 24))
        b = ToolVersion(parts=(0, 6, 23))
        assert a > b

    def test_comparison_across_fields(self):
        """Test comparison across major/minor/patch."""
        versions = [
            ToolVersion(parts=(0, 1, 0)),
            ToolVersion(parts=(0, 6, 0)),
            ToolVersion(parts=(0, 6, 24)),
            ToolVersion(parts=(0, 7, 0)),
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
        mock_result.stdout = "migratorxpress 0.6.24\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"migratorxpress\s+(\d+\.\d+\.\d+)",
            "MigratorXpress",
        )
        version = detector.detect()

        assert version == ToolVersion(parts=(0, 6, 24))
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
            r"migratorxpress\s+(\d+\.\d+\.\d+)",
            "MigratorXpress",
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
            r"migratorxpress\s+(\d+\.\d+\.\d+)",
            "MigratorXpress",
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
            r"migratorxpress\s+(\d+\.\d+\.\d+)",
            "MigratorXpress",
        )
        version = detector.detect()

        assert version is None

    @patch("src.base.version_detector.subprocess.run")
    def test_detect_caching(self, mock_run):
        """Test that second call returns cached result without re-running subprocess."""
        mock_result = Mock()
        mock_result.stdout = "migratorxpress 0.6.24\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"migratorxpress\s+(\d+\.\d+\.\d+)",
            "MigratorXpress",
        )
        v1 = detector.detect()
        v2 = detector.detect()

        assert v1 == v2
        assert mock_run.call_count == 1

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_known_version(self, mock_run):
        """Test capabilities resolution for a known version."""
        mock_result = Mock()
        mock_result.stdout = "migratorxpress 0.6.24\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"migratorxpress\s+(\d+\.\d+\.\d+)",
            "MigratorXpress",
        )
        detector.detect()
        caps = detector.capabilities

        assert "oracle" in caps.source_databases
        assert "postgresql" in caps.target_databases
        assert "sqlserver" in caps.migration_db_types
        assert "translate" in caps.tasks
        assert "trusted" in caps.fk_modes
        assert caps.supports_no_banner is True
        assert caps.supports_version_flag is True
        assert caps.supports_fasttransfer is True
        assert caps.supports_license is True

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_newer_unknown_version(self, mock_run):
        """Test capabilities falls back to latest known for newer unknown version."""
        mock_result = Mock()
        mock_result.stdout = "migratorxpress 1.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"migratorxpress\s+(\d+\.\d+\.\d+)",
            "MigratorXpress",
        )
        detector.detect()
        caps = detector.capabilities

        # Should get the latest known capabilities (0.6.32)
        assert caps == VERSION_REGISTRY["0.6.32"]

    @patch("src.base.version_detector.subprocess.run")
    def test_capabilities_undetected_version(self, mock_run):
        """Test capabilities falls back to latest known when detection fails."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector(
            "/fake/binary",
            VERSION_REGISTRY,
            r"migratorxpress\s+(\d+\.\d+\.\d+)",
            "MigratorXpress",
        )
        detector.detect()
        caps = detector.capabilities

        # Should fall back to latest known
        assert caps == VERSION_REGISTRY["0.6.32"]

    def test_registry_0624_source_completeness(self):
        """Test that 0.6.24 registry has all 4 expected source databases."""
        caps = VERSION_REGISTRY["0.6.24"]
        expected = {"oracle", "postgresql", "sqlserver", "netezza"}
        assert caps.source_databases == expected
        assert len(caps.source_databases) == 4

    def test_registry_0624_target_completeness(self):
        """Test that 0.6.24 registry has all 2 expected target databases."""
        caps = VERSION_REGISTRY["0.6.24"]
        expected = {"postgresql", "sqlserver"}
        assert caps.target_databases == expected
        assert len(caps.target_databases) == 2

    def test_registry_0624_migration_db_completeness(self):
        """Test that 0.6.24 registry has 1 migration database type."""
        caps = VERSION_REGISTRY["0.6.24"]
        expected = {"sqlserver"}
        assert caps.migration_db_types == expected
        assert len(caps.migration_db_types) == 1

    def test_registry_0624_task_completeness(self):
        """Test that 0.6.24 registry has all 8 tasks."""
        caps = VERSION_REGISTRY["0.6.24"]
        expected = {
            "translate",
            "create",
            "transfer",
            "diff",
            "copy_pk",
            "copy_ak",
            "copy_fk",
            "all",
        }
        assert caps.tasks == expected
        assert len(caps.tasks) == 8

    def test_registry_0624_fk_modes_completeness(self):
        """Test that 0.6.24 registry has all 3 FK modes."""
        caps = VERSION_REGISTRY["0.6.24"]
        expected = {"trusted", "untrusted", "disabled"}
        assert caps.fk_modes == expected
        assert len(caps.fk_modes) == 3

    def test_registry_0624_migration_db_modes_completeness(self):
        """Test that 0.6.24 registry has all 3 migration DB modes."""
        caps = VERSION_REGISTRY["0.6.24"]
        expected = {"preserve", "truncate", "drop"}
        assert caps.migration_db_modes == expected
        assert len(caps.migration_db_modes) == 3

    def test_registry_0624_load_modes_completeness(self):
        """Test that 0.6.24 registry has all 2 load modes."""
        caps = VERSION_REGISTRY["0.6.24"]
        expected = {"truncate", "append"}
        assert caps.load_modes == expected
        assert len(caps.load_modes) == 2


class TestCheckVersionCompatibility:
    """Tests for check_version_compatibility function."""

    def test_basic_params_no_warnings(self):
        """Basic params produce no warnings."""
        caps = VERSION_REGISTRY["0.6.24"]
        version = ToolVersion(parts=(0, 6, 24))
        warnings = check_version_compatibility(
            {"source_db_name": "mydb"}, caps, version
        )
        assert warnings == []

    def test_empty_params_no_warnings(self):
        """Empty params produce no warnings."""
        caps = VERSION_REGISTRY["0.6.24"]
        version = ToolVersion(parts=(0, 6, 24))
        warnings = check_version_compatibility({}, caps, version)
        assert warnings == []

    def test_none_version_no_warnings(self):
        """None detected version with basic params produces no warnings."""
        caps = VERSION_REGISTRY["0.6.24"]
        warnings = check_version_compatibility(
            {"source_db_name": "mydb"}, caps, None
        )
        assert warnings == []

    def test_project_on_0629_produces_warning(self):
        """--project on 0.6.29 (pre-0.6.30) produces a version-gated warning."""
        caps = VERSION_REGISTRY["0.6.29"]
        version = ToolVersion(parts=(0, 6, 29))
        warnings = check_version_compatibility(
            {"project": "bob_bods_oracle"}, caps, version
        )
        assert len(warnings) == 1
        assert "--project" in warnings[0]
        assert "0.6.30" in warnings[0]

    def test_project_on_0630_no_warning(self):
        """--project on 0.6.30+ produces no warning."""
        caps = VERSION_REGISTRY["0.6.30"]
        version = ToolVersion(parts=(0, 6, 30))
        warnings = check_version_compatibility(
            {"project": "bob_bods_oracle"}, caps, version
        )
        assert warnings == []

    def test_postgres_migration_db_on_0631_produces_warning(self):
        """migration_db_type='postgres' on 0.6.31 produces a warning."""
        caps = VERSION_REGISTRY["0.6.31"]
        version = ToolVersion(parts=(0, 6, 31))
        warnings = check_version_compatibility(
            {"migration_db_type": "postgres"}, caps, version
        )
        assert len(warnings) == 1
        assert "postgres" in warnings[0].lower()
        assert "0.6.32" in warnings[0]

    def test_postgres_migration_db_on_0632_no_warning(self):
        """migration_db_type='postgres' on 0.6.32 produces no warning."""
        caps = VERSION_REGISTRY["0.6.32"]
        version = ToolVersion(parts=(0, 6, 32))
        warnings = check_version_compatibility(
            {"migration_db_type": "postgres"}, caps, version
        )
        assert warnings == []


class TestProjectFieldValidation:
    """Tests for the --project regex validator on MigrationParams."""

    def _base_params(self, **overrides):
        """Build a minimal MigrationParams kwargs dict with overrides."""
        from src.migratorxpress.validators import MigrationParams

        base = {
            "auth_file": "/tmp/auth.json",
            "source_db_auth_id": "src",
            "source_db_name": "srcdb",
            "target_db_auth_id": "tgt",
            "target_db_name": "tgtdb",
            "migration_db_auth_id": "mig",
        }
        base.update(overrides)
        return MigrationParams(**base)

    def test_project_accepts_valid(self):
        """Letters, digits, underscore, hyphen up to 64 chars are accepted."""
        params = self._base_params(project="bob_bods-oracle_42")
        assert params.project == "bob_bods-oracle_42"

    def test_project_accepts_max_length(self):
        """A 64-char tag is accepted."""
        tag = "a" * 64
        params = self._base_params(project=tag)
        assert params.project == tag

    def test_project_rejects_too_long(self):
        """A 65-char tag is rejected."""
        import pytest

        with pytest.raises(ValueError, match="project"):
            self._base_params(project="a" * 65)

    def test_project_rejects_bad_characters(self):
        """Characters outside [A-Za-z0-9_-] are rejected."""
        import pytest

        with pytest.raises(ValueError, match="project"):
            self._base_params(project="bad$tag")

    def test_project_rejects_empty(self):
        """An empty string is rejected."""
        import pytest

        with pytest.raises(ValueError, match="project"):
            self._base_params(project="")

    def test_project_none_ok(self):
        """project=None (default) passes validation."""
        params = self._base_params()
        assert params.project is None
