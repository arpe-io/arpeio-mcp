"""Guard tests for the evaluation answer key.

These re-derive every ground-truth answer in evaluations/arpeio_eval.xml from the
server's source of truth (the parallelism recommender and the version registries),
so the eval answers cannot silently drift as the registries evolve. If a registry
change moves an answer, the corresponding assertion here fails — update both the
registry and arpeio_eval.xml together.
"""

from pathlib import Path

# defusedxml hardens against XXE / billion-laughs; the file is ours, but parse safely.
from defusedxml import ElementTree as ET

from src.fastbcp.command_builder import suggest_parallelism_method, get_supported_formats
from src.lakexpress.version import VERSION_REGISTRY as LX_REGISTRY
from src.migratorxpress.version import VERSION_REGISTRY as MX_REGISTRY

EVAL_XML = Path(__file__).resolve().parent.parent / "evaluations" / "arpeio_eval.xml"


def _answers():
    """Return the ordered list of <answer> strings from the eval XML."""
    tree = ET.parse(EVAL_XML)
    return [(qa.findtext("answer") or "").strip() for qa in tree.findall("qa_pair")]


def test_eval_file_has_ten_questions():
    answers = _answers()
    assert len(answers) == 10


def test_eval_answers_match_xml_order():
    """The XML answer key, in order, must equal the expected ground truth."""
    expected = [
        "Ctid", "Rowid", "Physloc", "NZDataSlice",
        "0.6.30", "0.6.32", "0.4.0", "Redshift", "Teradata",
        "MigratorXpress",
    ]
    assert _answers() == expected


# --- Re-derive each fact from the source of truth -------------------------- #

def test_parallelism_recommendations_no_key():
    assert suggest_parallelism_method("pgsql", False, False, "large")["method"] == "Ctid"
    assert suggest_parallelism_method("oraodp", False, False, "large")["method"] == "Rowid"
    assert suggest_parallelism_method("mssql", False, False, "large")["method"] == "Physloc"
    assert suggest_parallelism_method("netezza", False, False, "large")["method"] == "NZDataSlice"


def test_migratorxpress_project_min_version():
    assert not MX_REGISTRY["0.6.29"].supports_project
    assert MX_REGISTRY["0.6.30"].supports_project


def test_migratorxpress_postgres_tracking_db_min_version():
    assert not MX_REGISTRY["0.6.31"].supports_postgres_migration_db
    assert MX_REGISTRY["0.6.32"].supports_postgres_migration_db
    assert "postgres" in MX_REGISTRY["0.6.32"].migration_db_types


def test_lakexpress_sync_registry_removed_in_040():
    assert LX_REGISTRY["0.3.0"].supports_sync_registry
    assert not LX_REGISTRY["0.4.0"].supports_sync_registry


def test_lakexpress_redshift_added_in_040():
    assert "redshift" not in LX_REGISTRY["0.3.0"].publish_targets
    assert "redshift" in LX_REGISTRY["0.4.0"].publish_targets


def test_lakexpress_teradata_added_in_040():
    assert "teradata" not in LX_REGISTRY["0.3.0"].source_databases
    assert "teradata" in LX_REGISTRY["0.4.0"].source_databases


def test_fastbcp_supports_xlsx_and_parquet():
    formats = [f.lower() for f in get_supported_formats()["Output Formats"]]
    assert "parquet" in formats
    assert "xlsx" in formats
