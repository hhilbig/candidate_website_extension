import json
from pathlib import Path

import pytest

from scripts.validate_mayoral_source_registry import validate_registry


def valid_source() -> dict:
    return {
        "source_id": "fixture",
        "enabled": True,
        "adapter": "adapter.py",
        "input_files": ["input.csv"],
        "output_files": ["output.csv"],
        "years": {"start": 2000, "end": 2007},
        "jurisdictions": ["Fixture City, AA"],
        "attempt_tracking": False,
        "attempt_tracking_reason": "Fixture adapter has no exhaustive negative records.",
    }


def write_fixture(tmp_path: Path, sources: list[dict]) -> Path:
    (tmp_path / "adapter.py").write_text("# fixture\n", encoding="utf-8")
    (tmp_path / "input.csv").write_text("x\n1\n", encoding="utf-8")
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(sources), encoding="utf-8")
    return path


def test_validates_operational_fields_and_paths(tmp_path):
    path = write_fixture(tmp_path, [valid_source()])
    report = validate_registry(path, tmp_path)
    assert report == {
        "schema_version": 1,
        "registry_path": str(path),
        "sources": 1,
        "enabled_sources": 1,
        "disabled_sources": 0,
        "enabled_input_files": 1,
        "enabled_output_files": 1,
        "status": "valid",
    }


def test_rejects_duplicate_source_ids(tmp_path):
    source = valid_source()
    path = write_fixture(tmp_path, [source, source.copy()])
    with pytest.raises(ValueError, match="Duplicate source IDs"):
        validate_registry(path, tmp_path)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"adapter": None}, "enabled source needs an adapter"),
        ({"input_files": ["missing.csv"]}, "input files do not exist"),
        ({"years": {"start": 2007, "end": 2000}}, "invalid start/end"),
        ({"jurisdictions": []}, "jurisdictions must contain"),
    ],
)
def test_rejects_invalid_enabled_sources(tmp_path, change, message):
    source = valid_source()
    source.update(change)
    path = write_fixture(tmp_path, [source])
    with pytest.raises(ValueError, match=message):
        validate_registry(path, tmp_path)


def test_disabled_source_can_have_no_adapter_or_outputs(tmp_path):
    source = valid_source()
    source.update({"enabled": False, "adapter": None, "input_files": [], "output_files": []})
    path = write_fixture(tmp_path, [source])
    report = validate_registry(path, tmp_path)
    assert report["enabled_sources"] == 0


def test_accepts_structured_jurisdiction_scope(tmp_path):
    source = valid_source()
    source["jurisdictions"] = [{"geo_name": "fixture city", "state_abb": "AA"}]
    path = write_fixture(tmp_path, [source])
    assert validate_registry(path, tmp_path)["status"] == "valid"


def test_rejects_enabled_source_without_attempt_tracking(tmp_path):
    source = valid_source()
    del source["attempt_tracking"]
    path = write_fixture(tmp_path, [source])
    with pytest.raises(ValueError, match="must declare attempt_tracking"):
        validate_registry(path, tmp_path)


def test_rejects_unexplained_untracked_source(tmp_path):
    source = valid_source()
    source["attempt_tracking_reason"] = ""
    path = write_fixture(tmp_path, [source])
    with pytest.raises(ValueError, match="requires attempt_tracking_reason"):
        validate_registry(path, tmp_path)


def test_validates_legacy_attempt_mapping(tmp_path):
    source = valid_source()
    source.update(
        {
            "attempt_tracking": True,
            "attempt_scope": {"all_in_scope": True},
            "attempt_status_mapping": {
                "mapping_version": "fixture-v1",
                "files": ["input.csv"],
                "key": {"contest_id_column": "contest_id"},
                "status_column": "status",
                "status_map": {"checked": "no_result"},
                "aggregation_precedence": [
                    "found",
                    "request_error",
                    "match_unresolved",
                    "no_result",
                    "source_unavailable",
                ],
            },
        }
    )
    path = write_fixture(tmp_path, [source])
    assert validate_registry(path, tmp_path)["status"] == "valid"


def test_rejects_invalid_mapped_terminal_status(tmp_path):
    source = valid_source()
    source.update(
        {
            "attempt_tracking": True,
            "attempt_scope": {},
            "attempt_status_mapping": {
                "mapping_version": "fixture-v1",
                "files": ["input.csv"],
                "key": {"contest_id_column": "contest_id"},
                "status_column": "status",
                "status_map": {"checked": "finished"},
                "aggregation_precedence": [
                    "found",
                    "request_error",
                    "match_unresolved",
                    "no_result",
                    "source_unavailable",
                ],
            },
        }
    )
    path = write_fixture(tmp_path, [source])
    with pytest.raises(ValueError, match="invalid terminal statuses"):
        validate_registry(path, tmp_path)
