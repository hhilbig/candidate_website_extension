"""The bundle file changes the corpus-specific bound values; the default stays."""
import importlib
import json

import pytest

import src.candidate_extraction_bundle as bundle


@pytest.fixture(autouse=True)
def clear_cache():
    bundle._load.cache_clear()
    yield
    bundle._load.cache_clear()


def write(tmp_path, payload):
    path = tmp_path / "bundle.json"
    path.write_text(json.dumps(payload))
    return str(path)


def test_default_bundle_returns_the_literal(monkeypatch):
    monkeypatch.delenv(bundle.ENVIRONMENT_VARIABLE, raising=False)
    assert bundle.bundle_id() == "2002_2024"
    assert bundle.bound("universe_rows", 15_041) == 15_041


def test_active_bundle_replaces_the_literal_and_never_falls_back(monkeypatch, tmp_path):
    path = write(tmp_path, {"bundle_id": "2026", "values": {"universe_rows": 7, "source_corpus_sha256": None}})
    monkeypatch.setenv(bundle.ENVIRONMENT_VARIABLE, path)
    assert bundle.bundle_id() == "2026"
    assert bundle.bound("universe_rows", 15_041) == 7
    assert bundle.bound("source_corpus_sha256", "c53d") is None
    with pytest.raises(KeyError):
        bundle.bound("parent_counts", {})


def test_unknown_names_fail(monkeypatch, tmp_path):
    with pytest.raises(KeyError):
        bundle.bound("no_such_name", 1)
    monkeypatch.setenv(bundle.ENVIRONMENT_VARIABLE, write(tmp_path, {"bundle_id": "2026", "values": {"typo": 1}}))
    with pytest.raises(ValueError):
        bundle.bound("universe_rows", 1)


def test_default_values_of_the_importer_are_the_september_values(monkeypatch):
    monkeypatch.delenv(bundle.ENVIRONMENT_VARIABLE, raising=False)
    importer = importlib.import_module("scripts.import_candidate_extraction_v2")
    assert importer.FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256 == (
        "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3")
    assert importer.FROZEN_SUFFIX_P_COMPLETE_LINE_CENSUS["endorsement_segments"] == 6_096
