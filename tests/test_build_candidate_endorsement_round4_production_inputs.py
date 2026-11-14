"""Tests for the round-four production input builder and Stage-A authenticator.

`authenticate_production_stage_a_run` is the gate that runs the moment Hive
returns: Stage B is built from whatever it accepts. The development
authenticator cannot be reused, because it requires `development_only` to be
true and compares against the development authorizations and generation
configuration. These tests cover the accept path and every refusal.

Nothing here needs a GPU, a model, or the network.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

import scripts.build_candidate_endorsement_round4_production_inputs as builder
from scripts.hive import (
    extract_candidate_endorsement_round4_production_vllm as prod,
)
from src.candidate_endorsement_round4_production import (
    ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
    ROUND4_DEVELOPMENT_AUTHORIZATIONS,
    ROUND4_PRODUCTION_ARTIFACT_SCOPE,
    ROUND4_PRODUCTION_AUTHORIZATIONS,
    get_round4_production_contract,
)

CONTRACT_SHA256 = get_round4_production_contract()[
    "round4_production_contract_sha256"
]


def _gz_jsonl(rows):
    payload = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
        for row in rows
    ).encode("utf-8")
    import io

    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, mtime=0) as handle:
        handle.write(payload)
    return buffer.getvalue()


def _batch(batch_id, parent="parent-1"):
    return {
        "version": "candidate_endorsement_round4_production_input_v1",
        "stage": "line_decision",
        "batch_id": batch_id,
        "document_id": batch_id,
        "parent_document_id": parent,
        "candidate_cycle_id": "H0AA00001-2020",
        "batch_index_within_parent": 0,
        "target_slots": ["slot_00"],
        "padding_slots": [],
        "targets": [],
        "visible_lines": [],
        "full_parent_line_catalog_sha256": "catalog",
    }


def _metadata(manifest_sha256, **overrides):
    row = {
        "artifact_scope": ROUND4_PRODUCTION_ARTIFACT_SCOPE,
        "development_only": False,
        "authorizations": dict(ROUND4_PRODUCTION_AUTHORIZATIONS),
        "stage": "line_decision",
        "input_manifest_sha256": manifest_sha256,
        "generation_config_sha256": "g" * 64,
        "technical_fail_count": 0,
        "processed_output_count": 1,
    }
    row.update(overrides)
    return row


def _make_run(tmp_path: Path, *, full_corpus=True, metadata_overrides=None,
              output_rows=None, drop_output=False):
    """Build a minimal but structurally valid production Stage-A run."""
    inputs = tmp_path / "in"
    chunks = inputs / "chunks"
    chunks.mkdir(parents=True)
    outputs = tmp_path / "out"
    outputs.mkdir()

    batches = [_batch("batch-a")]
    payload = _gz_jsonl(batches)
    (chunks / "chunk_00000.jsonl.gz").write_bytes(payload)
    manifest = {
        "version": prod.MANIFEST_VERSION,
        "stage": "line_decision",
        "artifact_scope": ROUND4_PRODUCTION_ARTIFACT_SCOPE,
        "authorizations": dict(ROUND4_PRODUCTION_AUTHORIZATIONS),
        "full_corpus": full_corpus,
        "round4_production_contract_sha256": CONTRACT_SHA256,
        "batch_count": len(batches),
        "expected_batch_ids": [b["batch_id"] for b in batches],
        "selected_parent_ids": ["parent-1"],
        "chunks": [{
            "file": "chunk_00000.jsonl.gz",
            "sha256": hashlib.sha256(payload).hexdigest(),
        }],
    }
    manifest_path = inputs / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    if not drop_output:
        rows = output_rows if output_rows is not None else [{
            "batch_id": "batch-a", "stage": "line_decision",
            "parent_document_id": "parent-1", "technical_pass": True,
            "model_output": {"slot_00": {"role": "record_anchor"}},
        }]
        (outputs / "chunk_00000.out.jsonl.gz").write_bytes(_gz_jsonl(rows))
    meta = _metadata(manifest_sha256, **(metadata_overrides or {}))
    (outputs / "run_metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    return inputs, outputs, manifest_sha256


def _authenticate(inputs, outputs, manifest_sha256):
    return builder.authenticate_production_stage_a_run(
        inputs, outputs, expected_input_manifest_sha256=manifest_sha256,
    )


def test_a_complete_production_run_authenticates(tmp_path: Path, monkeypatch):
    inputs, outputs, sha = _make_run(tmp_path)
    import scripts.build_candidate_endorsement_round4_development_inputs as dev
    monkeypatch.setattr(
        dev, "_validated_parent_from_stage_a_batches", lambda g: {"ok": True})
    batches, rows, authenticated, provenance = _authenticate(inputs, outputs, sha)
    assert [b["batch_id"] for b in batches] == ["batch-a"]
    assert [r["batch_id"] for r in rows] == ["batch-a"]
    assert authenticated["manifest"]["full_corpus"] is True
    assert provenance["input_manifest_sha256"] == sha
    assert provenance["output_inventory_sha256"]


def test_a_bounded_pilot_is_refused(tmp_path: Path):
    """A pilot selects batches, so its parents are partial and Stage B cannot use it."""
    inputs, outputs, sha = _make_run(tmp_path, full_corpus=False)
    with pytest.raises(ValueError, match="full-corpus Stage-A run"):
        _authenticate(inputs, outputs, sha)


def test_a_development_scoped_run_is_refused(tmp_path: Path):
    inputs, outputs, sha = _make_run(tmp_path, metadata_overrides={
        "artifact_scope": ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
        "development_only": True,
        "authorizations": dict(ROUND4_DEVELOPMENT_AUTHORIZATIONS),
    })
    with pytest.raises(ValueError, match="run metadata provenance differs"):
        _authenticate(inputs, outputs, sha)


def test_a_run_claiming_release_authority_is_refused(tmp_path: Path):
    claim = dict(ROUND4_PRODUCTION_AUTHORIZATIONS)
    claim["release"] = True
    inputs, outputs, sha = _make_run(
        tmp_path, metadata_overrides={"authorizations": claim})
    with pytest.raises(ValueError, match="run metadata provenance differs"):
        _authenticate(inputs, outputs, sha)


def test_a_technical_failure_blocks_stage_b(tmp_path: Path):
    """A failed shard must be repaired before Stage B is built on top of it."""
    inputs, outputs, sha = _make_run(
        tmp_path, metadata_overrides={"technical_fail_count": 3})
    with pytest.raises(ValueError, match="technical failure"):
        _authenticate(inputs, outputs, sha)


def test_a_row_that_did_not_pass_is_refused(tmp_path: Path, monkeypatch):
    import scripts.build_candidate_endorsement_round4_development_inputs as dev
    monkeypatch.setattr(dev, "_validated_parent_from_stage_a_batches", lambda g: {"ok": True})
    inputs, outputs, sha = _make_run(tmp_path, output_rows=[{
        "batch_id": "batch-a", "stage": "line_decision",
        "parent_document_id": "parent-1", "technical_pass": False,
        "model_output": {},
    }])
    with pytest.raises(ValueError, match="did not pass"):
        _authenticate(inputs, outputs, sha)


def test_a_missing_output_chunk_is_refused(tmp_path: Path):
    inputs, outputs, sha = _make_run(tmp_path, drop_output=True)
    with pytest.raises(ValueError, match="output chunks are incomplete"):
        _authenticate(inputs, outputs, sha)


def test_a_tampered_manifest_is_refused(tmp_path: Path):
    inputs, outputs, _sha = _make_run(tmp_path)
    with pytest.raises(ValueError, match="manifest bytes differ"):
        _authenticate(inputs, outputs, "0" * 64)


def test_mismatched_batch_identities_are_refused(tmp_path: Path, monkeypatch):
    import scripts.build_candidate_endorsement_round4_development_inputs as dev
    monkeypatch.setattr(dev, "_validated_parent_from_stage_a_batches", lambda g: {"ok": True})
    inputs, outputs, sha = _make_run(tmp_path, output_rows=[{
        "batch_id": "batch-other", "stage": "line_decision",
        "parent_document_id": "parent-1", "technical_pass": True,
        "model_output": {},
    }])
    with pytest.raises(ValueError, match="output batch identities differ"):
        _authenticate(inputs, outputs, sha)


def test_a_selected_parent_with_no_decidable_line_is_allowed(monkeypatch):
    """Two real parents have every line outside the attested context scope."""
    batches = [_batch("batch-a", parent="has-lines")]
    manifest = {"selected_parent_ids": ["has-lines", "empty-parent"]}
    monkeypatch.setattr(
        builder, "endorsement_rows_by_parent", lambda: {"empty-parent": []})
    monkeypatch.setattr(
        builder, "parent_inventory_from_export_rows",
        lambda rows: {"lines": [{"model_decision_required": False}] * 25})
    import scripts.build_candidate_endorsement_round4_development_inputs as dev
    monkeypatch.setattr(
        dev, "_validated_parent_from_stage_a_batches", lambda g: {"ok": True})
    parents = builder._production_stage_a_parents(batches, manifest)
    assert parents == [{"ok": True}]


def test_a_selected_parent_with_decidable_lines_must_have_a_batch(monkeypatch):
    """Silence is only acceptable when there was genuinely nothing to decide."""
    batches = [_batch("batch-a", parent="has-lines")]
    manifest = {"selected_parent_ids": ["has-lines", "should-have-batched"]}
    monkeypatch.setattr(
        builder, "endorsement_rows_by_parent",
        lambda: {"should-have-batched": []})
    monkeypatch.setattr(
        builder, "parent_inventory_from_export_rows",
        lambda rows: {"lines": [{"model_decision_required": True}] * 3})
    with pytest.raises(ValueError, match="3 decidable"):
        builder._production_stage_a_parents(batches, manifest)


def test_a_batch_naming_an_unselected_parent_is_refused():
    batches = [_batch("batch-a", parent="stranger")]
    manifest = {"selected_parent_ids": ["known"]}
    with pytest.raises(ValueError, match="outside the selection"):
        builder._production_stage_a_parents(batches, manifest)
