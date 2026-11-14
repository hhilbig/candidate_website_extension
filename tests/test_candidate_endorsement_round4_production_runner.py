"""Tests for the round-four production runner's scope and budget logic.

The runner imports vLLM only inside its inference path, so every function
here runs without a GPU and without the model.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.hive import (
    extract_candidate_endorsement_round4_production_vllm as runner,
)
from src.candidate_endorsement_round4_production import (
    ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
    ROUND4_DEVELOPMENT_AUTHORIZATIONS,
    ROUND4_PRODUCTION_ARTIFACT_SCOPE,
    ROUND4_PRODUCTION_AUTHORIZATIONS,
    get_round4_production_contract,
    validate_scope_authorizations,
)


def test_the_production_window_is_larger_than_the_development_window():
    """The corpus does not fit the window the 40-parent sample used."""
    assert runner.MAX_MODEL_LEN == 49_152
    assert runner.DEVELOPMENT_MAX_MODEL_LEN == 32_768
    assert runner.MAX_MODEL_LEN > runner.DEVELOPMENT_MAX_MODEL_LEN


def test_the_measured_maximum_prompt_fits_production_but_not_development():
    measured = runner.MEASURED_MAX_STAGE_A_PROMPT_TOKENS
    assert measured == 34_248
    runner.assert_prompt_budget_fits("line_decision", measured)
    with pytest.raises(ValueError, match="raise max_model_len"):
        runner.assert_prompt_budget_fits(
            "line_decision", measured,
            max_model_len=runner.DEVELOPMENT_MAX_MODEL_LEN,
        )


def test_prompt_budget_subtracts_output_and_margin():
    budget = runner.prompt_budget("line_decision")
    assert budget == runner.MAX_MODEL_LEN - 2_048 - 256
    assert runner.prompt_budget("record_extraction") == (
        runner.MAX_MODEL_LEN - 16_384 - 256
    )


def test_restamping_replaces_only_the_scope_labels():
    """The substantive content must survive untouched."""
    row = {
        "version": "candidate_endorsement_round4_development_output_v1",
        "runner_version": "dev-runner",
        "artifact_scope": ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
        "development_only": True,
        "authorizations": dict(ROUND4_DEVELOPMENT_AUTHORIZATIONS),
        "batch_id": "keep-me",
        "raw_output": "{}",
        "technical_pass": True,
    }
    out = runner._restamp(row, version=runner.OUTPUT_VERSION)
    assert out["artifact_scope"] == ROUND4_PRODUCTION_ARTIFACT_SCOPE
    assert out["development_only"] is False
    assert out["authorizations"] == ROUND4_PRODUCTION_AUTHORIZATIONS
    assert out["version"] == runner.OUTPUT_VERSION
    assert out["runner_version"] == runner.RUNNER_VERSION
    # Everything substantive is preserved.
    assert out["batch_id"] == "keep-me"
    assert out["raw_output"] == "{}"
    assert out["technical_pass"] is True
    # The input row is never mutated.
    assert row["artifact_scope"] == ROUND4_DEVELOPMENT_ARTIFACT_SCOPE


def test_a_restamped_row_never_claims_release_authority():
    out = runner._restamp({}, version=runner.OUTPUT_VERSION)
    assert out["authorizations"]["release"] is False
    assert out["authorizations"]["production_integration"] is True
    validate_scope_authorizations(
        out["artifact_scope"], out["authorizations"]
    )


def _manifest(tmp_path: Path, **overrides) -> tuple[Path, Path, str]:
    chunks = tmp_path / "chunks"
    chunks.mkdir()
    payload = b"placeholder"
    (chunks / "chunk_00000.jsonl.gz").write_bytes(payload)
    import hashlib

    manifest = {
        "version": runner.MANIFEST_VERSION,
        "stage": "line_decision",
        "artifact_scope": ROUND4_PRODUCTION_ARTIFACT_SCOPE,
        "authorizations": dict(ROUND4_PRODUCTION_AUTHORIZATIONS),
        "full_corpus": True,
        "round4_production_contract_sha256": get_round4_production_contract()[
            "round4_production_contract_sha256"
        ],
        "batch_count": 1,
        "chunks": [{
            "file": "chunk_00000.jsonl.gz",
            "sha256": hashlib.sha256(payload).hexdigest(),
        }],
    }
    manifest.update(overrides)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path, chunks, runner.file_sha256(path)


def test_a_production_manifest_authenticates(tmp_path: Path):
    path, chunks, digest = _manifest(tmp_path)
    loaded = runner.validate_production_manifest(
        path, chunks, stage="line_decision", expected_manifest_sha256=digest,
    )
    assert loaded["artifact_scope"] == ROUND4_PRODUCTION_ARTIFACT_SCOPE


def test_a_bounded_pilot_manifest_is_accepted(tmp_path: Path):
    """A pilot over the corpus is production scope, not full corpus."""
    path, chunks, digest = _manifest(tmp_path, full_corpus=False)
    loaded = runner.validate_production_manifest(
        path, chunks, stage="line_decision", expected_manifest_sha256=digest,
    )
    assert loaded["full_corpus"] is False


def test_a_development_scoped_manifest_is_refused(tmp_path: Path):
    path, chunks, digest = _manifest(
        tmp_path,
        artifact_scope=ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
        authorizations=dict(ROUND4_DEVELOPMENT_AUTHORIZATIONS),
    )
    with pytest.raises(ValueError, match="scope differs"):
        runner.validate_production_manifest(
            path, chunks, stage="line_decision",
            expected_manifest_sha256=digest,
        )


def test_a_manifest_claiming_release_authority_is_refused(tmp_path: Path):
    claim = dict(ROUND4_PRODUCTION_AUTHORIZATIONS)
    claim["release"] = True
    path, chunks, digest = _manifest(tmp_path, authorizations=claim)
    with pytest.raises(ValueError, match="do not match the artifact scope"):
        runner.validate_production_manifest(
            path, chunks, stage="line_decision",
            expected_manifest_sha256=digest,
        )


def test_tampered_manifest_bytes_are_refused(tmp_path: Path):
    path, chunks, _digest = _manifest(tmp_path)
    with pytest.raises(ValueError, match="manifest bytes differ"):
        runner.validate_production_manifest(
            path, chunks, stage="line_decision",
            expected_manifest_sha256="0" * 64,
        )


def test_a_tampered_chunk_is_refused(tmp_path: Path):
    path, chunks, digest = _manifest(tmp_path)
    (chunks / "chunk_00000.jsonl.gz").write_bytes(b"changed")
    with pytest.raises(ValueError, match="chunk bytes differ"):
        runner.validate_production_manifest(
            path, chunks, stage="line_decision",
            expected_manifest_sha256=digest,
        )


def test_a_contract_mismatch_is_refused(tmp_path: Path):
    path, chunks, digest = _manifest(
        tmp_path, round4_production_contract_sha256="0" * 64,
    )
    with pytest.raises(ValueError, match="contract differs"):
        runner.validate_production_manifest(
            path, chunks, stage="line_decision",
            expected_manifest_sha256=digest,
        )


def test_an_unknown_stage_is_refused(tmp_path: Path):
    path, chunks, digest = _manifest(tmp_path)
    with pytest.raises(ValueError, match="unsupported production stage"):
        runner.validate_production_manifest(
            path, chunks, stage="line_adjudication",
            expected_manifest_sha256=digest,
        )


def test_the_generation_config_binds_the_production_scope():
    config = runner.generation_config_payload(
        "line_decision", input_manifest_sha256="a" * 64,
    )
    assert config["artifact_scope"] == ROUND4_PRODUCTION_ARTIFACT_SCOPE
    assert config["authorizations"]["release"] is False
    assert config["seed"] == runner.GENERATION_SEED
    assert config["temperature"] == 0.0
    assert config["thinking_enabled"] is False
    assert config["system_prompt_sha256"] == (
        "35a8a6cc907568fa6e4d7e19ec0e92cd2e926f1cec7e7fd99a027e1637549546"
    )
