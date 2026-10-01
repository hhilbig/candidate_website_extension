from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.freeze_municipal_curated_merged import (
    ACQUISITION_DIR,
    AUDIT_DIR,
    MunicipalMergeError,
    build_merged_freeze,
    source_contexts_from_evidence,
    write_merged_freeze,
)


SHARDS = {0: ACQUISITION_DIR / "shard_0", 1: ACQUISITION_DIR / "shard_1"}


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _all_shard_files() -> list[Path]:
    return sorted(
        path
        for root in SHARDS.values()
        for path in root.rglob("*")
        if path.is_file()
    )


def test_exact_shards_merge_after_structural_audit_without_row_loss() -> None:
    result = build_merged_freeze(AUDIT_DIR, SHARDS)
    assert len(result["attempts"]) == 47
    assert result["attempts"]["attempt_key"].nunique() == 47
    assert len(result["downloaded_files"]) == 28
    assert len(result["evidence"]) == 40
    assert result["evidence"]["evidence_id"].nunique() == 40
    assert len(result["source_contexts"]) == 40
    assert len(result["identity_decisions"]) == 40
    assert result["audit"]["structural_audit"]["verdict"] == "pass"
    assert result["audit"]["structural_audit"]["terminal_attempts"] == 47
    assert result["audit"]["structural_audit"]["evidence_rows"] == 40
    assert result["audit"]["identity_attribution"]["candidate_source_rows"] == 264
    assert result["audit"]["identity_attribution"]["claim_approved_rows"] == 0


def test_source_contexts_use_preserved_source_labels_and_locators() -> None:
    result = build_merged_freeze(AUDIT_DIR, SHARDS)
    contexts = result["source_contexts"].set_index("evidence_id")
    evidence = result["evidence"].set_index("evidence_id")
    betsy_id = result["identity_decisions"].loc[
        result["identity_decisions"]["assigned_candidate_name"].eq("betsy hodges"),
        "evidence_id",
    ].item()
    assert contexts.loc[betsy_id, "source_locator"] == "anchor:157"
    assert contexts.loc[betsy_id, "context_candidate_name"] == "Betsy Hodges"
    assert contexts.loc[betsy_id, "association_kind"] == "direct_anchor_text"
    assert contexts.loc[betsy_id, "context_text"] == evidence.loc[betsy_id, "evidence_text"]

    alameda_id = result["identity_decisions"].loc[
        result["identity_decisions"]["assigned_candidate_name"].eq("barbara halliday"),
        "evidence_id",
    ].item()
    assert contexts.loc[alameda_id, "source_locator"] == "page:65"
    assert contexts.loc[alameda_id, "context_candidate_name"] == "barbara halliday"
    assert contexts.loc[alameda_id, "association_kind"] == "manual_exact_context"


def test_cbs_exact_names_supported_but_troy_is_not_coerced_to_tony() -> None:
    decisions = build_merged_freeze(AUDIT_DIR, SHARDS)["identity_decisions"]
    by_name = decisions.set_index("assigned_candidate_name")
    for name in ["betsy hodges", "don samuels", "cam winton"]:
        assert by_name.loc[name, "identity_attribution_status"] == "identity_supported"
        assert by_name.loc[name, "identity_attribution_reason"] == (
            "context_exactly_names_assigned_candidate"
        )
    troy = decisions.loc[decisions["context_candidate_name"].eq("Troy Benjegerdes")]
    assert len(troy) == 1
    assert troy.iloc[0]["assigned_candidate_name"] == "tony benjegerdes"
    assert troy.iloc[0]["identity_attribution_status"] in {
        "match_unresolved", "reject_identity"
    }
    assert troy.iloc[0]["required_attempt_status"] in {
        "match_unresolved", "must_not_be_found"
    }
    assert not decisions["claim_approved_by_this_audit"].astype(bool).any()


def test_freeze_hashes_every_input_and_output_without_modifying_shards(tmp_path: Path) -> None:
    before = {str(path): _hash(path) for path in _all_shard_files()}
    result = build_merged_freeze(AUDIT_DIR, SHARDS)
    manifest = write_merged_freeze(result, tmp_path / "merged", AUDIT_DIR, SHARDS)
    after = {str(path): _hash(path) for path in _all_shard_files()}
    assert before == after
    assert manifest["claim_status"] == "not_approved"
    assert len(manifest["inputs"]) == 37
    assert len(manifest["outputs"]) == 6
    for record in manifest["inputs"] + manifest["outputs"]:
        path = Path(record["path"])
        if not path.is_absolute():
            path = Path(__file__).resolve().parents[1] / path
        assert path.is_file()
        assert path.stat().st_size == record["bytes"]
        assert _hash(path) == record["sha256"]
    output = tmp_path / "merged"
    assert set(path.name for path in output.iterdir()) == {
        "attempts.csv", "downloaded_files.csv", "evidence.csv", "source_contexts.csv",
        "identity_decisions.csv", "audit.json", "manifest.json",
    }
    audit = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert audit["merged_counts"]["attempts"] == 47
    assert audit["merged_counts"]["evidence"] == 40
    assert pd.read_csv(output / "attempts.csv").shape[0] == 47
    assert pd.read_csv(output / "evidence.csv").shape[0] == 40


def test_source_context_derivation_fails_closed_on_unstructured_text() -> None:
    evidence = pd.DataFrame(
        [{
            "evidence_id": "bad",
            "source_id": "source",
            "source_file_id": "sha256:file",
            "exact_normalized_url": "https://candidate.example/",
            "evidence_text": "whole page contains several names and one URL",
            "identity_match": "candidate_name_exact",
        }]
    )
    with pytest.raises(MunicipalMergeError, match="no exact source-label/locator"):
        source_contexts_from_evidence(evidence)
