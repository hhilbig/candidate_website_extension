from __future__ import annotations

import copy
import gzip
import json
from collections import Counter
from pathlib import Path

import pytest

from scripts.build_candidate_endorsement_line_ledger_a3_handcode import (
    A3_SPEC_SHA256,
    build_a3_provenance,
    derive_a3_residuals,
    sha256_file,
)
from scripts.evaluate_candidate_endorsement_line_ledger_pilot import (
    evaluate_stage_a2_only,
    replay_stage_a2_merge,
)
from scripts.import_candidate_endorsement_line_ledger_a3_handcode import (
    merge_human_a3,
    unchanged_gate_replay,
    validate_results,
    validate_task_provenance,
)
from scripts.build_candidate_endorsement_line_ledger import (
    build_human_a3_record_extraction_export,
    read_jsonl_gz,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    file_sha256,
    validate_expected_input_manifest_sha256,
    validate_input_manifest,
    _validate_stage_b_anchor_coverage,
)
from src.candidate_endorsement_line_ledger import (
    RECORD_EXTRACTION_STAGE,
    get_line_ledger_contract,
    sha256_json,
)


REPO = Path(__file__).resolve().parents[1]
A1_EXPORT = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_a_preflight_20260825b"
A1_OUTPUT = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_a_preflight_hive_output_20260825b_21437036"
A2_EXPORT = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_a2_preflight_20260825f"
A2_OUTPUT = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_a2_preflight_hive_output_20260825f_21447139"
FULL_EXPORT = REPO / "build/hive_candidate_extraction/v2_1_full_export_20260824p"
TASK = REPO / "build/candidate_endorsement_line_ledger_a3_handcode_21447139/task.json"
HTML = TASK.parent / "candidate_endorsement_line_ledger_human_a3_21447139.html"
RESULTS = TASK.parent / "candidate_endorsement_line_ledger_human_a3_21447139_results.json"
IMPORT_ARTIFACT = REPO / "quality_reports/candidate_endorsement_line_ledger_human_a3_21447139.json"
EVALUATION = REPO / "quality_reports/candidate_endorsement_line_ledger_stage_a2_preflight_21447139_2026-08-25.json"


@pytest.fixture(scope="module")
def replay_bundle():
    replayed = replay_stage_a2_merge(
        A1_EXPORT, A1_OUTPUT, A2_EXPORT, A2_OUTPUT, FULL_EXPORT
    )
    evaluation = evaluate_stage_a2_only(
        A1_EXPORT, A1_OUTPUT, A2_EXPORT, A2_OUTPUT, FULL_EXPORT
    )
    return replayed, evaluation


def test_a3_task_is_exact_authenticated_14_row_residual(replay_bundle) -> None:
    replayed, evaluation = replay_bundle
    residuals = derive_a3_residuals(replayed, evaluation)
    assert len(residuals) == 14
    assert len({row["source_line_id"] for row in residuals}) == 14
    assert Counter(row["residual_reason"] for row in residuals) == {
        "invalid_continuation_owner": 8,
        "interface_role_mismatch": 6,
    }
    assert len({row["parent_document_id"] for row in residuals}) == 3
    task = json.loads(TASK.read_text())
    assert task["provenance"]["a3_spec_sha256"] == A3_SPEC_SHA256
    assert task["provenance"]["residual_count"] == 14
    assert [item["residual"] for item in task["items"]] == residuals
    assert sha256_file(TASK) == (
        "e834f23d55804bc138d6dac1441dfbd96ab0449ddd34e1eeb0edf2585d96bfe5"
    )


def _results(task, *, role="navigation_form_footer"):
    return {
        "task_id": task["task_id"],
        "format": "handcode-results-v1",
        "exported_at": "2026-08-26T00:00:00Z",
        "n_items": len(task["items"]),
        "n_complete": len(task["items"]),
        "responses": [{
            "item_id": item["id"],
            "status": "complete",
            "answers": {"resolution": "terminal", "final_role": role},
        } for item in task["items"]],
    }


def test_a3_typed_import_and_three_pass_unchanged_gate_replay(replay_bundle) -> None:
    replayed, evaluation = replay_bundle
    task = json.loads(TASK.read_text())
    decisions = validate_results(task, _results(task))
    residuals = derive_a3_residuals(replayed, evaluation)
    merged = merge_human_a3(
        replayed["parents"], replayed["stage_a2_merge"], residuals, decisions,
        task_sha256="a" * 64, results_sha256="b" * 64,
    )
    assert merged["validation"]["stage_a3_expected_residual_count"] == 14
    assert merged["validation"]["stage_a3_coverage_complete"] is True
    assert merged["validation"]["ownership_conflict_count"] == 0
    assert merged["validation"]["validation_passed"] is True
    gate_replay = unchanged_gate_replay(replayed, merged)["evaluation"]
    assert len(gate_replay["checks"]) == 11
    assert gate_replay["gate_decision"] == "GO"
    assert all(check["passed"] for check in gate_replay["checks"])


def test_a3_import_rejects_incomplete_wrong_and_ineligible_decisions() -> None:
    task = json.loads(TASK.read_text())
    incomplete = _results(task)
    incomplete["n_complete"] -= 1
    with pytest.raises(ValueError, match="completion"):
        validate_results(task, incomplete)

    wrong_role = _results(task)
    wrong_role["responses"][0]["answers"]["final_role"] = "record_anchor"
    with pytest.raises(ValueError, match="terminal"):
        validate_results(task, wrong_role)

    ineligible = _results(task)
    ineligible["responses"][0]["answers"] = {
        "resolution": "attach", "anchor_source_line_id": "f" * 64,
    }
    with pytest.raises(ValueError, match="attachment"):
        validate_results(task, ineligible)

    reordered = _results(task)
    reordered["responses"] = list(reversed(reordered["responses"]))
    with pytest.raises(ValueError, match="typed response"):
        validate_results(task, reordered)


def test_a3_import_binds_full_upstream_provenance(replay_bundle) -> None:
    replayed, evaluation = replay_bundle
    task = json.loads(TASK.read_text())
    residuals = derive_a3_residuals(replayed, evaluation)
    expected = build_a3_provenance(
        replayed,
        residuals,
        stage_a2_export_dir=A2_EXPORT,
        stage_a2_evaluation_report=(
            REPO / "quality_reports/candidate_endorsement_line_ledger_stage_a2_preflight_21447139_2026-08-25.json"
        ),
    )
    validate_task_provenance(task, expected)
    for field in (
        "stage_a2_input_manifest_sha256",
        "stage_a2_output_artifacts",
        "stage_a2_evaluation_report_sha256",
        "stage_a2_final_decisions_sha256",
        "stage_a2_per_line_provenance_sha256",
    ):
        attacked = copy.deepcopy(expected)
        attacked[field] = "f" * 64 if isinstance(attacked[field], str) else {}
        with pytest.raises(ValueError, match="provenance replay"):
            validate_task_provenance(task, attacked)

    html = (
        REPO / "build/candidate_endorsement_line_ledger_a3_handcode_21447139/"
        "candidate_endorsement_line_ledger_human_a3_21447139.html"
    ).read_text()
    assert "function options(item)" in html
    assert "item.residual.eligible_final_anchors" in html
    assert "Complete all ${task.items.length} rows first." in html


def test_three_pass_stage_b_build_replays_exact_a3_authorization(tmp_path) -> None:
    output_dir = tmp_path / "stage_b"
    manifest = build_human_a3_record_extraction_export(
        FULL_EXPORT, A1_EXPORT, A1_OUTPUT, A2_EXPORT, A2_OUTPUT,
        EVALUATION, TASK, HTML, RESULTS, IMPORT_ARTIFACT, output_dir,
    )
    assert manifest["mode"] == "record_extraction_preflight_from_stage_a3"
    assert manifest["stage_b_upstream_pass_count"] == 3
    assert manifest["upstream_stage_a3"]["task_sha256"] == sha256_file(TASK)
    assert manifest["upstream_stage_a3"]["results_sha256"] == sha256_file(RESULTS)
    assert manifest["three_pass_merge"]["validation"]["validation_passed"] is True
    assert manifest["three_pass_merge"]["unchanged_gate_replay"]["gate_decision"] == "GO"
    assert len(manifest["three_pass_merge"]["unchanged_gate_replay"]["checks"]) == 11
    assert manifest["exported_records"] > 0
    contract = get_line_ledger_contract()
    manifest["contract_is_frozen"] = True
    manifest["frozen_line_ledger_contract_sha256"] = contract[
        "line_ledger_contract_sha256"
    ]
    payload = dict(manifest)
    payload.pop("manifest_payload_sha256")
    manifest["manifest_payload_sha256"] = sha256_json(payload)
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    validated, _, paths = validate_input_manifest(
        output_dir / "manifest.json", output_dir / "chunks",
        stage=RECORD_EXTRACTION_STAGE, contract=contract,
    )
    assert validated["three_pass_merge"] == manifest["three_pass_merge"]
    assert len(paths) == len(manifest["files"]["chunks"])
    ordered_anchor_ids = [
        target["source_line_id"]
        for path in paths
        for batch in read_jsonl_gz(path)
        for target in batch["targets"]
    ]
    _validate_stage_b_anchor_coverage(ordered_anchor_ids)
    with pytest.raises(ValueError, match="authenticated-anchor coverage"):
        _validate_stage_b_anchor_coverage([
            *ordered_anchor_ids, ordered_anchor_ids[0]
        ])

    first_chunk = paths[0]
    original_chunk_bytes = first_chunk.read_bytes()
    original_manifest_text = (output_dir / "manifest.json").read_text()
    for attack_kind in ("legacy_fields", "stage_a_locator", "inline_provenance"):
        rows = []
        with gzip.open(first_chunk, "rt", encoding="utf-8") as handle:
            rows = [json.loads(line) for line in handle]
        if attack_kind == "legacy_fields":
            rows[0]["stage_b_two_pass_provenance_spec_sha256"] = "f" * 64
            rows[0]["two_pass_line_provenance_sha256"] = "e" * 64
        elif attack_kind == "stage_a_locator":
            rows[0]["targets"][0]["stage_a_batch_id"] = "d" * 64
        else:
            rows[0]["targets"][0]["record_block_line_decision_provenance"][0][
                "final_role"
            ] = "candidate_self"
        with gzip.open(first_chunk, "wt", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        attacked_manifest = json.loads(original_manifest_text)
        entry = attacked_manifest["files"]["chunks"][0]
        entry["sha256"] = file_sha256(first_chunk)
        entry["bytes"] = first_chunk.stat().st_size
        entry["content_sha256"] = sha256_json(rows)
        payload = dict(attacked_manifest)
        payload.pop("manifest_payload_sha256")
        attacked_manifest["manifest_payload_sha256"] = sha256_json(payload)
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(attacked_manifest, indent=2, sort_keys=True) + "\n")
        with pytest.raises(ValueError):
            validate_input_manifest(
                manifest_path, output_dir / "chunks",
                stage=RECORD_EXTRACTION_STAGE, contract=get_line_ledger_contract(),
            )
        first_chunk.write_bytes(original_chunk_bytes)
        manifest_path.write_text(original_manifest_text)

    manifest_path = output_dir / "manifest.json"
    expected_manifest_sha256 = file_sha256(manifest_path)
    float_attack = json.loads(original_manifest_text)
    float_attack["stage_b_upstream_pass_count"] = 3.0
    float_payload = dict(float_attack)
    float_payload.pop("manifest_payload_sha256")
    float_attack["manifest_payload_sha256"] = sha256_json(float_payload)
    manifest_path.write_text(json.dumps(float_attack, indent=2, sort_keys=True) + "\n")
    with pytest.raises(ValueError, match=r"A1\+A2\+A3"):
        validate_input_manifest(
            manifest_path, output_dir / "chunks",
            stage=RECORD_EXTRACTION_STAGE, contract=get_line_ledger_contract(),
        )
    manifest_path.write_text(original_manifest_text)
    structural_attacks = []
    for mutate in (
        lambda value: value.update({"mode": "record_extraction_preflight_from_stage_a2"}),
        lambda value: value["three_pass_merge"]["validation"].update({
            "stage_a3_expected_residual_count": 0,
            "stage_a3_adjudicated_residual_count": 0,
            "stage_a3_coverage_complete": False,
        }),
        lambda value: value["three_pass_merge"].update({
            "unchanged_gate_replay": {
                "gate_decision": "GO", "checks": [{"passed": True}] * 11,
            }
        }),
        lambda value: value["three_pass_merge"].update({
            "ownership_by_continuation_sha256": "e" * 64,
        }),
    ):
        value = json.loads(original_manifest_text)
        mutate(value)
        payload = dict(value)
        payload.pop("manifest_payload_sha256")
        value["manifest_payload_sha256"] = sha256_json(payload)
        structural_attacks.append(value)
    for value in structural_attacks:
        manifest_path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        with pytest.raises(ValueError):
            validate_input_manifest(
                manifest_path, output_dir / "chunks",
                stage=RECORD_EXTRACTION_STAGE, contract=get_line_ledger_contract(),
            )
    manifest_path.write_text(original_manifest_text)
    attacked_manifest = json.loads(manifest_path.read_text())
    attacked_manifest["upstream_stage_a3"]["results_sha256"] = "f" * 64
    payload = dict(attacked_manifest)
    payload.pop("manifest_payload_sha256")
    attacked_manifest["manifest_payload_sha256"] = sha256_json(payload)
    manifest_path.write_text(json.dumps(attacked_manifest, indent=2, sort_keys=True) + "\n")
    with pytest.raises(ValueError, match="externally frozen"):
        validate_expected_input_manifest_sha256(
            manifest_path, expected_manifest_sha256
        )

    attacked = json.loads(IMPORT_ARTIFACT.read_text())
    attacked["stage_b_authorized"] = False
    attacked_path = tmp_path / "attacked.json"
    attacked_path.write_text(json.dumps(attacked, indent=2, sort_keys=True) + "\n")
    with pytest.raises(ValueError, match="imported authorization"):
        build_human_a3_record_extraction_export(
            FULL_EXPORT, A1_EXPORT, A1_OUTPUT, A2_EXPORT, A2_OUTPUT,
            EVALUATION, TASK, HTML, RESULTS, attacked_path, tmp_path / "rejected",
        )
