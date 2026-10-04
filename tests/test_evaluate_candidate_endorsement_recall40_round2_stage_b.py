from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pytest

from scripts.build_candidate_endorsement_recall40_round2_human_task import (
    build_task,
    render_html,
)
from scripts.evaluate_candidate_endorsement_recall40_round2_stage_b import (
    _canonical_report,
)
from scripts.audit_candidate_endorsement_recall40_round2_stage_b import build_audit
from scripts.diagnose_candidate_endorsement_recall40_round2 import build_diagnostic
from scripts.score_candidate_endorsement_recall40_round2_human_truth import score
from src.candidate_endorsement_line_ledger import (
    FROZEN_RECALL40_ROUND2_STAGE_B_CONTRACT_SHA256,
    RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256,
    align_record_extraction_output,
    get_frozen_recall40_round2_stage_b_contract,
    sanitize_recall40_round2_stage_b_optional_fields,
    validate_record_extraction_output,
)


def _row(index: int) -> dict[str, object]:
    parent_id = f"parent-{index % 32:02d}"
    record = {
        "endorser_name": f"Person {index}",
        "source_model_item_id": f"item-{index}",
    }
    return {
        "batch_id": f"batch-{index:03d}",
        "parent_document_id": parent_id,
        "technical_pass": True,
        "stage_validation": {
            "schema_valid": True,
            "coverage_complete": True,
            "errors": [],
        },
        "stage_alignment": {
            "coverage_complete": True,
            "aligned_records": [record],
            "withheld_records": [],
            "withheld_record_count": 0,
        },
    }


def test_round2_stage_b_gate_requires_complete_zero_withheld_run() -> None:
    manifest = {
        "line_ledger_contract_sha256": (
            FROZEN_RECALL40_ROUND2_STAGE_B_CONTRACT_SHA256
        ),
        "selected_parent_ids": [f"parent-{index:02d}" for index in range(40)],
    }
    rows = [_row(index) for index in range(304)]
    report = _canonical_report(
        manifest,
        rows,
        {"generation_config_sha256": "a" * 64},
    )
    assert report["gate_decision"] == (
        "GO_BUILD_PREDICTION_BLIND_HUMAN_TRUTH_TASK"
    )
    assert report["validated_batch_count"] == 304
    assert report["aligned_record_count"] == 304
    assert report["withheld_record_count"] == 0
    assert report["prediction_blind_human_task_authorized"] is True
    assert report["full_private_inference_authorized"] is False
    assert report["release_authorized"] is False

    attacked = copy.deepcopy(rows)
    attacked[0]["stage_alignment"]["withheld_records"] = [{"reason": "test"}]
    attacked[0]["stage_alignment"]["withheld_record_count"] = 1
    attacked[0]["stage_alignment"]["coverage_complete"] = False
    with pytest.raises(ValueError, match="technically releasable"):
        _canonical_report(manifest, attacked, {})


def test_round2_stage_b_gate_rejects_missing_or_duplicate_batches() -> None:
    manifest = {
        "line_ledger_contract_sha256": (
            FROZEN_RECALL40_ROUND2_STAGE_B_CONTRACT_SHA256
        ),
        "selected_parent_ids": [f"parent-{index:02d}" for index in range(40)],
    }
    rows = [_row(index) for index in range(304)]
    with pytest.raises(ValueError, match="row count"):
        _canonical_report(manifest, rows[:-1], {})
    duplicated = copy.deepcopy(rows)
    duplicated[-1]["batch_id"] = duplicated[0]["batch_id"]
    with pytest.raises(ValueError, match="duplicated"):
        _canonical_report(manifest, duplicated, {})


def test_round2_human_app_is_prediction_blind_and_selection_safe() -> None:
    task = {
        "task_id": "test-round2-task",
        "ordered_parent_ids_sha256": "a" * 64,
        "stage_b_gate_report_sha256": "b" * 64,
        "instructions": "Code source spans only.",
        "parents": [
            {
                "parent_document_id": f"parent-{index:02d}",
                "parent_source_text": "Name 😀 supports Candidate",
            }
            for index in range(40)
        ],
    }
    html = render_html(task)
    assert "aligned_records" not in html
    assert "model_output" not in html
    assert "selectionSpan()||last" in html
    assert "Array.from" in html
    assert "Complete all 40 parents first" in html
    assert "localStorage.setItem" in html


def test_round2_failed_stage_b_audit_rederives_every_row() -> None:
    output = Path(
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_recall40_round2_stage_b_outputs_20260902a"
    )
    if not output.is_dir():
        pytest.skip("round-two Stage-B failed output is not available")
    audit = build_audit(output)
    census = audit["authenticated_census"]
    assert census["output_rows"] == 304
    assert census["technical_pass"] == 261
    assert census["technical_fail"] == 43
    assert census["invalid_stage_output"] == 39
    assert census["invalid_stage_alignment"] == 4
    assert census["valid_row_aligned_records"] == 1983
    assert census["valid_row_withheld_records"] == 0
    assert census["failed_parent_count"] == 13
    assert audit["authorization"] == {
        "blind_retry_authorized": False,
        "prediction_blind_human_truth_task_authorized": True,
        "technical_failure_scoring_only": True,
        "full_private_inference_authorized": False,
        "release_authorized": False,
    }


def test_round2_repair_only_nulls_unsupported_optional_fields() -> None:
    export = Path(
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_recall40_round2_stage_b_preflight_20260902a"
    )
    output = Path(
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_recall40_round2_stage_b_outputs_20260902a"
    )
    if not output.is_dir():
        pytest.skip("round-two Stage-B failed output is not available")

    def read_rows(paths: list[Path]) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for path in paths:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                rows.extend(json.loads(line) for line in handle)
        return rows

    batches = {
        row["batch_id"]: row
        for row in read_rows(sorted((export / "chunks").glob("*.jsonl.gz")))
    }
    failed = [
        row
        for row in read_rows(sorted(output.glob("chunk_*.out.jsonl.gz")))
        if row["technical_pass"] is False
    ]
    contract = get_frozen_recall40_round2_stage_b_contract()
    closed = 0
    for row in failed:
        original = row["model_output"]
        repaired, changes = sanitize_recall40_round2_stage_b_optional_fields(
            batches[row["batch_id"]], original
        )
        for change in changes:
            assert change["field"] in {"title", "affiliation", "jurisdiction"}
            assert change["replacement_value"] is None
        for slot, item in original.items():
            for index, record in enumerate(item["records"]):
                repaired_record = repaired[slot]["records"][index]
                assert repaired_record["endorser_name"] == record["endorser_name"]
                assert repaired_record["entity_type"] == record["entity_type"]
                assert repaired_record["endorser_category"] == record["endorser_category"]
        validation = validate_record_extraction_output(
            batches[row["batch_id"]], repaired,
            residual_projection_spec_sha256=(
                RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256
            ),
        )
        if validation["schema_valid"] and validation["coverage_complete"]:
            alignment = align_record_extraction_output(
                batches[row["batch_id"]], repaired,
                line_ledger_contract_sha256=contract["line_ledger_contract_sha256"],
                residual_projection_spec_sha256=(
                    RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256
                ),
            )
            closed += int(alignment["public_coverage_complete"] is True)
    assert closed == 18


def test_failed_audit_authorizes_prediction_blind_task_only() -> None:
    output = Path(
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_recall40_round2_stage_b_outputs_20260902a"
    )
    audit = Path(
        "quality_reports/"
        "candidate_endorsement_recall40_round2_stage_b_failed_2026-09-03.json"
    )
    if not output.is_dir() or not audit.is_file():
        pytest.skip("round-two Stage-B failed audit is not available")
    task = build_task(output, audit)
    assert task["predictions_included"] is False
    assert task["stage_b_authorization_status"] == (
        "authenticated_technical_failure_scoring_only"
    )
    assert task["stage_b_technical_pass_batch_count"] == 261
    assert task["stage_b_record_ledger_sha256"] is None
    serialized = json.dumps(task, ensure_ascii=False)
    assert "model_output" not in serialized
    assert "aligned_records" not in serialized


def test_round2_scorer_preserves_technical_failure_gate(tmp_path: Path) -> None:
    task_path = Path(
        "build/private_candidate_endorsement_recall40_round2_human_truth_20260905/"
        "task.json"
    )
    if not task_path.is_file():
        pytest.skip("round-two human truth task is not available")
    task = json.loads(task_path.read_text(encoding="utf-8"))
    results = {
        "task_id": task["task_id"],
        "format": "recall40-human-truth-v1",
        "exported_at": "2026-09-05T00:00:00.000Z",
        "parent_count": 40,
        "responses": [
            {
                "parent_document_id": parent["parent_document_id"],
                "true_zero": True,
                "occurrences": [],
            }
            for parent in task["parents"]
        ],
    }
    results_path = tmp_path / "results.json"
    results_path.write_text(json.dumps(results), encoding="utf-8")
    report = score(task_path, results_path)
    technical = report["checks"][0]
    assert technical["id"] == "technical_batch_validity_percent"
    assert technical["observed"] == pytest.approx(261 / 304 * 100)
    assert technical["passed"] is False
    assert report["gate_decision"] == "NO_GO"
    assert report["full_private_inference_authorized"] is False
    assert report["release_authorized"] is False


def test_round2_completed_human_truth_diagnostic_replays() -> None:
    task_dir = Path(
        "build/private_candidate_endorsement_recall40_round2_human_truth_20260905"
    )
    results = task_dir / "results.json"
    score_path = Path(
        "quality_reports/"
        "candidate_endorsement_recall40_round2_human_truth_score_2026-09-05.json"
    )
    if not results.is_file() or not score_path.is_file():
        pytest.skip("completed private human-truth inputs are absent")
    report = build_diagnostic(task_dir / "task.json", results, score_path)
    assert report["strict_score_gate_decision"] == "NO_GO"
    assert report["raw_output_latent_census"] == {
        "human_occurrences": 2206,
        "raw_model_records": 2307,
        "matched_occurrences": 2081,
        "support_covered_occurrences": 2066,
        "complete_pages": 20,
        "detected_anchor_blocks": 2276,
        "anchor_blocks_with_human_occurrence": 2176,
        "human_occurrences_in_anchor_block": 2202,
    }
    assert report["unmatched_human_occurrence_causes"] == {
        "detected_block_but_name_differs": 121,
        "no_detected_anchor_block": 4,
    }
    assert report["largest_false_anchor_concentration"] == {
        "task_parent_ordinal": 28,
        "anchor_blocks_without_human_occurrence": 80,
    }
    assert report["full_private_inference_authorized"] is False
    assert report["release_authorized"] is False
