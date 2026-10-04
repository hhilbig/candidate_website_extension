from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

import scripts.build_candidate_endorsement_recall40_stage_b_residual_handcode as builder
import scripts.import_candidate_endorsement_recall40_stage_b_residual as importer
import scripts.evaluate_candidate_endorsement_recall40_stage_b_residual as evaluator

REPO = Path(__file__).resolve().parents[1]
TASK_DIR = REPO / "build/candidate_endorsement_recall40_stage_b_residual_21576064"
TASK = TASK_DIR / "task.json"
HTML = TASK_DIR / "candidate_endorsement_recall40_stage_b_residual_21576064.html"


def _results() -> dict[str, object]:
    return {
        "task_id": builder.build_task()["task_id"],
        "format": "handcode-results-v1",
        "exported_at": "2026-08-28T10:00:00.000Z",
        "n_items": 7,
        "n_complete": 7,
        "responses": [
            {"item_id": batch_id, "status": "complete", "answers": {
                "approve_exact_tuple": "yes", "notes": "",
            }}
            for batch_id in builder.FAILED_BATCH_IDS
        ],
    }


def test_frozen_task_and_html_reproduce_exact_authenticated_scope() -> None:
    assert hashlib.sha256(TASK.read_bytes()).hexdigest() == importer.FROZEN_TASK_SHA256
    assert hashlib.sha256(HTML.read_bytes()).hexdigest() == importer.FROZEN_HTML_SHA256
    task = json.loads(TASK.read_text(encoding="utf-8"))
    assert task == builder.build_task()
    assert builder.render_html(task) == HTML.read_text(encoding="utf-8")
    assert len(task["items"]) == 7
    assert task["provenance"]["failed_batch_ids"] == list(builder.FAILED_BATCH_IDS)
    html = HTML.read_text(encoding="utf-8")
    assert 'task_id: TASK.task_id, format: "handcode-results-v1"' in html
    assert "Complete all 7 items before exporting." in html
    assert "partial exports can be re-imported later" not in html
    assert hashlib.sha256((TASK_DIR / "results.json").read_bytes()).hexdigest() == (
        "12611bcaba1cedb566d431c91f6d380b237d677255381fc28e9c1db10c29660e"
    )


def test_exact_confirmations_replay_all_62_rows_without_withholding(tmp_path: Path) -> None:
    results_path = tmp_path / "results.json"
    results_path.write_text(json.dumps(_results()), encoding="utf-8")
    artifact = importer.import_results(TASK, HTML, results_path)
    assert artifact["merged_row_count"] == 62
    assert artifact["technical_pass_count"] == 62
    assert artifact["aligned_record_count"] == 362
    assert artifact["withheld_record_count"] == 0
    assert artifact["content_gates_evaluated"] is False
    assert artifact["larger_run_or_release_authorized"] is False
    report = evaluator.evaluate(TASK, HTML, results_path)
    assert report["gate_decision"] == "GO_SCORE_UNTOUCHED_RECALL40"
    assert [check["passed"] for check in report["checks"]] == [True, True, True]
    assert report["content_gates_evaluated"] is False
    assert report["larger_run_or_release_authorized"] is False


def test_results_and_task_mutations_fail_closed(tmp_path: Path) -> None:
    attacked = _results()
    attacked["responses"][0]["answers"]["approve_exact_tuple"] = "no"
    path = tmp_path / "attacked.json"
    path.write_text(json.dumps(attacked), encoding="utf-8")
    with pytest.raises(ValueError, match="explicit yes"):
        importer.import_results(TASK, HTML, path)
    attacked = _results()
    attacked["responses"][0]["answers"]["entity_type"] = "organization"
    path.write_text(json.dumps(attacked), encoding="utf-8")
    with pytest.raises(ValueError, match="explicit yes"):
        importer.import_results(TASK, HTML, path)
    task_copy = tmp_path / "task.json"
    task = json.loads(TASK.read_text(encoding="utf-8"))
    task["items"][0]["display"][2]["content"]["slot_00"]["records"][0][
        "entity_type"
    ] = "organization"
    task_copy.write_text(json.dumps(task), encoding="utf-8")
    path.write_text(json.dumps(_results()), encoding="utf-8")
    with pytest.raises(ValueError, match="task/app bytes"):
        importer.import_results(task_copy, HTML, path)


def test_duplicate_result_keys_reject(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    payload = json.dumps(_results())
    path.write_text(payload[:-1] + ',"n_items":7}', encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate JSON key"):
        importer.import_results(TASK, HTML, path)


def test_real_ui_answer_shape_without_empty_notes_imports(tmp_path: Path) -> None:
    results = _results()
    for response in results["responses"]:
        response["answers"].pop("notes")
    path = tmp_path / "results.json"
    path.write_text(json.dumps(results), encoding="utf-8")
    assert importer.import_results(TASK, HTML, path)["technical_pass_count"] == 62


@pytest.mark.parametrize("timestamp", [
    "2026-08-28T10:00:00Z", "2026-08-28T10:00:00.1Z",
    "2026-08-28T10:00:00.000000+00:00", "2026-08-28T12:00:00.000+02:00",
    "2026-02-29T00:00:00.000Z",
])
def test_noncanonical_or_impossible_timestamps_reject(tmp_path: Path, timestamp: str) -> None:
    results = _results()
    results["exported_at"] = timestamp
    path = tmp_path / "results.json"
    path.write_text(json.dumps(results), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical"):
        importer.import_results(TASK, HTML, path)


def test_evaluator_cli_loads_from_repo_root() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/evaluate_candidate_endorsement_recall40_stage_b_residual.py", "--help"],
        cwd=REPO, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
