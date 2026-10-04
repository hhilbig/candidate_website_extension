from __future__ import annotations

import json
import copy
import shutil
import hashlib
from pathlib import Path

import pytest
import scripts.build_candidate_endorsement_line_ledger as builder
import scripts.evaluate_candidate_endorsement_line_ledger_pilot as evaluator

from scripts.build_candidate_endorsement_line_ledger import (
    DEFAULT_FULL_EXPORT_DIR,
    build_recall40_line_decision_export,
    build_recall40_round3_line_decision_export,
    build_recall40_line_adjudication_export,
    build_recall40_record_extraction_batches_after_gate,
    build_recall40_record_extraction_export,
    derive_recall40_stage_a2_exception_inventory,
    read_jsonl_gz,
    write_batch_chunks,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    reject_stale_output_temporaries,
    load_validated_stage_run,
    validate_expected_generation_config_sha256,
    validate_input_manifest,
    schema_variant_inventory,
)
from src.candidate_endorsement_line_ledger import (
    LINE_DECISION_STAGE, computed_recall40_stage_a2_contract,
    computed_recall40_round3_stage_a_contract,
    computed_recall40_stage_b_contract,
    computed_recall40_stage_b_residual_contract,
    get_frozen_recall40_stage_b_contract,
    RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256,
    align_record_extraction_output,
    validate_record_extraction_output,
    get_line_ledger_contract,
    validate_round3_stage_a_context_batch,
)
from src.candidate_endorsement_line_ledger import sha256_json
from src.candidate_endorsement_line_ledger import (
    line_adjudication_exception_identity_ledger,
)
from scripts.evaluate_candidate_endorsement_line_ledger_pilot import (
    evaluate_recall40_stage_a_for_a2,
    evaluate_recall40_stage_a2_for_stage_b,
)

REPO = Path(__file__).resolve().parents[1]
PILOT = REPO / "build/private_candidate_endorsement_40_parent_selection_20260827/pilot40_selection.json"
ROUND2_SELECTION = (
    REPO / "build/private_candidate_endorsement_recall40_round2_selection_20260901/selection.json"
)


def test_private_pilot40_build_replays_and_never_loads_reserve(tmp_path: Path) -> None:
    output = tmp_path / "export"
    manifest = build_recall40_line_decision_export(DEFAULT_FULL_EXPORT_DIR, PILOT, output)
    assert manifest["private_recall40"]["reserve80_loaded"] is False
    assert manifest["private_recall40"]["selected_parent_count"] == 40
    assert manifest["expected_stage_batch_count"] == 85
    loaded, _, chunks = validate_input_manifest(
        output / "manifest.json", output / "chunks",
        stage=LINE_DECISION_STAGE, contract=get_line_ledger_contract(),
    )
    assert loaded == manifest
    assert len(chunks) == 2

    attacked = json.loads(PILOT.read_text())
    attacked["reserve_components"] = []
    attacked_path = tmp_path / "attacked.json"
    attacked_path.write_text(json.dumps(attacked, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(ValueError, match="private recall40 selection bytes"):
        build_recall40_line_decision_export(DEFAULT_FULL_EXPORT_DIR, attacked_path, tmp_path / "bad")


def test_round3_stage_a_frozen_build_reuses_sample_and_full_parent_context(
    tmp_path: Path,
) -> None:
    output = tmp_path / "round3"
    manifest = build_recall40_round3_line_decision_export(
        DEFAULT_FULL_EXPORT_DIR, ROUND2_SELECTION, output,
    )
    contract = computed_recall40_round3_stage_a_contract()
    assert manifest["mode"] == "line_decision_recall40_round3"
    assert manifest["contract_is_frozen"] is True
    assert manifest["frozen_line_ledger_contract_sha256"] == contract[
        "line_ledger_contract_sha256"
    ]
    assert manifest["line_ledger_contract"] == contract
    assert manifest["expected_stage_batch_count"] == 270
    assert manifest["private_recall40_round3"]["selected_parent_count"] == 40
    assert manifest["private_recall40_round3"]["hive_submission_authorized"] is False
    validated, _, validated_chunks = validate_input_manifest(
        output / "manifest.json", output / "chunks",
        stage=LINE_DECISION_STAGE,
        contract=contract,
    )
    assert validated == manifest
    assert len(validated_chunks) == 5
    batches = [
        row for entry in manifest["files"]["chunks"]
        for row in read_jsonl_gz(output / "chunks" / entry["file"])
    ]
    assert len(batches) == 270
    for batch in batches:
        validate_round3_stage_a_context_batch(batch)
    by_parent: dict[str, list[dict]] = {}
    for batch in batches:
        by_parent.setdefault(batch["parent_document_id"], []).append(batch)
    assert len(by_parent) == 40
    for parent_batches in by_parent.values():
        context_digests = {
            batch["full_parent_physical_lines_sha256"]
            for batch in parent_batches
        }
        assert len(context_digests) == 1


def test_round3_stage_a_runner_rejects_rehashed_parent_context_tamper(
    tmp_path: Path,
) -> None:
    output = tmp_path / "round3"
    manifest = build_recall40_round3_line_decision_export(
        DEFAULT_FULL_EXPORT_DIR, ROUND2_SELECTION, output,
    )
    chunk_entry = manifest["files"]["chunks"][0]
    chunk_path = output / "chunks" / chunk_entry["file"]
    rows = list(read_jsonl_gz(chunk_path))
    rows[0]["visible_lines"][0]["line_text"] = "INJECTED CONTEXT"
    builder.write_jsonl_gz(chunk_path, rows)
    chunk_entry["content_sha256"] = sha256_json(rows)
    chunk_entry["bytes"] = chunk_path.stat().st_size
    chunk_entry["sha256"] = hashlib.sha256(chunk_path.read_bytes()).hexdigest()
    payload = dict(manifest)
    payload.pop("manifest_payload_sha256")
    manifest["manifest_payload_sha256"] = sha256_json(payload)
    (output / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
    )
    with pytest.raises(ValueError, match="catalog digest"):
        validate_input_manifest(
            output / "manifest.json", output / "chunks",
            stage=LINE_DECISION_STAGE,
            contract=computed_recall40_round3_stage_a_contract(),
        )


def test_round3_stage_a_runner_rejects_closed_world_and_duplicate_target_attacks(
    tmp_path: Path,
) -> None:
    contract = computed_recall40_round3_stage_a_contract()

    extra = tmp_path / "extra"
    extra_manifest = build_recall40_round3_line_decision_export(
        DEFAULT_FULL_EXPORT_DIR, ROUND2_SELECTION, extra,
    )
    extra_manifest["uncontracted_field"] = "injected"
    extra_payload = dict(extra_manifest)
    extra_payload.pop("manifest_payload_sha256")
    extra_manifest["manifest_payload_sha256"] = sha256_json(extra_payload)
    (extra / "manifest.json").write_text(
        json.dumps(extra_manifest, sort_keys=True, separators=(",", ":")) + "\n"
    )
    with pytest.raises(ValueError, match="closed inventory"):
        validate_input_manifest(
            extra / "manifest.json", extra / "chunks",
            stage=LINE_DECISION_STAGE, contract=contract,
        )

    duplicate = tmp_path / "duplicate"
    manifest = build_recall40_round3_line_decision_export(
        DEFAULT_FULL_EXPORT_DIR, ROUND2_SELECTION, duplicate,
    )
    chunk_entry = manifest["files"]["chunks"][0]
    chunk_path = duplicate / "chunks" / chunk_entry["file"]
    rows = list(read_jsonl_gz(chunk_path))
    batch = rows[0]
    copied_target = copy.deepcopy(batch["targets"][0])
    copied_target["slot"] = batch["targets"][1]["slot"]
    batch["targets"][1] = copied_target
    identity = {
        "stage": LINE_DECISION_STAGE,
        "contract": contract["line_ledger_contract_sha256"],
        "parent_document_id": batch["parent_document_id"],
        "batch_index": batch["batch_index_within_parent"],
        "source_line_ids": [
            target["source_line_id"] for target in batch["targets"]
        ],
        "stage_a_context_profile_sha256": batch[
            "stage_a_context_profile_sha256"
        ],
        "full_parent_physical_lines_sha256": batch[
            "full_parent_physical_lines_sha256"
        ],
    }
    batch_id = sha256_json(identity)
    batch["batch_id"] = batch_id
    batch["document_id"] = batch_id
    builder.write_jsonl_gz(chunk_path, rows)
    chunk_entry["content_sha256"] = sha256_json(rows)
    chunk_entry["bytes"] = chunk_path.stat().st_size
    chunk_entry["sha256"] = hashlib.sha256(chunk_path.read_bytes()).hexdigest()
    chunk_entry["first_document_id"] = batch_id
    manifest["expected_batch_ids"][0] = batch_id
    manifest["expected_batch_ids_sha256"] = sha256_json(
        manifest["expected_batch_ids"]
    )
    duplicate_payload = dict(manifest)
    duplicate_payload.pop("manifest_payload_sha256")
    manifest["manifest_payload_sha256"] = sha256_json(duplicate_payload)
    (duplicate / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
    )
    with pytest.raises(ValueError, match="target source-line ID is duplicated"):
        validate_input_manifest(
            duplicate / "manifest.json", duplicate / "chunks",
            stage=LINE_DECISION_STAGE, contract=contract,
        )


@pytest.mark.parametrize("attack", (
    "reserve", "mode", "parent", "counts", "chunk_size", "source",
    "selection_rows", "selection_columns", "selection_party",
))
def test_runner_rejects_coordinated_private_manifest_attacks(
    tmp_path: Path, attack: str,
) -> None:
    source = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_preflight_20260827c"
    target = tmp_path / attack
    shutil.copytree(source, target)
    manifest_path = target / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if attack == "reserve":
        manifest["private_recall40"]["reserve80_loaded"] = True
    elif attack == "mode":
        manifest["mode"] = "line_decision_full"
    elif attack == "parent":
        manifest["selected_parent_ids"][0] = "a" * 64
        digest = sha256_json(manifest["selected_parent_ids"])
        manifest["selected_parent_ids_sha256"] = digest
        manifest["private_recall40"]["selected_parent_ids_sha256"] = digest
    elif attack == "counts":
        manifest["expected_batch_counts"]["record_extraction_batches_maximum"] = 159
    elif attack == "chunk_size":
        manifest["chunk_size"] = 61
    elif attack == "source":
        manifest["source_export"]["source_export_manifest_sha256"] = "a" * 64
    elif attack == "selection_rows":
        manifest["files"]["selection"]["rows"] = 41
    elif attack == "selection_columns":
        manifest["files"]["selection"]["columns"][0] = "forged_group"
    else:
        selection_path = target / "selection.csv"
        text = selection_path.read_text()
        selection_path.write_text(text.replace(",democrat,", ",republican,", 1))
        manifest["files"]["selection"]["sha256"] = hashlib.sha256(
            selection_path.read_bytes()
        ).hexdigest()
        manifest["files"]["selection"]["bytes"] = selection_path.stat().st_size
    payload = dict(manifest)
    payload.pop("manifest_payload_sha256")
    manifest["manifest_payload_sha256"] = sha256_json(payload)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(ValueError):
        validate_input_manifest(
            manifest_path, target / "chunks", stage=LINE_DECISION_STAGE,
            contract=get_line_ledger_contract(),
        )


def test_external_generation_and_stale_temporary_gates(tmp_path: Path) -> None:
    expected = "7c89e227eb81f372125be51660d7d2adec406d3250c2f4df4e7c3a923e7c6ede"
    validate_expected_generation_config_sha256(expected, expected)
    with pytest.raises(ValueError, match="external expectation"):
        validate_expected_generation_config_sha256(expected, "a" * 64)
    (tmp_path / "chunk_00000.out.jsonl.gz.tmp-123").write_bytes(b"partial")
    with pytest.raises(ValueError, match="stale temporary"):
        reject_stale_output_temporaries(tmp_path)


def test_recall40_a2_inventory_refuses_absent_real_outputs(tmp_path: Path) -> None:
    source = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_preflight_20260827d"
    with pytest.raises(FileNotFoundError, match="run metadata is absent"):
        derive_recall40_stage_a2_exception_inventory(
            DEFAULT_FULL_EXPORT_DIR, source, tmp_path / "no_outputs",
        )
    with pytest.raises(FileNotFoundError, match="run metadata is absent"):
        evaluate_recall40_stage_a_for_a2(
            source, tmp_path / "no_outputs", DEFAULT_FULL_EXPORT_DIR,
        )
    destination = tmp_path / "a2"
    with pytest.raises(FileNotFoundError, match="run metadata is absent"):
        build_recall40_line_adjudication_export(
            DEFAULT_FULL_EXPORT_DIR, source, tmp_path / "no_outputs", destination,
        )
    assert not destination.exists()


def test_recall40_a2_contract_is_distinct_open_scope() -> None:
    upstream = get_line_ledger_contract()["line_ledger_contract_sha256"]
    contract = computed_recall40_stage_a2_contract(upstream)
    assert contract["line_ledger_contract_sha256"] != upstream
    assert contract["upstream_stage_a_v2_contract_sha256"] == upstream
    assert contract["contract_version"] == "candidate_endorsement_recall40_stage_a2_v1"


def test_unfrozen_review_flag_rejects_non_a2_modes() -> None:
    source = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_preflight_20260827d"
    with pytest.raises(ValueError, match="restricted to exact recall40 stages"):
        validate_input_manifest(
            source / "manifest.json", source / "chunks",
            stage=LINE_DECISION_STAGE, contract=get_line_ledger_contract(),
            allow_unfrozen_review=True,
        )


def test_default_loader_auto_routes_historical_stage_a2_contract() -> None:
    export = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_a2_preflight_20260825f"
    outputs = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_a2_preflight_hive_output_20260825f_21447139"
    if not export.is_dir() or not outputs.is_dir():
        pytest.skip("historical suffix-f replay bytes unavailable")
    manifest, batches, rows, artifacts = load_validated_stage_run(
        export, outputs, stage="line_adjudication",
    )
    assert manifest["line_ledger_contract_sha256"] == (
        "d8072b6269438b1742c47f1a25ff271edab32add331a18db9898d439f9b25bb0"
    )
    assert len(batches) == len(rows) == artifacts["validated_output_batches"]


def test_manifest_rejects_payload_rehashed_extra_selection_column(
    tmp_path: Path,
) -> None:
    from src.candidate_endorsement_line_ledger import get_frozen_stage_a2_v4_contract
    source = REPO / "build/hive_candidate_extraction/v2_1_endorsement_line_ledger_stage_a2_preflight_20260825f"
    if not source.is_dir():
        pytest.skip("historical suffix-f input unavailable")
    target = tmp_path / "a2"
    shutil.copytree(source, target)
    selection_path = target / "selection.csv"
    lines = selection_path.read_text(encoding="utf-8").splitlines()
    selection_path.write_text("\n".join([lines[0] + ",junk"] + [line + ",x" for line in lines[1:]]) + "\n")
    manifest_path = target / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"]["selection"]["sha256"] = hashlib.sha256(
        selection_path.read_bytes()
    ).hexdigest()
    manifest["files"]["selection"]["bytes"] = selection_path.stat().st_size
    payload = dict(manifest)
    payload.pop("manifest_payload_sha256")
    manifest["manifest_payload_sha256"] = sha256_json(payload)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(ValueError, match="declared schema/count"):
        validate_input_manifest(
            manifest_path, target / "chunks", stage="line_adjudication",
            contract=get_frozen_stage_a2_v4_contract(),
        )


def test_real_stage_a40_evaluator_derives_exact_a2_inventory() -> None:
    source = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_preflight_20260827d"
    outputs = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_outputs_20260827d"
    if not outputs.is_dir():
        pytest.skip("job 21479441 output shards unavailable")
    report = evaluate_recall40_stage_a_for_a2(
        source, outputs, DEFAULT_FULL_EXPORT_DIR,
    )
    assert report["gate_decision"] == "GO_BUILD_FROZEN_A2_INPUTS"
    inventory = report["stage_a2_exception_inventory"]
    assert inventory["exception_count"] == 607
    assert inventory["exception_reason_counts"] == {
        "stage_a_unclear": 586,
        "stage_a_unowned_continuation": 21,
    }


def test_real_stage_a40_builds_frozen_non_submittable_a2_inputs(
    tmp_path: Path,
) -> None:
    source = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_preflight_20260827d"
    outputs = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_outputs_20260827d"
    if not outputs.is_dir():
        pytest.skip("job 21479441 output shards unavailable")
    destination = tmp_path / "a2"
    manifest = build_recall40_line_adjudication_export(
        DEFAULT_FULL_EXPORT_DIR, source, outputs, destination,
    )
    assert manifest["contract_is_frozen"] is True
    assert manifest["private_recall40_stage_a2"]["hive_submission_authorized"] is False
    assert manifest["expected_stage_a2_batch_count"] == 96
    assert manifest["stage_a_exception_count"] == 607
    contract = computed_recall40_stage_a2_contract(
        get_line_ledger_contract()["line_ledger_contract_sha256"]
    )
    validated, _, paths = validate_input_manifest(
        destination / "manifest.json", destination / "chunks",
        stage="line_adjudication", contract=contract,
    )
    assert validated == manifest
    assert [entry["records"] for entry in manifest["files"]["chunks"]] == [60, 36]
    assert len(paths) == 2


def test_recall40_post_a2_gate_refuses_absent_outputs(tmp_path: Path) -> None:
    stage_a = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_preflight_20260827d"
    stage_a_outputs = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_outputs_20260827d"
    stage_a2 = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a2_preflight_20260827a"
    with pytest.raises(FileNotFoundError, match="run metadata is absent"):
        evaluate_recall40_stage_a2_for_stage_b(
            stage_a,
            stage_a_outputs,
            stage_a2,
            tmp_path / "no_a2_outputs",
            DEFAULT_FULL_EXPORT_DIR,
        )


def test_recall40_stage_b_batch_shell_requires_exact_go_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation = {
        "expected_exception_count": 607,
        "adjudicated_exception_count": 607,
        "unresolved_line_count": 0,
        "withheld_line_count": 0,
        "unowned_continuation_count": 0,
        "duplicate_continuation_owner_count": 0,
        "ownership_conflict_count": 0,
        "coverage_complete": True,
        "validation_passed": True,
    }
    replay = {
        "stage_a_provenance": {
            "stage_a_input_manifest_sha256": "a" * 64,
            "stage_a_generation_config_sha256": "b" * 64,
        },
        "stage_a2_artifacts": {
            "input_manifest_sha256": "c" * 64,
            "generation_config_sha256": "d" * 64,
            "output_batch_ids_sha256": "e" * 64,
        },
        "merge": {
            "validation": validation,
            "final_decisions_sha256": "f" * 64,
            "per_line_provenance_sha256": "0" * 64,
            "final_decisions": {str(index): {} for index in range(1115)},
        },
        "parents": [{"parent_document_id": str(index)} for index in range(40)],
        "stage_a_decisions": {str(index): {} for index in range(1115)},
        "stage_a2_decisions": {str(index): {} for index in range(607)},
    }
    monkeypatch.setattr(builder, "replay_recall40_stage_a_and_a2", lambda *_: replay)
    monkeypatch.setattr(
        builder,
        "build_record_extraction_batches_from_adjudicated_merge",
        lambda *_, **__: [{"batch_id": "batch"}],
    )
    report = builder.recall40_post_a2_evaluation_report(replay)
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    batches, authorization = build_recall40_record_extraction_batches_after_gate(
        tmp_path, tmp_path, tmp_path, tmp_path, tmp_path, path,
    )
    assert batches == [{"batch_id": "batch"}]
    assert authorization["stage_b_hive_submission_authorized"] is False
    report["merge_validation"]["withheld_line_count"] = 1
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical A2 evaluation"):
        build_recall40_record_extraction_batches_after_gate(
            tmp_path, tmp_path, tmp_path, tmp_path, tmp_path, path,
        )
    bad_replay = copy.deepcopy(replay)
    bad_replay["merge"]["validation"]["withheld_line_count"] = 1
    forged = builder.recall40_post_a2_evaluation_report(bad_replay)
    path.write_text(json.dumps(forged), encoding="utf-8")
    monkeypatch.setattr(
        builder, "replay_recall40_stage_a_and_a2", lambda *_: bad_replay
    )
    with pytest.raises(ValueError, match="does not authorize"):
        build_recall40_record_extraction_batches_after_gate(
            tmp_path, tmp_path, tmp_path, tmp_path, tmp_path, path,
        )
    monkeypatch.setattr(builder, "replay_recall40_stage_a_and_a2", lambda *_: replay)
    canonical = json.dumps(builder.recall40_post_a2_evaluation_report(replay))
    path.write_text('{"gate_decision":"NO_GO",' + canonical[1:], encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate JSON key"):
        build_recall40_record_extraction_batches_after_gate(
            tmp_path, tmp_path, tmp_path, tmp_path, tmp_path, path,
        )


def test_recall40_named_go_cli_exits_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        evaluator,
        "evaluate_recall40_stage_a2_for_stage_b",
        lambda *_: {
            "gate_decision": "GO_BUILD_BOUNDED_STAGE_B_INPUTS",
            "passed_checks": 5,
            "failed_checks": 0,
        },
    )
    report = tmp_path / "report.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "evaluate",
            "--evaluation-mode", "recall40-stage-a2-for-stage-b",
            "--stage-a-export-dir", str(tmp_path / "a"),
            "--stage-a-output-dir", str(tmp_path / "ao"),
            "--stage-a2-export-dir", str(tmp_path / "a2"),
            "--stage-a2-output-dir", str(tmp_path / "a2o"),
            "--output-report", str(report),
        ],
    )
    assert evaluator.main() == 0
    assert json.loads(report.read_text())["gate_decision"] == (
        "GO_BUILD_BOUNDED_STAGE_B_INPUTS"
    )


def test_real_recall40_stage_b_frozen_artifact_is_bounded_and_valid(
    tmp_path: Path,
) -> None:
    stage_a = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_preflight_20260827d"
    stage_a_outputs = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_outputs_20260827d"
    stage_a2 = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a2_preflight_20260827a"
    stage_a2_outputs = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a2_outputs_20260827a"
    report = REPO / "quality_reports/candidate_endorsement_40_parent_stage_a2_21484617_2026-08-27.json"
    if not stage_a2_outputs.is_dir():
        pytest.skip("job 21484617 output shards unavailable")
    output = tmp_path / "stage_b"
    manifest = build_recall40_record_extraction_export(
        DEFAULT_FULL_EXPORT_DIR, stage_a, stage_a_outputs, stage_a2,
        stage_a2_outputs, report, output,
    )
    assert manifest["contract_is_frozen"] is True
    assert manifest["stage_b_upstream_pass_count"] == 2
    assert manifest["private_recall40_stage_b"]["hive_submission_authorized"] is False
    assert manifest["exported_records"] == 62
    validated, _, chunks = validate_input_manifest(
        output / "manifest.json", output / "chunks",
        stage="record_extraction", contract=get_frozen_recall40_stage_b_contract(),
    )
    assert validated == manifest
    assert [entry["records"] for entry in manifest["files"]["chunks"]] == [60, 2]
    assert len(chunks) == 2


def test_recall40_stage_b_failed_audit_replays_exact_technical_census() -> None:
    output = REPO / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_40_parent_stage_b_outputs_20260827b"
    )
    audit_path = REPO / (
        "quality_reports/"
        "candidate_endorsement_40_parent_stage_b_failed_21576064_2026-08-28.json"
    )
    if not output.is_dir():
        pytest.skip("job 21576064 output shards unavailable")
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert sorted(path.name for path in output.iterdir()) == sorted(
        entry["file"] for entry in audit["output_inventory"]
    )
    for entry in audit["output_inventory"]:
        path = output / entry["file"]
        assert path.stat().st_size == entry["bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
    rows = [
        row
        for path in sorted(output.glob("chunk_*.out.jsonl.gz"))
        for row in read_jsonl_gz(path)
    ]
    census = audit["authenticated_census"]
    assert len(rows) == census["output_rows"] == 62
    assert sum(row["technical_pass"] is True for row in rows) == 55
    failed = [row for row in rows if row["technical_pass"] is False]
    assert len(failed) == census["technical_fail"] == 7
    assert sum(row["parse_status"] == "invalid_stage_output" for row in failed) == 6
    assert sum(row["parse_status"] == "invalid_stage_alignment" for row in failed) == 1
    errors = [
        error
        for row in failed
        for error in row["stage_validation"].get("errors", [])
    ]
    assert sum(error.endswith("name_not_exact_in_block") for error in errors) == 8
    valid = [row for row in rows if row["technical_pass"] is True]
    assert sum(row["stage_alignment"]["aligned_record_count"] for row in valid) == 331
    assert sum(row["stage_alignment"]["withheld_record_count"] for row in valid) == 0


def test_recall40_residual_projection_repairs_exact_direct_support_context_only() -> None:
    export = REPO / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_40_parent_stage_b_preflight_20260827a"
    )
    output = REPO / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_40_parent_stage_b_outputs_20260827b"
    )
    if not output.is_dir():
        pytest.skip("job 21576064 output shards unavailable")
    batches = {
        row["batch_id"]: row
        for path in sorted((export / "chunks").glob("*.jsonl.gz"))
        for row in read_jsonl_gz(path)
    }
    rows = {
        row["batch_id"]: row
        for path in sorted(output.glob("chunk_*.out.jsonl.gz"))
        for row in read_jsonl_gz(path)
    }
    batch_id = "b2124cddaa9eae746d4965f18c20b450445b0ed896f9d9f6a4ff4895d52da8de"
    batch = batches[batch_id]
    model_output = rows[batch_id]["model_output"]
    historical = align_record_extraction_output(
        batch, model_output,
        line_ledger_contract_sha256=get_frozen_recall40_stage_b_contract()[
            "line_ledger_contract_sha256"
        ],
    )
    assert historical["public_coverage_complete"] is False
    residual_contract = computed_recall40_stage_b_residual_contract()
    repaired = align_record_extraction_output(
        batch, model_output,
        line_ledger_contract_sha256=residual_contract[
            "line_ledger_contract_sha256"
        ],
        residual_projection_spec_sha256=(
            RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256
        ),
    )
    assert repaired["public_coverage_complete"] is True
    assert repaired["withheld_record_count"] == 0
    item = repaired["aligned_records"][0]
    assert item["support_mode"] == "explicit_text"
    assert item["endorsement_context_evidence"] == item["record_evidence"]
    assert item["endorsement_context_evidence_parent_start_char"] == item[
        "record_evidence_parent_start_char"
    ]
    assert item["endorsement_context_evidence_parent_end_char"] == item[
        "record_evidence_parent_end_char"
    ]
    with pytest.raises(ValueError, match="unknown record-extraction"):
        align_record_extraction_output(
            batch, model_output,
            residual_projection_spec_sha256="0" * 64,
        )


def test_recall40_residual_name_boundaries_are_narrow_and_entity_typed() -> None:
    export = REPO / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_40_parent_stage_b_preflight_20260827a"
    )
    output = REPO / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_40_parent_stage_b_outputs_20260827b"
    )
    if not output.is_dir():
        pytest.skip("job 21576064 output shards unavailable")
    batches = {
        row["batch_id"]: row
        for path in sorted((export / "chunks").glob("*.jsonl.gz"))
        for row in read_jsonl_gz(path)
    }
    rows = {
        row["batch_id"]: row
        for path in sorted(output.glob("chunk_*.out.jsonl.gz"))
        for row in read_jsonl_gz(path)
    }
    spec = RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256
    relationship_id = next(key for key in rows if key.startswith("7959"))
    assert validate_record_extraction_output(
        batches[relationship_id], rows[relationship_id]["model_output"],
        residual_projection_spec_sha256=spec,
    )["schema_valid"]
    truncated = copy.deepcopy(rows[relationship_id]["model_output"])
    truncated["slot_01"]["records"][0]["endorser_name"] = "Bolton"
    assert not validate_record_extraction_output(
        batches[relationship_id], truncated,
        residual_projection_spec_sha256=spec,
    )["schema_valid"]
    absorbed_candidate = copy.deepcopy(rows[relationship_id]["model_output"])
    absorbed_candidate["slot_01"]["records"][0]["endorser_name"] = (
        "Ambassador Bolton on Rep. Don Bacon"
    )
    assert not validate_record_extraction_output(
        batches[relationship_id], absorbed_candidate,
        residual_projection_spec_sha256=spec,
    )["schema_valid"]
    for prefix in ("cc97", "bbc8", "4339"):
        batch_id = next(key for key in rows if key.startswith(prefix))
        assert validate_record_extraction_output(
            batches[batch_id], rows[batch_id]["model_output"],
            residual_projection_spec_sha256=spec,
        )["schema_valid"]
    absorbed_status_cases = (
        ("cc97", "slot_01"),
        ("bbc8", "slot_03"),
        ("4339", "slot_00"),
        ("4339", "slot_07"),
    )
    for prefix, slot in absorbed_status_cases:
        batch_id = next(key for key in rows if key.startswith(prefix))
        attacked = copy.deepcopy(rows[batch_id]["model_output"])
        target = next(
            target for target in batches[batch_id]["targets"]
            if target["slot"] == slot
        )
        attacked[slot]["records"][0]["endorser_name"] = target["line_text"]
        assert not validate_record_extraction_output(
            batches[batch_id], attacked,
            residual_projection_spec_sha256=spec,
        )["schema_valid"]
    compound_id = next(key for key in rows if key.startswith("8840"))
    corrected_compound = copy.deepcopy(rows[compound_id]["model_output"])
    corrected_compound["slot_05"]["records"][0].update({
        "endorser_name": "Ryan Zinke", "title": "Secretary",
    })
    assert validate_record_extraction_output(
        batches[compound_id], corrected_compound,
        residual_projection_spec_sha256=spec,
    )["schema_valid"]
    missing_title = copy.deepcopy(corrected_compound)
    missing_title["slot_05"]["records"][0]["title"] = None
    assert not validate_record_extraction_output(
        batches[compound_id], missing_title,
        residual_projection_spec_sha256=spec,
    )["schema_valid"]


def test_recall40_a2_rejects_coherent_parent_exception_omission(
    tmp_path: Path,
) -> None:
    source = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_preflight_20260827d"
    outputs = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_a_outputs_20260827d"
    if not outputs.is_dir():
        pytest.skip("job 21479441 output shards unavailable")
    destination = tmp_path / "a2"
    manifest = build_recall40_line_adjudication_export(
        DEFAULT_FULL_EXPORT_DIR, source, outputs, destination,
    )
    batches = [
        row for path in sorted((destination / "chunks").glob("*.jsonl.gz"))
        for row in read_jsonl_gz(path)
    ]
    omitted_parent = next(str(batch["parent_document_id"]) for batch in batches)
    attacked_batches = [
        batch for batch in batches
        if str(batch["parent_document_id"]) != omitted_parent
    ]
    shutil.rmtree(destination / "chunks")
    chunk_entries = write_batch_chunks(destination / "chunks", attacked_batches, 60)
    attacked = copy.deepcopy(manifest)
    attacked["files"]["chunks"] = chunk_entries
    attacked["exported_records"] = len(attacked_batches)
    attacked["expected_stage_batch_count"] = len(attacked_batches)
    attacked["expected_stage_a2_batch_count"] = len(attacked_batches)
    attacked["expected_batch_ids"] = [batch["batch_id"] for batch in attacked_batches]
    attacked["expected_batch_ids_sha256"] = sha256_json(attacked["expected_batch_ids"])
    attacked.update(schema_variant_inventory(attacked_batches))
    exceptions = [
        {key: value for key, value in target.items()
         if key not in {"slot", "stage_a_provenance_locator"}}
        for batch in attacked_batches for target in batch["targets"]
    ]
    source_ids = [str(row["source_line_id"]) for row in exceptions]
    exception_ids = [str(row["exception_id"]) for row in exceptions]
    reasons: dict[str, int] = {}
    for row in exceptions:
        reason = str(row["exception_reason"])
        reasons[reason] = reasons.get(reason, 0) + 1
    identities = line_adjudication_exception_identity_ledger(attacked_batches)
    attacked["stage_a_exception_count"] = len(exceptions)
    attacked["stage_a_exception_ledger_sha256"] = sha256_json(exceptions)
    attacked["stage_a_exception_source_line_ids_sha256"] = sha256_json(source_ids)
    attacked["stage_a_exception_reason_counts"] = dict(sorted(reasons.items()))
    attacked["stage_a_exception_identity_ledger"] = identities
    attacked["stage_a_exception_identity_ledger_sha256"] = sha256_json(identities)
    attacked["exception_ledger"] = {
        "count": len(exceptions),
        "ordered_exception_ids_sha256": sha256_json(exception_ids),
        "ordered_source_line_ids_sha256": sha256_json(source_ids),
        "reason_counts": dict(sorted(reasons.items())),
    }
    attacked["private_recall40_stage_a2"]["exception_count"] = len(exceptions)
    attacked["private_recall40_stage_a2"]["exception_reason_counts"] = dict(sorted(reasons.items()))
    payload = dict(attacked)
    payload.pop("manifest_payload_sha256")
    attacked["manifest_payload_sha256"] = sha256_json(payload)
    (destination / "manifest.json").write_text(
        json.dumps(attacked, sort_keys=True, separators=(",", ":")) + "\n"
    )
    contract = computed_recall40_stage_a2_contract(
        get_line_ledger_contract()["line_ledger_contract_sha256"]
    )
    with pytest.raises(ValueError, match="frozen exception closure"):
        validate_input_manifest(
            destination / "manifest.json", destination / "chunks",
            stage="line_adjudication", contract=contract,
        )


def test_round2_open_stage_a_uses_revised_scope_and_private_selection(tmp_path: Path):
    from scripts.build_candidate_endorsement_line_ledger import (
        build_recall40_round2_line_decision_export,
    )
    source = REPO / "build/hive_candidate_extraction/v2_1_full_export_20260824p"
    selection = REPO / "build/private_candidate_endorsement_recall40_round2_selection_20260901/selection.json"
    manifest = build_recall40_round2_line_decision_export(
        source, selection, tmp_path / "round2"
    )
    assert manifest["mode"] == "line_decision_recall40_round2"
    assert manifest["contract_is_frozen"] is True
    assert manifest["private_recall40_round2"]["hive_submission_authorized"] is False
    assert manifest["inventory_census"]["model_decision_lines"] == 4019
    assert manifest["exported_records"] == 270
