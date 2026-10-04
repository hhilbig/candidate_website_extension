from __future__ import annotations

import copy
import gzip
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.build_candidate_endorsement_line_ledger as builder
import scripts.evaluate_candidate_endorsement_recall40_round3_stage_a2 as evaluator
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    DEFAULT_MAX_OUTPUT_TOKENS,
    FROZEN_GENERATION_SEED,
    _derive_output_row,
    _run_metadata,
    generation_config_sha256,
    load_validated_stage_run,
    read_gzip_jsonl,
    schema_variant_inventory,
    validate_input_manifest,
)
from src.candidate_endorsement_line_ledger import (
    FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256,
    LINE_ADJUDICATION_STAGE,
    LINE_INVENTORY_SPEC_SHA256,
    get_frozen_recall40_round3_stage_a2_contract,
    line_adjudication_exception_ledger,
    merge_line_adjudication_ledger,
    round3_line_adjudication_exception_ledger,
    sha256_json,
    sha256_text,
)


REPO = Path(__file__).resolve().parents[1]
STAGE_A_EXPORT = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a_preflight_20260906a"
)
STAGE_A_OUTPUT = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a_outputs_20260906a"
)
STAGE_A2_EXPORT = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a2_preflight_20260906a"
)
PROMPT_TOKEN_COUNTS = [5_970, 23_058, 7_739]


def _valid_model_output(batch: dict, *, withhold_first: bool = False) -> dict:
    output = {}
    for index, target in enumerate(batch["targets"]):
        output[target["slot"]] = (
            {
                "decision": "withheld_validation",
                "final_role": None,
                "anchor_source_line_id": None,
            }
            if withhold_first and index == 0
            else {
                "decision": "not_continuation",
                "final_role": "nonrecord_content",
                "anchor_source_line_id": None,
            }
        )
    for slot in batch["padding_slots"]:
        output[slot] = {
            "decision": "padding",
            "final_role": None,
            "anchor_source_line_id": None,
        }
    return output


def _write_gzip_rows(path: Path, rows: list[dict]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(
                json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                + "\n"
            )


def _write_complete_synthetic_a2_output(output_dir: Path) -> list[dict]:
    contract = get_frozen_recall40_round3_stage_a2_contract()
    manifest, manifest_sha256, input_paths = validate_input_manifest(
        STAGE_A2_EXPORT / "manifest.json",
        STAGE_A2_EXPORT / "chunks",
        stage=LINE_ADJUDICATION_STAGE,
        contract=contract,
    )
    batches = [
        batch for path in input_paths for batch in read_gzip_jsonl(path)
    ]
    assert len(batches) == len(PROMPT_TOKEN_COUNTS) == 3
    first = batches[0]
    args = SimpleNamespace(
        stage=LINE_ADJUDICATION_STAGE,
        seed=FROZEN_GENERATION_SEED,
        max_model_len=32_768,
        max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS[
            LINE_ADJUDICATION_STAGE
        ],
        expected_input_manifest_sha256=manifest_sha256,
        upstream_stage_a_contract_sha256=first[
            "upstream_stage_a_contract_sha256"
        ],
        upstream_stage_a_input_manifest_sha256=first[
            "upstream_stage_a_input_manifest_sha256"
        ],
        upstream_stage_a_generation_config_sha256=first[
            "upstream_stage_a_generation_config_sha256"
        ],
        upstream_stage_a_output_batch_ids_sha256=first[
            "upstream_stage_a_output_batch_ids_sha256"
        ],
        upstream_stage_a_decision_ledger_sha256=first[
            "upstream_stage_a_decision_ledger_sha256"
        ],
        stage_a_provenance_sha256=first["stage_a_provenance_sha256"],
        shard_index=0,
        shard_count=1,
    )
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_sha256
    )
    assert generation_hash == (
        builder.FROZEN_RECALL40_ROUND3_STAGE_A2_GENERATION_CONFIG_SHA256
    )
    rows = []
    for batch, prompt_tokens in zip(
        batches, PROMPT_TOKEN_COUNTS, strict=True
    ):
        model_output = _valid_model_output(batch)
        rows.append(_derive_output_row(
            batch=batch,
            raw_output=json.dumps(
                model_output,
                ensure_ascii=False,
            ),
            finish_reason="stop",
            stop_reason=None,
            output_tokens=64,
            prompt_tokens=prompt_tokens,
            args=args,
            contract=contract,
            input_manifest_sha256=manifest_sha256,
            generation_hash=generation_hash,
        ))
    output_dir.mkdir(exist_ok=True)
    _write_gzip_rows(output_dir / "chunk_00000.out.jsonl.gz", rows)
    metadata = _run_metadata(
        args=args,
        contract=contract,
        generation_hash=generation_hash,
        input_manifest_sha256=manifest_sha256,
        selected=len(batches),
        selected_variant_inventory=schema_variant_inventory(batches),
        selected_stage_a_provenance_sha256=[
            first["stage_a_provenance_sha256"]
        ],
        rows=rows,
        started="2026-09-06T12:00:00+00:00",
        recovery=False,
    )
    (output_dir / "run_metadata_shard_00000.json").write_text(
        json.dumps(metadata, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert manifest["expected_stage_a2_batch_count"] == 3
    return rows


@pytest.fixture(scope="module")
def exact_round3_replay(tmp_path_factory: pytest.TempPathFactory) -> dict:
    output_dir = tmp_path_factory.mktemp("round3-a2-output")
    _write_complete_synthetic_a2_output(output_dir)
    return builder.replay_recall40_round3_stage_a_and_a2(
        builder.DEFAULT_FULL_EXPORT_DIR,
        STAGE_A_EXPORT,
        STAGE_A_OUTPUT,
        STAGE_A2_EXPORT,
        output_dir,
    )


def test_exception_ledger_semantics_preserve_historical_default() -> None:
    parent_id = "historical-default-parent"
    text = "Unclear text"
    line_id = sha256_json({
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "parent_document_id": parent_id,
        "parent_line_index": 0,
        "parent_start_char": 0,
        "parent_end_char": len(text),
        "parent_full_end_char": len(text),
        "line_text_sha256": sha256_text(text),
        "line_terminator": "",
    })
    line = {
        "source_line_id": line_id,
        "parent_line_index": 0,
        "parent_start_char": 0,
        "parent_end_char": len(text),
        "parent_full_end_char": len(text),
        "line_text": text,
        "line_text_sha256": sha256_text(text),
        "line_terminator": "",
        "model_decision_required": True,
    }
    parent = {
        "parent_document_id": parent_id,
        "candidate_icpsr": "Candidate, Alice",
        "lines": [line],
    }
    stage_a = {line_id: {
        "source_line_id": line_id,
        "role": "unclear",
        "stage_a_batch_id": "batch-a",
        "stage_a_slot": "slot_00",
    }}
    historical = line_adjudication_exception_ledger([parent], stage_a)[0]
    adjudications = {line_id: {
        "source_line_id": line_id,
        "exception_id": historical["exception_id"],
        "exception_reason": historical["exception_reason"],
        "stage_a2_batch_id": "batch-a2",
        "stage_a2_slot": "slot_00",
        "stage_a2_decision": {
            "decision": "role",
            "final_role": "nonrecord_content",
            "anchor_source_line_id": None,
        },
    }}
    merged = merge_line_adjudication_ledger(
        [parent], stage_a, adjudications
    )
    assert merged["validation"]["validation_passed"] is True
    with pytest.raises(
        ValueError, match="round-three Stage-A2 upstream contract differs"
    ):
        merge_line_adjudication_ledger(
            [parent],
            stage_a,
            adjudications,
            exception_ledger_semantics="recall40_round3",
        )
    with pytest.raises(
        ValueError, match="unknown Stage-A2 exception-ledger semantics"
    ):
        merge_line_adjudication_ledger(
            [parent],
            stage_a,
            adjudications,
            exception_ledger_semantics="caller_supplied",
        )


def test_exact_round3_three_of_three_outputs_merge_and_authorize_only_build(
    exact_round3_replay: dict,
) -> None:
    replay = exact_round3_replay
    assert len(replay["stage_a_batches"]) == 270
    assert len(replay["stage_a_decisions"]) == 4_019
    assert len(replay["stage_a2_batches"]) == 3
    assert len(replay["stage_a2_decisions"]) == 11
    round3_exceptions = round3_line_adjudication_exception_ledger(
        replay["parents"],
        replay["stage_a_decisions"],
        upstream_stage_a_contract_sha256=(
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
    )
    assert {
        line_id: row["exception_id"]
        for line_id, row in replay["stage_a2_decisions"].items()
    } == {
        row["source_line_id"]: row["exception_id"]
        for row in round3_exceptions
    }
    with pytest.raises(ValueError, match="exception identity differs"):
        merge_line_adjudication_ledger(
            replay["parents"],
            replay["stage_a_decisions"],
            replay["stage_a2_decisions"],
            upstream_stage_a_contract_sha256=(
                FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
            ),
        )
    report = builder.recall40_round3_post_a2_evaluation_report(replay)
    assert report["gate_decision"] == "GO_BUILD_BOUNDED_STAGE_B_INPUTS"
    assert report["passed_checks"] == len(report["checks"]) == 6
    assert report["failed_checks"] == 0
    assert report["bounded_stage_b_input_build_authorized"] is True
    assert report["stage_b_hive_submission_authorized"] is False
    assert report["full_expansion_or_release_authorized"] is False
    assert builder.validate_recall40_round3_post_a2_evaluation_report(
        report, replay
    ) == report


@pytest.mark.parametrize("attack", ["incomplete", "duplicate", "tampered"])
def test_round3_a2_output_replay_rejects_incomplete_duplicate_and_tampered(
    tmp_path: Path,
    exact_round3_replay: dict,
    attack: str,
) -> None:
    source_dir = tmp_path / "source"
    _write_complete_synthetic_a2_output(source_dir)
    attacked_dir = tmp_path / attack
    shutil.copytree(source_dir, attacked_dir)
    output_path = attacked_dir / "chunk_00000.out.jsonl.gz"
    rows = read_gzip_jsonl(output_path)
    if attack == "incomplete":
        rows = rows[:-1]
    elif attack == "duplicate":
        rows[1] = copy.deepcopy(rows[0])
    else:
        rows[0]["input_manifest_sha256"] = "0" * 64
    _write_gzip_rows(output_path, rows)
    with pytest.raises(ValueError, match="typed restart replay"):
        load_validated_stage_run(
            STAGE_A2_EXPORT,
            attacked_dir,
            stage=LINE_ADJUDICATION_STAGE,
            contract_override=(
                get_frozen_recall40_round3_stage_a2_contract()
            ),
        )


def test_round3_merge_withheld_and_conflict_reports_fail_closed(
    exact_round3_replay: dict,
) -> None:
    withheld_replay = copy.deepcopy(exact_round3_replay)
    adjudications = withheld_replay["stage_a2_decisions"]
    first_line_id = next(iter(adjudications))
    adjudications[first_line_id]["stage_a2_decision"] = {
        "decision": "withheld_validation",
        "final_role": None,
        "anchor_source_line_id": None,
    }
    withheld_replay["merge"] = merge_line_adjudication_ledger(
        withheld_replay["parents"],
        withheld_replay["stage_a_decisions"],
        adjudications,
        upstream_stage_a_contract_sha256=(
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
        exception_ledger_semantics="recall40_round3",
    )
    withheld_report = builder.recall40_round3_post_a2_evaluation_report(
        withheld_replay
    )
    assert withheld_report["gate_decision"] == "NO_GO"
    assert withheld_report["merge_validation"]["withheld_line_count"] == 1
    assert withheld_report["bounded_stage_b_input_build_authorized"] is False

    conflict_replay = copy.deepcopy(exact_round3_replay)
    conflict_replay["merge"]["validation"][
        "ownership_conflict_count"
    ] = 1
    conflict_replay["merge"]["validation"]["validation_passed"] = False
    conflict_report = builder.recall40_round3_post_a2_evaluation_report(
        conflict_replay
    )
    assert conflict_report["gate_decision"] == "NO_GO"
    assert conflict_report["failed_checks"] == 1
    assert conflict_report["bounded_stage_b_input_build_authorized"] is False


def test_round3_report_rejects_duplicate_keys_stub_extra_and_tamper(
    tmp_path: Path,
    exact_round3_replay: dict,
) -> None:
    report = builder.recall40_round3_post_a2_evaluation_report(
        exact_round3_replay
    )
    path = tmp_path / "report.json"
    evaluator.write_report(report, path)
    assert builder.load_recall40_round3_post_a2_evaluation_report(
        path, exact_round3_replay
    ) == report

    path.write_text('{"version":"first","version":"second"}\n')
    with pytest.raises(ValueError, match="duplicate JSON key"):
        builder.load_recall40_round3_post_a2_evaluation_report(
            path, exact_round3_replay
        )

    path.write_text(json.dumps({"version": report["version"]}))
    with pytest.raises(ValueError, match="field set differs"):
        builder.load_recall40_round3_post_a2_evaluation_report(
            path, exact_round3_replay
        )

    extra = copy.deepcopy(report)
    extra["unexpected"] = True
    path.write_text(json.dumps(extra))
    with pytest.raises(ValueError, match="field set differs"):
        builder.load_recall40_round3_post_a2_evaluation_report(
            path, exact_round3_replay
        )

    tampered = copy.deepcopy(report)
    tampered["final_decisions_sha256"] = "0" * 64
    path.write_text(json.dumps(tampered))
    with pytest.raises(ValueError, match="payload hash differs"):
        builder.load_recall40_round3_post_a2_evaluation_report(
            path, exact_round3_replay
        )

    tampered["report_payload_sha256"] = sha256_json({
        key: value
        for key, value in tampered.items()
        if key != "report_payload_sha256"
    })
    path.write_text(json.dumps(tampered))
    with pytest.raises(ValueError, match="canonical post-A2 report differs"):
        builder.load_recall40_round3_post_a2_evaluation_report(
            path, exact_round3_replay
        )


def test_round3_named_cli_exit_semantics(
    tmp_path: Path,
    exact_round3_replay: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    go_report = builder.recall40_round3_post_a2_evaluation_report(
        exact_round3_replay
    )
    monkeypatch.setattr(evaluator, "evaluate", lambda *_args, **_kwargs: go_report)
    go_path = tmp_path / "go.json"
    assert evaluator.main([
        "--stage-a2-output-dir", str(tmp_path / "unused"),
        "--report", str(go_path),
    ]) == 0
    assert json.loads(go_path.read_text())["gate_decision"] == (
        "GO_BUILD_BOUNDED_STAGE_B_INPUTS"
    )

    no_go_replay = copy.deepcopy(exact_round3_replay)
    no_go_replay["merge"]["validation"]["withheld_line_count"] = 1
    no_go_replay["merge"]["validation"]["validation_passed"] = False
    no_go_report = builder.recall40_round3_post_a2_evaluation_report(
        no_go_replay
    )
    monkeypatch.setattr(
        evaluator, "evaluate", lambda *_args, **_kwargs: no_go_report
    )
    no_go_path = tmp_path / "no-go.json"
    assert evaluator.main([
        "--stage-a2-output-dir", str(tmp_path / "unused"),
        "--report", str(no_go_path),
    ]) == 2
    assert json.loads(no_go_path.read_text())["gate_decision"] == "NO_GO"
