from __future__ import annotations

import copy
import csv
import gzip
import json
import shutil
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest

from scripts.build_candidate_endorsement_line_ledger import (
    DEFAULT_FULL_EXPORT_DIR,
    build_recall40_round3_record_extraction_export,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    DEFAULT_MAX_OUTPUT_TOKENS,
    FROZEN_GENERATION_SEED,
    _derive_output_row,
    _expected_prompt_token_counts_sha256,
    _run_metadata,
    file_sha256,
    generation_config_payload,
    generation_config_sha256,
    line_ledger_chunk_is_complete,
    load_validated_stage_run,
    read_gzip_jsonl,
    schema_variant_inventory,
    validate_input_manifest,
)
from src.candidate_endorsement_line_ledger import (
    FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256,
    RECORD_EXTRACTION_STAGE,
    RECALL40_ROUND3_RECORD_EXTRACTION_PROMPT,
    RECALL40_ROUND3_STAGE_B_SCOPE_SPEC,
    RECALL40_ROUND3_STAGE_B_SCOPE_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_SEMANTIC_SPEC_SHA256,
    align_record_extraction_output,
    computed_recall40_round3_stage_b_contract,
    computed_recall40_stage_b_contract,
    get_frozen_recall40_round2_stage_b_contract,
    get_frozen_recall40_round3_stage_b_contract,
    get_line_ledger_contract,
    prompt_for_contract_stage,
    schema_binding_for_batch,
    sha256_json,
    sha256_text,
    validate_record_extraction_output,
)


REPO = Path(__file__).resolve().parents[1]
ROUND3_STAGE_A_EXPORT = (
    REPO / "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a_preflight_20260906a"
)
ROUND3_STAGE_A_OUTPUT = (
    REPO / "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a_outputs_20260906a"
)
ROUND3_STAGE_A2_EXPORT = (
    REPO / "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a2_preflight_20260906a"
)
ROUND3_STAGE_A2_OUTPUT = (
    REPO / "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a2_outputs_20260906a"
)
ROUND3_STAGE_A2_REPORT = (
    REPO / "quality_reports/"
    "candidate_endorsement_recall40_round3_stage_a2_22684044_2026-09-06.json"
)


@pytest.fixture(scope="module")
def open_artifact(tmp_path_factory: pytest.TempPathFactory) -> Path:
    destination = tmp_path_factory.mktemp("round3-stage-b") / "open-artifact"
    build_recall40_round3_record_extraction_export(
        DEFAULT_FULL_EXPORT_DIR,
        ROUND3_STAGE_A_EXPORT,
        ROUND3_STAGE_A_OUTPUT,
        ROUND3_STAGE_A2_EXPORT,
        ROUND3_STAGE_A2_OUTPUT,
        ROUND3_STAGE_A2_REPORT,
        destination,
    )
    return destination


def _load_manifest(root: Path) -> dict[str, Any]:
    return json.loads((root / "manifest.json").read_text(encoding="utf-8"))


def _write_manifest(root: Path, manifest: dict[str, Any]) -> None:
    manifest.pop("manifest_payload_sha256", None)
    manifest["manifest_payload_sha256"] = sha256_json(manifest)
    (root / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_gzip(path: Path, rows: list[dict[str, Any]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _copy_artifact(source: Path, tmp_path: Path) -> Path:
    destination = tmp_path / "attack"
    shutil.copytree(source, destination)
    return destination


def _chunk_rows(root: Path) -> list[list[dict[str, Any]]]:
    return [
        read_gzip_jsonl(path)
        for path in sorted((root / "chunks").glob("*.jsonl.gz"))
    ]


def _rewrite_chunks(
    root: Path,
    manifest: dict[str, Any],
    chunks: list[list[dict[str, Any]]],
) -> None:
    entries: list[dict[str, Any]] = []
    for index, rows in enumerate(chunks):
        path = root / "chunks" / f"chunk_{index:05d}.jsonl.gz"
        _write_gzip(path, rows)
        ids = [str(row["batch_id"]) for row in rows]
        entries.append({
            "file": path.name,
            "records": len(rows),
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
            "content_sha256": sha256_json(rows),
            "first_document_id": ids[0],
            "last_document_id": ids[-1],
        })
    batches = [row for rows in chunks for row in rows]
    ids = [str(row["batch_id"]) for row in batches]
    manifest["files"]["chunks"] = entries
    manifest["expected_batch_ids"] = ids
    manifest["expected_batch_ids_sha256"] = sha256_json(ids)
    manifest["expected_stage_batch_count"] = len(batches)
    manifest["exported_records"] = len(batches)
    variant_ids = [str(batch["json_schema_variant_id"]) for batch in batches]
    variant_hashes = [str(batch["json_schema_sha256"]) for batch in batches]
    variant_counts = Counter(variant_ids)
    variant_hashes_used: dict[str, str] = {}
    for variant_id, variant_hash in zip(variant_ids, variant_hashes):
        prior = variant_hashes_used.setdefault(variant_id, variant_hash)
        if prior != variant_hash:
            raise AssertionError("attack declares inconsistent schema hashes")
    manifest.update({
        "schema_variant_counts": dict(sorted(variant_counts.items())),
        "schema_variant_hashes_used": dict(sorted(variant_hashes_used.items())),
        "ordered_schema_variant_ids_sha256": sha256_json(variant_ids),
        "ordered_schema_variant_hashes_sha256": sha256_json(variant_hashes),
    })
    _write_manifest(root, manifest)


def _rebind_batch_id(batch: dict[str, Any]) -> None:
    identity = {
        "stage": RECORD_EXTRACTION_STAGE,
        "contract": computed_recall40_round3_stage_b_contract()[
            "line_ledger_contract_sha256"
        ],
        "parent_document_id": batch["parent_document_id"],
        "batch_index": batch["batch_index_within_parent"],
        "anchor_line_ids": [
            target["source_line_id"] for target in batch["targets"]
        ],
        "final_decisions_sha256": batch["final_line_decisions_sha256"],
        "two_pass_provenance_sha256": batch[
            "two_pass_line_provenance_sha256"
        ],
        "stage_b_two_pass_provenance_spec_sha256": batch[
            "stage_b_two_pass_provenance_spec_sha256"
        ],
        "record_extraction_batch_schema_rule_sha256": batch[
            "record_extraction_batch_schema_rule_sha256"
        ],
        "recall40_round3_stage_b_semantic_spec_sha256": batch[
            "recall40_round3_stage_b_semantic_spec_sha256"
        ],
        "recall40_round3_stage_b_source_catalog_spec_sha256": batch[
            "recall40_round3_stage_b_source_catalog_spec_sha256"
        ],
        "recall40_round3_stage_b_record_projection_spec_sha256": batch[
            "recall40_round3_stage_b_record_projection_spec_sha256"
        ],
        "recall40_round3_stage_b_rendering_spec_sha256": batch[
            "recall40_round3_stage_b_rendering_spec_sha256"
        ],
        "recall40_round3_stage_b_scope_spec_sha256": batch[
            "recall40_round3_stage_b_scope_spec_sha256"
        ],
    }
    batch_id = sha256_json(identity)
    batch["batch_id"] = batch_id
    batch["document_id"] = batch_id


def _rebind_schema(batch: dict[str, Any]) -> None:
    binding = schema_binding_for_batch(batch)
    for field in (
        "json_schema_variant_id", "json_schema_sha256",
        "json_schema_catalog_sha256", "target_slot_count",
    ):
        batch[field] = binding[field]


def _isolate_target(
    batches: list[dict[str, Any]], line_text: str,
) -> dict[str, Any]:
    for source_batch in batches:
        for source_target in source_batch["targets"]:
            if source_target["line_text"] == line_text:
                batch = copy.deepcopy(source_batch)
                target = copy.deepcopy(source_target)
                target["slot"] = "slot_00"
                batch["targets"] = [target]
                batch["target_slots"] = ["slot_00"]
                batch["padding_slots"] = [
                    f"slot_{index:02d}" for index in range(1, 8)
                ]
                _rebind_schema(batch)
                return batch
    raise AssertionError(f"target is absent: {line_text}")


def _output(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        f"slot_{index:02d}": {"records": records if index == 0 else []}
        for index in range(8)
    }


def _record(
    name: str,
    *,
    title: str | None = None,
    category: str = "private_individual",
    entity_type: str = "individual",
) -> dict[str, Any]:
    return {
        "endorser_name": name,
        "entity_type": entity_type,
        "endorser_category": category,
        "title": title,
        "affiliation": None,
        "jurisdiction": None,
    }


def _validate_open(root: Path) -> None:
    validate_input_manifest(
        root / "manifest.json",
        root / "chunks",
        stage=RECORD_EXTRACTION_STAGE,
        contract=get_frozen_recall40_round3_stage_b_contract(),
    )


def test_round3_stage_b_contract_is_distinct_frozen_and_two_pass() -> None:
    contract = computed_recall40_round3_stage_b_contract()
    assert contract["contract_version"] == (
        "candidate_endorsement_recall40_round3_stage_b_v1"
    )
    assert contract["active_stage"] == RECORD_EXTRACTION_STAGE
    assert set(contract["prompt_sha256"]) == {RECORD_EXTRACTION_STAGE}
    assert prompt_for_contract_stage(contract, RECORD_EXTRACTION_STAGE) == (
        RECALL40_ROUND3_RECORD_EXTRACTION_PROMPT
    )
    assert contract["recall40_round3_stage_b_semantic_spec_sha256"] == (
        RECALL40_ROUND3_STAGE_B_SEMANTIC_SPEC_SHA256
    )
    assert "stage_b_two_pass_provenance_spec_sha256" in contract
    assert "stage_b_three_pass_provenance_spec_sha256" not in contract
    assert FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256 == (
        "51b14a9138379754b164baff885e07d3565537eebd2dca5fe750da8d7a02538a"
    )
    assert get_frozen_recall40_round3_stage_b_contract() == contract

    source_files = (
        REPO / "src/candidate_endorsement_line_ledger.py",
        REPO / "scripts/build_candidate_endorsement_line_ledger.py",
        REPO / "scripts/hive/extract_candidate_endorsement_line_ledger_vllm.py",
    )
    assert all(
        "candidate_endorsement_round3_name_semantics" not in path.read_text()
        for path in source_files
    )
    assert get_line_ledger_contract()["line_ledger_contract_sha256"] == (
        "37dd3a17f5b70e377df4e2b9f1c8213d7909b5838cf99b6c28676e7ac7c8c4fc"
    )
    assert computed_recall40_stage_b_contract()[
        "line_ledger_contract_sha256"
    ] == "f1672447cd5f938eefbdfc173a24e6bc93565c460830867fdd494be1f6fbad31"
    assert get_frozen_recall40_round2_stage_b_contract()[
        "line_ledger_contract_sha256"
    ] == "019a97f9c37569b65b0689a78747afff19b05df02a8e729472c3b9571029bfb3"


def test_round3_stage_b_real_frozen_artifact_has_exact_authenticated_closure(
    open_artifact: Path,
) -> None:
    manifest, manifest_hash, paths = validate_input_manifest(
        open_artifact / "manifest.json",
        open_artifact / "chunks",
        stage=RECORD_EXTRACTION_STAGE,
        contract=get_frozen_recall40_round3_stage_b_contract(),
    )
    assert len(paths) == 5
    assert manifest["contract_is_frozen"] is True
    assert manifest["frozen_line_ledger_contract_sha256"] == (
        FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
    )
    assert manifest["round3_stage_b_gate"]["report_file_sha256"] == (
        "10f7c77763411d3487626be163dedfc90f72e88a45b2da34152bfcbdb4d2e0ae"
    )
    assert manifest["round3_stage_b_gate"]["report_object_sha256"] == (
        "1592af11e4a6e54ddad8554af7c2339330e3bb7e62fca27197334f9710b2cfd3"
    )
    assert manifest["stage_b_closure"] == {
        key: RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[key]
        for key in manifest["stage_b_closure"]
    }
    assert manifest["stage_b_closure"]["stage_b_batch_count"] == 298
    assert manifest["stage_b_closure"]["stage_b_anchor_count"] == 2221
    assert manifest["stage_b_closure"]["ordinary_anchor_count"] == 2217
    assert manifest["stage_b_closure"]["compound_anchor_count"] == 4
    assert manifest["stage_b_closure"][
        "stage_b_used_line_provenance_count"
    ] == 2829
    assert manifest["recall40_round3_stage_b_scope_spec_sha256"] == (
        RECALL40_ROUND3_STAGE_B_SCOPE_SPEC_SHA256
    )
    assert len(manifest_hash) == 64

    with pytest.raises(FileNotFoundError, match="run metadata is absent"):
        load_validated_stage_run(
            open_artifact,
            open_artifact.parent / "absent-output",
            stage=RECORD_EXTRACTION_STAGE,
        )


def test_round3_stage_b_schema_forces_cardinality_and_strict_names(
    open_artifact: Path,
) -> None:
    batches = [row for chunk in _chunk_rows(open_artifact) for row in chunk]
    ordinary = _isolate_target(batches, "Michael Rohr, CPA")
    ordinary_schema = schema_binding_for_batch(ordinary)["schema"][
        "properties"
    ]["slot_00"]["properties"]["records"]
    assert ordinary_schema["minItems"] == 1
    assert ordinary_schema["maxItems"] == 1
    assert "disposition" not in json.dumps(ordinary_schema)

    exact = _output([_record("Michael Rohr", title="CPA")])
    assert validate_record_extraction_output(ordinary, exact) == {
        "schema_valid": True,
        "errors": [],
        "anchor_decision_count": 1,
        "model_record_count": 1,
        "records_per_slot_guided_maximum": 32,
        "records_per_slot_cap_reached": False,
        "model_record_repetition_threshold": 3,
        "max_exact_model_record_multiplicity": 1,
        "unresolved_anchor_count": 0,
        "coverage_complete": True,
    }
    aligned = align_record_extraction_output(
        ordinary,
        exact,
        line_ledger_contract_sha256=computed_recall40_round3_stage_b_contract()[
            "line_ledger_contract_sha256"
        ],
    )
    assert aligned["aligned_records"][0]["record_evidence"] == (
        "Michael Rohr, CPA"
    )
    assert aligned["aligned_records"][0]["source_line_id"] == (
        ordinary["targets"][0]["source_line_id"]
    )
    assert aligned["withheld_record_count"] == 0
    no_record = validate_record_extraction_output(ordinary, _output([]))
    assert "slot_00:single_anchor_record_count_mismatch" in no_record["errors"]
    two_records = validate_record_extraction_output(
        ordinary, _output([_record("Michael Rohr", title="CPA")] * 2)
    )
    assert "slot_00:single_anchor_record_count_mismatch" in two_records["errors"]

    compound = _isolate_target(batches, "Gary & Geri Proctor")
    compound_schema = schema_binding_for_batch(compound)["schema"][
        "properties"
    ]["slot_00"]["properties"]["records"]
    assert compound_schema["minItems"] == 2
    assert compound_schema["maxItems"] == 32
    split = _output([_record("Gary Proctor"), _record("Geri Proctor")])
    assert validate_record_extraction_output(compound, split)["schema_valid"]
    compound_alignment = align_record_extraction_output(
        compound,
        split,
        line_ledger_contract_sha256=computed_recall40_round3_stage_b_contract()[
            "line_ledger_contract_sha256"
        ],
    )
    assert [
        row["endorser_name"] for row in compound_alignment["aligned_records"]
    ] == ["Gary Proctor", "Geri Proctor"]
    one_name = validate_record_extraction_output(
        compound, _output([_record("Gary Proctor")])
    )
    assert "slot_00:compound_anchor_record_count_mismatch" in one_name["errors"]
    combined = validate_record_extraction_output(
        compound, _output([_record("Gary & Geri Proctor"), _record("Geri Proctor")])
    )
    assert "slot_00:compound_anchor_combined_identity_name" in combined["errors"]


def test_round3_stage_b_generation_rows_metadata_and_restart_are_two_pass(
    open_artifact: Path, tmp_path: Path,
) -> None:
    manifest = _load_manifest(open_artifact)
    batches = [row for chunk in _chunk_rows(open_artifact) for row in chunk]
    batch = _isolate_target(batches, "Michael Rohr, CPA")
    output = _output([_record("Michael Rohr", title="CPA")])
    manifest_hash = file_sha256(open_artifact / "manifest.json")
    args = SimpleNamespace(
        stage=RECORD_EXTRACTION_STAGE,
        seed=FROZEN_GENERATION_SEED,
        max_model_len=32768,
        max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS[RECORD_EXTRACTION_STAGE],
        expected_input_manifest_sha256=manifest_hash,
        shard_index=0,
        shard_count=5,
    )
    payload = generation_config_payload(
        args,
        computed_recall40_round3_stage_b_contract(),
        input_manifest_sha256=manifest_hash,
    )
    assert payload["ordered_prompt_token_counts_sha256"] == (
        "800338c3730063d1c6b4d907a2b49e3fcc770a8bde1d30d437cd4582011849e9"
    )
    assert payload["tokenizer_transformers_version"] == "5.14.1"
    assert "stage_b_two_pass_provenance_spec_sha256" in payload
    assert "stage_b_three_pass_provenance_spec_sha256" not in payload
    generation_hash = generation_config_sha256(
        args,
        computed_recall40_round3_stage_b_contract(),
        input_manifest_sha256=manifest_hash,
    )
    row = _derive_output_row(
        batch=batch,
        raw_output=json.dumps(output),
        finish_reason="stop",
        stop_reason=None,
        output_tokens=64,
        prompt_tokens=876,
        args=args,
        contract=computed_recall40_round3_stage_b_contract(),
        input_manifest_sha256=manifest_hash,
        generation_hash=generation_hash,
    )
    assert row["technical_pass"] is True
    assert "stage_b_two_pass_provenance_spec_sha256" in row
    assert "stage_b_three_pass_provenance_spec_sha256" not in row
    metadata = _run_metadata(
        args=args,
        contract=computed_recall40_round3_stage_b_contract(),
        generation_hash=generation_hash,
        input_manifest_sha256=manifest_hash,
        selected=1,
        selected_variant_inventory=schema_variant_inventory([batch]),
        selected_stage_a_provenance_sha256=[],
        rows=[row],
        started="2026-09-06T12:00:00+00:00",
        recovery=False,
    )
    assert "stage_b_two_pass_provenance_spec_sha256" in metadata
    assert "stage_b_three_pass_provenance_spec_sha256" not in metadata

    first_input = open_artifact / "chunks/chunk_00000.jsonl.gz"
    assert _expected_prompt_token_counts_sha256(manifest, first_input) == (
        RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
            "chunk_ordered_prompt_token_counts_sha256"
        ][0]
    )
    isolated_input = tmp_path / "chunk_00000.jsonl.gz"
    isolated_output = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(isolated_input, [batch])
    _write_gzip(isolated_output, [row])
    replay = {
        "stage": RECORD_EXTRACTION_STAGE,
        "input_manifest_sha256": manifest_hash,
        "expected_generation_config_sha256": generation_hash,
        "expected_max_model_len": 32768,
        "expected_max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS[
            RECORD_EXTRACTION_STAGE
        ],
        "contract_override": computed_recall40_round3_stage_b_contract(),
    }
    assert not line_ledger_chunk_is_complete(
        isolated_input, isolated_output, **replay
    )
    assert not line_ledger_chunk_is_complete(
        isolated_input,
        isolated_output,
        expected_prompt_token_counts_sha256="0" * 64,
        **replay,
    )


def _apply_manifest_attack(label: str, manifest: dict[str, Any]) -> None:
    if label == "manifest_extra":
        manifest["unexpected"] = True
    elif label == "manifest_omission":
        manifest.pop("stage_b_closure")
    elif label == "private_extra":
        manifest["private_recall40_round3_stage_b"]["unexpected"] = False
    elif label == "report_extra":
        manifest["round3_stage_b_gate"]["report"]["unexpected"] = False
    elif label == "upstream_a_extra":
        manifest["upstream_stage_a"]["unexpected"] = "0" * 64
    elif label == "upstream_a2_extra":
        manifest["upstream_stage_a2"]["unexpected"] = "0" * 64
    elif label == "merge_extra":
        manifest["two_pass_merge"]["unexpected"] = "0" * 64
    elif label == "file_entry_extra":
        manifest["files"]["chunks"][0]["unexpected"] = 0
    elif label == "chunk_records_float":
        manifest["files"]["chunks"][0]["records"] = 60.0
    elif label == "chunk_records_bool":
        manifest["files"]["chunks"][0]["records"] = True
    elif label == "chunk_bytes_float":
        manifest["files"]["chunks"][0]["bytes"] = float(
            manifest["files"]["chunks"][0]["bytes"]
        )
    elif label == "selection_rows_float":
        manifest["files"]["selection"]["rows"] = 40.0
    elif label == "selection_bytes_float":
        manifest["files"]["selection"]["bytes"] = float(
            manifest["files"]["selection"]["bytes"]
        )
    elif label == "float_count":
        manifest["expected_stage_batch_count"] = 298.0
    elif label == "bool_count":
        manifest["stage_b_closure"]["stage_b_anchor_count"] = True
    elif label == "report_file_hash":
        manifest["round3_stage_b_gate"]["report_file_sha256"] = "0" * 64
    elif label == "report_object_hash":
        manifest["round3_stage_b_gate"]["report_object_sha256"] = "0" * 64
    elif label == "report_payload_hash":
        manifest["round3_stage_b_gate"]["report"][
            "report_payload_sha256"
        ] = "0" * 64
    elif label == "report_go":
        manifest["round3_stage_b_gate"]["report"]["gate_decision"] = "NO_GO"
    elif label == "stage_a_manifest_hash":
        manifest["upstream_artifact_hashes"][
            "stage_a_input_manifest_sha256"
        ] = "0" * 64
    elif label == "stage_a2_generation_hash":
        manifest["upstream_stage_a2"]["generation_config_sha256"] = "0" * 64
    elif label == "stage_a2_output_hash":
        manifest["upstream_stage_a2"]["output_files"][0]["sha256"] = "0" * 64
    elif label == "stage_a2_provenance_hash":
        manifest["upstream_stage_a2"]["decision_ledger_sha256"] = "0" * 64
    elif label == "source_export":
        manifest["source_export"]["source_document_ids_sha256"] = "0" * 64
    elif label == "schema_catalog":
        manifest["json_schema_catalog_sha256"] = "0" * 64
    else:
        raise AssertionError(label)


@pytest.mark.parametrize("label", [
    "manifest_extra", "manifest_omission", "private_extra", "report_extra",
    "upstream_a_extra", "upstream_a2_extra", "merge_extra",
    "file_entry_extra", "chunk_records_float", "chunk_records_bool",
    "chunk_bytes_float", "selection_rows_float", "selection_bytes_float",
    "float_count", "bool_count", "report_file_hash",
    "report_object_hash", "report_payload_hash", "report_go",
    "stage_a_manifest_hash", "stage_a2_generation_hash",
    "stage_a2_output_hash", "stage_a2_provenance_hash", "source_export",
    "schema_catalog",
])
def test_round3_stage_b_rejects_rehashed_manifest_attacks(
    label: str, open_artifact: Path, tmp_path: Path,
) -> None:
    root = _copy_artifact(open_artifact, tmp_path)
    manifest = _load_manifest(root)
    _apply_manifest_attack(label, manifest)
    _write_manifest(root, manifest)
    with pytest.raises((TypeError, ValueError)):
        _validate_open(root)


@pytest.mark.parametrize("label", [
    "schema", "omission", "reorder", "cell",
])
def test_round3_stage_b_rejects_rehashed_selection_attacks(
    label: str, open_artifact: Path, tmp_path: Path,
) -> None:
    root = _copy_artifact(open_artifact, tmp_path)
    manifest = _load_manifest(root)
    path = root / "selection.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        columns = list(reader.fieldnames or [])
    if label == "schema":
        columns[-1] = "changed_record_extraction_batch_count"
        rows = [
            {
                ("changed_record_extraction_batch_count" if key == (
                    "record_extraction_batch_count"
                ) else key): value
                for key, value in row.items()
            }
            for row in rows
        ]
    elif label == "omission":
        rows.pop()
    elif label == "reorder":
        rows[0], rows[1] = rows[1], rows[0]
        selected = [row["parent_document_id"] for row in rows]
        manifest["selected_parent_ids"] = selected
        manifest["selected_parent_ids_sha256"] = sha256_json(selected)
    elif label == "cell":
        rows[0]["party"] = "mutated-party"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    manifest["files"]["selection"] = {
        "file": path.name,
        "rows": len(rows),
        "columns": columns,
        "bytes": path.stat().st_size,
        "sha256": file_sha256(path),
    }
    _write_manifest(root, manifest)
    with pytest.raises((KeyError, TypeError, ValueError)):
        _validate_open(root)


def _apply_batch_attack(
    label: str, chunks: list[list[dict[str, Any]]],
) -> None:
    first = chunks[0][0]
    if label == "batch_omission":
        chunks[0].pop(0)
    elif label == "batch_duplicate":
        chunks[0][1] = copy.deepcopy(first)
    elif label == "batch_reorder":
        chunks[0][0], chunks[0][1] = chunks[0][1], chunks[0][0]
    elif label == "batch_repartition":
        chunks[1].insert(0, chunks[0].pop())
    elif label == "source_parent_reassignment":
        first["parent_document_id"] = "0" * 64
        _rebind_batch_id(first)
    elif label == "target_reorder":
        first["targets"][0], first["targets"][1] = (
            first["targets"][1], first["targets"][0]
        )
        for index, target in enumerate(first["targets"]):
            target["slot"] = f"slot_{index:02d}"
        _rebind_batch_id(first)
    elif label == "record_block_reorder":
        target = next(
            target for rows in chunks for batch in rows
            for target in batch["targets"]
            if len(target["record_block_lines"]) > 1
        )
        target["record_block_lines"].reverse()
        target["record_block_line_decision_provenance"].reverse()
    elif label == "record_block_text":
        target = first["targets"][0]
        line = next(
            line for line in target["record_block_lines"]
            if line["source_line_id"] == target["source_line_id"]
        )
        line["line_text"] += " X"
        line["line_text_sha256"] = sha256_text(line["line_text"])
        target["line_text"] = line["line_text"]
        target["line_text_sha256"] = line["line_text_sha256"]
    elif label == "record_block_span":
        target = first["targets"][0]
        line = next(
            line for line in target["record_block_lines"]
            if line["source_line_id"] == target["source_line_id"]
        )
        line["parent_end_char"] += 1
        target["parent_end_char"] += 1
    elif label == "carrier":
        first["targets"][0]["owner_segment_id"] = "0" * 64
    elif label == "consistent_provenance":
        target = first["targets"][0]
        target["stage_a_proposed_role"] = "nonrecord_content"
        target["anchor_line_decision_provenance"]["stage_a_proposal"][
            "role"
        ] = "nonrecord_content"
        for provenance in target["record_block_line_decision_provenance"]:
            if provenance["source_line_id"] == target["source_line_id"]:
                provenance["stage_a_proposal"]["role"] = "nonrecord_content"
    elif label == "ordinary_to_compound":
        target = first["targets"][0]
        assert target["stage_a_role"] == "record_anchor"
        target["stage_a_role"] = "compound_record_anchor"
        target["anchor_line_decision_provenance"]["final_role"] = (
            "compound_record_anchor"
        )
        for provenance in target["record_block_line_decision_provenance"]:
            if provenance["source_line_id"] == target["source_line_id"]:
                provenance["final_role"] = "compound_record_anchor"
        _rebind_schema(first)
    elif label == "schema_substitution":
        replacement = next(
            batch for rows in chunks for batch in rows
            if batch["target_slot_count"] == first["target_slot_count"]
            and batch["json_schema_variant_id"] != first["json_schema_variant_id"]
        )
        for field in (
            "json_schema_variant_id", "json_schema_sha256",
            "json_schema_catalog_sha256", "target_slot_count",
        ):
            first[field] = replacement[field]
    elif label == "batch_extra":
        first["unexpected"] = False
    elif label == "target_extra":
        first["targets"][0]["unexpected"] = False
    else:
        raise AssertionError(label)


@pytest.mark.parametrize("label", [
    "batch_omission", "batch_duplicate", "batch_reorder",
    "batch_repartition", "source_parent_reassignment", "target_reorder",
    "record_block_reorder", "record_block_text", "record_block_span",
    "carrier", "consistent_provenance", "ordinary_to_compound",
    "schema_substitution", "batch_extra", "target_extra",
])
def test_round3_stage_b_rejects_fully_rehashed_batch_attacks(
    label: str, open_artifact: Path, tmp_path: Path,
) -> None:
    root = _copy_artifact(open_artifact, tmp_path)
    manifest = _load_manifest(root)
    chunks = _chunk_rows(root)
    _apply_batch_attack(label, chunks)
    _rewrite_chunks(root, manifest, chunks)
    with pytest.raises((KeyError, TypeError, ValueError)):
        _validate_open(root)
