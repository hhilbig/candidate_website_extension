"""Focused integration tests for Stage-A2 export and evaluator plumbing."""
from __future__ import annotations

import json
import shutil
import sys
from argparse import Namespace
from collections import Counter
from pathlib import Path

import pytest

import scripts.build_candidate_endorsement_line_ledger as builder
import scripts.evaluate_candidate_endorsement_line_ledger_pilot as evaluator
from scripts.build_candidate_endorsement_line_ledger import (
    CHAMPION_INTERFACE_LINE_TEXT,
    DEFAULT_FULL_EXPORT_DIR,
    FROZEN_STAGE_A2_EXCEPTION_COUNT,
    FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256,
    FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256,
    FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256,
    LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR,
    PILOT_CONTENT_GATES,
    PILOT_PARENT_IDS,
    STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES,
    STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS,
    STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS,
    TANAKA_INTERFACE_LINE_TEXT,
    build_line_adjudication_export,
    build_adjudicated_record_extraction_export,
    load_frozen_stage_a_suffix_b,
    read_jsonl_gz,
    sha256_file,
    sha256_json,
    validate_manifest_payload_hash,
    write_jsonl_gz,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    FROZEN_GENERATION_SEED,
    FROZEN_MAX_MODEL_LEN,
    _derive_output_row,
    _run_metadata,
    generation_config_sha256,
    load_validated_stage_run,
    schema_variant_inventory,
    validate_expected_input_manifest_sha256,
    validate_input_manifest,
)
from src.candidate_endorsement_line_ledger import (
    ANCHOR_ROLES,
    FROZEN_STAGE_A_V2_CONTRACT_SHA256,
    LINE_ADJUDICATION_NEIGHBOR_LINES,
    LINE_ADJUDICATION_SPEC_SHA256,
    LINE_ADJUDICATION_STAGE,
    LINE_DECISION_STAGE,
    LINE_INVENTORY_SPEC_SHA256,
    build_stage_user_message,
    get_line_ledger_contract,
    schema_binding_for_batch,
    schema_binding_fields_for_batch,
    sha256_text,
    validate_line_adjudication_output,
)
from src.candidate_extraction import inference_display_escape


REPO = Path(__file__).resolve().parents[1]
HIVE_BUILD = REPO / "build/hive_candidate_extraction"
STAGE_A_EXPORT = (
    HIVE_BUILD / "v2_1_endorsement_line_ledger_stage_a_preflight_20260825b"
)
STAGE_A_OUTPUT = HIVE_BUILD / (
    "v2_1_endorsement_line_ledger_stage_a_preflight_"
    "hive_output_20260825b_21437036"
)
HAS_FROZEN_INPUTS = all(path.is_dir() for path in (
    DEFAULT_FULL_EXPORT_DIR, STAGE_A_EXPORT, STAGE_A_OUTPUT,
))


def _mark_test_manifest_frozen(path: Path, manifest: dict) -> dict:
    value = json.loads(json.dumps(manifest))
    contract_hash = value["line_ledger_contract_sha256"]
    value["contract_is_frozen"] = True
    value["frozen_line_ledger_contract_sha256"] = contract_hash
    payload = dict(value)
    payload.pop("manifest_payload_sha256", None)
    value["manifest_payload_sha256"] = sha256_json(payload)
    path.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return value


def _materialize_valid_stage_a2_run(tmp_path: Path) -> tuple[Path, Path]:
    export_dir = tmp_path / "stage_a2_export"
    manifest = build_line_adjudication_export(
        DEFAULT_FULL_EXPORT_DIR,
        STAGE_A_EXPORT,
        STAGE_A_OUTPUT,
        export_dir,
        chunk_size=60,
    )
    manifest = _mark_test_manifest_frozen(
        export_dir / "manifest.json", manifest
    )
    manifest_sha = sha256_file(export_dir / "manifest.json")
    contract = get_line_ledger_contract()
    args = Namespace(
        stage=LINE_ADJUDICATION_STAGE,
        seed=FROZEN_GENERATION_SEED,
        max_model_len=FROZEN_MAX_MODEL_LEN,
        max_output_tokens=2_048,
        shard_index=None,
        shard_count=None,
    )
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_sha
    )
    input_paths = sorted((export_dir / "chunks").glob("*.jsonl.gz"))
    batches = [row for path in input_paths for row in read_jsonl_gz(path)]
    rows = []
    output_dir = tmp_path / "stage_a2_output"
    output_dir.mkdir()
    for input_path in input_paths:
        chunk_rows = []
        for batch in read_jsonl_gz(input_path):
            target_by_slot = {
                str(target["slot"]): target for target in batch["targets"]
            }
            model_output = {}
            for index in range(8):
                slot = f"slot_{index:02d}"
                target = target_by_slot.get(slot)
                if target is None:
                    model_output[slot] = {
                        "decision": "padding",
                        "final_role": None,
                        "anchor_source_line_id": None,
                    }
                elif target["exception_reason"] == (
                    "stage_a_unowned_continuation"
                ):
                    model_output[slot] = {
                        "decision": "not_continuation",
                        "final_role": "nonrecord_content",
                        "anchor_source_line_id": None,
                    }
                else:
                    model_output[slot] = {
                        "decision": "role",
                        "final_role": "nonrecord_content",
                        "anchor_source_line_id": None,
                    }
            row = _derive_output_row(
                batch=batch,
                raw_output=json.dumps(model_output, ensure_ascii=False),
                finish_reason="stop",
                stop_reason=None,
                output_tokens=100,
                prompt_tokens=1_000,
                args=args,
                contract=contract,
                input_manifest_sha256=manifest_sha,
                generation_hash=generation_hash,
            )
            assert row["technical_pass"] is True
            rows.append(row)
            chunk_rows.append(row)
        write_jsonl_gz(
            output_dir / input_path.name.replace(
                ".jsonl.gz", ".out.jsonl.gz"
            ),
            chunk_rows,
        )
    metadata = _run_metadata(
        args=args,
        contract=contract,
        generation_hash=generation_hash,
        input_manifest_sha256=manifest_sha,
        selected=len(batches),
        selected_variant_inventory=schema_variant_inventory(batches),
        selected_stage_a_provenance_sha256=[
            str(batch["stage_a_provenance_sha256"]) for batch in batches
        ],
        rows=rows,
        started="2026-08-25T00:00:00+00:00",
        recovery=False,
    )
    (output_dir / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert manifest["exported_records"] == len(rows) == 29
    return export_dir, output_dir


@pytest.mark.skipif(
    not HAS_FROZEN_INPUTS,
    reason="exact suffix-b Stage-A and suffix-p source exports are unavailable",
)
def test_stage_a2_builder_derives_exact_exceptions_and_provenance(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "stage_a2"
    manifest = build_line_adjudication_export(
        DEFAULT_FULL_EXPORT_DIR,
        STAGE_A_EXPORT,
        STAGE_A_OUTPUT,
        output_dir,
        chunk_size=60,
    )
    validate_manifest_payload_hash(manifest)
    assert manifest["stage"] == LINE_ADJUDICATION_STAGE
    assert manifest["mode"] == (
        "line_adjudication_preflight_from_frozen_stage_a_suffix_b"
    )
    assert manifest["exported_records"] == 29
    assert manifest["stage_a_exception_count"] == 206
    assert manifest["stage_a_exception_reason_counts"] == {
        "stage_a_candidate_self_review": 2,
        "stage_a_unclear": 195,
        "stage_a_unowned_continuation": 9,
    }
    assert manifest["exception_ledger"] == {
        "count": 206,
        "ordered_exception_ids_sha256": manifest["exception_ledger"][
            "ordered_exception_ids_sha256"
        ],
        "ordered_source_line_ids_sha256": manifest["exception_ledger"][
            "ordered_source_line_ids_sha256"
        ],
        "reason_counts": manifest["stage_a_exception_reason_counts"],
    }
    assert all(
        isinstance(manifest["exception_ledger"][field], str)
        and len(manifest["exception_ledger"][field]) == 64
        for field in (
            "ordered_exception_ids_sha256",
            "ordered_source_line_ids_sha256",
        )
    )
    assert len(manifest["schema_variant_counts"]) == 29
    assert set(manifest["schema_variant_counts"].values()) == {1}
    assert manifest["bounded_preflight"]["exact_stage_a_suffix_b_only"] is True
    assert manifest["bounded_preflight"]["candidate_control_final_roles"] == (
        STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES
    )
    assert manifest["bounded_preflight"]["unowned_attachment_controls"] == (
        STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS
    )
    assert manifest["bounded_preflight"]["unowned_final_role_controls"] == (
        STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS
    )
    provenance = manifest["stage_a_provenance"]
    payload = dict(provenance)
    recorded = payload.pop("stage_a_provenance_sha256")
    assert recorded == sha256_json(payload)
    assert provenance["stage_a_input_manifest_sha256"] == (
        FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256
    )
    assert provenance["stage_a_generation_config_sha256"] == (
        FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256
    )
    assert provenance["stage_a_ordered_output_batch_ids_sha256"] == (
        FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256
    )

    batches = [
        row
        for entry in manifest["files"]["chunks"]
        for row in read_jsonl_gz(output_dir / "chunks" / entry["file"])
    ]
    targets = [target for batch in batches for target in batch["targets"]]
    assert Counter(len(batch["targets"]) for batch in batches) == {
        1: 1, 2: 2, 4: 1, 6: 1, 7: 1, 8: 23,
    }
    assert len(targets) == FROZEN_STAGE_A2_EXCEPTION_COUNT
    assert Counter(target["exception_reason"] for target in targets) == {
        "stage_a_candidate_self_review": 2,
        "stage_a_unclear": 195,
        "stage_a_unowned_continuation": 9,
    }
    assert all(
        target["stage_a_provenance_locator"]
        == provenance["by_batch_id"][
            target["stage_a_decision"]["stage_a_batch_id"]
        ]
        for target in targets
    )
    assert all(
        target["stage_a_provenance_locator"]["raw_output_sha256"]
        and target["stage_a_provenance_locator"]["model_output_sha256"]
        for target in targets
    )
    by_source_line_id = {
        target["source_line_id"]: target for target in targets
    }
    testimonial_owner = (
        "7d550c3dfd6691afeb17c49c6d1995d13ec48de0062f688a90fc07fc40637e15"
    )
    for line_id in (
        "29a627a195f761e26448d0259b7ff4749bfe9d0b65314e666fc45033b7ab6b66",
        "2d680e5f8e0164873011d06b65925bd8cd27fbe4b89c0fc36f1d219a63507425",
        "6d3204507829dbc39e3ed7d6eb4690eea96a87254ba7bd4c5c0114dd006375f7",
    ):
        assert by_source_line_id[line_id][
            "allowed_anchor_source_line_ids"
        ] == [testimonial_owner]
    assert by_source_line_id[
        "18829023194d146574c486c2bde03d9bcd09dd44432632153efdb6e59adf4cdc"
    ]["allowed_anchor_source_line_ids"] == []
    assert by_source_line_id[
        "7c42640254692dee92b9845f00ac172dc45dce7f31a07b3fc02c4bcef3f6e076"
    ]["deterministic_eligibility"]["terminal_roles"] == [
        "nonrecord_content"
    ]
    for line_id in (
        "5cac96d3488b283289854cc46cf973d12ac498a6d6c2649c9f3f07a9a18d10f1",
        "5c7724d06f37e57a91463b5ae414a325e3b44d951b9da1f1876d3333691639e0",
        "d70456a0840fe5ea8ec78402c3c2b7aa057f6c4bcff08b09e259cf3141008593",
        "ff2dc1f3299ef37af1aa9e169420354c20b4817121582140d3278c05074f2203",
    ):
        assert by_source_line_id[line_id][
            "deterministic_eligibility"
        ]["candidate_self_eligible"] is False
        assert "candidate_self" not in by_source_line_id[line_id][
            "deterministic_eligibility"
        ]["terminal_roles"]
    for line_id in (
        "af281dff680e36b1ac925ba0509577d5a648aba60189c65140e0cab0a72915bd",
        "edc7c8e209ff8511b6996e9080518b6e6f677a57b74876b91c84eb75fcf651ad",
    ):
        assert by_source_line_id[line_id][
            "deterministic_eligibility"
        ]["candidate_self_eligible"] is True

    failed_unclear_ids = {
        "2367c1126ed301056e6b08e86b859ee005fcc2cf66455524c9674232359de7a4",
        "406c1b97002b3913b85c2409a0ebf4795bb4a89351e9e3b3ece1b881ef9b0488",
        "b7c51c65d7d21f4f1260b9a0257918ac769dfad731a9ba65ff9ed9fd60ed2858",
        "438a4e78742f20fe6020b33ec3154c47ad8862a9f2ab3b13b37fa6958720441d",
        "40cb322dac2ad6a89709591f2c550104d7a6a733f6e67b92db6f0f1dcdb6ef96",
        "601c77592feab9baf74ba77db15db2892ccabe1ceb891a26a62d4826cc25b766",
        "963d5e9f2b2d019b0f84502f43b188e7c3a0da18cf04fbc04b93b0a9d577c1fa",
        "9d9cd7d1c78710c3149782988c15c5f4214f6a8dfe1a482bf82d363709cd1e5f",
    }
    self_anchor_failure_id = (
        "29a627a195f761e26448d0259b7ff4749bfe9d0b65314e666fc45033b7ab6b66"
    )
    observed_failed_unclear_ids: set[str] = set()
    observed_self_anchor = False
    for batch in batches:
        binding = schema_binding_for_batch(batch)
        assert batch["json_schema_sha256"] == binding["json_schema_sha256"]
        pending_schema_nodes = [binding["schema"]]
        while pending_schema_nodes:
            node = pending_schema_nodes.pop()
            if isinstance(node, dict):
                assert not (
                    {"$ref", "allOf", "const", "pattern", "minLength"}
                    & set(node)
                )
                pending_schema_nodes.extend(node.values())
            elif isinstance(node, list):
                pending_schema_nodes.extend(node)
        for target in batch["targets"]:
            slot = target["slot"]
            branches = binding["schema"]["properties"][slot]["oneOf"]
            by_decision = {
                branch["properties"]["decision"]["enum"][0]: branch
                for branch in branches
            }
            reason = target["exception_reason"]
            if reason in {
                "stage_a_unclear", "stage_a_candidate_self_review",
            }:
                assert "role" in by_decision
                assert "not_continuation" not in by_decision
            else:
                assert reason == "stage_a_unowned_continuation"
                assert "role" not in by_decision
                assert "not_continuation" in by_decision
            allowed_ids = target["allowed_anchor_source_line_ids"]
            if allowed_ids:
                assert by_decision["attach_to_anchor"]["properties"][
                    "anchor_source_line_id"
                ]["enum"] == allowed_ids
                assert target["source_line_id"] not in allowed_ids
            else:
                assert "attach_to_anchor" not in by_decision
            if target["source_line_id"] in failed_unclear_ids:
                observed_failed_unclear_ids.add(target["source_line_id"])
                assert "not_continuation" not in by_decision
            if target["source_line_id"] == self_anchor_failure_id:
                observed_self_anchor = True
                assert self_anchor_failure_id not in by_decision[
                    "attach_to_anchor"
                ]["properties"]["anchor_source_line_id"]["enum"]
    assert observed_failed_unclear_ids == failed_unclear_ids
    assert observed_self_anchor is True

    long_line_id = PILOT_CONTENT_GATES[PILOT_PARENT_IDS[5]][
        "required_source_line_id"
    ]
    long_batch = next(
        batch
        for batch in batches
        if any(
            target["source_line_id"] == long_line_id
            for target in batch["targets"]
        )
    )
    long_target = next(
        target for target in long_batch["targets"]
        if target["source_line_id"] == long_line_id
    )
    long_binding = schema_binding_for_batch(long_batch)
    long_branches = long_binding["schema"]["properties"][
        long_target["slot"]
    ]["oneOf"]
    assert [
        branch["properties"]["decision"]["enum"][0]
        for branch in long_branches
    ] == ["role"]
    assert long_branches[0]["properties"]["final_role"]["enum"] == [
        "nonrecord_content"
    ]
    message = build_stage_user_message(long_batch)
    catalog_lines = {
        str(line["source_line_id"]): line
        for line in long_batch["source_line_catalog"]
    }
    assert message.count("[REF L") == len(catalog_lines)
    assert all(
        message.count(f"source_line_id {line_id};") == 1
        and message.count(line_id) == 1
        for line_id in catalog_lines
    )
    long_text = str(catalog_lines[long_line_id]["line_text"])
    assert len(long_text) == 7_855
    assert message.count(inference_display_escape(long_text)) == 1
    assert len(message) < 30_000
    assert "Stage-A exception adjudications (catalog refs only):" in message
    assert "[ALLOWED REF L" in message

    attached_target = next(
        target for target in long_batch["targets"]
        if target["allowed_anchors"]
    )
    output = {}
    for slot in long_batch["target_slots"]:
        output[slot] = {
            "decision": "withheld_validation",
            "final_role": None,
            "anchor_source_line_id": None,
        }
        target = next(
            value for value in long_batch["targets"]
            if value["slot"] == slot
        )
        if target["deterministic_eligibility"]["standalone_machine_json"]:
            output[slot] = {
                "decision": "role",
                "final_role": "nonrecord_content",
                "anchor_source_line_id": None,
            }
    for slot in long_batch["padding_slots"]:
        output[slot] = {
            "decision": "padding",
            "final_role": None,
            "anchor_source_line_id": None,
        }
    output[attached_target["slot"]] = {
        "decision": "attach_to_anchor",
        "final_role": None,
        "anchor_source_line_id": attached_target[
            "allowed_anchor_source_line_ids"
        ][0],
    }
    assert validate_line_adjudication_output(
        long_batch, output
    )["schema_valid"] is True

    def rebind_batch_identity(value: dict) -> None:
        value["source_line_catalog_sha256"] = sha256_json(
            value["source_line_catalog"]
        )
        batch_id = sha256_json({
            "stage": LINE_ADJUDICATION_STAGE,
            "contract": get_line_ledger_contract()[
                "line_ledger_contract_sha256"
            ],
            "upstream_stage_a_contract": value[
                "upstream_stage_a_contract_sha256"
            ],
            "stage_a_provenance_sha256": value[
                "stage_a_provenance_sha256"
            ],
            "parent_document_id": value["parent_document_id"],
            "batch_index": value["batch_index_within_parent"],
            "parent_physical_line_count": value[
                "parent_physical_line_count"
            ],
            "exception_ids": [
                target["exception_id"] for target in value["targets"]
            ],
            "source_line_catalog_sha256": value[
                "source_line_catalog_sha256"
            ],
        })
        value["batch_id"] = batch_id
        value["document_id"] = batch_id

    def recompute_batch_relations(value: dict) -> None:
        catalog = sorted(
            value["source_line_catalog"],
            key=lambda line: int(line["parent_line_index"]),
        )
        value["source_line_catalog"] = catalog
        by_index = {
            int(line["parent_line_index"]): line for line in catalog
        }
        parent_line_count = int(value["parent_physical_line_count"])
        for target in value["targets"]:
            target_index = int(target["parent_line_index"])
            target["visible_source_line_ids"] = [
                str(by_index[index]["source_line_id"])
                for index in range(
                    max(0, target_index - LINE_ADJUDICATION_NEIGHBOR_LINES),
                    min(
                        parent_line_count,
                        target_index + LINE_ADJUDICATION_NEIGHBOR_LINES + 1,
                    ),
                )
            ]
            target["visible_source_line_ids_sha256"] = sha256_json(
                target["visible_source_line_ids"]
            )
            allowed = []
            for candidate in catalog:
                candidate_index = int(candidate["parent_line_index"])
                distance = abs(candidate_index - target_index)
                if (
                    candidate["source_line_id"] == target["source_line_id"]
                    or distance == 0
                    or distance > LINE_ADJUDICATION_NEIGHBOR_LINES
                    or not candidate["model_decision_required"]
                    or (
                        candidate["stage_a_role"] not in ANCHOR_ROLES
                        and not candidate["is_stage_a2_exception"]
                    )
                ):
                    continue
                allowed.append({
                    "anchor_source_line_id": candidate["source_line_id"],
                    "parent_line_index": candidate_index,
                    "relative_direction": (
                        "previous" if candidate_index < target_index
                        else "next"
                    ),
                    "distance_in_physical_lines": distance,
                    "stage_a_role": candidate["stage_a_role"],
                    "line_text": candidate["line_text"],
                    "line_text_sha256": candidate["line_text_sha256"],
                })
            allowed.sort(key=lambda item: (
                int(item["distance_in_physical_lines"]),
                int(item["parent_line_index"]),
                str(item["anchor_source_line_id"]),
            ))
            if target["exception_reason"] == "stage_a_unowned_continuation":
                component_ids = set(target["deterministic_eligibility"][
                    "continuation_component_anchor_source_line_ids"
                ])
                allowed = [
                    item for item in allowed
                    if item["anchor_source_line_id"] in component_ids
                ]
            target["allowed_anchors"] = allowed
            target["allowed_anchor_source_line_ids"] = [
                item["anchor_source_line_id"] for item in allowed
            ]
            stage_a = target["stage_a_decision"]
            target["exception_id"] = sha256_json({
                "line_adjudication_spec_sha256": (
                    LINE_ADJUDICATION_SPEC_SHA256
                ),
                "upstream_stage_a_contract_sha256": (
                    FROZEN_STAGE_A_V2_CONTRACT_SHA256
                ),
                "parent_document_id": value["parent_document_id"],
                "source_line_id": target["source_line_id"],
                "stage_a_batch_id": stage_a["stage_a_batch_id"],
                "stage_a_slot": stage_a["stage_a_slot"],
                "stage_a_role": stage_a["role"],
                "exception_reason": target["exception_reason"],
                "allowed_anchor_source_line_ids": target[
                    "allowed_anchor_source_line_ids"
                ],
                "deterministic_eligibility": target[
                    "deterministic_eligibility"
                ],
            })
        rebind_batch_identity(value)
        value.update(schema_binding_fields_for_batch(value))

    attacks = []
    mismatched_ids = json.loads(json.dumps(long_batch))
    mismatch_target = next(
        target for target in mismatched_ids["targets"]
        if target["allowed_anchors"]
    )
    replacement_id = next(
        line_id for line_id in mismatch_target["visible_source_line_ids"]
        if line_id not in mismatch_target["allowed_anchor_source_line_ids"]
        and line_id != mismatch_target["source_line_id"]
    )
    mismatch_target["allowed_anchor_source_line_ids"][0] = replacement_id
    attacks.append(mismatched_ids)

    duplicate_anchor = json.loads(json.dumps(long_batch))
    duplicate_target = next(
        target for target in duplicate_anchor["targets"]
        if target["allowed_anchors"]
    )
    duplicate_target["allowed_anchors"].append(
        json.loads(json.dumps(duplicate_target["allowed_anchors"][0]))
    )
    duplicate_target["allowed_anchor_source_line_ids"].append(
        duplicate_target["allowed_anchor_source_line_ids"][0]
    )
    attacks.append(duplicate_anchor)

    forged_relation = json.loads(json.dumps(long_batch))
    forged_target = next(
        target for target in forged_relation["targets"]
        if target["allowed_anchors"]
    )
    forged_target["allowed_anchors"][0].update({
        "relative_direction": "next",
        "distance_in_physical_lines": 999,
        "stage_a_role": "candidate_self",
    })
    attacks.append(forged_relation)

    collided_index = json.loads(json.dumps(long_batch))
    collided_index["source_line_catalog"][1]["parent_line_index"] = (
        collided_index["source_line_catalog"][0]["parent_line_index"]
    )
    attacks.append(collided_index)

    reversed_visible = json.loads(json.dumps(long_batch))
    reversed_target = reversed_visible["targets"][0]
    reversed_target["visible_source_line_ids"].reverse()
    reversed_target["visible_source_line_ids_sha256"] = sha256_json(
        reversed_target["visible_source_line_ids"]
    )
    attacks.append(reversed_visible)

    missing_visible = json.loads(json.dumps(long_batch))
    missing_target = missing_visible["targets"][0]
    removable = next(
        line_id for line_id in missing_target["visible_source_line_ids"]
        if line_id != missing_target["source_line_id"]
        and line_id not in missing_target["allowed_anchor_source_line_ids"]
    )
    missing_target["visible_source_line_ids"].remove(removable)
    missing_target["visible_source_line_ids_sha256"] = sha256_json(
        missing_target["visible_source_line_ids"]
    )
    attacks.append(missing_visible)

    false_text_hash = json.loads(json.dumps(long_batch))
    false_text_hash["source_line_catalog"][0]["line_text_sha256"] = "0" * 64
    false_text_hash["source_line_catalog_sha256"] = sha256_json(
        false_text_hash["source_line_catalog"]
    )
    attacks.append(false_text_hash)

    forged_source_id = json.loads(json.dumps(long_batch))
    forged_source_id["source_line_catalog"][0]["source_line_id"] = (
        "not-a-sha256-id"
    )
    forged_source_id["source_line_catalog_sha256"] = sha256_json(
        forged_source_id["source_line_catalog"]
    )
    attacks.append(forged_source_id)

    overlapping_offsets = json.loads(json.dumps(long_batch))
    first_catalog_line = overlapping_offsets["source_line_catalog"][0]
    second_catalog_line = overlapping_offsets["source_line_catalog"][1]
    second_catalog_line["parent_start_char"] = first_catalog_line[
        "parent_start_char"
    ]
    second_catalog_line["parent_end_char"] = (
        second_catalog_line["parent_start_char"]
        + len(second_catalog_line["line_text"])
    )
    second_catalog_line["parent_full_end_char"] = (
        second_catalog_line["parent_end_char"]
        + len(second_catalog_line["line_terminator"])
    )
    overlapping_offsets["source_line_catalog_sha256"] = sha256_json(
        overlapping_offsets["source_line_catalog"]
    )
    attacks.append(overlapping_offsets)

    forged_reason = json.loads(json.dumps(long_batch))
    reason_target = next(
        target for target in forged_reason["targets"]
        if target["exception_reason"] == "stage_a_unclear"
    )
    reason_target["exception_reason"] = "stage_a_candidate_self_review"
    attacks.append(forged_reason)

    reversed_slots = json.loads(json.dumps(long_batch))
    reversed_slots["target_slots"].reverse()
    attacks.append(reversed_slots)

    forged_batch_index = json.loads(json.dumps(long_batch))
    forged_batch_index["batch_index_within_parent"] = 999
    attacks.append(forged_batch_index)

    nonmodel_with_role = json.loads(json.dumps(long_batch))
    nonmodel_line = next(
        line for line in nonmodel_with_role["source_line_catalog"]
        if line["model_decision_required"] is True
        and line["stage_a_role"] not in {None, "padding"}
    )
    nonmodel_line["model_decision_required"] = False
    nonmodel_with_role["source_line_catalog_sha256"] = sha256_json(
        nonmodel_with_role["source_line_catalog"]
    )
    attacks.append(nonmodel_with_role)

    model_without_role = json.loads(json.dumps(long_batch))
    model_line = next(
        line for line in model_without_role["source_line_catalog"]
        if line["model_decision_required"] is True
    )
    model_line["stage_a_role"] = None
    model_without_role["source_line_catalog_sha256"] = sha256_json(
        model_without_role["source_line_catalog"]
    )
    attacks.append(model_without_role)

    nonmodel_padding_role = json.loads(json.dumps(long_batch))
    padding_role_line = nonmodel_padding_role["source_line_catalog"][0]
    padding_role_line["model_decision_required"] = False
    padding_role_line["stage_a_role"] = "padding"
    rebind_batch_identity(nonmodel_padding_role)
    attacks.append(nonmodel_padding_role)

    omitted_exception = json.loads(json.dumps(long_batch))
    unclear_line = next(
        line for line in omitted_exception["source_line_catalog"]
        if line["stage_a_role"] in {"unclear", "candidate_self"}
    )
    unclear_line["is_stage_a2_exception"] = False
    omitted_exception["source_line_catalog_sha256"] = sha256_json(
        omitted_exception["source_line_catalog"]
    )
    attacks.append(omitted_exception)

    unterminated_before_later = json.loads(json.dumps(long_batch))
    referenced_ids = {
        target["source_line_id"]
        for target in unterminated_before_later["targets"]
    } | {
        line_id
        for target in unterminated_before_later["targets"]
        for line_id in target["allowed_anchor_source_line_ids"]
    }
    unterminated_line = next(
        line
        for line in unterminated_before_later["source_line_catalog"][:-1]
        if line["source_line_id"] not in referenced_ids
    )
    old_line_id = unterminated_line["source_line_id"]
    unterminated_line["line_terminator"] = ""
    unterminated_line["parent_full_end_char"] = unterminated_line[
        "parent_end_char"
    ]
    new_line_id = sha256_json({
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "parent_document_id": unterminated_before_later[
            "parent_document_id"
        ],
        "parent_line_index": unterminated_line["parent_line_index"],
        "parent_start_char": unterminated_line["parent_start_char"],
        "parent_end_char": unterminated_line["parent_end_char"],
        "parent_full_end_char": unterminated_line["parent_full_end_char"],
        "line_text_sha256": unterminated_line["line_text_sha256"],
        "line_terminator": unterminated_line["line_terminator"],
    })
    unterminated_line["source_line_id"] = new_line_id
    for target in unterminated_before_later["targets"]:
        target["visible_source_line_ids"] = [
            new_line_id if line_id == old_line_id else line_id
            for line_id in target["visible_source_line_ids"]
        ]
        target["visible_source_line_ids_sha256"] = sha256_json(
            target["visible_source_line_ids"]
        )
    rebind_batch_identity(unterminated_before_later)
    attacks.append(unterminated_before_later)

    duplicate_target = json.loads(json.dumps(next(
        batch for batch in batches if len(batch["targets"]) >= 2
    )))
    replacement_slot = duplicate_target["targets"][1]["slot"]
    duplicate_target["targets"][1] = json.loads(json.dumps(
        duplicate_target["targets"][0]
    ))
    duplicate_target["targets"][1]["slot"] = replacement_slot
    rebind_batch_identity(duplicate_target)
    attacks.append(duplicate_target)

    injected_catalog = None
    for candidate_batch in batches:
        target_indices = [
            int(target["parent_line_index"])
            for target in candidate_batch["targets"]
        ]
        catalog = candidate_batch["source_line_catalog"]
        for previous, following in zip(catalog, catalog[1:]):
            injected_index = int(previous["parent_line_index"]) + 1
            injected_text = "INJECTED CONTEXT"
            if (
                int(following["parent_line_index"]) <= injected_index
                or any(
                    abs(injected_index - target_index)
                    <= 4 for target_index in target_indices
                )
                or int(following["parent_start_char"])
                - int(previous["parent_full_end_char"])
                < len(injected_text) + 1
            ):
                continue
            injected_catalog = json.loads(json.dumps(candidate_batch))
            inserted_start = int(previous["parent_full_end_char"])
            inserted_end = inserted_start + len(injected_text)
            inserted_text_sha256 = sha256_text(injected_text)
            inserted_line = {
                "source_line_id": sha256_json({
                    "line_inventory_spec_sha256": (
                        LINE_INVENTORY_SPEC_SHA256
                    ),
                    "parent_document_id": candidate_batch[
                        "parent_document_id"
                    ],
                    "parent_line_index": injected_index,
                    "parent_start_char": inserted_start,
                    "parent_end_char": inserted_end,
                    "parent_full_end_char": inserted_end + 1,
                    "line_text_sha256": inserted_text_sha256,
                    "line_terminator": "\n",
                }),
                "parent_line_index": injected_index,
                "parent_start_char": inserted_start,
                "parent_end_char": inserted_end,
                "parent_full_end_char": inserted_end + 1,
                "line_text": injected_text,
                "line_text_sha256": inserted_text_sha256,
                "line_terminator": "\n",
                "model_decision_required": False,
                "stage_a_role": None,
                "stage_a_batch_id": None,
                "stage_a_slot": None,
                "is_stage_a2_exception": False,
            }
            injected_catalog["source_line_catalog"].append(inserted_line)
            injected_catalog["source_line_catalog"].sort(
                key=lambda line: int(line["parent_line_index"])
            )
            rebind_batch_identity(injected_catalog)
            break
        if injected_catalog is not None:
            break
    assert injected_catalog is not None
    attacks.append(injected_catalog)

    phantom_prefix = json.loads(json.dumps(next(
        batch for batch in batches
        if batch["source_line_catalog"][0]["parent_line_index"] == 0
    )))
    first_line = phantom_prefix["source_line_catalog"][0]
    old_first_id = first_line["source_line_id"]
    first_line["parent_start_char"] = 1
    first_line["line_text"] = first_line["line_text"][1:]
    first_line["line_text_sha256"] = sha256_text(first_line["line_text"])
    new_first_id = sha256_json({
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "parent_document_id": phantom_prefix["parent_document_id"],
        "parent_line_index": first_line["parent_line_index"],
        "parent_start_char": first_line["parent_start_char"],
        "parent_end_char": first_line["parent_end_char"],
        "parent_full_end_char": first_line["parent_full_end_char"],
        "line_text_sha256": first_line["line_text_sha256"],
        "line_terminator": first_line["line_terminator"],
    })
    first_line["source_line_id"] = new_first_id
    for target in phantom_prefix["targets"]:
        if target["source_line_id"] == old_first_id:
            target.update({
                "source_line_id": new_first_id,
                "parent_start_char": first_line["parent_start_char"],
                "line_text": first_line["line_text"],
                "line_text_sha256": first_line["line_text_sha256"],
            })
            target["stage_a_decision"]["source_line_id"] = new_first_id
        target["visible_source_line_ids"] = [
            new_first_id if line_id == old_first_id else line_id
            for line_id in target["visible_source_line_ids"]
        ]
        target["visible_source_line_ids_sha256"] = sha256_json(
            target["visible_source_line_ids"]
        )
        target["allowed_anchor_source_line_ids"] = [
            new_first_id if line_id == old_first_id else line_id
            for line_id in target["allowed_anchor_source_line_ids"]
        ]
        for relation in target["allowed_anchors"]:
            if relation["anchor_source_line_id"] == old_first_id:
                relation.update({
                    "anchor_source_line_id": new_first_id,
                    "line_text": first_line["line_text"],
                    "line_text_sha256": first_line["line_text_sha256"],
                })
        stage_a = target["stage_a_decision"]
        target["exception_id"] = sha256_json({
            "line_adjudication_spec_sha256": (
                LINE_ADJUDICATION_SPEC_SHA256
            ),
            "upstream_stage_a_contract_sha256": (
                FROZEN_STAGE_A_V2_CONTRACT_SHA256
            ),
            "parent_document_id": phantom_prefix["parent_document_id"],
            "source_line_id": target["source_line_id"],
            "stage_a_batch_id": stage_a["stage_a_batch_id"],
            "stage_a_slot": stage_a["stage_a_slot"],
            "stage_a_role": stage_a["role"],
            "exception_reason": target["exception_reason"],
            "allowed_anchor_source_line_ids": target[
                "allowed_anchor_source_line_ids"
            ],
            "deterministic_eligibility": target[
                "deterministic_eligibility"
            ],
        })
    rebind_batch_identity(phantom_prefix)
    attacks.append(phantom_prefix)

    for attacked in attacks:
        with pytest.raises(ValueError):
            build_stage_user_message(attacked)
        assert validate_line_adjudication_output(
            attacked, output
        )["schema_valid"] is False

    frozen_manifest = _mark_test_manifest_frozen(
        output_dir / "manifest.json", manifest
    )
    frozen_manifest_sha256 = sha256_file(output_dir / "manifest.json")
    assert validate_expected_input_manifest_sha256(
        output_dir / "manifest.json", frozen_manifest_sha256
    ) == frozen_manifest_sha256
    for invalid_expected in (None, "0" * 64):
        with pytest.raises(ValueError, match="expected|externally frozen"):
            validate_expected_input_manifest_sha256(
                output_dir / "manifest.json", invalid_expected
            )
    validate_input_manifest(
        output_dir / "manifest.json",
        output_dir / "chunks",
        stage=LINE_ADJUDICATION_STAGE,
        contract=get_line_ledger_contract(),
    )
    assert len(frozen_manifest["files"]["chunks"]) == 1
    chunk_path = output_dir / "chunks" / frozen_manifest["files"][
        "chunks"
    ][0]["file"]

    def write_coordinated_manifest_attack(rows: list[dict]) -> None:
        write_jsonl_gz(chunk_path, rows)
        attacked_manifest = json.loads(json.dumps(frozen_manifest))
        entry = attacked_manifest["files"]["chunks"][0]
        entry.update({
            "records": len(rows),
            "bytes": chunk_path.stat().st_size,
            "sha256": sha256_file(chunk_path),
            "content_sha256": sha256_json(rows),
            "first_document_id": rows[0]["batch_id"],
            "last_document_id": rows[-1]["batch_id"],
        })
        batch_ids = [row["batch_id"] for row in rows]
        attacked_manifest.update({
            "exported_records": len(rows),
            "expected_stage_batch_count": len(rows),
            "expected_stage_a2_batch_count": len(rows),
            "expected_batch_ids": batch_ids,
            "expected_batch_ids_sha256": sha256_json(batch_ids),
        })
        attacked_manifest.update(schema_variant_inventory(rows))
        exceptions = [
            {
                key: value for key, value in target.items()
                if key not in {"slot", "stage_a_provenance_locator"}
            }
            for row in rows for target in row["targets"]
        ]
        source_line_ids = [
            str(row["source_line_id"]) for row in exceptions
        ]
        exception_ids = [str(row["exception_id"]) for row in exceptions]
        reason_counts = dict(sorted(Counter(
            str(row["exception_reason"]) for row in exceptions
        ).items()))
        attacked_manifest.update({
            "stage_a_exception_count": len(exceptions),
            "stage_a_exception_ledger_sha256": sha256_json(exceptions),
            "stage_a_exception_source_line_ids_sha256": sha256_json(
                source_line_ids
            ),
            "stage_a_exception_reason_counts": reason_counts,
            "exception_ledger": {
                "count": len(exceptions),
                "ordered_exception_ids_sha256": sha256_json(exception_ids),
                "ordered_source_line_ids_sha256": sha256_json(
                    source_line_ids
                ),
                "reason_counts": reason_counts,
            },
        })
        payload = dict(attacked_manifest)
        payload.pop("manifest_payload_sha256", None)
        attacked_manifest["manifest_payload_sha256"] = sha256_json(payload)
        (output_dir / "manifest.json").write_text(
            json.dumps(attacked_manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    role_flip_rows = json.loads(json.dumps(batches))
    role_flip_id = (
        "ae837b6f1c1fe778861b27279084bd6b899832cce9842dd0ee54cfab9f0e1d8f"
    )
    role_flip_batch = next(
        batch for batch in role_flip_rows
        if any(
            line["source_line_id"] == role_flip_id
            for line in batch["source_line_catalog"]
        )
    )
    role_flip_line = next(
        line for line in role_flip_batch["source_line_catalog"]
        if line["source_line_id"] == role_flip_id
    )
    assert role_flip_line["stage_a_role"] == "record_anchor"
    assert all(
        target["source_line_id"] != role_flip_id
        for target in role_flip_batch["targets"]
    )
    role_flip_line["stage_a_role"] = "nonrecord_content"
    recompute_batch_relations(role_flip_batch)
    write_coordinated_manifest_attack(role_flip_rows)
    with pytest.raises(ValueError, match="frozen Stage-A decision"):
        validate_input_manifest(
            output_dir / "manifest.json", output_dir / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=get_line_ledger_contract(),
        )

    nonmodel_substitution_rows = json.loads(json.dumps(batches))
    substitution_batch, substitution_line = next(
        (batch, line)
        for batch in nonmodel_substitution_rows
        for line in batch["source_line_catalog"]
        if line["model_decision_required"] is False
        and len(line["line_text"]) > 0
    )
    nonmodel_line_id = substitution_line["source_line_id"]
    assert substitution_line["model_decision_required"] is False
    assert all(
        target["source_line_id"] != nonmodel_line_id
        and nonmodel_line_id not in target["allowed_anchor_source_line_ids"]
        for target in substitution_batch["targets"]
    )
    replacement_text = "X" * len(substitution_line["line_text"])
    substitution_line["line_text"] = replacement_text
    substitution_line["line_text_sha256"] = sha256_text(replacement_text)
    replacement_line_id = sha256_json({
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "parent_document_id": substitution_batch["parent_document_id"],
        "parent_line_index": substitution_line["parent_line_index"],
        "parent_start_char": substitution_line["parent_start_char"],
        "parent_end_char": substitution_line["parent_end_char"],
        "parent_full_end_char": substitution_line[
            "parent_full_end_char"
        ],
        "line_text_sha256": substitution_line["line_text_sha256"],
        "line_terminator": substitution_line["line_terminator"],
    })
    substitution_line["source_line_id"] = replacement_line_id
    recompute_batch_relations(substitution_batch)
    write_coordinated_manifest_attack(nonmodel_substitution_rows)
    validate_input_manifest(
        output_dir / "manifest.json", output_dir / "chunks",
        stage=LINE_ADJUDICATION_STAGE,
        contract=get_line_ledger_contract(),
    )
    with pytest.raises(ValueError, match="externally frozen"):
        validate_expected_input_manifest_sha256(
            output_dir / "manifest.json", frozen_manifest_sha256
        )

    grouped_indices: dict[tuple[str, int], list[int]] = {}
    for index, batch in enumerate(batches):
        grouped_indices.setdefault((
            str(batch["parent_document_id"]), len(batch["targets"])
        ), []).append(index)
    source_index, replacement_index = next(
        indices[:2] for indices in grouped_indices.values()
        if len(indices) >= 2
    )
    duplicate_rows = json.loads(json.dumps(batches))
    replacement_batch_index = duplicate_rows[replacement_index][
        "batch_index_within_parent"
    ]
    duplicate_rows[replacement_index] = json.loads(json.dumps(
        duplicate_rows[source_index]
    ))
    duplicate_rows[replacement_index]["batch_index_within_parent"] = (
        replacement_batch_index
    )
    rebind_batch_identity(duplicate_rows[replacement_index])
    write_coordinated_manifest_attack(duplicate_rows)
    with pytest.raises(ValueError, match="frozen exception coverage"):
        validate_input_manifest(
            output_dir / "manifest.json", output_dir / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=get_line_ledger_contract(),
        )

    closure_rows = json.loads(json.dumps(batches))
    closure_rows[0]["batch_index_within_parent"] += 100
    rebind_batch_identity(closure_rows[0])
    write_coordinated_manifest_attack(closure_rows)
    with pytest.raises(ValueError, match="batch-index closure"):
        validate_input_manifest(
            output_dir / "manifest.json", output_dir / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=get_line_ledger_contract(),
        )

    write_coordinated_manifest_attack(json.loads(json.dumps(batches[:-1])))
    with pytest.raises(ValueError, match="frozen exception coverage"):
        validate_input_manifest(
            output_dir / "manifest.json", output_dir / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=get_line_ledger_contract(),
        )


@pytest.mark.skipif(
    not HAS_FROZEN_INPUTS,
    reason="exact suffix-b Stage-A and suffix-p source exports are unavailable",
)
def test_real_two_pass_stage_b_replay_and_metadata_tamper(
    tmp_path: Path,
) -> None:
    stage_a2_export, stage_a2_output = _materialize_valid_stage_a2_run(
        tmp_path
    )
    loaded = load_validated_stage_run(
        stage_a2_export,
        stage_a2_output,
        stage=LINE_ADJUDICATION_STAGE,
    )
    assert len(loaded[1]) == len(loaded[2]) == 29
    assert loaded[3]["typed_restart_replay_complete"] is True

    metadata_path = stage_a2_output / "run_metadata.json"
    valid_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    attacks = []
    forged_recovery = json.loads(json.dumps(valid_metadata))
    forged_recovery["recovery_attestation"] = "forged-string"
    attacks.append(forged_recovery)
    unknown = json.loads(json.dumps(valid_metadata))
    unknown["unknown_field"] = True
    attacks.append(unknown)
    bad_count = json.loads(json.dumps(valid_metadata))
    bad_count["technical_pass_outputs_this_run"] = 999_999
    attacks.append(bad_count)
    negative = json.loads(json.dumps(valid_metadata))
    negative["output_tokens_total_this_run"] = -123
    attacks.append(negative)
    bad_budget = json.loads(json.dumps(valid_metadata))
    bad_budget["max_model_len"] = FROZEN_MAX_MODEL_LEN - 1
    attacks.append(bad_budget)
    first_output = next(
        next(read_jsonl_gz(path))
        for path in sorted(stage_a2_output.glob("*.out.jsonl.gz"))
    )
    partial = json.loads(json.dumps(valid_metadata))
    partial.update({
        "processed_outputs_this_run": 1,
        "processed_schema_variant_counts_this_run": {
            first_output["json_schema_variant_id"]: 1
        },
        "technical_pass_outputs_this_run": 1,
        "parse_status_counts_this_run": {"parsed": 1},
        "finish_reason_counts_this_run": {"stop": 1},
        "output_tokens_total_this_run": first_output["output_tokens"],
        "output_tokens_max_this_run": first_output["output_tokens"],
        "prompt_tokens_max_this_run": first_output["prompt_tokens_used"],
        "minimum_context_margin_tokens_this_run": first_output[
            "context_margin_tokens"
        ],
    })
    attacks.append(partial)
    for attack in attacks:
        metadata_path.write_text(
            json.dumps(attack, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError):
            load_validated_stage_run(
                stage_a2_export,
                stage_a2_output,
                stage=LINE_ADJUDICATION_STAGE,
            )

    contract = get_line_ledger_contract()
    manifest_sha = sha256_file(stage_a2_export / "manifest.json")
    batches = [
        row
        for path in sorted((stage_a2_export / "chunks").glob("*.jsonl.gz"))
        for row in read_jsonl_gz(path)
    ]
    recovery_args = Namespace(
        stage=LINE_ADJUDICATION_STAGE,
        seed=FROZEN_GENERATION_SEED,
        max_model_len=FROZEN_MAX_MODEL_LEN,
        max_output_tokens=2_048,
        shard_index=None,
        shard_count=None,
    )
    recovery_metadata = _run_metadata(
        args=recovery_args,
        contract=contract,
        generation_hash=generation_config_sha256(
            recovery_args, contract, input_manifest_sha256=manifest_sha
        ),
        input_manifest_sha256=manifest_sha,
        selected=len(batches),
        selected_variant_inventory=schema_variant_inventory(batches),
        selected_stage_a_provenance_sha256=[
            str(batch["stage_a_provenance_sha256"]) for batch in batches
        ],
        rows=[],
        started="2026-08-25T00:00:00+00:00",
        recovery=True,
    )
    metadata_path.write_text(
        json.dumps(recovery_metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert load_validated_stage_run(
        stage_a2_export,
        stage_a2_output,
        stage=LINE_ADJUDICATION_STAGE,
    )[3]["typed_restart_replay_complete"] is True

    metadata_path.write_text(
        json.dumps(valid_metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    stage_b_export = tmp_path / "stage_b_export"
    stage_b_manifest = build_adjudicated_record_extraction_export(
        DEFAULT_FULL_EXPORT_DIR,
        STAGE_A_EXPORT,
        STAGE_A_OUTPUT,
        stage_a2_export,
        stage_a2_output,
        stage_b_export,
    )
    stage_b_manifest = _mark_test_manifest_frozen(
        stage_b_export / "manifest.json", stage_b_manifest
    )
    with pytest.raises(ValueError):
        validate_input_manifest(
            stage_b_export / "manifest.json",
            stage_b_export / "chunks",
            stage="record_extraction",
            contract=contract,
        )
    return
    assert validated_stage_b["two_pass_merge"]["validation"][
        "validation_passed"
    ] is True

    replayed = evaluator.replay_stage_a2_merge(
        STAGE_A_EXPORT,
        STAGE_A_OUTPUT,
        stage_a2_export,
        stage_a2_output,
        DEFAULT_FULL_EXPORT_DIR,
    )
    provenance_attacks = []
    wrong_a1 = json.loads(json.dumps(stage_b_manifest))
    wrong_a1["upstream_stage_a"]["input_manifest_sha256"] = "9" * 64
    provenance_attacks.append(wrong_a1)
    wrong_a2 = json.loads(json.dumps(stage_b_manifest))
    wrong_a2["upstream_stage_a2"]["output_files"][0]["sha256"] = "8" * 64
    provenance_attacks.append(wrong_a2)
    wrong_ownership = json.loads(json.dumps(stage_b_manifest))
    wrong_ownership["two_pass_merge"][
        "ownership_by_continuation_sha256"
    ] = "7" * 64
    provenance_attacks.append(wrong_ownership)
    wrong_count = json.loads(json.dumps(stage_b_manifest))
    wrong_count["two_pass_merge"]["validation"][
        "expected_exception_count"
    ] += 1
    provenance_attacks.append(wrong_count)
    replayed_a2_artifacts = dict(replayed["stage_a2_artifacts"])
    replayed_a2_artifacts.pop("stage_a2_decision_ledger_sha256")
    for attack in provenance_attacks:
        with pytest.raises(ValueError, match="replay binding"):
            builder.validate_two_pass_stage_b_manifest_provenance(
                attack,
                replayed["stage_a_artifacts"],
                replayed_a2_artifacts,
                replayed["stage_a2_decisions"],
                replayed["stage_a2_merge"],
            )

    first_path = stage_b_paths[0]
    stage_b_rows = list(read_jsonl_gz(first_path))
    stage_b_rows[0]["targets"][0]["anchor_line_decision_provenance"][
        "final_role"
    ] = "nonrecord_content"
    write_jsonl_gz(first_path, stage_b_rows)
    entry = next(
        item for item in stage_b_manifest["files"]["chunks"]
        if item["file"] == first_path.name
    )
    entry.update({
        "records": len(stage_b_rows),
        "content_sha256": sha256_json(stage_b_rows),
        "bytes": first_path.stat().st_size,
        "sha256": sha256_file(first_path),
    })
    _mark_test_manifest_frozen(
        stage_b_export / "manifest.json", stage_b_manifest
    )
    with pytest.raises(ValueError, match="anchor provenance differs"):
        validate_input_manifest(
            stage_b_export / "manifest.json",
            stage_b_export / "chunks",
            stage="record_extraction",
            contract=contract,
        )


@pytest.mark.skipif(
    not HAS_FROZEN_INPUTS,
    reason="exact suffix-b Stage-A and suffix-p source exports are unavailable",
)
def test_suffix_b_loader_rejects_one_byte_output_tamper(tmp_path: Path) -> None:
    copied_export = tmp_path / "stage_a_export"
    copied_output = tmp_path / "stage_a_output"
    shutil.copytree(STAGE_A_EXPORT, copied_export)
    shutil.copytree(STAGE_A_OUTPUT, copied_output)
    target = copied_output / "chunk_00000.out.jsonl.gz"
    target.write_bytes(target.read_bytes() + b"x")
    with pytest.raises(ValueError, match="SHA-256|bytes"):
        load_frozen_stage_a_suffix_b(copied_export, copied_output)


@pytest.mark.skipif(
    not HAS_FROZEN_INPUTS,
    reason="exact suffix-b Stage-A and suffix-p source exports are unavailable",
)
def test_generic_evaluator_loader_routes_frozen_stage_a_to_historical_replay() -> None:
    manifest, batches, rows, artifacts = evaluator.load_validated_run(
        STAGE_A_EXPORT, STAGE_A_OUTPUT, stage=LINE_DECISION_STAGE
    )
    assert manifest["line_ledger_contract_sha256"] == (
        artifacts["stage_a_line_ledger_contract_sha256"]
    )
    assert len(batches) == len(rows) == 113
    assert artifacts["exact_frozen_byte_inventory_validated"] is True


def test_stage_a2_evaluator_gates_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        evaluator,
        "evaluate_pilot_observations",
        lambda observations, issue_control: {"checks": []},
    )
    validation = {
        "expected_exception_count": 206,
        "adjudicated_exception_count": 206,
        "unresolved_line_count": 0,
        "withheld_line_count": 0,
        "unowned_continuation_count": 0,
        "duplicate_continuation_owner_count": 0,
        "ownership_conflict_count": 0,
        "coverage_complete": True,
        "validation_passed": True,
    }
    observations = {
        "stage_a2_merge_validation": validation,
        "stage_a2_candidate_self_exception_line_ids": [],
        "stage_a_roles": dict(STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES),
        "stage_b_dispositions": {},
        "parents": {},
    }
    assert evaluator.evaluate_stage_a2_pilot_observations(
        observations, {}
    )["gate_decision"] == "GO"

    for field, bad_value in (
        ("adjudicated_exception_count", 205),
        ("unresolved_line_count", 1),
        ("withheld_line_count", 1),
        ("unowned_continuation_count", 1),
        ("duplicate_continuation_owner_count", 1),
        ("ownership_conflict_count", 1),
        ("coverage_complete", False),
        ("validation_passed", False),
    ):
        attacked = json.loads(json.dumps(observations))
        attacked["stage_a2_merge_validation"][field] = bad_value
        assert evaluator.evaluate_stage_a2_pilot_observations(
            attacked, {}
        )["gate_decision"] == "NO_GO"

    candidate_attack = json.loads(json.dumps(observations))
    candidate_id = next(iter(STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES))
    candidate_attack["stage_a_roles"][candidate_id] = "record_anchor"
    candidate_attack["stage_b_dispositions"][candidate_id] = "records"
    assert evaluator.evaluate_stage_a2_pilot_observations(
        candidate_attack, {}
    )["gate_decision"] == "NO_GO"


def test_stage_a2_only_gate_covers_every_pre_stage_b_control() -> None:
    validation = {
        "expected_exception_count": 206,
        "adjudicated_exception_count": 206,
        "unresolved_line_count": 0,
        "withheld_line_count": 0,
        "unowned_continuation_count": 0,
        "duplicate_continuation_owner_count": 0,
        "ownership_conflict_count": 0,
        "coverage_complete": True,
        "validation_passed": True,
    }
    quote_id = PILOT_CONTENT_GATES[PILOT_PARENT_IDS[2]][
        "required_source_line_id"
    ]
    compound_id = PILOT_CONTENT_GATES[PILOT_PARENT_IDS[3]][
        "required_compound_source_line_id"
    ]
    json_id = PILOT_CONTENT_GATES[PILOT_PARENT_IDS[5]][
        "required_source_line_id"
    ]
    vets_id = PILOT_CONTENT_GATES[PILOT_PARENT_IDS[7]][
        "required_source_line_id"
    ]
    testimonial_owners = set(
        LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR.values()
    )
    roles = {
        **{
            line_id: "navigation_form_footer"
            for line_id in (
                *TANAKA_INTERFACE_LINE_TEXT,
                *CHAMPION_INTERFACE_LINE_TEXT,
            )
        },
        **{line_id: "record_anchor" for line_id in testimonial_owners},
        **STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES,
        **{
            line_id: "record_continuation_next"
            for line_id in STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS
        },
        **STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS,
        quote_id: "record_anchor",
        compound_id: "compound_record_anchor",
        json_id: "nonrecord_content",
        vets_id: "record_anchor",
    }
    observations = {
        "stage_a2_merge_validation": validation,
        "candidate_self_exception_line_ids": [],
        "final_roles": roles,
        "ownership_by_continuation": dict(
            LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR
        ) | dict(
            STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS
        ),
        "anchor_counts_by_parent": {
            PILOT_PARENT_IDS[0]: 390,
            PILOT_PARENT_IDS[1]: 230,
        },
    }
    assert evaluator.evaluate_stage_a2_only_observations(observations)[
        "gate_decision"
    ] == "GO"

    def assert_attack(mutate, expected_gate: str) -> None:
        attacked = json.loads(json.dumps(observations))
        mutate(attacked)
        report = evaluator.evaluate_stage_a2_only_observations(attacked)
        assert report["gate_decision"] == "NO_GO"
        assert expected_gate in {
            check["gate"] for check in report["checks"]
            if not check["passed"]
        }

    for field, bad_value, gate in (
        ("expected_exception_count", 205, "stage_a2_exception_coverage"),
        ("adjudicated_exception_count", 205, "stage_a2_exception_coverage"),
        ("coverage_complete", False, "stage_a2_exception_coverage"),
        ("unresolved_line_count", 1, "stage_a2_zero_unresolved_or_withheld"),
        ("withheld_line_count", 1, "stage_a2_zero_unresolved_or_withheld"),
        (
            "unowned_continuation_count", 1,
            "stage_a2_continuation_ownership_complete",
        ),
        (
            "duplicate_continuation_owner_count", 1,
            "stage_a2_continuation_ownership_complete",
        ),
        (
            "ownership_conflict_count", 1,
            "stage_a2_continuation_ownership_complete",
        ),
        ("validation_passed", False, "stage_a2_final_validation_passed"),
    ):
        assert_attack(
            lambda row, field=field, value=bad_value: row[
                "stage_a2_merge_validation"
            ].__setitem__(field, value),
            gate,
        )

    for line_id in STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS:
        assert_attack(
            lambda row, target=line_id: row[
                "ownership_by_continuation"
            ].__setitem__(target, "wrong-anchor"),
            "stage_a2_exact_nine_unowned_resolutions",
        )
    attachment_id = next(iter(STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS))
    assert_attack(
        lambda row: row["final_roles"].__setitem__(
            attachment_id, "record_continuation_previous"
        ),
        "stage_a2_exact_nine_unowned_resolutions",
    )
    for line_id in STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS:
        assert_attack(
            lambda row, target=line_id: row["final_roles"].__setitem__(
                target, "record_anchor"
            ),
            "stage_a2_exact_nine_unowned_resolutions",
        )
    terminal_id = next(iter(STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS))
    assert_attack(
        lambda row: row["ownership_by_continuation"].__setitem__(
            terminal_id, next(iter(testimonial_owners))
        ),
        "stage_a2_exact_nine_unowned_resolutions",
    )

    assert_attack(
        lambda row: row["final_roles"].__setitem__(
            next(iter(TANAKA_INTERFACE_LINE_TEXT)), "record_anchor"
        ),
        "stage_a2_all_33_interface_lines_excluded",
    )
    assert_attack(
        lambda row: row["final_roles"].__setitem__(json_id, "record_anchor"),
        "stage_a2_long_json_nonrecord",
    )
    for candidate_id in STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES:
        assert_attack(
            lambda row, line_id=candidate_id: row["final_roles"].__setitem__(
                line_id, "record_continuation_previous"
            ),
            "stage_a2_exact_candidate_controls",
        )
    assert_attack(
        lambda row: row["ownership_by_continuation"].__setitem__(
            next(iter(LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR)),
            "wrong-anchor",
        ),
        "stage_a2_exact_testimonial_ownership",
    )
    assert_attack(
        lambda row: row["final_roles"].__setitem__(
            next(iter(testimonial_owners)), "nonrecord_content"
        ),
        "stage_a2_exact_testimonial_ownership",
    )
    for line_id, wrong_role in (
        (quote_id, "nonrecord_content"),
        (compound_id, "record_anchor"),
        (vets_id, "compound_record_anchor"),
    ):
        assert_attack(
            lambda row, target=line_id, role=wrong_role: row[
                "final_roles"
            ].__setitem__(target, role),
            "stage_a2_quote_compound_vets_anchor_roles",
        )
    for parent_id, bad_count in (
        (PILOT_PARENT_IDS[0], 389),
        (PILOT_PARENT_IDS[1], 229),
    ):
        assert_attack(
            lambda row, parent=parent_id, count=bad_count: row[
                "anchor_counts_by_parent"
            ].__setitem__(parent, count),
            "stage_a2_dense_parent_anchor_counts",
        )


def test_stage_a2_only_cli_has_no_stage_b_dependency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: dict[str, object] = {}

    def fake_evaluate(*args):
        called["args"] = args
        return {
            "gate_decision": "GO",
            "passed_checks": 10,
            "failed_checks": 0,
        }

    report_path = tmp_path / "a2_report.json"
    monkeypatch.setattr(evaluator, "evaluate_stage_a2_only", fake_evaluate)
    monkeypatch.setattr(sys, "argv", [
        "evaluate_candidate_endorsement_line_ledger_pilot.py",
        "--stage-a-export-dir", "a-export",
        "--stage-a-output-dir", "a-output",
        "--stage-a2-export-dir", "a2-export",
        "--stage-a2-output-dir", "a2-output",
        "--output-report", str(report_path),
    ])
    assert evaluator.main() == 0
    assert called["args"][:4] == (
        Path("a-export"), Path("a-output"),
        Path("a2-export"), Path("a2-output"),
    )
    assert json.loads(report_path.read_text())["gate_decision"] == "GO"


def test_post_go_stage_b_builder_requires_exact_two_pass_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_a_provenance = {
        "stage_a_line_ledger_contract_sha256": "a" * 64,
        "stage_a_input_manifest_sha256": "b" * 64,
        "stage_a_generation_config_sha256": "c" * 64,
        "stage_a_ordered_output_batch_ids_sha256": "d" * 64,
        "source_line_decision_ledger_sha256": "e" * 64,
        "stage_a_provenance_sha256": "f" * 64,
    }
    stage_a_manifest = {
        "selected_parent_ids": list(PILOT_PARENT_IDS),
        "complete_parent_closure": True,
        "source_export": {"source_export_manifest_sha256": "source"},
    }
    stage_a2_manifest = {
        "selected_parent_ids": list(PILOT_PARENT_IDS),
        "complete_parent_closure": True,
        "source_export": {"source_export_manifest_sha256": "source"},
        "stage_a_provenance": stage_a_provenance,
        "stage_a_exception_count": FROZEN_STAGE_A2_EXCEPTION_COUNT,
    }
    stage_a_decisions = {"line": {"role": "record_anchor"}}
    stage_a2_batches = [{"batch_id": "a2-batch"}]
    stage_a2_rows = [{"batch_id": "a2-batch"}]
    stage_a2_artifacts = {
        "input_manifest_sha256": "1" * 64,
        "generation_config_sha256": "2" * 64,
        "output_files": [{"file": "chunk.out", "sha256": "3" * 64}],
    }
    parents = [
        {"parent_document_id": parent_id} for parent_id in PILOT_PARENT_IDS
    ]
    final_decisions = {"line": {"role": "record_anchor"}}
    per_line_provenance = {"line": {"stage_a_proposal": {"role": "record_anchor"}}}
    merge = {
        "validation": {
            "expected_exception_count": 206,
            "adjudicated_exception_count": 206,
            "unresolved_line_count": 0,
            "withheld_line_count": 0,
            "unowned_continuation_count": 0,
            "duplicate_continuation_owner_count": 0,
            "ownership_conflict_count": 0,
            "coverage_complete": True,
            "validation_passed": True,
        },
        "final_decisions": final_decisions,
        "per_line_provenance": per_line_provenance,
        "ownership_by_continuation": {},
        "final_decisions_sha256": sha256_json(final_decisions),
        "per_line_provenance_sha256": sha256_json(per_line_provenance),
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        builder,
        "load_frozen_stage_a_suffix_b",
        lambda *_args: (
            stage_a_manifest, [], [], stage_a_decisions, stage_a_provenance
        ),
    )
    monkeypatch.setattr(
        builder,
        "load_validated_stage_run",
        lambda *_args, **_kwargs: (
            stage_a2_manifest, stage_a2_batches, stage_a2_rows,
            stage_a2_artifacts,
        ),
    )
    monkeypatch.setattr(
        builder, "replay_frozen_source",
        lambda *_args, **_kwargs: {"manifest_sha256": "source"},
    )
    monkeypatch.setattr(
        builder, "reconstruct_selected_parents", lambda *_args: parents
    )
    monkeypatch.setattr(
        builder, "build_line_adjudication_batches",
        lambda *_args, **_kwargs: stage_a2_batches,
    )
    monkeypatch.setattr(
        builder, "line_adjudication_ledger",
        lambda *_args: {"line": {"stage_a2_decision": {"decision": "role"}}},
    )
    monkeypatch.setattr(
        builder, "merge_line_adjudication_ledger", lambda *_args: merge
    )

    def fake_stage_b(_parents, merged):
        captured["decisions"] = merged["final_decisions"]
        captured["provenance"] = merged["per_line_provenance"]
        return [{"batch_id": "stage-b"}]

    monkeypatch.setattr(
        builder,
        "build_record_extraction_batches_from_adjudicated_merge",
        fake_stage_b,
    )
    monkeypatch.setattr(builder, "_selection_rows", lambda *_args, **_kwargs: [{}])
    monkeypatch.setattr(builder, "write_selection", lambda *_args: {"file": "selection.csv"})
    monkeypatch.setattr(builder, "write_batch_chunks", lambda *_args: [{"file": "chunk.gz"}])
    monkeypatch.setattr(
        builder,
        "_base_manifest",
        lambda **_kwargs: {"stage": "record_extraction"},
    )
    monkeypatch.setattr(
        builder, "finalize_manifest", lambda _path, payload: dict(payload)
    )

    result = builder.build_adjudicated_record_extraction_export(
        tmp_path / "source",
        tmp_path / "a-export",
        tmp_path / "a-output",
        tmp_path / "a2-export",
        tmp_path / "a2-output",
        tmp_path / "b-export",
        enforce_frozen_source=False,
    )
    assert captured == {
        "decisions": final_decisions,
        "provenance": per_line_provenance,
    }
    assert result["two_pass_merge"]["validation"]["validation_passed"] is True
    assert result["upstream_stage_a2"]["input_manifest_sha256"] == "1" * 64

    attacked = json.loads(json.dumps(merge))
    attacked["validation"]["withheld_line_count"] = 1
    attacked["validation"]["validation_passed"] = False
    monkeypatch.setattr(
        builder, "merge_line_adjudication_ledger", lambda *_args: attacked
    )
    with pytest.raises(ValueError, match="final merge gate"):
        builder.build_adjudicated_record_extraction_export(
            tmp_path / "source-2",
            tmp_path / "a-export-2",
            tmp_path / "a-output-2",
            tmp_path / "a2-export-2",
            tmp_path / "a2-output-2",
            tmp_path / "b-export-2",
            enforce_frozen_source=False,
        )


def test_current_cli_cannot_bypass_stage_a2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "argv", [
        "build_candidate_endorsement_line_ledger.py",
        "--mode", "record-extraction-from-stage-a",
        "--output-dir", str(tmp_path / "forbidden"),
    ])
    with pytest.raises(SystemExit, match="current contract requires"):
        builder.main()
