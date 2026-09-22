from __future__ import annotations

import argparse
import copy
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import build_candidate_endorsement_round4_human_review_app as review
from scripts.build_candidate_endorsement_round4_development_inputs import (
    _write_artifact,
    build_fresh_stage_a_batches,
    build_fresh_stage_b_artifact,
    sha256_file,
    sha256_json,
)
from scripts.hive import (
    extract_candidate_endorsement_round4_development_vllm as runner,
)
from src.candidate_endorsement_line_ledger import LINE_INVENTORY_SPEC_SHA256


def _line(
    parent_id: str,
    index: int,
    text: str,
    start: int,
    *,
    model: bool,
) -> dict:
    terminator = "\n"
    end = start + len(text)
    full_end = end + len(terminator)
    text_sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
    identity = {
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "parent_document_id": parent_id,
        "parent_line_index": index,
        "parent_start_char": start,
        "parent_end_char": end,
        "parent_full_end_char": full_end,
        "line_text_sha256": text_sha256,
        "line_terminator": terminator,
    }
    return {
        "source_line_id": sha256_json(identity),
        "parent_line_index": index,
        "parent_start_char": start,
        "parent_end_char": end,
        "parent_full_end_char": full_end,
        "line_text": text,
        "line_text_sha256": text_sha256,
        "line_terminator": terminator,
        "model_decision_required": model,
        "deterministic_disposition": (
            "model_decision_required"
            if model
            else "outside_attested_context_scope"
        ),
    }


def _parent(index: int) -> dict:
    parent_id = hashlib.sha256(f"parent:{index}".encode()).hexdigest()
    heading = "ENDORSEMENTS"
    name = f"Alice {index:02d}"
    first = _line(parent_id, 0, heading, 0, model=False)
    second = _line(
        parent_id,
        1,
        name,
        first["parent_full_end_char"],
        model=True,
    )
    source = f"{heading}\n{name}\n"
    return {
        "candidate_cycle_id": f"candidate-{index:02d}-2024",
        "candidate_icpsr": f"Candidate, {index:02d}",
        "state": "CA",
        "office": "house",
        "year": 2024,
        "party": "democrat" if index % 2 == 0 else "republican",
        "parent_document_id": parent_id,
        "extraction_source_text_sha256": hashlib.sha256(
            source.encode("utf-8")
        ).hexdigest(),
        "extraction_source_n_chars": len(source),
        "endorsement_line_context": {"context_text": heading},
        "parent_source_text": source,
        "segment_descriptors": [],
        "lines": [first, second],
    }


def _parent_with_names(index: int, names: list[str]) -> dict:
    parent_id = hashlib.sha256(f"parent:{index}".encode()).hexdigest()
    heading = "ENDORSEMENTS"
    lines = [_line(parent_id, 0, heading, 0, model=False)]
    cursor = lines[0]["parent_full_end_char"]
    for line_index, name in enumerate(names, start=1):
        line = _line(parent_id, line_index, name, cursor, model=True)
        lines.append(line)
        cursor = line["parent_full_end_char"]
    source = "".join(
        line["line_text"] + line["line_terminator"] for line in lines
    )
    return {
        "candidate_cycle_id": f"candidate-{index:02d}-2024",
        "candidate_icpsr": f"Candidate, {index:02d}",
        "state": "CA",
        "office": "house",
        "year": 2024,
        "party": "democrat" if index % 2 == 0 else "republican",
        "parent_document_id": parent_id,
        "extraction_source_text_sha256": hashlib.sha256(
            source.encode("utf-8")
        ).hexdigest(),
        "extraction_source_n_chars": len(source),
        "endorsement_line_context": {"context_text": heading},
        "parent_source_text": source,
        "segment_descriptors": [],
        "lines": lines,
    }


def _runner_args(stage: str) -> argparse.Namespace:
    return argparse.Namespace(
        stage=stage,
        seed=runner.GENERATION_SEED,
        max_model_len=runner.MAX_MODEL_LEN,
        max_output_tokens=runner.MAX_OUTPUT_TOKENS[stage],
        gpu_memory_utilization=runner.GPU_MEMORY_UTILIZATION,
    )


def _write_completed_run(
    input_dir: Path,
    output_dir: Path,
    *,
    stage: str,
    model_output_for_batch,
) -> None:
    manifest_path = input_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_sha256 = sha256_file(manifest_path)
    args = _runner_args(stage)
    generation_sha256 = runner.generation_config_sha256(
        args, input_manifest_sha256=manifest_sha256
    )
    output_dir.mkdir()
    all_rows = []
    selected_paths = []
    for entry in manifest["chunks"]:
        input_path = input_dir / "chunks" / entry["file"]
        selected_paths.append(input_path)
        batches = runner.read_gzip_jsonl(input_path)
        rows = []
        for batch in batches:
            model_output = model_output_for_batch(batch)
            raw_output = runner.canonical_json(model_output)
            rows.append(
                runner.derive_output_row(
                    batch=batch,
                    raw_output=raw_output,
                    finish_reason="stop",
                    stop_reason=None,
                    output_tokens=100,
                    prompt_tokens=500,
                    args=args,
                    input_manifest_sha256=manifest_sha256,
                    input_chunk_sha256=sha256_file(input_path),
                    generation_config_sha256_value=generation_sha256,
                )
            )
        output_name = entry["file"].replace(".jsonl.gz", ".out.jsonl.gz")
        (output_dir / output_name).write_bytes(runner._gzip_jsonl_bytes(rows))
        all_rows.extend(rows)
    metadata = runner.run_metadata(
        args=args,
        input_manifest_sha256=manifest_sha256,
        generation_config_sha256_value=generation_sha256,
        selected_chunks=selected_paths,
        rows=all_rows,
        started_at="2026-09-07T00:00:00+00:00",
    )
    (output_dir / "run_metadata.json").write_text(
        runner.canonical_json(metadata) + "\n", encoding="utf-8"
    )


@pytest.fixture()
def authenticated_run(tmp_path: Path, request, monkeypatch) -> dict:
    parents = [_parent(index) for index in range(review.EXPECTED_PARENT_COUNT)]
    selected_ids = [parent["parent_document_id"] for parent in parents]
    sample_round = getattr(request, "param", 3)
    provenance = {
        "sample_selection_sha256": "1" * 64,
        "sample_selection_spec_sha256": "2" * 64,
        "source_export_manifest_sha256": "3" * 64,
        "sealed_reserve_source_text_reconstructed": False,
        "ai_draft_used_in_prompt": False,
    }
    if sample_round == 4:
        pairs = {
            parent["parent_document_id"]: parent["extraction_source_text_sha256"]
            for parent in parents
        }
        binding = copy.deepcopy(runner.INDEPENDENT_SAMPLE4_BINDING)
        binding["selected_parent_ids_sha256"] = sha256_json(sorted(selected_ids))
        binding["selected_source_pairs_sha256"] = sha256_json(pairs)
        monkeypatch.setattr(runner, "INDEPENDENT_SAMPLE4_BINDING", binding)
        provenance.update(
            independent_sample_binding=binding,
            independent_sample_source_pairs=pairs,
        )
    stage_a_input = tmp_path / "stage-a-input"
    stage_a_batches = build_fresh_stage_a_batches(parents)
    _write_artifact(
        output_dir=stage_a_input,
        stage="line_decision",
        batches=stage_a_batches,
        selected_parent_ids=selected_ids,
        provenance=provenance,
        chunk_size=60,
        sample_round=sample_round,
    )
    stage_a_manifest_sha256 = sha256_file(stage_a_input / "manifest.json")
    stage_a_output = tmp_path / "stage-a-output"

    def stage_a_model(batch: dict) -> dict:
        output = {
            slot: {"role": "record_anchor"} for slot in batch["target_slots"]
        }
        output.update(
            {slot: {"role": "padding"} for slot in batch["padding_slots"]}
        )
        return output

    _write_completed_run(
        stage_a_input,
        stage_a_output,
        stage="line_decision",
        model_output_for_batch=stage_a_model,
    )
    stage_b_input = tmp_path / "stage-b-input"
    build_fresh_stage_b_artifact(
        stage_a_input,
        stage_a_output,
        stage_b_input,
        expected_stage_a_input_manifest_sha256=stage_a_manifest_sha256,
        chunk_size=60,
    )
    stage_b_manifest_sha256 = sha256_file(stage_b_input / "manifest.json")
    stage_b_output = tmp_path / "stage-b-output"

    def stage_b_model(batch: dict) -> dict:
        output = {}
        for target in batch["targets"]:
            parent_position = selected_ids.index(batch["parent_document_id"])
            if parent_position == 1:
                decision = {
                    "disposition": "unclear",
                    "source_mentions_fully_accounted": False,
                    "records": [],
                }
            elif parent_position == 2:
                decision = {
                    "disposition": "not_endorsement",
                    "source_mentions_fully_accounted": True,
                    "records": [],
                }
            else:
                decision = {
                    "disposition": "records",
                    "source_mentions_fully_accounted": True,
                    "records": [
                        {
                            "name_parts": [target["line_text"]],
                            "entity_type": (
                                "unclear" if parent_position == 0 else "individual"
                            ),
                            "endorser_category": (
                                "unclear"
                                if parent_position == 0
                                else "private_individual"
                            ),
                            "title": "Mayor" if parent_position == 0 else None,
                            "affiliation": None,
                            "jurisdiction": None,
                        }
                    ],
                }
            output[target["slot"]] = decision
        output.update(
            {
                slot: {
                    "disposition": "padding",
                    "source_mentions_fully_accounted": True,
                    "records": [],
                }
                for slot in batch["padding_slots"]
            }
        )
        return output

    _write_completed_run(
        stage_b_input,
        stage_b_output,
        stage="record_extraction",
        model_output_for_batch=stage_b_model,
    )
    stage_b_output_hashes = {
        path.name: sha256_file(path) for path in stage_b_output.iterdir()
    }
    draft, report, reconstructed, manifest = (
        review.prepare_provisional_draft_from_authenticated_runs(
            stage_a_input_dir=stage_a_input,
            stage_a_output_dir=stage_a_output,
            expected_stage_a_input_manifest_sha256=stage_a_manifest_sha256,
            stage_b_input_dir=stage_b_input,
            stage_b_output_dir=stage_b_output,
            expected_stage_b_input_manifest_sha256=stage_b_manifest_sha256,
        )
    )
    return {
        "parents": reconstructed,
        "stage_a_input": stage_a_input,
        "stage_a_output": stage_a_output,
        "stage_a_manifest": manifest,
        "stage_a_manifest_sha256": stage_a_manifest_sha256,
        "stage_b_input": stage_b_input,
        "stage_b_output": stage_b_output,
        "stage_b_manifest_sha256": stage_b_manifest_sha256,
        "draft": draft,
        "report": report,
        "stage_b_output_hashes": stage_b_output_hashes,
    }


def _task(run: dict) -> tuple[dict, str]:
    draft_raw = review.canonical_bytes(run["draft"])
    draft_sha256 = review.sha256_bytes(draft_raw)
    task = review.build_task(
        parents=run["parents"],
        stage_a_manifest=run["stage_a_manifest"],
        stage_a_manifest_sha256=run["stage_a_manifest_sha256"],
        draft=run["draft"],
        draft_sha256=draft_sha256,
    )
    return task, review.sha256_bytes(review.canonical_bytes(task))


def _complete_results(task: dict, task_sha256: str) -> dict:
    responses = []
    for parent in task["parents"]:
        children = []
        for seed in parent["ai_children"]:
            children.append(
                {
                    "human_confirmed": True,
                    "entity_type": (
                        "individual"
                        if seed["entity_type"] == "unclear"
                        else seed["entity_type"]
                    ),
                    "name_spans": copy.deepcopy(seed["name_spans"]),
                    "support_spans": copy.deepcopy(seed["support_spans"]),
                }
            )
        responses.append(
            {
                "parent_document_id": parent["parent_document_id"],
                "parent_confirmed": True,
                "true_zero": len(children) == 0,
                "children": children,
                "reviewed_unresolved_block_ids": [
                    block["unresolved_block_id"]
                    for block in parent["ai_unresolved_blocks"]
                ],
                "notes": "",
            }
        )
    return {
        "task_id": task["task_id"],
        "task_revision": task["task_revision"],
        "task_sha256": task_sha256,
        "format": review.RESULT_FORMAT,
        "exported_at": "2026-09-07T12:00:00.000Z",
        "parent_count": review.EXPECTED_PARENT_COUNT,
        "review_status": review.COMPLETED_REVIEW_STATUS,
        "authorizations": copy.deepcopy(review.RESULT_AUTHORIZATIONS),
        "responses": responses,
    }


@pytest.mark.parametrize("authenticated_run", [3, 4], indirect=True)
def test_converter_projects_exact_parent_spans_and_marks_uncertainty(
    authenticated_run: dict,
) -> None:
    draft = authenticated_run["draft"]
    report = authenticated_run["report"]
    assert draft["sample_round"] == authenticated_run["stage_a_manifest"]["sample_round"]

    assert len(draft["parents"]) == 40
    assert sum(len(parent["children"]) for parent in draft["parents"]) == 38
    first = draft["parents"][0]
    source = authenticated_run["parents"][0]["parent_source_text"]
    child = first["children"][0]
    assert " ".join(source[start:end] for start, end in child["name_spans"]) == (
        "Alice 00"
    )
    assert source[slice(*child["support_spans"][0])].strip() == "Alice 00"
    assert first["provisional_parent_disposition"] == "needs_review"
    assert draft["parents"][1]["provisional_parent_disposition"] == "needs_review"
    assert draft["parents"][2]["provisional_parent_disposition"] == "true_zero"
    assert draft["conversion"]["optional_field_policy"].startswith("sanitize_")
    assert draft["conversion"]["optional_field_change_count"] == 1
    assert len(report["optional_field_changes"]) == 1
    repair = report["optional_field_changes"][0]
    assert repair["field"] == "title"
    assert repair["original_value"] == "Mayor"
    assert repair["replacement_value"] is None
    salvage = [
        issue
        for issue in report["issues"]
        if issue["issue_type"] == "stage_b_failed_batch_slot_salvage"
    ]
    assert len(salvage) == 1
    assert salvage[0]["accepted_target_slots"] == ["slot_00"]
    assert salvage[0]["optional_repaired_target_slots"] == ["slot_00"]
    assert salvage[0]["unresolved_target_slots"] == []
    assert set(child) == review.DRAFT_CHILD_FIELDS
    assert report["needs_review_parent_count"] == 2
    assert draft["authorizations"] == review.SOURCE_AUTHORIZATIONS
    assert {
        path.name: sha256_file(path)
        for path in authenticated_run["stage_b_output"].iterdir()
    } == authenticated_run["stage_b_output_hashes"]


@pytest.mark.parametrize("authenticated_run", [3, 4], indirect=True)
def test_source_loader_authenticates_bound_sample_round(authenticated_run: dict) -> None:
    parents, manifest, manifest_hash = review.load_stage_a_source_artifact(
        authenticated_run["stage_a_input"],
        expected_manifest_sha256=authenticated_run["stage_a_manifest_sha256"],
    )
    assert manifest == authenticated_run["stage_a_manifest"]
    assert manifest_hash == authenticated_run["stage_a_manifest_sha256"]
    assert parents == authenticated_run["parents"]


def test_independent_adapter_artifact_loads_exact_source_fields(tmp_path: Path) -> None:
    from scripts import build_candidate_endorsement_independent40_stage_a as adapter

    output_dir = tmp_path / "independent-stage-a"
    expected_manifest, expected_hash = adapter.build_artifact(output_dir)
    parents, manifest, observed_hash = review.load_stage_a_source_artifact(
        output_dir, expected_manifest_sha256=expected_hash,
    )
    package = adapter.selection_helper.prior._authenticated_json(
        adapter.selection_helper.OUTPUT / "selected_sources.json",
        runner.INDEPENDENT_SAMPLE4_BINDING["selected_sources_sha256"],
        "Independent selected sources",
    )
    expected_parents = sorted(package["parents"], key=lambda parent: parent["parent_document_id"])
    assert manifest == expected_manifest
    assert observed_hash == expected_hash
    assert manifest["sample_round"] == 4
    assert len(parents) == len(expected_parents) == 40
    source_fields = (
        "candidate_cycle_id", "candidate_icpsr", "state", "office", "year", "party",
        "parent_document_id", "extraction_source_text_sha256", "extraction_source_n_chars",
        "endorsement_line_context", "parent_source_text",
    )
    line_fields = (
        "source_line_id", "parent_line_index", "parent_start_char", "parent_end_char",
        "parent_full_end_char", "line_text", "line_text_sha256", "line_terminator",
        "model_decision_required", "deterministic_disposition",
    )
    for parent, expected in zip(parents, expected_parents, strict=True):
        assert {key: parent[key] for key in source_fields} == {
            key: expected[key] for key in source_fields
        }
        assert parent["lines"] == [
            {key: line[key] for key in line_fields} for line in expected["lines"]
        ]
        assert parent["source_line_ids_sha256"] == sha256_json(
            [line["source_line_id"] for line in expected["lines"]]
        )
    assert set(manifest["provenance"]) == review.SOURCE_PROVENANCE_FIELDS | {
        "independent_sample_binding", "independent_sample_source_pairs",
    }
    assert manifest["provenance"]["sample_selection_sha256"] == (
        runner.INDEPENDENT_SAMPLE4_BINDING["selection_sha256"]
    )
    assert manifest["provenance"]["sample_selection_spec_sha256"] == (
        runner.INDEPENDENT_SAMPLE4_BINDING["selection_spec_sha256"]
    )


@pytest.mark.parametrize("authenticated_run", [4], indirect=True)
@pytest.mark.parametrize("mutation", ["unbound", "source_pair", "bound_wrong_source"])
def test_source_loader_rejects_invalid_sample_four_binding(
    authenticated_run: dict, mutation: str, monkeypatch,
) -> None:
    manifest = copy.deepcopy(authenticated_run["stage_a_manifest"])
    if mutation == "unbound":
        manifest["provenance"].pop("independent_sample_binding")
        manifest["provenance"].pop("independent_sample_source_pairs")
    else:
        pairs = manifest["provenance"]["independent_sample_source_pairs"]
        pairs[manifest["selected_parent_ids"][0]] = "0" * 64
        if mutation == "bound_wrong_source":
            binding = manifest["provenance"]["independent_sample_binding"]
            binding["selected_source_pairs_sha256"] = sha256_json(pairs)
            monkeypatch.setattr(runner, "INDEPENDENT_SAMPLE4_BINDING", binding)
    manifest.pop("manifest_payload_sha256")
    manifest["manifest_payload_sha256"] = sha256_json(manifest)
    path = authenticated_run["stage_a_input"] / "manifest.json"
    path.write_bytes(review.canonical_bytes(manifest))
    error = "independent sample binding" if mutation == "bound_wrong_source" else "Sample four"
    with pytest.raises(ValueError, match=error):
        review.load_stage_a_source_artifact(
            authenticated_run["stage_a_input"],
            expected_manifest_sha256=sha256_file(path),
        )


@pytest.mark.parametrize("authenticated_run", [3, 4], indirect=True)
def test_draft_loader_rejects_sample_round_mismatch(
    authenticated_run: dict, tmp_path: Path,
) -> None:
    draft = copy.deepcopy(authenticated_run["draft"])
    draft["sample_round"] = 4 if draft["sample_round"] == 3 else 3
    path = tmp_path / "mismatched-draft.json"
    path.write_bytes(review.canonical_bytes(draft))
    with pytest.raises(ValueError, match="draft identity"):
        review.load_provisional_draft(
            path, expected_sha256=sha256_file(path),
            parents=authenticated_run["parents"],
            stage_a_manifest=authenticated_run["stage_a_manifest"],
            stage_a_manifest_sha256=authenticated_run["stage_a_manifest_sha256"],
        )


@pytest.mark.parametrize("authenticated_run", [4], indirect=True)
def test_draft_loader_rejects_unbound_sample_four_source(
    authenticated_run: dict, tmp_path: Path,
) -> None:
    manifest = copy.deepcopy(authenticated_run["stage_a_manifest"])
    manifest["provenance"].pop("independent_sample_binding")
    path = tmp_path / "draft.json"
    path.write_bytes(review.canonical_bytes(authenticated_run["draft"]))
    with pytest.raises(ValueError, match="Sample four"):
        review.load_provisional_draft(
            path, expected_sha256=sha256_file(path),
            parents=authenticated_run["parents"], stage_a_manifest=manifest,
            stage_a_manifest_sha256=authenticated_run["stage_a_manifest_sha256"],
        )


@pytest.mark.parametrize("authenticated_run", [3, 4], indirect=True)
def test_stage_b_authenticator_rejects_stage_a_round_mismatch(
    authenticated_run: dict,
) -> None:
    stage_a_round = authenticated_run["stage_a_manifest"]["sample_round"]
    wrong_round = 4 if stage_a_round == 3 else 3
    with pytest.raises(ValueError, match="sample round"):
        review.authenticate_fresh_stage_b_run(
            authenticated_run["stage_b_input"], authenticated_run["stage_b_output"],
            expected_input_manifest_sha256=authenticated_run["stage_b_manifest_sha256"],
            stage_a_input_manifest_sha256=authenticated_run["stage_a_manifest_sha256"],
            stage_a_output_provenance={},
            selected_parent_ids=authenticated_run["stage_a_manifest"]["selected_parent_ids"],
            stage_a_sample_round=wrong_round,
        )


def test_task_seeds_every_ai_judgment_unconfirmed_and_html_has_review_gate(
    authenticated_run: dict,
) -> None:
    task, task_sha256 = _task(authenticated_run)
    html = review.render_html(task, task_sha256)

    assert task["parent_count"] == len(task["parents"]) == 40
    assert all(parent["human_parent_confirmed"] is False for parent in task["parents"])
    assert all(
        child["human_confirmed"] is False
        for parent in task["parents"]
        for child in parent["ai_children"]
    )
    assert all(
        block["human_reviewed"] is False
        for parent in task["parents"]
        for block in parent["ai_unresolved_blocks"]
    )
    assert task["human_truth_status"] == "pending_explicit_human_review"
    assert task["result_contract"]["review_status"] == review.COMPLETED_REVIEW_STATUS
    assert task["authorizations"] == review.REVIEW_AUTHORIZATIONS
    assert all(value is False for value in task["authorizations"].values())
    assert "AI first cut—human review pending" in html
    assert "-webkit-user-select:text" in html
    assert "selectionSpan()" in html
    assert "pointerdown" in html and "event.preventDefault()" in html
    assert "response.parent_confirmed!==true" in html
    assert "child.confirmed===true" in html
    assert "reviewed_unresolved_block_ids" in html
    assert "Failed model source block" in html
    assert "Split current child (S)" in html
    assert "window.__round4ReviewApp" in html


def test_completed_result_validator_requires_explicit_parent_and_child_flags(
    authenticated_run: dict,
) -> None:
    task, task_sha256 = _task(authenticated_run)
    results = _complete_results(task, task_sha256)

    assert review.validate_completed_results(
        results, task=task, task_sha256=task_sha256
    ) == results
    assert results["authorizations"]["human_review_complete"] is True
    assert all(
        results["authorizations"][field] is False
        for field in (
            "development",
            "human_truth",
            "production_integration",
            "full_expansion",
            "release",
        )
    )

    unconfirmed_parent = copy.deepcopy(results)
    unconfirmed_parent["responses"][0]["parent_confirmed"] = False
    with pytest.raises(ValueError, match="incomplete or inconsistent"):
        review.validate_completed_results(
            unconfirmed_parent, task=task, task_sha256=task_sha256
        )

    unconfirmed_child = copy.deepcopy(results)
    unconfirmed_child["responses"][0]["children"][0]["human_confirmed"] = False
    with pytest.raises(ValueError, match="not explicitly confirmed"):
        review.validate_completed_results(
            unconfirmed_child, task=task, task_sha256=task_sha256
        )


def test_shared_surname_multi_spans_are_allowed_but_duplicates_are_rejected() -> None:
    source = "Steve & Becky Nelson endorse Casey"
    steve = {
        "name_spans": [[0, 5], [14, 20]],
        "support_spans": [[0, len(source)]],
    }
    becky = {
        "name_spans": [[8, 13], [14, 20]],
        "support_spans": [[0, len(source)]],
    }

    assert review.duplicate_child_reason(source, [steve, becky]) is None
    assert review.duplicate_child_reason(source, [steve, copy.deepcopy(steve)])


@pytest.mark.parametrize("authenticated_run", [3, 4], indirect=True)
def test_prepared_draft_loader_accepts_blank_ai_support_but_final_review_does_not(
    authenticated_run: dict,
    tmp_path: Path,
) -> None:
    draft = copy.deepcopy(authenticated_run["draft"])
    first = draft["parents"][0]
    first["children"][0]["support_spans"] = []
    first["children"][0]["provisional_child_id"] = review.provisional_child_id(
        stage_b_output_inventory_sha256=draft["provenance"][
            "stage_b_output_inventory_sha256"
        ],
        parent_document_id=first["parent_document_id"],
        provisional_child_index=0,
        entity_type=first["children"][0]["entity_type"],
        name_spans=first["children"][0]["name_spans"],
        support_spans=[],
    )
    path = tmp_path / "draft.json"
    path.write_bytes(review.canonical_bytes(draft))
    loaded, _ = review.load_provisional_draft(
        path,
        expected_sha256=review.sha256_file(path),
        parents=authenticated_run["parents"],
        stage_a_manifest=authenticated_run["stage_a_manifest"],
        stage_a_manifest_sha256=authenticated_run["stage_a_manifest_sha256"],
    )
    assert loaded["parents"][0]["children"][0]["support_spans"] == []

    task = review.build_task(
        parents=authenticated_run["parents"],
        stage_a_manifest=authenticated_run["stage_a_manifest"],
        stage_a_manifest_sha256=authenticated_run["stage_a_manifest_sha256"],
        draft=loaded,
        draft_sha256=review.sha256_file(path),
    )
    task_sha256 = review.sha256_bytes(review.canonical_bytes(task))
    results = _complete_results(task, task_sha256)
    with pytest.raises(ValueError, match="nonempty"):
        review.validate_completed_results(
            results, task=task, task_sha256=task_sha256
        )


def test_hash_tampering_and_existing_output_are_rejected(
    authenticated_run: dict,
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="raw bytes differ"):
        review.load_stage_a_source_artifact(
            authenticated_run["stage_a_input"],
            expected_manifest_sha256="0" * 64,
        )

    draft_path = tmp_path / "draft.json"
    draft_path.write_bytes(review.canonical_bytes(authenticated_run["draft"]))
    with pytest.raises(ValueError, match="raw bytes differ"):
        review.load_provisional_draft(
            draft_path,
            expected_sha256="0" * 64,
            parents=authenticated_run["parents"],
            stage_a_manifest=authenticated_run["stage_a_manifest"],
            stage_a_manifest_sha256=authenticated_run["stage_a_manifest_sha256"],
        )

    output = tmp_path / "review-app"
    output.mkdir()
    with pytest.raises(ValueError, match="must not already exist"):
        review.build_files(
            stage_a_input_dir=authenticated_run["stage_a_input"],
            expected_stage_a_manifest_sha256=authenticated_run[
                "stage_a_manifest_sha256"
            ],
            provisional_draft_path=draft_path,
            expected_provisional_draft_sha256=review.sha256_file(draft_path),
            output_dir=output,
        )


def test_converter_surfaces_nonoptional_stage_b_failure_without_using_records(
    authenticated_run: dict,
    tmp_path: Path,
) -> None:
    copied_output = tmp_path / "stage-b-output-nonrepairable"
    shutil.copytree(authenticated_run["stage_b_output"], copied_output)
    input_dir = authenticated_run["stage_b_input"]
    manifest_path = input_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_sha256 = sha256_file(manifest_path)
    args = _runner_args("record_extraction")
    generation_sha256 = runner.generation_config_sha256(
        args, input_manifest_sha256=manifest_sha256
    )
    all_rows = []
    selected_paths = []
    changed = False
    changed_parent_id = None
    for entry in manifest["chunks"]:
        input_path = input_dir / "chunks" / entry["file"]
        output_path = copied_output / entry["file"].replace(
            ".jsonl.gz", ".out.jsonl.gz"
        )
        batches = runner.read_gzip_jsonl(input_path)
        rows = runner.read_gzip_jsonl(output_path)
        rewritten = []
        for batch, row in zip(batches, rows, strict=True):
            if not changed:
                model_output = copy.deepcopy(row["model_output"])
                first_slot = batch["target_slots"][0]
                model_output[first_slot]["records"][0]["name_parts"] = [
                    "Ghost Name"
                ]
                raw_output = runner.canonical_json(model_output)
                row = runner.derive_output_row(
                    batch=batch,
                    raw_output=raw_output,
                    finish_reason="stop",
                    stop_reason=None,
                    output_tokens=100,
                    prompt_tokens=500,
                    args=args,
                    input_manifest_sha256=manifest_sha256,
                    input_chunk_sha256=sha256_file(input_path),
                    generation_config_sha256_value=generation_sha256,
                )
                changed = True
                changed_parent_id = batch["parent_document_id"]
            rewritten.append(row)
        output_path.write_bytes(runner._gzip_jsonl_bytes(rewritten))
        all_rows.extend(rewritten)
        selected_paths.append(input_path)
    metadata = runner.run_metadata(
        args=args,
        input_manifest_sha256=manifest_sha256,
        generation_config_sha256_value=generation_sha256,
        selected_chunks=selected_paths,
        rows=all_rows,
        started_at="2026-09-07T00:00:00+00:00",
    )
    (copied_output / "run_metadata.json").write_text(
        runner.canonical_json(metadata) + "\n", encoding="utf-8"
    )

    draft, report, parents, stage_a_manifest = (
        review.prepare_provisional_draft_from_authenticated_runs(
            stage_a_input_dir=authenticated_run["stage_a_input"],
            stage_a_output_dir=authenticated_run["stage_a_output"],
            expected_stage_a_input_manifest_sha256=authenticated_run[
                "stage_a_manifest_sha256"
            ],
            stage_b_input_dir=input_dir,
            stage_b_output_dir=copied_output,
            expected_stage_b_input_manifest_sha256=authenticated_run[
                "stage_b_manifest_sha256"
            ],
        )
    )

    assert changed_parent_id is not None
    failed_parent = next(
        parent
        for parent in draft["parents"]
        if parent["parent_document_id"] == changed_parent_id
    )
    assert failed_parent["children"] == []
    assert failed_parent["provisional_parent_disposition"] == "needs_review"
    assert len(failed_parent["unresolved_blocks"]) == 1
    unresolved = failed_parent["unresolved_blocks"][0]
    source_parent = next(
        parent
        for parent in parents
        if parent["parent_document_id"] == changed_parent_id
    )
    source = source_parent["parent_source_text"]
    assert source[slice(*unresolved["block_span"])].strip().startswith("Alice ")
    assert unresolved["parse_status"] == "invalid_stage_output"
    assert report["unresolved_block_count"] == 1
    issues = [
        issue
        for issue in report["issues"]
        if issue["issue_type"] == "stage_b_nonrepairable_target_block"
    ]
    assert len(issues) == 1
    assert issues[0]["unresolved_block_id"] == unresolved["unresolved_block_id"]
    assert any(
        "name_parts: no ordered source projection" in error
        for error in issues[0]["stage_validation_errors"]
    )

    draft_sha256 = review.sha256_bytes(review.canonical_bytes(draft))
    task = review.build_task(
        parents=parents,
        stage_a_manifest=stage_a_manifest,
        stage_a_manifest_sha256=authenticated_run["stage_a_manifest_sha256"],
        draft=draft,
        draft_sha256=draft_sha256,
    )
    task_sha256 = review.sha256_bytes(review.canonical_bytes(task))
    task_parent_index = next(
        index
        for index, parent in enumerate(task["parents"])
        if parent["parent_document_id"] == changed_parent_id
    )
    assert task["parents"][task_parent_index]["ai_children"] == []
    assert (
        task["parents"][task_parent_index]["ai_unresolved_blocks"][0][
            "human_reviewed"
        ]
        is False
    )

    results = _complete_results(task, task_sha256)
    assert review.validate_completed_results(
        results, task=task, task_sha256=task_sha256
    ) == results
    unresolved_not_reviewed = copy.deepcopy(results)
    unresolved_not_reviewed["responses"][task_parent_index][
        "reviewed_unresolved_block_ids"
    ] = []
    with pytest.raises(ValueError, match="incomplete or inconsistent"):
        review.validate_completed_results(
            unresolved_not_reviewed, task=task, task_sha256=task_sha256
        )


def test_converter_salvages_valid_sibling_slots_from_failed_batch(
    tmp_path: Path,
) -> None:
    special_names = ["Alpha One", "Bravo Two", "Charlie Three", "Delta Four"]
    parents = [
        _parent_with_names(0, special_names),
        *[_parent(index) for index in range(1, review.EXPECTED_PARENT_COUNT)],
    ]
    selected_ids = [parent["parent_document_id"] for parent in parents]
    special_parent_id = selected_ids[0]
    stage_a_input = tmp_path / "slot-stage-a-input"
    _write_artifact(
        output_dir=stage_a_input,
        stage="line_decision",
        batches=build_fresh_stage_a_batches(parents),
        selected_parent_ids=selected_ids,
        provenance={
            "sample_selection_sha256": "1" * 64,
            "sample_selection_spec_sha256": "2" * 64,
            "source_export_manifest_sha256": "3" * 64,
            "sealed_reserve_source_text_reconstructed": False,
            "ai_draft_used_in_prompt": False,
        },
        chunk_size=60,
    )
    stage_a_manifest_sha256 = sha256_file(stage_a_input / "manifest.json")
    stage_a_output = tmp_path / "slot-stage-a-output"

    def stage_a_model(batch: dict) -> dict:
        output = {
            slot: {"role": "record_anchor"} for slot in batch["target_slots"]
        }
        output.update(
            {slot: {"role": "padding"} for slot in batch["padding_slots"]}
        )
        return output

    _write_completed_run(
        stage_a_input,
        stage_a_output,
        stage="line_decision",
        model_output_for_batch=stage_a_model,
    )
    stage_b_input = tmp_path / "slot-stage-b-input"
    build_fresh_stage_b_artifact(
        stage_a_input,
        stage_a_output,
        stage_b_input,
        expected_stage_a_input_manifest_sha256=stage_a_manifest_sha256,
        chunk_size=60,
    )
    stage_b_manifest_sha256 = sha256_file(stage_b_input / "manifest.json")
    all_stage_b_batches = []
    stage_b_manifest = json.loads(
        (stage_b_input / "manifest.json").read_text(encoding="utf-8")
    )
    for entry in stage_b_manifest["chunks"]:
        all_stage_b_batches.extend(
            runner.read_gzip_jsonl(stage_b_input / "chunks" / entry["file"])
        )
    special_batch = next(
        batch
        for batch in all_stage_b_batches
        if batch["parent_document_id"] == special_parent_id
    )
    assert special_batch["target_slots"] == [
        "slot_00",
        "slot_01",
        "slot_02",
        "slot_03",
    ]
    stage_b_output = tmp_path / "slot-stage-b-output"

    def stage_b_model(batch: dict) -> dict:
        output = {}
        for target in batch["targets"]:
            name = target["line_text"]
            if (
                batch["parent_document_id"] == special_parent_id
                and target["slot"] == "slot_02"
            ):
                name = "Ghost Name"
            output[target["slot"]] = {
                "disposition": "records",
                "source_mentions_fully_accounted": True,
                "records": [
                    {
                        "name_parts": [name],
                        "entity_type": "individual",
                        "endorser_category": "private_individual",
                        "title": None,
                        "affiliation": None,
                        "jurisdiction": None,
                    }
                ],
            }
        output.update(
            {
                slot: {
                    "disposition": "padding",
                    "source_mentions_fully_accounted": True,
                    "records": [],
                }
                for slot in batch["padding_slots"]
            }
        )
        return output

    _write_completed_run(
        stage_b_input,
        stage_b_output,
        stage="record_extraction",
        model_output_for_batch=stage_b_model,
    )
    draft, report, reconstructed, _manifest = (
        review.prepare_provisional_draft_from_authenticated_runs(
            stage_a_input_dir=stage_a_input,
            stage_a_output_dir=stage_a_output,
            expected_stage_a_input_manifest_sha256=stage_a_manifest_sha256,
            stage_b_input_dir=stage_b_input,
            stage_b_output_dir=stage_b_output,
            expected_stage_b_input_manifest_sha256=stage_b_manifest_sha256,
        )
    )

    special = draft["parents"][0]
    source = reconstructed[0]["parent_source_text"]
    retained_names = [
        " ".join(source[start:end] for start, end in child["name_spans"])
        for child in special["children"]
    ]
    assert retained_names == ["Alpha One", "Bravo Two", "Delta Four"]
    assert len(special["unresolved_blocks"]) == 1
    unresolved = special["unresolved_blocks"][0]
    assert unresolved["stage_b_slot"] == "slot_02"
    assert source[slice(*unresolved["block_span"])].strip() == "Charlie Three"
    assert special["provisional_parent_disposition"] == "needs_review"
    isolated_issue = next(
        issue
        for issue in report["issues"]
        if issue["issue_type"] == "stage_b_nonrepairable_target_block"
    )
    isolated_errors = isolated_issue["isolated_stage_validation"]["errors"]
    assert isolated_errors
    assert all(error.startswith("$.slot_02.") for error in isolated_errors)
    salvage = next(
        issue
        for issue in report["issues"]
        if issue["issue_type"] == "stage_b_failed_batch_slot_salvage"
    )
    assert salvage["accepted_target_slots"] == [
        "slot_00",
        "slot_01",
        "slot_03",
    ]
    assert salvage["unresolved_target_slots"] == ["slot_02"]
    assert report["unresolved_block_count"] == 1


def test_isolated_slot_rejects_schema_invalid_record_as_unresolved(
    authenticated_run: dict,
) -> None:
    manifest = json.loads(
        (authenticated_run["stage_b_input"] / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    input_path = authenticated_run["stage_b_input"] / "chunks" / manifest[
        "chunks"
    ][0]["file"]
    batch = runner.read_gzip_jsonl(input_path)[0]
    slot = batch["target_slots"][0]
    target = batch["targets"][0]
    output_row = {
        "model_output": {
            slot_name: (
                {
                    "disposition": "records",
                    "source_mentions_fully_accounted": True,
                    "records": [42],
                }
                if slot_name == slot
                else {
                    "disposition": "not_endorsement",
                    "source_mentions_fully_accounted": True,
                    "records": [],
                }
            )
            for slot_name in batch["target_slots"]
        },
        "raw_output_sha256": "1" * 64,
        "model_output_sha256": "2" * 64,
    }

    with pytest.raises(review.NonrepairableStageBOutput) as raised:
        review._sanitized_stage_b_slot_decision(
            runner=runner,
            batch=batch,
            output_row=output_row,
            slot=slot,
            target=target,
        )
    assert raised.value.isolated_validation is not None
    assert raised.value.isolated_validation["schema_valid"] is False


def test_one_call_direct_run_builder_writes_authenticated_nontrivial_artifact(
    authenticated_run: dict,
    tmp_path: Path,
) -> None:
    output = tmp_path / "review-app"
    auth = review.build_files_from_authenticated_runs(
        stage_a_input_dir=authenticated_run["stage_a_input"],
        stage_a_output_dir=authenticated_run["stage_a_output"],
        expected_stage_a_manifest_sha256=authenticated_run[
            "stage_a_manifest_sha256"
        ],
        stage_b_input_dir=authenticated_run["stage_b_input"],
        stage_b_output_dir=authenticated_run["stage_b_output"],
        expected_stage_b_manifest_sha256=authenticated_run[
            "stage_b_manifest_sha256"
        ],
        output_dir=output,
    )

    assert sorted(path.name for path in output.iterdir()) == [
        "auth.json",
        "index.html",
        "provisional_conversion_report.json",
        "provisional_draft.json",
        "task.json",
    ]
    for path in output.iterdir():
        assert path.stat().st_size > 100
    assert sha256_file(output / "task.json") == auth["task_sha256"]
    assert sha256_file(output / "index.html") == auth["html_sha256"]
    assert sha256_file(output / "provisional_draft.json") == auth[
        "provisional_draft_sha256"
    ]
    assert auth["authorizations"] == review.REVIEW_AUTHORIZATIONS
    assert all(value is False for value in auth["authorizations"].values())
    assert auth["conversion"]["converter_version"] == review.CONVERTER_VERSION


def test_direct_cli_runs_outside_repository_and_pins_both_output_inventories(
    authenticated_run: dict,
    tmp_path: Path,
) -> None:
    output = tmp_path / "cli-review-app"
    provenance = authenticated_run["draft"]["provenance"]
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(review.__file__).resolve()),
            "--stage-a-input-dir",
            str(authenticated_run["stage_a_input"]),
            "--stage-a-output-dir",
            str(authenticated_run["stage_a_output"]),
            "--expected-stage-a-manifest-sha256",
            authenticated_run["stage_a_manifest_sha256"],
            "--expected-stage-a-output-inventory-sha256",
            provenance["stage_a_output_inventory_sha256"],
            "--stage-b-input-dir",
            str(authenticated_run["stage_b_input"]),
            "--stage-b-output-dir",
            str(authenticated_run["stage_b_output"]),
            "--expected-stage-b-manifest-sha256",
            authenticated_run["stage_b_manifest_sha256"],
            "--expected-stage-b-output-inventory-sha256",
            provenance["stage_b_output_inventory_sha256"],
            "--output-dir",
            str(output),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    auth = json.loads(completed.stdout)
    assert auth["task_sha256"] == sha256_file(output / "task.json")
    assert auth["stage_a_output_inventory_sha256"] == provenance[
        "stage_a_output_inventory_sha256"
    ]
    assert auth["stage_b_output_inventory_sha256"] == provenance[
        "stage_b_output_inventory_sha256"
    ]
