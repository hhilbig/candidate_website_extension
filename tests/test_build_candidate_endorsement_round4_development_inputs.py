from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
import sys
from argparse import Namespace
from pathlib import Path

import pytest

from scripts.build_candidate_endorsement_round4_development_inputs import (
    AI_DRAFT_SHA256,
    REPO,
    STAGE_A_SLOT_COUNT,
    STAGE_B_SLOT_COUNT,
    _write_artifact,
    authenticate_fresh_stage_a_run,
    build_fresh_stage_a_batches,
    build_fresh_stage_b_artifact,
    build_fresh_stage_b_batches,
    build_regression_stage_b_batches,
    canonical_json,
    deterministic_gzip_jsonl,
    sha256_json,
)
from src.candidate_endorsement_line_ledger import LINE_INVENTORY_SPEC_SHA256


def _line(index: int, text: str, *, model: bool = True) -> dict:
    start = sum(len(value) + 1 for value in ["Heading", "Alice", "supports"][:index])
    end = start + len(text)
    return {
        "source_line_id": f"line-{index}",
        "parent_line_index": index,
        "parent_start_char": start,
        "parent_end_char": end,
        "parent_full_end_char": end + 1,
        "line_text": text,
        "line_text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "line_terminator": "\n",
        "model_decision_required": model,
        "deterministic_disposition": (
            "model_decision_required" if model else "outside_attested_context_scope"
        ),
    }


def _parent() -> dict:
    source = "Heading\nAlice\nsupports\n"
    return {
        "candidate_cycle_id": "candidate-2024",
        "candidate_icpsr": "Candidate, Casey",
        "state": "CA",
        "office": "house",
        "year": 2024,
        "party": "democrat",
        "parent_document_id": "p" * 64,
        "extraction_source_text_sha256": hashlib.sha256(source.encode()).hexdigest(),
        "extraction_source_n_chars": len(source),
        "endorsement_line_context": {"context_text": "Heading"},
        "parent_source_text": source,
        "segment_descriptors": [],
        "lines": [
            _line(0, "Heading", model=False),
            _line(1, "Alice"),
            _line(2, "supports"),
        ],
    }


def test_stage_a_batches_repeat_full_context_and_bind_target_prefix() -> None:
    batches = build_fresh_stage_a_batches([_parent()])

    assert len(batches) == 1
    batch = batches[0]
    assert batch["stage"] == "line_decision"
    assert batch["target_slots"] == ["slot_00", "slot_01"]
    assert batch["padding_slots"] == [
        f"slot_{index:02d}" for index in range(2, STAGE_A_SLOT_COUNT)
    ]
    assert [line["line_text"] for line in batch["visible_lines"]] == [
        "Heading", "Alice", "supports",
    ]
    assert [target["line_text"] for target in batch["targets"]] == [
        "Alice", "supports",
    ]
    assert batch["full_parent_line_catalog_sha256"] == sha256_json(
        batch["visible_lines"]
    )


def test_deterministic_gzip_has_stable_bytes_and_rows() -> None:
    rows = [{"b": 2, "a": "é"}, {"value": True}]
    first = deterministic_gzip_jsonl(rows)
    second = deterministic_gzip_jsonl(rows)

    assert first == second
    assert gzip.decompress(first).decode() == "".join(
        canonical_json(row) + "\n" for row in rows
    )


def test_real_regression_repackage_preserves_every_anchor_once() -> None:
    batches, parent_ids = build_regression_stage_b_batches(REPO)
    source_ids = [
        target["source_line_id"] for batch in batches for target in batch["targets"]
    ]

    assert len(parent_ids) == len(set(parent_ids)) == 40
    assert len(batches) == 571
    assert len(source_ids) == len(set(source_ids)) == 2_221
    assert all(1 <= len(batch["targets"]) <= STAGE_B_SLOT_COUNT for batch in batches)
    assert all(
        batch["target_slots"]
        == [f"slot_{index:02d}" for index in range(len(batch["targets"]))]
        for batch in batches
    )
    serialized = canonical_json(batches)
    assert AI_DRAFT_SHA256 not in serialized
    assert "results_ai_coded_draft" not in serialized


def test_artifact_manifest_authenticates_chunks_and_stays_development_only(
    tmp_path: Path,
) -> None:
    batch = build_fresh_stage_a_batches([_parent()])[0]
    output = tmp_path / "stage-a"
    manifest = _write_artifact(
        output_dir=output,
        stage="line_decision",
        batches=[batch],
        selected_parent_ids=[batch["parent_document_id"]],
        provenance={"ai_draft_used_in_prompt": False},
        chunk_size=1,
    )

    manifest_path = output / "manifest.json"
    observed = json.loads(manifest_path.read_text())
    payload = dict(observed)
    recorded = payload.pop("manifest_payload_sha256")
    assert recorded == sha256_json(payload)
    assert observed == manifest
    assert observed["authorizations"] == {
        "bounded_development_inference": True,
        "development_labels_authoritative": False,
        "human_truth": False,
        "production_integration": False,
        "full_expansion": False,
        "release": False,
    }
    chunk = output / "chunks" / observed["chunks"][0]["file"]
    assert chunk.stat().st_size == observed["chunks"][0]["bytes"]
    assert hashlib.sha256(chunk.read_bytes()).hexdigest() == observed["chunks"][0][
        "sha256"
    ]
    assert json.loads(gzip.decompress(chunk.read_bytes()).decode()) == batch

    with pytest.raises(ValueError, match="already exists"):
        _write_artifact(
            output_dir=output,
            stage="line_decision",
            batches=[batch],
            selected_parent_ids=[batch["parent_document_id"]],
            provenance={},
            chunk_size=1,
        )


def test_regression_cli_runs_outside_repo_working_directory(tmp_path: Path) -> None:
    output = tmp_path / "regression-stage-b"
    script = REPO / "scripts/build_candidate_endorsement_round4_development_inputs.py"

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "regression-stage-b",
            "--repo",
            str(REPO),
            "--output-dir",
            str(output),
            "--chunk-size",
            "571",
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["stage"] == "record_extraction"
    assert manifest["batch_count"] == 571
    assert manifest["selected_parent_count"] == 40


def test_fresh_stage_a_cli_runs_outside_repo_working_directory(
    tmp_path: Path,
) -> None:
    output = tmp_path / "fresh-stage-a"
    script = REPO / "scripts/build_candidate_endorsement_round4_development_inputs.py"
    selection = REPO / (
        "build/private_candidate_endorsement_recall40_sample_round3_selection_"
        "20260907b/selection.json"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "fresh-stage-a",
            "--repo",
            str(REPO),
            "--selection",
            str(selection),
            "--expected-selection-sha256",
            "e41cd15f3c17660588710642b1a0f82bf82e9473097c285377ba4e30bee84300",
            "--output-dir",
            str(output),
            "--chunk-size",
            "10000",
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["stage"] == "line_decision"
    assert manifest["selected_parent_count"] == 40
    assert manifest["provenance"]["sample_selection_sha256"] == (
        "e41cd15f3c17660588710642b1a0f82bf82e9473097c285377ba4e30bee84300"
    )


def _fresh_handoff_parent() -> dict:
    parent_id = "a" * 64
    values = [
        ("Heading", "\n", False),
        ("Alice Example", "\n", True),
        ("Chief Executive Officer", "\n", True),
        ("Mystery fragment", "\n", True),
        ("Footer", "\n", True),
        ("Orphan biography", "", True),
    ]
    lines = []
    cursor = 0
    for index, (text, terminator, model) in enumerate(values):
        start = cursor
        end = start + len(text)
        full_end = end + len(terminator)
        text_hash = hashlib.sha256(text.encode()).hexdigest()
        identity = {
            "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
            "parent_document_id": parent_id,
            "parent_line_index": index,
            "parent_start_char": start,
            "parent_end_char": end,
            "parent_full_end_char": full_end,
            "line_text_sha256": text_hash,
            "line_terminator": terminator,
        }
        lines.append(
            {
                "source_line_id": sha256_json(identity),
                "parent_line_index": index,
                "parent_start_char": start,
                "parent_end_char": end,
                "parent_full_end_char": full_end,
                "line_text": text,
                "line_text_sha256": text_hash,
                "line_terminator": terminator,
                "model_decision_required": model,
                "deterministic_disposition": (
                    "model_decision_required"
                    if model
                    else "outside_attested_context_scope"
                ),
            }
        )
        cursor = full_end
    source = "".join(text + terminator for text, terminator, _model in values)
    return {
        "candidate_cycle_id": "candidate-2024",
        "candidate_icpsr": "Candidate, Casey",
        "state": "CA",
        "office": "house",
        "year": 2024,
        "party": "democrat",
        "parent_document_id": parent_id,
        "extraction_source_text_sha256": hashlib.sha256(
            source.encode()
        ).hexdigest(),
        "extraction_source_n_chars": len(source),
        "endorsement_line_context": {"context_text": "Heading"},
        "parent_source_text": source,
        "segment_descriptors": [],
        "lines": lines,
    }


def _completed_stage_a_run(
    tmp_path: Path,
) -> tuple[Path, Path, str]:
    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as runner,
    )

    input_dir = tmp_path / "fresh-stage-a-input"
    output_dir = tmp_path / "fresh-stage-a-output"
    batches = build_fresh_stage_a_batches([_fresh_handoff_parent()])
    _write_artifact(
        output_dir=input_dir,
        stage="line_decision",
        batches=batches,
        selected_parent_ids=[batches[0]["parent_document_id"]],
        provenance={"fresh_sample": True},
        chunk_size=60,
    )
    manifest_hash = hashlib.sha256(
        (input_dir / "manifest.json").read_bytes()
    ).hexdigest()
    args = Namespace(
        stage="line_decision",
        seed=runner.GENERATION_SEED,
        max_model_len=runner.MAX_MODEL_LEN,
        max_output_tokens=runner.MAX_OUTPUT_TOKENS["line_decision"],
        gpu_memory_utilization=runner.GPU_MEMORY_UTILIZATION,
    )
    generation_hash = runner.generation_config_sha256(
        args, input_manifest_sha256=manifest_hash
    )
    roles = [
        "record_anchor",
        "record_continuation_previous",
        "unclear",
        "navigation_form_footer",
        "record_continuation_previous",
    ]
    model_output = {
        f"slot_{index:02d}": {"role": role}
        for index, role in enumerate(roles)
    }
    model_output.update(
        {
            f"slot_{index:02d}": {"role": "padding"}
            for index in range(len(roles), STAGE_A_SLOT_COUNT)
        }
    )
    input_chunk = input_dir / "chunks/chunk_00000.jsonl.gz"
    row = runner.derive_output_row(
        batch=batches[0],
        raw_output=canonical_json(model_output),
        finish_reason="stop",
        stop_reason=None,
        output_tokens=100,
        prompt_tokens=500,
        args=args,
        input_manifest_sha256=manifest_hash,
        input_chunk_sha256=hashlib.sha256(input_chunk.read_bytes()).hexdigest(),
        generation_config_sha256_value=generation_hash,
    )
    output_dir.mkdir()
    (output_dir / "chunk_00000.out.jsonl.gz").write_bytes(
        runner._gzip_jsonl_bytes([row])
    )
    metadata = runner.run_metadata(
        args=args,
        input_manifest_sha256=manifest_hash,
        generation_config_sha256_value=generation_hash,
        selected_chunks=[input_chunk],
        rows=[row],
        started_at="2026-09-07T00:00:00+00:00",
    )
    (output_dir / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n"
    )
    return input_dir, output_dir, manifest_hash


def test_fresh_stage_a_handoff_builds_exact_advisory_blocks_and_logs_issues(
    tmp_path: Path,
) -> None:
    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as runner,
    )

    input_dir, stage_a_output, manifest_hash = _completed_stage_a_run(tmp_path)
    output_dir = tmp_path / "fresh-stage-b-input"
    manifest = build_fresh_stage_b_artifact(
        input_dir,
        stage_a_output,
        output_dir,
        expected_stage_a_input_manifest_sha256=manifest_hash,
        chunk_size=60,
    )

    assert manifest["stage"] == "record_extraction"
    assert manifest["batch_count"] == 1
    diagnostics = manifest["provenance"]["handoff_diagnostics"]
    assert diagnostics["anchor_count"] == 1
    assert diagnostics["stage_b_target_count"] == 1
    assert diagnostics["unclear_line_count"] == 1
    assert diagnostics["unowned_continuation_count"] == 1
    assert diagnostics["ambiguous_ownership_line_count"] == 0
    assert diagnostics["ambiguous_line_count"] == 1
    batch = json.loads(
        gzip.decompress((output_dir / "chunks/chunk_00000.jsonl.gz").read_bytes())
    )
    target = batch["targets"][0]
    assert target["stage_a_role"] == "record_anchor"
    assert target["stage_a_role_status"] == "advisory_non_authoritative"
    assert [line["line_text"] for line in target["record_block_lines"]] == [
        "Alice Example",
        "Chief Executive Officer",
    ]
    assert target["record_block_text"] == (
        "Alice Example\nChief Executive Officer\n"
    )
    assert target["record_block_text_sha256"] == hashlib.sha256(
        target["record_block_text"].encode()
    ).hexdigest()
    observed_manifest_hash = hashlib.sha256(
        (output_dir / "manifest.json").read_bytes()
    ).hexdigest()
    validated, _, paths = runner.validate_input_manifest(
        output_dir / "manifest.json",
        output_dir / "chunks",
        stage="record_extraction",
        expected_manifest_sha256=observed_manifest_hash,
    )
    assert validated["expected_batch_ids"] == [batch["batch_id"]]
    assert len(paths) == 1


def test_fresh_stage_a_handoff_rejects_unpinned_output_inventory(
    tmp_path: Path,
) -> None:
    input_dir, output_dir, manifest_hash = _completed_stage_a_run(tmp_path)

    with pytest.raises(ValueError, match="output inventory differs"):
        authenticate_fresh_stage_a_run(
            input_dir,
            output_dir,
            expected_input_manifest_sha256=manifest_hash,
            expected_output_inventory_sha256="f" * 64,
        )


def test_fresh_stage_a_handoff_replays_stage_a_schema(
    tmp_path: Path,
) -> None:
    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as runner,
    )

    input_dir, output_dir, manifest_hash = _completed_stage_a_run(tmp_path)
    output_path = output_dir / "chunk_00000.out.jsonl.gz"
    rows = runner.read_gzip_jsonl(output_path)
    row = rows[0]
    row["model_output"]["slot_00"] = {"role": "invented_role"}
    row["raw_output"] = canonical_json(row["model_output"])
    row["raw_output_sha256"] = hashlib.sha256(
        row["raw_output"].encode()
    ).hexdigest()
    row["raw_output_n_chars"] = len(row["raw_output"])
    row["raw_output_utf8_n_bytes"] = len(row["raw_output"].encode())
    row["model_output_sha256"] = sha256_json(row["model_output"])
    input_batch = json.loads(
        gzip.decompress(
            (input_dir / "chunks/chunk_00000.jsonl.gz").read_bytes()
        )
    )
    row["stage_validation"] = runner.validate_stage_output(
        input_batch, row["model_output"]
    )
    output_path.write_bytes(runner._gzip_jsonl_bytes(rows))

    with pytest.raises(ValueError, match="fails exact schema replay"):
        authenticate_fresh_stage_a_run(
            input_dir,
            output_dir,
            expected_input_manifest_sha256=manifest_hash,
        )


def test_fresh_stage_b_context_default_and_real_bacon() -> None:
    base = REPO / "build/hive_candidate_extraction"
    old_dir = base / "v2_1_endorsement_recall40_sample_round3_round4_stage_b_20260907d"
    old_manifest = json.loads((old_dir / "manifest.json").read_text())
    provenance = old_manifest["provenance"]
    batches, rows, authenticated, _ = authenticate_fresh_stage_a_run(
        base / "v2_1_endorsement_recall40_sample_round3_round4_stage_a_20260907a",
        base / "v2_1_endorsement_recall40_sample_round3_round4_stage_a_outputs_20260907a",
        expected_input_manifest_sha256=provenance["stage_a_input_manifest_sha256"],
        expected_output_inventory_sha256=provenance["stage_a_output"]["output_inventory_sha256"],
    )
    kwargs = dict(
        parents=authenticated["parents"], stage_a_batches=batches,
        stage_a_output_rows=rows,
        stage_a_input_manifest_sha256=provenance["stage_a_input_manifest_sha256"],
        stage_a_output_inventory_sha256=provenance["stage_a_output"]["output_inventory_sha256"],
    )
    default, diagnostics = build_fresh_stage_b_batches(**kwargs)
    explicit_zero, zero_diagnostics = build_fresh_stage_b_batches(
        **kwargs, read_only_context_radius=0
    )
    old = [json.loads(line) for path in sorted((old_dir / "chunks").glob("*.gz"))
           for line in gzip.decompress(path.read_bytes()).splitlines()]
    assert canonical_json(default) == canonical_json(old)
    assert canonical_json(default) == canonical_json(explicit_zero)
    assert diagnostics == zero_diagnostics == provenance["handoff_diagnostics"]
    context, context_diagnostics = build_fresh_stage_b_batches(
        **kwargs, read_only_context_radius=10
    )
    old_bacon = next(batch for batch in old if batch["batch_id"] ==
        "e8e6ae127e33247f1a918204ecc92416d35c747a43f5fb542632cbe2c79c9a03")
    bacon = next(batch for batch in context if
        batch["targets"][0]["source_line_id"] == old_bacon["targets"][0]["source_line_id"])
    lines = {line["parent_line_index"] + 1: line["line_text"]
             for line in bacon["read_only_context_lines"]}
    assert lines[249] == "Don Bacon has earned"
    assert lines[250] == "STRONG PUBLIC SUPPORT from 18 Labor Unions"
    assert bacon["batch_id"] != old_bacon["batch_id"]
    assert context_diagnostics["read_only_context_radius"] == 10
    output_by_id = {row["batch_id"]: row["model_output"] for row in rows}
    heading_ids_by_parent: dict[str, set[str]] = {}
    for stage_a_batch in batches:
        heading_ids = heading_ids_by_parent.setdefault(
            stage_a_batch["parent_document_id"], set())
        for target in stage_a_batch["targets"]:
            if output_by_id[stage_a_batch["batch_id"]][target["slot"]]["role"] == "support_or_category_heading":
                heading_ids.add(target["source_line_id"])
    from scripts.hive import extract_candidate_endorsement_round4_development_vllm as runner
    for batch in context:
        owned_ids = {line["source_line_id"] for target in batch["targets"]
                     for line in target["record_block_lines"]}
        context_ids = {line["source_line_id"] for line in batch["read_only_context_lines"]}
        assert not owned_ids.intersection(context_ids)
        assert heading_ids_by_parent[batch["parent_document_id"]] <= owned_ids | context_ids
        rendered = runner.render_batch_prompt(batch)
        assert rendered.startswith("READ_ONLY_CONTEXT_INSTRUCTIONS\n")
        assert '"parent_source_text"' not in rendered


@pytest.mark.parametrize("radius", [-1, True, 1.5])
def test_fresh_stage_b_context_rejects_invalid_radius(radius) -> None:
    with pytest.raises(ValueError, match="nonnegative integer"):
        build_fresh_stage_b_batches(
            parents=[], stage_a_batches=[], stage_a_output_rows=[],
            stage_a_input_manifest_sha256="a" * 64,
            stage_a_output_inventory_sha256="b" * 64,
            read_only_context_radius=radius,
        )
