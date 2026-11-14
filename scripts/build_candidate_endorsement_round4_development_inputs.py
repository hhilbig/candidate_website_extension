#!/usr/bin/env python3
"""Build bounded round-four development inputs without touching frozen runs.

The builder supports three paths:

* repackage the authenticated round-three Stage-B blocks for a prompt-regression
  run against provisional labels; and
* build Stage-A inputs for a separately selected, untouched 40-parent sample;
  and
* hand a completed authenticated fresh Stage-A run to fresh Stage B.

It performs no inference and never authorizes human truth, expansion, or release.
"""
from __future__ import annotations

import argparse
import copy
import csv
import gzip
import hashlib
import io
import json
import sys
from collections import Counter
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
SOURCE_EXPORT = Path(
    "build/hive_candidate_extraction/v2_1_full_export_20260824p"
)
ROUND3_STAGE_B_INPUT = Path(
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_b_preflight_20260906a"
)
ROUND3_STAGE_B_REPORT = Path(
    "quality_reports/"
    "candidate_endorsement_recall40_round3_stage_b_22685041_2026-09-06.json"
)
ROUND3_STAGE_B_REPORT_SHA256 = (
    "f775b6e3d7d27111f926516b449b85203d0bbdb263c1f1eeb8de9d7a58b082fa"
)
AI_DRAFT = Path(
    "build/private_candidate_endorsement_recall40_name_truth_amendment_"
    "20260906a/results_ai_coded_draft.json"
)
AI_DRAFT_SHA256 = (
    "1b461c191c1a8394c54f0f66f6bed56e9da5ced84fb8777a3aa0e2d4169f4c6d"
)
BUILDER_VERSION = "candidate_endorsement_round4_development_input_v1"
STAGE_A_SLOT_COUNT = 16
STAGE_B_SLOT_COUNT = 4
READ_ONLY_CONTEXT_POLICY = (
    "read_only_context_is_evidence_only;_names_must_originate_exclusively_"
    "from_their_owned_target_record_block_lines"
)

_PARENT_METADATA_FIELDS = (
    "candidate_cycle_id",
    "candidate_icpsr",
    "state",
    "office",
    "year",
    "party",
    "parent_document_id",
    "extraction_source_text_sha256",
    "extraction_source_n_chars",
    "endorsement_line_context",
)
_SIMPLE_LINE_FIELDS = (
    "source_line_id",
    "parent_line_index",
    "parent_start_char",
    "parent_end_char",
    "parent_full_end_char",
    "line_text",
    "line_text_sha256",
    "line_terminator",
    "model_decision_required",
    "deterministic_disposition",
)
_STAGE_A_BATCH_FIELDS = {
    "version",
    "stage",
    "batch_id",
    "document_id",
    *_PARENT_METADATA_FIELDS,
    "batch_index_within_parent",
    "target_slots",
    "padding_slots",
    "targets",
    "visible_lines",
    "full_parent_line_catalog_sha256",
}


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strict_json(path: Path) -> Any:
    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, child in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON key in {path}: {key}")
            value[key] = child
        return value

    return json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=hook,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"nonfinite JSON constant in {path}: {value}")
        ),
    )


def read_gzip_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                raise ValueError(f"blank JSONL row: {path}:{line_number}")
            value = json.loads(line)
            if type(value) is not dict:
                raise ValueError(f"non-object JSONL row: {path}:{line_number}")
            rows.append(value)
    return rows


def deterministic_gzip_jsonl(rows: Iterable[Mapping[str, Any]]) -> bytes:
    raw = "".join(canonical_json(dict(row)) + "\n" for row in rows).encode(
        "utf-8"
    )
    target = io.BytesIO()
    with gzip.GzipFile(fileobj=target, mode="wb", filename="", mtime=0) as handle:
        handle.write(raw)
    return target.getvalue()


def _round4_module() -> Any:
    repo_text = str(REPO)
    if repo_text not in sys.path:
        sys.path.insert(0, repo_text)
    from src import candidate_endorsement_round4

    return candidate_endorsement_round4


def _round4_runner_module() -> Any:
    """Load the shared round-four validator without importing vLLM."""
    repo_text = str(REPO)
    if repo_text not in sys.path:
        sys.path.insert(0, repo_text)
    from scripts.hive import extract_candidate_endorsement_round4_development_vllm

    return extract_candidate_endorsement_round4_development_vllm


def _schema_catalog(stage: str, slot_count: int) -> dict[str, str]:
    round4 = _round4_module()
    builder = (
        round4.stage_a_schema if stage == "line_decision" else round4.stage_b_schema
    )
    return {
        str(target_count): sha256_json(
            builder(target_count=target_count, slot_count=slot_count)
        )
        for target_count in range(1, slot_count + 1)
    }


def _write_artifact(
    *,
    output_dir: Path,
    stage: str,
    batches: Sequence[Mapping[str, Any]],
    selected_parent_ids: Sequence[str],
    provenance: Mapping[str, Any],
    chunk_size: int,
    sample_round: int = 3,
) -> dict[str, Any]:
    from scripts.hive.extract_candidate_endorsement_round4_development_vllm import validate_sample_round_binding
    validate_sample_round_binding(sample_round, dict(provenance), list(selected_parent_ids))
    if output_dir.exists():
        raise ValueError(f"output directory already exists: {output_dir}")
    if stage not in {"line_decision", "record_extraction"}:
        raise ValueError(f"unsupported round-four stage: {stage}")
    if not batches or chunk_size <= 0:
        raise ValueError("round-four artifact requires batches and a positive chunk size")
    if len(selected_parent_ids) != len(set(selected_parent_ids)):
        raise ValueError("selected parent IDs are not unique")
    batch_ids = [str(batch.get("batch_id")) for batch in batches]
    if len(batch_ids) != len(set(batch_ids)) or any(not value for value in batch_ids):
        raise ValueError("round-four batch IDs are absent or duplicated")
    expected_stage = {str(batch.get("stage")) for batch in batches}
    if expected_stage != {stage}:
        raise ValueError("round-four batch stages differ")

    round4 = _round4_module()
    prompt = (
        round4.ROUND4_STAGE_A_PROMPT
        if stage == "line_decision"
        else round4.ROUND4_STAGE_B_PROMPT
    )
    slot_count = STAGE_A_SLOT_COUNT if stage == "line_decision" else STAGE_B_SLOT_COUNT
    schema_catalog = _schema_catalog(stage, slot_count)
    chunk_payloads: list[tuple[str, bytes, list[str]]] = []
    for index, offset in enumerate(range(0, len(batches), chunk_size)):
        chunk_rows = batches[offset : offset + chunk_size]
        name = f"chunk_{index:05d}.jsonl.gz"
        payload = deterministic_gzip_jsonl(chunk_rows)
        chunk_payloads.append(
            (name, payload, [str(row["batch_id"]) for row in chunk_rows])
        )
    chunk_entries = [
        {
            "file": name,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "records": len(ids),
            "batch_ids_sha256": sha256_json(ids),
        }
        for name, payload, ids in chunk_payloads
    ]
    manifest: dict[str, Any] = {
        "version": BUILDER_VERSION,
        "stage": stage,
        "sample_round": sample_round,
        "pipeline_revision": 4,
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "schema_catalog": schema_catalog,
        "schema_catalog_sha256": sha256_json(schema_catalog),
        "slot_count": slot_count,
        "chunk_size": chunk_size,
        "batch_count": len(batches),
        "expected_batch_ids": batch_ids,
        "expected_batch_ids_sha256": sha256_json(batch_ids),
        "selected_parent_count": len(selected_parent_ids),
        "selected_parent_ids": list(selected_parent_ids),
        "selected_parent_ids_sha256": sha256_json(list(selected_parent_ids)),
        "chunks": chunk_entries,
        "provenance": copy.deepcopy(dict(provenance)),
        "authorizations": {
            "bounded_development_inference": True,
            "development_labels_authoritative": False,
            "human_truth": False,
            "production_integration": False,
            "full_expansion": False,
            "release": False,
        },
    }
    manifest["manifest_payload_sha256"] = sha256_json(manifest)
    output_dir.mkdir(parents=True)
    chunks_dir = output_dir / "chunks"
    chunks_dir.mkdir()
    for name, payload, _ids in chunk_payloads:
        (chunks_dir / name).write_bytes(payload)
    (output_dir / "manifest.json").write_text(
        canonical_json(manifest) + "\n", encoding="utf-8"
    )
    return manifest


def _simple_line(line: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: copy.deepcopy(line[key])
        for key in (
            "source_line_id",
            "parent_line_index",
            "parent_start_char",
            "parent_end_char",
            "parent_full_end_char",
            "line_text",
            "line_text_sha256",
            "line_terminator",
            "model_decision_required",
            "deterministic_disposition",
        )
    }


def _parent_metadata(parent: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: copy.deepcopy(parent[key])
        for key in (
            "candidate_cycle_id",
            "candidate_icpsr",
            "state",
            "office",
            "year",
            "party",
            "parent_document_id",
            "extraction_source_text_sha256",
            "extraction_source_n_chars",
            "endorsement_line_context",
        )
    }


def build_fresh_stage_a_batches(
    parents: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    prompt_sha256 = hashlib.sha256(
        _round4_module().ROUND4_STAGE_A_PROMPT.encode("utf-8")
    ).hexdigest()
    for parent in sorted(parents, key=lambda value: str(value["parent_document_id"])):
        lines = [_simple_line(line) for line in parent["lines"]]
        targets = [line for line in lines if line["model_decision_required"] is True]
        if not targets:
            continue
        line_catalog_sha256 = sha256_json(lines)
        for batch_index, offset in enumerate(range(0, len(targets), STAGE_A_SLOT_COUNT)):
            selected = targets[offset : offset + STAGE_A_SLOT_COUNT]
            target_rows = [
                {"slot": f"slot_{index:02d}", **copy.deepcopy(line)}
                for index, line in enumerate(selected)
            ]
            target_slots = [row["slot"] for row in target_rows]
            padding_slots = [
                f"slot_{index:02d}"
                for index in range(len(target_rows), STAGE_A_SLOT_COUNT)
            ]
            identity = {
                "version": BUILDER_VERSION,
                "stage": "line_decision",
                "pipeline_revision": 4,
                "prompt_sha256": prompt_sha256,
                "parent_document_id": parent["parent_document_id"],
                "batch_index_within_parent": batch_index,
                "target_source_line_ids": [
                    row["source_line_id"] for row in target_rows
                ],
                "full_parent_line_catalog_sha256": line_catalog_sha256,
            }
            batch_id = sha256_json(identity)
            batches.append(
                {
                    "version": BUILDER_VERSION,
                    "stage": "line_decision",
                    "batch_id": batch_id,
                    "document_id": batch_id,
                    **_parent_metadata(parent),
                    "batch_index_within_parent": batch_index,
                    "target_slots": target_slots,
                    "padding_slots": padding_slots,
                    "targets": target_rows,
                    "visible_lines": copy.deepcopy(lines),
                    "full_parent_line_catalog_sha256": line_catalog_sha256,
                }
            )
    if len({batch["batch_id"] for batch in batches}) != len(batches):
        raise ValueError("fresh Stage-A batch identities duplicate")
    return batches


def selected_parents_from_sample(
    repo: Path,
    selection_path: Path,
    *,
    expected_selection_sha256: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    from scripts.build_candidate_endorsement_line_ledger import (
        reconstruct_selected_parents,
        replay_frozen_source,
    )

    if sha256_file(selection_path) != expected_selection_sha256:
        raise ValueError("fresh private selection bytes differ")
    selection = strict_json(selection_path)
    if (
        type(selection) is not dict
        or selection.get("version")
        != "candidate_endorsement_private_recall40_sample_round3_selection_v1"
        or selection.get("sample_round") != 3
        or selection.get("pilot_parent_count") != 40
        or selection.get("authorization_scope")
        != "private_sample_assignment_only_no_source_task_inference_or_release"
    ):
        raise ValueError("fresh private selection identity differs")
    components = selection.get("pilot_components")
    if type(components) is not list or not components:
        raise ValueError("fresh private selection has no components")
    selected_ids = [
        str(parent_id)
        for component in components
        for parent_id in component["parent_document_ids"]
    ]
    if len(selected_ids) != 40 or len(set(selected_ids)) != 40:
        raise ValueError("fresh private selection parent closure differs")
    replay = replay_frozen_source(repo / SOURCE_EXPORT, enforce_frozen_source=True)
    parents = reconstruct_selected_parents(
        replay, selected_ids, round2_route_revision=True
    )
    selected_hashes = [
        str(parent["extraction_source_text_sha256"]) for parent in parents
    ]
    declared_hashes = {
        str(component["source_text_sha256"]) for component in components
    }
    if set(selected_hashes) != declared_hashes:
        raise ValueError("fresh parent sources differ from selected components")
    return parents, selection, selected_ids


def build_fresh_stage_a_artifact(
    repo: Path,
    selection_path: Path,
    output_dir: Path,
    *,
    expected_selection_sha256: str,
    chunk_size: int = 60,
) -> dict[str, Any]:
    parents, selection, selected_ids = selected_parents_from_sample(
        repo,
        selection_path,
        expected_selection_sha256=expected_selection_sha256,
    )
    batches = build_fresh_stage_a_batches(parents)
    return _write_artifact(
        output_dir=output_dir,
        stage="line_decision",
        batches=batches,
        selected_parent_ids=selected_ids,
        provenance={
            "sample_selection_sha256": expected_selection_sha256,
            "sample_selection_spec_sha256": selection["selection_spec_sha256"],
            "source_export_manifest_sha256": sha256_file(
                repo / SOURCE_EXPORT / "manifest.json"
            ),
            "sealed_reserve_source_text_reconstructed": False,
            "ai_draft_used_in_prompt": False,
        },
        chunk_size=chunk_size,
    )


def _typed_int(value: Any) -> bool:
    return type(value) is int


def _validated_parent_from_stage_a_batches(
    parent_batches: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reconstruct and validate one exact parent carried by fresh Stage A."""
    if not parent_batches:
        raise ValueError("fresh Stage-A parent has no batches")
    for batch in parent_batches:
        if type(batch) is not dict or set(batch) != _STAGE_A_BATCH_FIELDS:
            raise ValueError("fresh Stage-A batch fields differ from the builder")
        if (
            not _typed_int(batch.get("batch_index_within_parent"))
            or batch["batch_index_within_parent"] < 0
        ):
            raise ValueError("fresh Stage-A parent batch index is invalid")
    ordered = sorted(
        parent_batches, key=lambda row: row["batch_index_within_parent"]
    )
    if [row.get("batch_index_within_parent") for row in ordered] != list(
        range(len(ordered))
    ):
        raise ValueError("fresh Stage-A parent batch indexes are not contiguous")
    exemplar = ordered[0]
    parent_id = exemplar.get("parent_document_id")
    if type(parent_id) is not str or not parent_id:
        raise ValueError("fresh Stage-A parent identity is invalid")
    for batch in ordered:
        if batch.get("parent_document_id") != parent_id:
            raise ValueError("fresh Stage-A parent grouping differs")
        for field in _PARENT_METADATA_FIELDS:
            if canonical_json(batch.get(field)) != canonical_json(exemplar.get(field)):
                raise ValueError(f"fresh Stage-A parent metadata differs: {field}")
        if batch.get("visible_lines") != exemplar.get("visible_lines"):
            raise ValueError("fresh Stage-A parent line catalogs differ")
        if batch.get("full_parent_line_catalog_sha256") != sha256_json(
            batch["visible_lines"]
        ):
            raise ValueError("fresh Stage-A parent line catalog SHA-256 differs")

    from src.candidate_endorsement_line_ledger import (
        DETERMINISTIC_LINE_DISPOSITIONS,
        LINE_INVENTORY_SPEC_SHA256,
    )

    lines = copy.deepcopy(list(exemplar["visible_lines"]))
    if not lines:
        raise ValueError("fresh Stage-A parent line catalog is empty")
    cursor = 0
    reconstructed: list[str] = []
    source_line_ids: list[str] = []
    by_id: dict[str, dict[str, Any]] = {}
    for expected_index, line in enumerate(lines):
        if type(line) is not dict or set(line) != set(_SIMPLE_LINE_FIELDS):
            raise ValueError("fresh Stage-A source-line fields differ")
        line_id = line.get("source_line_id")
        text = line.get("line_text")
        terminator = line.get("line_terminator")
        start = line.get("parent_start_char")
        end = line.get("parent_end_char")
        full_end = line.get("parent_full_end_char")
        model_required = line.get("model_decision_required")
        disposition = line.get("deterministic_disposition")
        if (
            type(line_id) is not str
            or not line_id
            or type(text) is not str
            or terminator not in {"", "\n", "\r\n"}
            or not _typed_int(start)
            or not _typed_int(end)
            or not _typed_int(full_end)
            or line.get("parent_line_index") != expected_index
            or start != cursor
            or end != start + len(text)
            or full_end != end + len(terminator)
            or line.get("line_text_sha256")
            != hashlib.sha256(text.encode("utf-8")).hexdigest()
            or type(model_required) is not bool
            or disposition not in DETERMINISTIC_LINE_DISPOSITIONS
            or model_required != (disposition == "model_decision_required")
            or (model_required and not text)
            or (expected_index < len(lines) - 1 and terminator == "")
        ):
            raise ValueError("fresh Stage-A physical source-line layout differs")
        line_identity = {
            "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
            "parent_document_id": parent_id,
            "parent_line_index": expected_index,
            "parent_start_char": start,
            "parent_end_char": end,
            "parent_full_end_char": full_end,
            "line_text_sha256": line["line_text_sha256"],
            "line_terminator": terminator,
        }
        if line_id != sha256_json(line_identity) or line_id in by_id:
            raise ValueError("fresh Stage-A source-line identity differs")
        by_id[line_id] = line
        source_line_ids.append(line_id)
        reconstructed.append(text + terminator)
        cursor = full_end
    parent_source_text = "".join(reconstructed)
    if (
        cursor != exemplar.get("extraction_source_n_chars")
        or len(parent_source_text) != exemplar.get("extraction_source_n_chars")
        or hashlib.sha256(parent_source_text.encode("utf-8")).hexdigest()
        != exemplar.get("extraction_source_text_sha256")
    ):
        raise ValueError("fresh Stage-A reconstructed parent source differs")

    expected_targets = [line for line in lines if line["model_decision_required"]]
    observed_targets: list[dict[str, Any]] = []
    prompt_sha256 = hashlib.sha256(
        _round4_module().ROUND4_STAGE_A_PROMPT.encode("utf-8")
    ).hexdigest()
    for batch_index, batch in enumerate(ordered):
        targets = batch.get("targets")
        if type(targets) is not list or not 1 <= len(targets) <= STAGE_A_SLOT_COUNT:
            raise ValueError("fresh Stage-A target count differs")
        expected_slots = [f"slot_{index:02d}" for index in range(len(targets))]
        if batch.get("target_slots") != expected_slots or batch.get(
            "padding_slots"
        ) != [
            f"slot_{index:02d}"
            for index in range(len(targets), STAGE_A_SLOT_COUNT)
        ]:
            raise ValueError("fresh Stage-A slot inventory differs")
        for slot, target in zip(expected_slots, targets, strict=True):
            if type(target) is not dict or set(target) != {
                "slot", *_SIMPLE_LINE_FIELDS
            }:
                raise ValueError("fresh Stage-A target fields differ")
            if target.get("slot") != slot:
                raise ValueError("fresh Stage-A target slot differs")
            line = by_id.get(str(target.get("source_line_id")))
            if line is None or {key: target[key] for key in _SIMPLE_LINE_FIELDS} != line:
                raise ValueError("fresh Stage-A target differs from parent catalog")
            observed_targets.append(copy.deepcopy(line))
        identity = {
            "version": BUILDER_VERSION,
            "stage": "line_decision",
            "pipeline_revision": 4,
            "prompt_sha256": prompt_sha256,
            "parent_document_id": parent_id,
            "batch_index_within_parent": batch_index,
            "target_source_line_ids": [
                target["source_line_id"] for target in targets
            ],
            "full_parent_line_catalog_sha256": batch[
                "full_parent_line_catalog_sha256"
            ],
        }
        if batch.get("batch_id") != sha256_json(identity):
            raise ValueError("fresh Stage-A batch identity differs")
    if observed_targets != expected_targets:
        raise ValueError("fresh Stage-A targets do not cover model lines exactly once")
    return {
        **_parent_metadata(exemplar),
        "parent_source_text": parent_source_text,
        "source_line_ids_sha256": sha256_json(source_line_ids),
        "lines": lines,
    }


def _validated_fresh_stage_a_parents(
    batches: Sequence[Mapping[str, Any]], manifest: Mapping[str, Any]
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for batch in batches:
        grouped[str(batch.get("parent_document_id"))].append(batch)
    selected_ids = list(manifest["selected_parent_ids"])
    if set(grouped) != set(selected_ids):
        raise ValueError("fresh Stage-A batches do not cover every selected parent")
    return [
        _validated_parent_from_stage_a_batches(grouped[parent_id])
        for parent_id in sorted(grouped)
    ]


def _validate_stage_a_output_row(
    *,
    runner: Any,
    batch: Mapping[str, Any],
    row: Mapping[str, Any],
    input_manifest_sha256: str,
    input_chunk_sha256: str,
    generation_config_sha256: str,
) -> None:
    """Replay every deterministic Stage-A output and provenance check."""
    if type(row) is not dict:
        raise ValueError("fresh Stage-A output row is not an object")
    expected_fields = {
        "version",
        "runner_version",
        "round",
        "artifact_scope",
        "development_only",
        "authorizations",
        "document_id",
        "batch_id",
        "parent_document_id",
        "candidate_cycle_id",
        "stage",
        "input_batch_sha256",
        "input_manifest_sha256",
        "input_chunk_sha256",
        "generation_config_sha256",
        "model_id",
        "model_revision",
        "runtime_lock_sha256",
        "system_prompt_sha256",
        "user_message_sha256",
        "json_schema_sha256",
        "structured_decoding_backend",
        "structured_decoding_disable_any_whitespace",
        "temperature",
        "thinking_enabled",
        "seed",
        "max_model_len",
        "max_output_tokens",
        "prompt_tokens_used",
        "output_tokens",
        "context_margin_tokens",
        "input_truncated",
        "finish_reason",
        "stop_reason",
        "raw_output",
        "raw_output_sha256",
        "raw_output_n_chars",
        "raw_output_utf8_n_bytes",
        "model_output",
        "model_output_sha256",
        "parse_status",
        "parse_error",
        "stage_validation",
        "technical_pass",
    }
    if set(row) != expected_fields:
        raise ValueError("fresh Stage-A output wrapper fields differ")
    fixed = {
        "version": runner.OUTPUT_VERSION,
        "runner_version": runner.RUNNER_VERSION,
        "round": 4,
        "artifact_scope": runner.ARTIFACT_SCOPE,
        "development_only": True,
        "authorizations": runner.DEVELOPMENT_AUTHORIZATIONS,
        "document_id": batch["document_id"],
        "batch_id": batch["batch_id"],
        "parent_document_id": batch["parent_document_id"],
        "candidate_cycle_id": batch["candidate_cycle_id"],
        "stage": "line_decision",
        "input_batch_sha256": sha256_json(batch),
        "input_manifest_sha256": input_manifest_sha256,
        "input_chunk_sha256": input_chunk_sha256,
        "generation_config_sha256": generation_config_sha256,
        "model_id": runner.MODEL_ID,
        "model_revision": runner.MODEL_REVISION,
        "runtime_lock_sha256": runner.RUNTIME_LOCK_SHA256,
        "system_prompt_sha256": hashlib.sha256(
            _round4_module().ROUND4_STAGE_A_PROMPT.encode("utf-8")
        ).hexdigest(),
        "user_message_sha256": hashlib.sha256(
            runner.render_batch_prompt(batch).encode("utf-8")
        ).hexdigest(),
        "json_schema_sha256": sha256_json(runner.schema_for_batch(batch)),
        "structured_decoding_backend": runner.STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            runner.STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "temperature": 0.0,
        "thinking_enabled": False,
        "seed": runner.GENERATION_SEED,
        "input_truncated": False,
        "finish_reason": "stop",
        "parse_status": "parsed",
        "parse_error": None,
        "technical_pass": True,
    }
    for field, expected in fixed.items():
        if type(row.get(field)) is not type(expected) or row.get(field) != expected:
            raise ValueError(f"fresh Stage-A output provenance differs: {field}")
    raw_output = row.get("raw_output")
    model_output = row.get("model_output")
    if type(raw_output) is not str or type(model_output) is not dict:
        raise ValueError("fresh Stage-A output payload is absent")
    parsed_raw = runner.strict_json_loads(raw_output)
    if type(parsed_raw) is not dict or parsed_raw != model_output:
        raise ValueError("fresh Stage-A raw and parsed outputs differ")
    if (
        row.get("raw_output_sha256")
        != hashlib.sha256(raw_output.encode("utf-8")).hexdigest()
        or row.get("raw_output_n_chars") != len(raw_output)
        or row.get("raw_output_utf8_n_bytes") != len(raw_output.encode("utf-8"))
        or row.get("model_output_sha256") != sha256_json(model_output)
    ):
        raise ValueError("fresh Stage-A output payload hashes differ")
    validation = runner.validate_stage_output(batch, model_output)
    if (
        validation.get("schema_valid") is not True
        or validation.get("coverage_complete") is not True
        or validation.get("source_projection_valid") is not True
        or validation.get("errors") != []
        or row.get("stage_validation") != validation
    ):
        raise ValueError("fresh Stage-A model output fails exact schema replay")
    for field in (
        "max_model_len",
        "max_output_tokens",
        "prompt_tokens_used",
        "output_tokens",
        "context_margin_tokens",
    ):
        if not _typed_int(row.get(field)):
            raise ValueError(f"fresh Stage-A output numeric field differs: {field}")
    if (
        row["max_model_len"] != runner.MAX_MODEL_LEN
        or row["max_output_tokens"] != runner.MAX_OUTPUT_TOKENS["line_decision"]
        or row["prompt_tokens_used"] < 1
        or row["output_tokens"] < 0
        or row["context_margin_tokens"]
        != row["max_model_len"]
        - row["prompt_tokens_used"]
        - row["max_output_tokens"]
        or row["context_margin_tokens"] < runner.CONTEXT_SAFETY_MARGIN_TOKENS
    ):
        raise ValueError("fresh Stage-A output token accounting differs")


def authenticate_fresh_stage_a_run(
    stage_a_input_dir: Path,
    stage_a_output_dir: Path,
    *,
    expected_input_manifest_sha256: str,
    expected_output_inventory_sha256: str | None = None,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
]:
    """Authenticate a complete fresh Stage-A run and bind its exact bytes."""
    runner = _round4_runner_module()
    manifest, manifest_sha256, input_paths = runner.validate_input_manifest(
        stage_a_input_dir / "manifest.json",
        stage_a_input_dir / "chunks",
        stage="line_decision",
        expected_manifest_sha256=expected_input_manifest_sha256,
    )
    input_rows_by_name = {
        path.name: runner.read_gzip_jsonl(path) for path in input_paths
    }
    batches = [
        row for path in input_paths for row in input_rows_by_name[path.name]
    ]
    parents = _validated_fresh_stage_a_parents(batches, manifest)

    if stage_a_output_dir.is_symlink() or not stage_a_output_dir.is_dir():
        raise ValueError("fresh Stage-A output directory is absent or a symlink")
    expected_output_names = [
        path.name.replace(".jsonl.gz", ".out.jsonl.gz") for path in input_paths
    ]
    observed_output_names = sorted(
        path.name for path in stage_a_output_dir.glob("chunk_*.out.jsonl.gz")
    )
    if observed_output_names != expected_output_names:
        raise ValueError("fresh Stage-A output chunks are incomplete or unexpected")
    metadata_paths = sorted(stage_a_output_dir.glob("run_metadata*.json"))
    if not metadata_paths:
        raise ValueError("fresh Stage-A run metadata is absent")
    if any(path.is_symlink() or not path.is_file() for path in metadata_paths):
        raise ValueError("fresh Stage-A run metadata is not a regular file")
    metadata_names = [path.name for path in metadata_paths]
    if "run_metadata.json" in metadata_names:
        if metadata_names != ["run_metadata.json"]:
            raise ValueError("fresh Stage-A mixes sharded and unsharded metadata")
    else:
        expected_metadata_names = [
            f"run_metadata_shard_{index:05d}.json"
            for index in range(len(metadata_paths))
        ]
        if metadata_names != expected_metadata_names:
            raise ValueError("fresh Stage-A shard metadata names are not canonical")

    metadata_rows = [strict_json(path) for path in metadata_paths]
    generation_hashes: set[str] = set()
    selected_chunk_names: list[str] = []
    metadata_by_selected_name: dict[str, Mapping[str, Any]] = {}
    for metadata in metadata_rows:
        if type(metadata) is not dict:
            raise ValueError("fresh Stage-A run metadata is not an object")
        if set(metadata) != {
            "version",
            "runner_version",
            "round",
            "artifact_scope",
            "development_only",
            "authorizations",
            "stage",
            "input_manifest_sha256",
            "generation_config_sha256",
            "generation_config",
            "selected_chunk_names",
            "selected_chunk_count",
            "processed_output_count",
            "technical_pass_count",
            "technical_fail_count",
            "parse_status_counts",
            "started_at",
            "completed_at",
            "host",
            "slurm_job_id",
            "slurm_array_task_id",
        }:
            raise ValueError("fresh Stage-A run metadata fields differ")
        if (
            metadata.get("version") != runner.RUN_METADATA_VERSION
            or metadata.get("runner_version") != runner.RUNNER_VERSION
            or metadata.get("round") != 4
            or metadata.get("artifact_scope") != runner.ARTIFACT_SCOPE
            or metadata.get("development_only") is not True
            or metadata.get("authorizations") != runner.DEVELOPMENT_AUTHORIZATIONS
            or metadata.get("stage") != "line_decision"
            or metadata.get("input_manifest_sha256") != manifest_sha256
        ):
            raise ValueError("fresh Stage-A run metadata provenance differs")
        generation = metadata.get("generation_config")
        generation_sha256 = metadata.get("generation_config_sha256")
        expected_generation = runner.generation_config_payload(
            argparse.Namespace(
                stage="line_decision",
                seed=runner.GENERATION_SEED,
                max_model_len=runner.MAX_MODEL_LEN,
                max_output_tokens=runner.MAX_OUTPUT_TOKENS["line_decision"],
                gpu_memory_utilization=runner.GPU_MEMORY_UTILIZATION,
            ),
            input_manifest_sha256=manifest_sha256,
        )
        if (
            type(generation) is not dict
            or type(generation_sha256) is not str
            or sha256_json(generation) != generation_sha256
            or generation != expected_generation
        ):
            raise ValueError("fresh Stage-A generation configuration differs")
        generation_hashes.add(generation_sha256)
        names = metadata.get("selected_chunk_names")
        if (
            type(names) is not list
            or any(type(name) is not str for name in names)
            or metadata.get("selected_chunk_count") != len(names)
        ):
            raise ValueError("fresh Stage-A metadata chunk inventory differs")
        for name in names:
            if name in metadata_by_selected_name:
                raise ValueError("fresh Stage-A metadata selects a chunk twice")
            metadata_by_selected_name[name] = metadata
        selected_chunk_names.extend(names)
    if len(generation_hashes) != 1:
        raise ValueError("fresh Stage-A shards use different generation settings")
    if sorted(selected_chunk_names) != [path.name for path in input_paths]:
        raise ValueError("fresh Stage-A metadata does not cover every input chunk")
    generation_sha256 = next(iter(generation_hashes))

    output_rows_by_input_name: dict[str, list[dict[str, Any]]] = {}
    output_entries: list[dict[str, Any]] = []
    output_rows: list[dict[str, Any]] = []
    for input_path in input_paths:
        output_path = stage_a_output_dir / input_path.name.replace(
            ".jsonl.gz", ".out.jsonl.gz"
        )
        if output_path.is_symlink() or not output_path.is_file():
            raise ValueError("fresh Stage-A output chunk is not a regular file")
        rows = runner.read_gzip_jsonl(output_path)
        input_rows = input_rows_by_name[input_path.name]
        if len(rows) != len(input_rows):
            raise ValueError("fresh Stage-A input/output row counts differ")
        if [row.get("batch_id") for row in rows] != [
            batch["batch_id"] for batch in input_rows
        ]:
            raise ValueError("fresh Stage-A input/output batch order differs")
        for batch, row in zip(input_rows, rows, strict=True):
            _validate_stage_a_output_row(
                runner=runner,
                batch=batch,
                row=row,
                input_manifest_sha256=manifest_sha256,
                input_chunk_sha256=sha256_file(input_path),
                generation_config_sha256=generation_sha256,
            )
        output_rows_by_input_name[input_path.name] = rows
        output_rows.extend(rows)
        output_entries.append(
            {
                "file": output_path.name,
                "bytes": output_path.stat().st_size,
                "sha256": sha256_file(output_path),
                "records": len(rows),
                "batch_ids_sha256": sha256_json(
                    [str(row["batch_id"]) for row in rows]
                ),
                "rows_sha256": sha256_json(rows),
            }
        )

    for metadata in metadata_rows:
        selected = list(metadata["selected_chunk_names"])
        rows = [
            row for name in selected for row in output_rows_by_input_name[name]
        ]
        counts = dict(
            sorted(Counter(str(row["parse_status"]) for row in rows).items())
        )
        if (
            metadata.get("processed_output_count") != len(rows)
            or metadata.get("technical_pass_count") != len(rows)
            or metadata.get("technical_fail_count") != 0
            or metadata.get("parse_status_counts") != counts
        ):
            raise ValueError("fresh Stage-A run metadata counts differ")
    if [row["batch_id"] for row in output_rows] != manifest["expected_batch_ids"]:
        raise ValueError("fresh Stage-A completed output order differs")

    metadata_entries = [
        {
            "file": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in metadata_paths
    ]
    inventory_payload = {
        "version": "candidate_endorsement_round4_stage_a_output_inventory_v1",
        "input_manifest_sha256": manifest_sha256,
        "generation_config_sha256": generation_sha256,
        "ordered_output_batch_ids_sha256": sha256_json(
            [row["batch_id"] for row in output_rows]
        ),
        "output_files": output_entries,
        "run_metadata_files": metadata_entries,
    }
    output_inventory_sha256 = sha256_json(inventory_payload)
    if (
        expected_output_inventory_sha256 is not None
        and expected_output_inventory_sha256 != output_inventory_sha256
    ):
        raise ValueError("fresh Stage-A output inventory differs from expectation")
    provenance = {
        **inventory_payload,
        "output_inventory_sha256": output_inventory_sha256,
    }
    return batches, output_rows, {"manifest": manifest, "parents": parents}, provenance


def _fresh_stage_a_decision_ledger(
    batches: Sequence[Mapping[str, Any]],
    output_rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    if len(batches) != len(output_rows):
        raise ValueError("fresh Stage-A decision input/output counts differ")
    decisions: dict[str, dict[str, Any]] = {}
    for batch, output_row in zip(batches, output_rows, strict=True):
        if output_row.get("batch_id") != batch.get("batch_id"):
            raise ValueError("fresh Stage-A decision batch identity differs")
        model_output = output_row["model_output"]
        for target in batch["targets"]:
            line_id = str(target["source_line_id"])
            if line_id in decisions:
                raise ValueError("fresh Stage-A source line has two decisions")
            slot = str(target["slot"])
            decisions[line_id] = {
                "source_line_id": line_id,
                "role": model_output[slot]["role"],
                "stage_a_batch_id": batch["batch_id"],
                "stage_a_slot": slot,
                "stage_a_role_status": "advisory_non_authoritative",
            }
    return decisions


def _line_issue(
    parent: Mapping[str, Any], line_id: str, role: str
) -> dict[str, Any]:
    by_id = {
        str(line["source_line_id"]): line for line in parent["lines"]
    }
    line = by_id[line_id]
    return {
        "parent_document_id": parent["parent_document_id"],
        "source_line_id": line_id,
        "parent_line_index": line["parent_line_index"],
        "line_text_sha256": line["line_text_sha256"],
        "stage_a_role": role,
    }


def build_fresh_stage_b_batches(
    *,
    parents: Sequence[Mapping[str, Any]],
    stage_a_batches: Sequence[Mapping[str, Any]],
    stage_a_output_rows: Sequence[Mapping[str, Any]],
    stage_a_input_manifest_sha256: str,
    stage_a_output_inventory_sha256: str,
    read_only_context_radius: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build Stage B from authenticated Stage A without semantic inference."""
    from src.candidate_endorsement_line_ledger import (
        ANCHOR_ROLES,
        _record_block,
        continuation_ownership_report,
    )

    if type(read_only_context_radius) is not int or read_only_context_radius < 0:
        raise ValueError("read_only_context_radius must be a nonnegative integer")
    decisions = _fresh_stage_a_decision_ledger(
        stage_a_batches, stage_a_output_rows
    )
    expected_decision_ids = {
        str(line["source_line_id"])
        for parent in parents
        for line in parent["lines"]
        if line["model_decision_required"]
    }
    if set(decisions) != expected_decision_ids:
        raise ValueError("fresh Stage-A decision ledger coverage differs")
    prompt_sha256 = hashlib.sha256(
        _round4_module().ROUND4_STAGE_B_PROMPT.encode("utf-8")
    ).hexdigest()
    decisions_sha256 = sha256_json(decisions)
    result: list[dict[str, Any]] = []
    unclear_lines: list[dict[str, Any]] = []
    unowned_lines: list[dict[str, Any]] = []
    ambiguous_ownership_lines: list[dict[str, Any]] = []
    parent_issue_counts: list[dict[str, Any]] = []
    role_counts: Counter[str] = Counter(
        str(decision["role"]) for decision in decisions.values()
    )
    parent_without_anchor_ids: list[str] = []
    anchor_count = 0
    owned_continuation_count = 0

    for parent in sorted(parents, key=lambda row: str(row["parent_document_id"])):
        parent_id = str(parent["parent_document_id"])
        parent_line_ids = {
            str(line["source_line_id"]) for line in parent["lines"]
        }
        parent_decisions = {
            line_id: decision
            for line_id, decision in decisions.items()
            if line_id in parent_line_ids
        }
        ownership = continuation_ownership_report(parent, parent_decisions)
        unowned_ids = list(ownership["unowned_continuation_ids"])
        ambiguous_ids = sorted(
            {
                *ownership["invalid_continuation_owner_ids"],
                *ownership["duplicate_continuation_owner_ids"],
            }
        )
        parent_unclear_ids = sorted(
            line_id
            for line_id, decision in parent_decisions.items()
            if decision["role"] == "unclear"
        )
        unclear_lines.extend(
            _line_issue(parent, line_id, "unclear")
            for line_id in parent_unclear_ids
        )
        unowned_lines.extend(
            _line_issue(parent, line_id, str(parent_decisions[line_id]["role"]))
            for line_id in unowned_ids
        )
        ambiguous_ownership_lines.extend(
            _line_issue(parent, line_id, str(parent_decisions[line_id]["role"]))
            for line_id in ambiguous_ids
        )
        owned_continuation_count += int(ownership["owned_continuation_count"])
        anchors = [
            line
            for line in parent["lines"]
            if parent_decisions.get(str(line["source_line_id"]), {}).get("role")
            in ANCHOR_ROLES
        ]
        if not anchors:
            parent_without_anchor_ids.append(parent_id)
        anchor_count += len(anchors)
        parent_issue_counts.append(
            {
                "parent_document_id": parent_id,
                "anchor_count": len(anchors),
                "unclear_line_count": len(parent_unclear_ids),
                "unowned_continuation_count": len(unowned_ids),
                "ambiguous_ownership_line_count": len(ambiguous_ids),
            }
        )

        targets: list[dict[str, Any]] = []
        continuation_uses: Counter[str] = Counter()
        for anchor in anchors:
            anchor_id = str(anchor["source_line_id"])
            decision = parent_decisions[anchor_id]
            block = _record_block(
                parent,
                anchor,
                parent_decisions,
                ownership["ownership_by_continuation"],
            )
            for previous, current in zip(block, block[1:]):
                if current["parent_start_char"] != previous["parent_full_end_char"]:
                    raise ValueError("fresh Stage-B record block is not contiguous")
            for line in block:
                line_id = str(line["source_line_id"])
                if line_id != anchor_id:
                    continuation_uses[line_id] += 1
            block_text = "".join(
                str(line["line_text"]) + str(line["line_terminator"])
                for line in block
            )
            targets.append(
                {
                    **copy.deepcopy(anchor),
                    "stage_a_batch_id": decision["stage_a_batch_id"],
                    "stage_a_slot": decision["stage_a_slot"],
                    "stage_a_role": decision["role"],
                    "stage_a_role_status": "advisory_non_authoritative",
                    "record_block_lines": copy.deepcopy(block),
                    "record_block_text": block_text,
                    "record_block_text_sha256": hashlib.sha256(
                        block_text.encode("utf-8")
                    ).hexdigest(),
                    "record_block_source_line_ids_sha256": sha256_json(
                        [line["source_line_id"] for line in block]
                    ),
                }
            )
        owned_ids = set(ownership["ownership_by_continuation"])
        if set(continuation_uses) != owned_ids or any(
            count != 1 for count in continuation_uses.values()
        ):
            raise ValueError("fresh Stage-B owned continuations are not used once")

        for group_index, offset in enumerate(
            range(0, len(targets), STAGE_B_SLOT_COUNT)
        ):
            selected = targets[offset : offset + STAGE_B_SLOT_COUNT]
            slotted = [
                {"slot": f"slot_{index:02d}", **copy.deepcopy(target)}
                for index, target in enumerate(selected)
            ]
            identity = {
                "version": BUILDER_VERSION,
                "stage": "record_extraction",
                "pipeline_revision": 4,
                "prompt_sha256": prompt_sha256,
                "parent_document_id": parent_id,
                "group_index": group_index,
                "anchor_source_line_ids": [
                    target["source_line_id"] for target in selected
                ],
                "stage_a_input_manifest_sha256": stage_a_input_manifest_sha256,
                "stage_a_output_inventory_sha256": (
                    stage_a_output_inventory_sha256
                ),
                "stage_a_decisions_sha256": decisions_sha256,
            }
            context_fields: dict[str, Any] = {}
            if read_only_context_radius:
                owned_line_ids = {
                    line["source_line_id"]
                    for target in selected
                    for line in target["record_block_lines"]
                }
                context_indices = {
                    int(line["parent_line_index"])
                    for line in parent["lines"]
                    if parent_decisions.get(str(line["source_line_id"]), {}).get(
                        "role"
                    ) == "support_or_category_heading"
                }
                for target in selected:
                    indices = [int(line["parent_line_index"])
                               for line in target["record_block_lines"]]
                    context_indices.update(range(
                        max(0, min(indices) - read_only_context_radius),
                        max(indices) + read_only_context_radius + 1,
                    ))
                context_lines = [
                    _simple_line(line) for line in parent["lines"]
                    if int(line["parent_line_index"]) in context_indices
                    and line["source_line_id"] not in owned_line_ids
                ]
                context_fields = {
                    "read_only_context_radius": read_only_context_radius,
                    "read_only_context_policy": READ_ONLY_CONTEXT_POLICY,
                    "read_only_context_lines": context_lines,
                    "read_only_context_lines_sha256": sha256_json(context_lines),
                    "parent_source_text": parent["parent_source_text"],
                }
                identity.update({key: context_fields[key] for key in (
                    "read_only_context_radius", "read_only_context_policy",
                    "read_only_context_lines_sha256",
                )})
            batch_id = sha256_json(identity)
            result.append(
                {
                    "version": BUILDER_VERSION,
                    "stage": "record_extraction",
                    "batch_id": batch_id,
                    "document_id": batch_id,
                    **_parent_metadata(parent),
                    "stage_a_input_manifest_sha256": (
                        stage_a_input_manifest_sha256
                    ),
                    "stage_a_output_inventory_sha256": (
                        stage_a_output_inventory_sha256
                    ),
                    "stage_a_decisions_sha256": decisions_sha256,
                    "batch_index_within_parent": group_index,
                    "target_slots": [target["slot"] for target in slotted],
                    "padding_slots": [
                        f"slot_{index:02d}"
                        for index in range(len(slotted), STAGE_B_SLOT_COUNT)
                    ],
                    "targets": slotted,
                    **context_fields,
                }
            )

    ambiguous_line_ids = sorted(
        {
            *(row["source_line_id"] for row in unclear_lines),
            *(row["source_line_id"] for row in ambiguous_ownership_lines),
        }
    )
    diagnostics = {
        "version": "candidate_endorsement_round4_fresh_stage_a_handoff_v1",
        "stage_a_roles_are_advisory": True,
        "stage_a_decision_count": len(decisions),
        "stage_a_decisions_sha256": decisions_sha256,
        "stage_a_role_counts": dict(sorted(role_counts.items())),
        "anchor_count": anchor_count,
        "owned_continuation_count": owned_continuation_count,
        "stage_b_batch_count": len(result),
        "stage_b_target_count": sum(len(batch["targets"]) for batch in result),
        "unclear_line_count": len(unclear_lines),
        "unclear_lines": unclear_lines,
        "unowned_continuation_count": len(unowned_lines),
        "unowned_continuation_lines": unowned_lines,
        "ambiguous_ownership_line_count": len(ambiguous_ownership_lines),
        "ambiguous_ownership_lines": ambiguous_ownership_lines,
        "ambiguous_line_definition": (
            "union_of_stage_a_unclear_and_invalid_or_duplicate_continuation_"
            "ownership;_logged_for_later_human_review"
        ),
        "ambiguous_line_count": len(ambiguous_line_ids),
        "ambiguous_line_ids": ambiguous_line_ids,
        "parent_without_anchor_count": len(parent_without_anchor_ids),
        "parent_without_anchor_ids": parent_without_anchor_ids,
        "parent_issue_counts": parent_issue_counts,
        "handoff_policy": (
            "build_blocks_for_stage_a_anchor_proposals;_attach_only_"
            "deterministically_owned_directional_continuations;_exclude_and_"
            "log_unclear_unowned_or_ambiguous_lines"
        ),
    }
    if read_only_context_radius:
        diagnostics.update({
            "read_only_context_radius": read_only_context_radius,
            "read_only_context_policy": READ_ONLY_CONTEXT_POLICY,
            "read_only_context_line_count": sum(
                len(batch["read_only_context_lines"]) for batch in result
            ),
            "read_only_context_inventory_sha256": sha256_json([
                {"batch_id": batch["batch_id"],
                 "read_only_context_lines_sha256": batch["read_only_context_lines_sha256"]}
                for batch in result
            ]),
        })
    diagnostics["handoff_diagnostics_sha256"] = sha256_json(diagnostics)
    if diagnostics["stage_b_target_count"] != anchor_count:
        raise ValueError("fresh Stage-B target coverage differs from anchors")
    if len({batch["batch_id"] for batch in result}) != len(result):
        raise ValueError("fresh Stage-B batch identities duplicate")
    return result, diagnostics


def build_fresh_stage_b_artifact(
    stage_a_input_dir: Path,
    stage_a_output_dir: Path,
    output_dir: Path,
    *,
    expected_stage_a_input_manifest_sha256: str,
    expected_stage_a_output_inventory_sha256: str | None = None,
    chunk_size: int = 60,
    read_only_context_radius: int = 0,
) -> dict[str, Any]:
    batches, output_rows, authenticated, stage_a_provenance = (
        authenticate_fresh_stage_a_run(
            stage_a_input_dir,
            stage_a_output_dir,
            expected_input_manifest_sha256=(
                expected_stage_a_input_manifest_sha256
            ),
            expected_output_inventory_sha256=(
                expected_stage_a_output_inventory_sha256
            ),
        )
    )
    manifest = authenticated["manifest"]
    stage_b_batches, diagnostics = build_fresh_stage_b_batches(
        parents=authenticated["parents"],
        stage_a_batches=batches,
        stage_a_output_rows=output_rows,
        stage_a_input_manifest_sha256=(
            expected_stage_a_input_manifest_sha256
        ),
        stage_a_output_inventory_sha256=stage_a_provenance[
            "output_inventory_sha256"
        ],
        read_only_context_radius=read_only_context_radius,
    )
    if not stage_b_batches:
        raise ValueError("fresh Stage A produced no Stage-B anchor proposals")
    independent_provenance = {}
    if manifest['sample_round'] == 4:
        independent_provenance = {
            key: copy.deepcopy(manifest['provenance'][key])
            for key in ('independent_sample_binding', 'independent_sample_source_pairs')
        }
    return _write_artifact(
        output_dir=output_dir,
        stage="record_extraction",
        batches=stage_b_batches,
        selected_parent_ids=list(manifest["selected_parent_ids"]),
        provenance={
            "handoff_version": diagnostics["version"],
            "stage_a_input_manifest_sha256": (
                expected_stage_a_input_manifest_sha256
            ),
            "stage_a_output": stage_a_provenance,
            "stage_a_roles_are_advisory": True,
            "handoff_diagnostics": diagnostics,
            "ai_draft_used_in_prompt": False,
            "semantic_inference_performed_by_handoff": False,
            **independent_provenance,
        },
        chunk_size=chunk_size,
        sample_round=manifest['sample_round'],
    )


def _verified_round3_stage_b_batches(repo: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    report_path = repo / ROUND3_STAGE_B_REPORT
    draft_path = repo / AI_DRAFT
    if sha256_file(report_path) != ROUND3_STAGE_B_REPORT_SHA256:
        raise ValueError("round-three Stage-B report bytes differ")
    if sha256_file(draft_path) != AI_DRAFT_SHA256:
        raise ValueError("provisional AI draft bytes differ")
    report = strict_json(report_path)
    if report.get("gate_decision") != "NO_GO":
        raise ValueError("round-three Stage-B report status differs")
    root = repo / ROUND3_STAGE_B_INPUT
    manifest = strict_json(root / "manifest.json")
    entries = manifest.get("files", {}).get("chunks")
    if type(entries) is not list or not entries:
        raise ValueError("round-three Stage-B chunk inventory is absent")
    rows: list[dict[str, Any]] = []
    for entry in entries:
        path = root / "chunks" / entry["file"]
        if (
            sha256_file(path) != entry["sha256"]
            or path.stat().st_size != entry["bytes"]
        ):
            raise ValueError(f"round-three Stage-B input bytes differ: {path}")
        chunk_rows = read_gzip_jsonl(path)
        if len(chunk_rows) != entry["records"]:
            raise ValueError("round-three Stage-B input row count differs")
        rows.extend(chunk_rows)
    if [row["batch_id"] for row in rows] != manifest["expected_batch_ids"]:
        raise ValueError("round-three Stage-B input order differs")
    return rows, manifest


def build_regression_stage_b_batches(repo: Path) -> tuple[list[dict[str, Any]], list[str]]:
    source_batches, manifest = _verified_round3_stage_b_batches(repo)
    prompt_sha256 = hashlib.sha256(
        _round4_module().ROUND4_STAGE_B_PROMPT.encode("utf-8")
    ).hexdigest()
    result: list[dict[str, Any]] = []
    for source_batch in source_batches:
        targets = list(source_batch["targets"])
        for group_index, offset in enumerate(range(0, len(targets), STAGE_B_SLOT_COUNT)):
            selected = targets[offset : offset + STAGE_B_SLOT_COUNT]
            target_rows = []
            for slot_index, target in enumerate(selected):
                target_rows.append(
                    {
                        "slot": f"slot_{slot_index:02d}",
                        "source_line_id": target["source_line_id"],
                        "parent_line_index": target["parent_line_index"],
                        "parent_start_char": target["parent_start_char"],
                        "parent_end_char": target["parent_end_char"],
                        "line_text": target["line_text"],
                        "line_text_sha256": target["line_text_sha256"],
                        "stage_a_role": target["stage_a_role"],
                        "record_block_lines": copy.deepcopy(
                            target["record_block_lines"]
                        ),
                    }
                )
            identity = {
                "version": BUILDER_VERSION,
                "stage": "record_extraction",
                "pipeline_revision": 4,
                "prompt_sha256": prompt_sha256,
                "source_batch_id": source_batch["batch_id"],
                "group_index": group_index,
                "source_line_ids": [row["source_line_id"] for row in target_rows],
            }
            batch_id = sha256_json(identity)
            result.append(
                {
                    "version": BUILDER_VERSION,
                    "stage": "record_extraction",
                    "batch_id": batch_id,
                    "document_id": batch_id,
                    "source_round3_batch_id": source_batch["batch_id"],
                    "parent_document_id": source_batch["parent_document_id"],
                    "candidate_cycle_id": source_batch["candidate_cycle_id"],
                    "candidate_icpsr": source_batch["candidate_icpsr"],
                    "state": source_batch["state"],
                    "office": source_batch["office"],
                    "year": source_batch["year"],
                    "party": source_batch["party"],
                    "extraction_source_text_sha256": source_batch[
                        "extraction_source_text_sha256"
                    ],
                    "extraction_source_n_chars": source_batch[
                        "extraction_source_n_chars"
                    ],
                    "endorsement_line_context": copy.deepcopy(
                        source_batch["endorsement_line_context"]
                    ),
                    "parent_source_text": source_batch["parent_source_text"],
                    "target_slots": [row["slot"] for row in target_rows],
                    "padding_slots": [
                        f"slot_{index:02d}"
                        for index in range(len(target_rows), STAGE_B_SLOT_COUNT)
                    ],
                    "targets": target_rows,
                }
            )
    parent_ids = [str(value) for value in manifest["selected_parent_ids"]]
    if len(result) == 0 or len(parent_ids) != 40:
        raise ValueError("round-four regression Stage-B closure differs")
    return result, parent_ids


def build_regression_stage_b_artifact(
    repo: Path, output_dir: Path, *, chunk_size: int = 60
) -> dict[str, Any]:
    batches, parent_ids = build_regression_stage_b_batches(repo)
    return _write_artifact(
        output_dir=output_dir,
        stage="record_extraction",
        batches=batches,
        selected_parent_ids=parent_ids,
        provenance={
            "round3_stage_b_report_sha256": ROUND3_STAGE_B_REPORT_SHA256,
            "round3_stage_b_input_manifest_sha256": sha256_file(
                repo / ROUND3_STAGE_B_INPUT / "manifest.json"
            ),
            "ai_draft_results_sha256": AI_DRAFT_SHA256,
            "development_labels_authoritative": False,
            "ai_draft_used_in_prompt": False,
            "source_sample_reused_for_prompt_regression_only": True,
        },
        chunk_size=chunk_size,
    )


def _print_manifest(output_dir: Path, manifest: Mapping[str, Any]) -> None:
    manifest_path = output_dir / "manifest.json"
    summary: dict[str, Any] = {
        "output_dir": str(output_dir),
        "manifest_sha256": sha256_file(manifest_path),
        "stage": manifest["stage"],
        "batch_count": manifest["batch_count"],
        "parent_count": manifest["selected_parent_count"],
        "chunk_record_counts": [
            entry["records"] for entry in manifest["chunks"]
        ],
    }
    diagnostics = manifest.get("provenance", {}).get("handoff_diagnostics")
    if isinstance(diagnostics, Mapping):
        summary["handoff_counts"] = {
            "stage_a_decisions": diagnostics["stage_a_decision_count"],
            "stage_b_targets": diagnostics["stage_b_target_count"],
            "unclear_lines": diagnostics["unclear_line_count"],
            "unowned_continuations": diagnostics[
                "unowned_continuation_count"
            ],
            "ambiguous_ownership_lines": diagnostics[
                "ambiguous_ownership_line_count"
            ],
        }
    print(canonical_json(summary))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    regression = subparsers.add_parser("regression-stage-b")
    regression.add_argument("--repo", type=Path, default=REPO)
    regression.add_argument("--output-dir", type=Path, required=True)
    regression.add_argument("--chunk-size", type=int, default=60)
    fresh = subparsers.add_parser("fresh-stage-a")
    fresh.add_argument("--repo", type=Path, default=REPO)
    fresh.add_argument("--selection", type=Path, required=True)
    fresh.add_argument("--expected-selection-sha256", required=True)
    fresh.add_argument("--output-dir", type=Path, required=True)
    fresh.add_argument("--chunk-size", type=int, default=60)
    handoff = subparsers.add_parser("fresh-stage-b")
    handoff.add_argument("--stage-a-input-dir", type=Path, required=True)
    handoff.add_argument("--stage-a-output-dir", type=Path, required=True)
    handoff.add_argument(
        "--expected-stage-a-input-manifest-sha256", required=True
    )
    handoff.add_argument("--expected-stage-a-output-inventory-sha256")
    handoff.add_argument("--output-dir", type=Path, required=True)
    handoff.add_argument("--chunk-size", type=int, default=60)
    handoff.add_argument("--read-only-context-radius", type=int, default=0)
    args = parser.parse_args(argv)
    output_dir = args.output_dir.resolve()
    if args.command == "regression-stage-b":
        manifest = build_regression_stage_b_artifact(
            args.repo.resolve(), output_dir, chunk_size=args.chunk_size
        )
    elif args.command == "fresh-stage-a":
        manifest = build_fresh_stage_a_artifact(
            args.repo.resolve(),
            args.selection.resolve(),
            output_dir,
            expected_selection_sha256=args.expected_selection_sha256,
            chunk_size=args.chunk_size,
        )
    else:
        manifest = build_fresh_stage_b_artifact(
            args.stage_a_input_dir.resolve(),
            args.stage_a_output_dir.resolve(),
            output_dir,
            expected_stage_a_input_manifest_sha256=(
                args.expected_stage_a_input_manifest_sha256
            ),
            expected_stage_a_output_inventory_sha256=(
                args.expected_stage_a_output_inventory_sha256
            ),
            chunk_size=args.chunk_size,
            read_only_context_radius=args.read_only_context_radius,
        )
    _print_manifest(output_dir, manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
