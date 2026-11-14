#!/usr/bin/env python3
"""Run byte-distinct round-four endorsement development batches on Hive.

The runner accepts only explicitly development-scoped round-four artifacts.  It
authenticates the externally reviewed manifest bytes and every chunk before it
loads vLLM, uses the round-four JSON schemas for constrained decoding, and
publishes each output with an exclusive atomic link.  Existing outputs are
never reused or overwritten.  This module does not submit Slurm jobs and none
of its artifacts authorize human truth, production integration, expansion, or
release.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
import re
import socket
import sys
from collections import Counter
from datetime import datetime, timezone
from numbers import Integral
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from src.candidate_endorsement_round4 import (  # noqa: E402
    ROUND4_DEVELOPMENT_BINDING,
    ROUND4_DEVELOPMENT_BINDING_SHA256,
    ROUND4_STAGE_A_PROMPT,
    ROUND4_STAGE_A_PROMPT_SHA256,
    ROUND4_STAGE_B_PROMPT,
    ROUND4_STAGE_B_PROMPT_SHA256,
    STAGE_A_SLOT_COUNT,
    STAGE_B_SLOT_COUNT,
    canonical_name_from_parts,
    normalize_name_for_match,
    resolve_ordered_name_parts,
    sanitize_optional_fields,
    stage_a_schema,
    stage_b_schema,
)


LINE_DECISION_STAGE = "line_decision"
RECORD_EXTRACTION_STAGE = "record_extraction"
STAGES = (LINE_DECISION_STAGE, RECORD_EXTRACTION_STAGE)

# This opt-in binds sample four to one frozen independent assignment.
INDEPENDENT_SAMPLE4_BINDING = {
    'version': 'candidate_endorsement_independent_sample4_binding_v1',
    'sample_round': 4, 'seed': 20263827,
    'selection_sha256': '33e81123f6318e2cbcbd3f3f8cd02899111d574de3911a314495073cf45257f6',
    'selected_sources_sha256': 'f487dd6137f4235abfeae15333684aa804687d9666f2c8112623fb78ff44cf3e',
    'selection_spec_sha256': '41db6f5d76968f9dd760ac8bbdfda3a155a5d5ce95a1f153a493aa2a36569f09',
    'selected_parent_ids_sha256': 'e39edf37e6e54d5e11eb796e2645f54c49475e6ee5fe7917bade126036aee2be',
    'selected_source_pairs_sha256': '92f5effe22c3ce55cb7890f8054a1bd88dcb56a1595f725e889380397ecabb50',
}


def validate_sample_round_binding(sample_round, provenance, selected_parent_ids):
    if type(sample_round) is not int or sample_round not in (3, 4):
        raise ValueError('Input manifest sample round is unsupported.')
    if sample_round == 3:
        return None
    if (type(provenance) is not dict
            or type(provenance.get('independent_sample_binding')) is not dict
            or sha256_json(provenance['independent_sample_binding']) != sha256_json(INDEPENDENT_SAMPLE4_BINDING)):
        raise ValueError('Sample four lacks its pinned independent selection binding.')
    pairs = provenance.get('independent_sample_source_pairs')
    if (type(pairs) is not dict or len(pairs) != 40
            or any(type(key) is not str or not key or not _is_sha256(value) for key, value in pairs.items())
            or sha256_json(pairs) != INDEPENDENT_SAMPLE4_BINDING['selected_source_pairs_sha256']
            or sorted(pairs) != sorted(selected_parent_ids)
            or sha256_json(sorted(selected_parent_ids)) != INDEPENDENT_SAMPLE4_BINDING['selected_parent_ids_sha256']):
        raise ValueError('Sample four source or parent closure differs from its binding.')
    return pairs

RUNNER_VERSION = "candidate_endorsement_round4_development_vllm_v2"
MANIFEST_VERSION = "candidate_endorsement_round4_development_input_v1"
OUTPUT_VERSION = "candidate_endorsement_round4_development_output_v1"
RUN_METADATA_VERSION = "candidate_endorsement_round4_development_run_metadata_v1"
ARTIFACT_SCOPE = "candidate_endorsement_round4_development_only"

LINE_DECISION_USER_MESSAGE_RENDERING = "compact_line_decision_model_view_v1"
RECORD_EXTRACTION_USER_MESSAGE_RENDERING = "full_authenticated_batch_v1"
READ_ONLY_CONTEXT_POLICY = (
    "read_only_context_is_evidence_only;_names_must_originate_exclusively_"
    "from_their_owned_target_record_block_lines"
)
_READ_ONLY_CONTEXT_FIELDS = {
    "read_only_context_radius", "read_only_context_policy",
    "read_only_context_lines", "read_only_context_lines_sha256",
}

DEVELOPMENT_AUTHORIZATIONS = {
    "bounded_development_inference": True,
    "development_labels_authoritative": False,
    "human_truth": False,
    "production_integration": False,
    "full_expansion": False,
    "release": False,
}

MODEL_ID = "Qwen/Qwen3.6-27B-FP8"
MODEL_REVISION = "e89b16ebf1988b3d6befa7de50abc2d76f26eb09"
RUNTIME_LOCK_SHA256 = (
    "4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3"
)
GENERATION_SEED = 20260907
MAX_MODEL_LEN = 32_768
GPU_MEMORY_UTILIZATION = 0.92
MAX_OUTPUT_TOKENS = {
    LINE_DECISION_STAGE: 2_048,
    RECORD_EXTRACTION_STAGE: 16_384,
}
CONTEXT_SAFETY_MARGIN_TOKENS = 256
STRUCTURED_DECODING_BACKEND = "xgrammar"
STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE = True

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_CHUNK_RE = re.compile(r"chunk_(\d{5})\.jsonl\.gz\Z")
_MANIFEST_FIELDS = {
    "version",
    "stage",
    "sample_round",
    "pipeline_revision",
    "prompt_sha256",
    "schema_catalog",
    "schema_catalog_sha256",
    "slot_count",
    "chunk_size",
    "batch_count",
    "expected_batch_ids",
    "expected_batch_ids_sha256",
    "selected_parent_count",
    "selected_parent_ids",
    "selected_parent_ids_sha256",
    "chunks",
    "provenance",
    "authorizations",
    "manifest_payload_sha256",
}
_CHUNK_ENTRY_FIELDS = {
    "file",
    "sha256",
    "bytes",
    "records",
    "batch_ids_sha256",
}
_BATCH_REQUIRED_FIELDS = {
    "version",
    "batch_id",
    "document_id",
    "parent_document_id",
    "candidate_cycle_id",
    "stage",
    "target_slots",
    "padding_slots",
    "targets",
}


class StrictJSONError(ValueError):
    """Raised when JSON is duplicate-keyed, non-finite, or otherwise invalid."""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


def sha256_json(value: Any) -> str:
    return sha256_text(canonical_json(value))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_sha256(value: Any) -> bool:
    return type(value) is str and _SHA256_RE.fullmatch(value) is not None


def _reject_constant(value: str) -> None:
    raise StrictJSONError(f"non-finite JSON number is forbidden: {value}")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise StrictJSONError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def strict_json_loads(text: str) -> Any:
    try:
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except StrictJSONError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise StrictJSONError(str(error)) from error


def _require_regular_file(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a non-symlink regular file")


def read_gzip_jsonl(path: Path) -> list[dict[str, Any]]:
    _require_regular_file(path, str(path))
    return read_gzip_jsonl_bytes(path.read_bytes(), label=str(path))


def read_gzip_jsonl_bytes(payload: bytes, *, label: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        text = gzip.decompress(payload).decode("utf-8")
        with io.StringIO(text, newline="") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    raise StrictJSONError(
                        f"{label}:{line_number}: blank JSONL row"
                    )
                value = strict_json_loads(line)
                if type(value) is not dict:
                    raise StrictJSONError(
                        f"{label}:{line_number}: row is not an object"
                    )
                rows.append(value)
    except (OSError, EOFError, UnicodeError) as error:
        raise StrictJSONError(f"cannot read {label}: {error}") from error
    return rows


def _validate_authorizations(value: Any, *, label: str) -> None:
    if type(value) is not dict or value != DEVELOPMENT_AUTHORIZATIONS:
        raise ValueError(
            f"{label} must carry the exact development-only authorizations"
        )


def _stage_slot_count(stage: str) -> int:
    if stage == LINE_DECISION_STAGE:
        return STAGE_A_SLOT_COUNT
    if stage == RECORD_EXTRACTION_STAGE:
        return STAGE_B_SLOT_COUNT
    raise ValueError(f"unsupported round-four stage: {stage}")


def _validate_source_line(value: Any, *, label: str) -> None:
    if type(value) is not dict:
        raise ValueError(f"{label} must be an object")
    for field in ("source_line_id", "line_text_sha256"):
        if type(value.get(field)) is not str or not value[field]:
            raise ValueError(f"{label}.{field} must be a nonblank string")
    if type(value.get("line_text")) is not str:
        raise ValueError(f"{label}.line_text must be a string")
    if not _is_sha256(value["line_text_sha256"]):
        raise ValueError(f"{label}.line_text_sha256 is invalid")
    if sha256_text(value["line_text"]) != value["line_text_sha256"]:
        raise ValueError(f"{label}.line_text_sha256 differs from line_text")
    if "line_terminator" in value and type(value["line_terminator"]) is not str:
        raise ValueError(f"{label}.line_terminator must be a string")


def _validate_read_only_context(batch: Mapping[str, Any], *, label: str) -> None:
    """Authenticate opt-in evidence without expanding extraction ownership."""
    if not _READ_ONLY_CONTEXT_FIELDS.intersection(batch):
        return
    from src.candidate_endorsement_line_ledger import (
        LINE_INVENTORY_SPEC_SHA256, _physical_lines,
    )

    if not _READ_ONLY_CONTEXT_FIELDS.issubset(batch):
        raise ValueError(f"{label} has incomplete read-only context fields")
    radius = batch["read_only_context_radius"]
    if type(radius) is not int or radius <= 0:
        raise ValueError(f"{label} read-only context radius must be positive")
    if batch["read_only_context_policy"] != READ_ONLY_CONTEXT_POLICY:
        raise ValueError(f"{label} read-only context policy differs")
    context = batch["read_only_context_lines"]
    if type(context) is not list:
        raise ValueError(f"{label} read-only context lines must be a list")
    if batch["read_only_context_lines_sha256"] != sha256_json(context):
        raise ValueError(f"{label} read-only context line hash differs")
    source = batch.get("parent_source_text")
    if (type(source) is not str
            or sha256_text(source) != batch.get("extraction_source_text_sha256")
            or len(source) != batch.get("extraction_source_n_chars")):
        raise ValueError(f"{label} read-only context parent source hash/length differs")
    physical_lines = _physical_lines(source)
    owned = [line for target in batch["targets"]
             for line in target["record_block_lines"]]
    for line in [*owned, *context]:
        _validate_source_line(line, label=f"{label} read-only/owned source line")
    ids = [line.get("source_line_id") for line in [*owned, *context]]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{label} read-only/owned lines have duplicate source IDs")
    context_indices = [line.get("parent_line_index") for line in context]
    if (any(type(index) is not int for index in context_indices)
            or context_indices != sorted(set(context_indices))):
        raise ValueError(f"{label} read-only context line indices are not unique/ordered")
    for line in [*owned, *context]:
        index = line.get("parent_line_index")
        if type(index) is not int or not 0 <= index < len(physical_lines):
            raise ValueError(f"{label} source line index is out of bounds")
        if any(line.get(key) != value for key, value in physical_lines[index].items()):
            raise ValueError(f"{label} source line exact projection differs")
        identity = {
            "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
            "parent_document_id": batch["parent_document_id"],
            **{key: value for key, value in physical_lines[index].items()
               if key != "line_text"},
        }
        if line["source_line_id"] != sha256_json(identity):
            raise ValueError(f"{label} source line identity differs from exact projection")


def _validate_batch(batch: Mapping[str, Any], *, stage: str, label: str) -> None:
    if type(batch) is not dict:
        raise ValueError(f"{label} must be an object")
    missing = _BATCH_REQUIRED_FIELDS - set(batch)
    if missing:
        raise ValueError(f"{label} is missing fields: {sorted(missing)}")
    if batch.get("version") != MANIFEST_VERSION:
        raise ValueError(f"{label} has the wrong round-four input version")
    if batch.get("stage") != stage:
        raise ValueError(f"{label} has the wrong stage")
    for field in (
        "batch_id",
        "document_id",
        "parent_document_id",
        "candidate_cycle_id",
    ):
        if type(batch.get(field)) is not str or not batch[field]:
            raise ValueError(f"{label}.{field} must be a nonblank string")
    if not _is_sha256(batch["batch_id"]):
        raise ValueError(f"{label}.batch_id must be a SHA-256 identity")
    if batch["document_id"] != batch["batch_id"]:
        raise ValueError(f"{label}.document_id must equal batch_id")

    slot_count = _stage_slot_count(stage)
    targets = batch.get("targets")
    target_slots = batch.get("target_slots")
    padding_slots = batch.get("padding_slots")
    if type(targets) is not list or not 1 <= len(targets) <= slot_count:
        raise ValueError(f"{label}.targets has an invalid count")
    expected_targets = [f"slot_{index:02d}" for index in range(len(targets))]
    expected_padding = [
        f"slot_{index:02d}" for index in range(len(targets), slot_count)
    ]
    if target_slots != expected_targets or padding_slots != expected_padding:
        raise ValueError(f"{label} target/padding slots are not canonical")
    for index, target in enumerate(targets):
        if type(target) is not dict or target.get("slot") != expected_targets[index]:
            raise ValueError(f"{label}.targets[{index}] has the wrong slot")
        _validate_source_line(target, label=f"{label}.targets[{index}]")

    if stage == LINE_DECISION_STAGE:
        visible_lines = batch.get("visible_lines")
        if type(visible_lines) is not list or not visible_lines:
            raise ValueError(f"{label}.visible_lines must be a nonempty list")
        for index, line in enumerate(visible_lines):
            _validate_source_line(line, label=f"{label}.visible_lines[{index}]")
        catalog_hash = batch.get("full_parent_line_catalog_sha256")
        if not _is_sha256(catalog_hash) or catalog_hash != sha256_json(visible_lines):
            raise ValueError(f"{label} full parent line catalog hash differs")
        visible_ids = [line["source_line_id"] for line in visible_lines]
        if len(visible_ids) != len(set(visible_ids)):
            raise ValueError(f"{label}.visible_lines has duplicate source IDs")
        if any(target["source_line_id"] not in visible_ids for target in targets):
            raise ValueError(f"{label} target is absent from visible_lines")
    else:
        for index, target in enumerate(targets):
            block_lines = target.get("record_block_lines")
            if type(block_lines) is not list or not block_lines:
                raise ValueError(
                    f"{label}.targets[{index}].record_block_lines is empty"
                )
            for line_index, line in enumerate(block_lines):
                _validate_source_line(
                    line,
                    label=(
                        f"{label}.targets[{index}].record_block_lines"
                        f"[{line_index}]"
                    ),
                )
        _validate_read_only_context(batch, label=label)


def validate_expected_input_manifest_sha256(
    manifest_path: Path, expected_sha256: Any
) -> str:
    """Authenticate manifest bytes before parsing any manifest-controlled path."""
    if not _is_sha256(expected_sha256):
        raise ValueError("expected input manifest SHA-256 is invalid")
    _require_regular_file(manifest_path, "input manifest")
    observed = file_sha256(manifest_path)
    if observed != expected_sha256:
        raise ValueError("input manifest bytes differ from expected SHA-256")
    return observed


def validate_input_manifest(
    manifest_path: Path,
    chunks_dir: Path,
    *,
    stage: str,
    expected_manifest_sha256: str,
) -> tuple[dict[str, Any], str, list[Path]]:
    """Authenticate a closed round-four manifest and all of its chunk bytes."""
    if stage not in STAGES:
        raise ValueError(f"unsupported round-four stage: {stage}")
    if manifest_path.is_symlink() or chunks_dir.is_symlink():
        raise ValueError("manifest and chunks paths must not be symlinks")
    if not chunks_dir.is_dir():
        raise ValueError("chunks directory does not exist")
    if manifest_path.parent.resolve() != chunks_dir.parent.resolve():
        raise ValueError("manifest and chunks must share one artifact root")
    if chunks_dir.resolve() != (manifest_path.parent / "chunks").resolve():
        raise ValueError("chunks directory must be the manifest's chunks path")

    if not _is_sha256(expected_manifest_sha256):
        raise ValueError("expected input manifest SHA-256 is invalid")
    _require_regular_file(manifest_path, "input manifest")
    manifest_bytes = manifest_path.read_bytes()
    manifest_hash = sha256_bytes(manifest_bytes)
    if manifest_hash != expected_manifest_sha256:
        raise ValueError("input manifest bytes differ from expected SHA-256")
    try:
        manifest_text = manifest_bytes.decode("utf-8")
    except UnicodeError as error:
        raise StrictJSONError(f"cannot decode input manifest: {error}") from error
    manifest = strict_json_loads(manifest_text)
    if type(manifest) is not dict:
        raise ValueError("input manifest must be an object")
    if set(manifest) != _MANIFEST_FIELDS:
        raise ValueError("input manifest fields differ from the reviewed contract")
    if manifest.get("version") != MANIFEST_VERSION:
        raise ValueError("input manifest version is not round-four development")
    if manifest.get("stage") != stage:
        raise ValueError("input manifest stage differs from invocation")
    sample_source_pairs = validate_sample_round_binding(
        manifest.get('sample_round'), manifest.get('provenance'),
        manifest.get('selected_parent_ids') or [],
    )
    if manifest.get("pipeline_revision") != 4 or type(
        manifest.get("pipeline_revision")
    ) is not int:
        raise ValueError("input manifest pipeline revision is not four")
    _validate_authorizations(manifest.get("authorizations"), label="input manifest")

    payload = dict(manifest)
    recorded_payload_sha = payload.pop("manifest_payload_sha256", None)
    if not _is_sha256(recorded_payload_sha):
        raise ValueError("manifest payload SHA-256 is invalid")
    if recorded_payload_sha != sha256_json(payload):
        raise ValueError("manifest payload SHA-256 mismatch")

    expected_prompt_sha = stage_prompt_sha256(stage)
    if manifest.get("prompt_sha256") != expected_prompt_sha:
        raise ValueError("manifest prompt SHA-256 differs from round-four source")
    slot_count = _stage_slot_count(stage)
    if manifest.get("slot_count") != slot_count or type(
        manifest.get("slot_count")
    ) is not int:
        raise ValueError("manifest slot count differs from round-four source")
    schema_builder = stage_a_schema if stage == LINE_DECISION_STAGE else stage_b_schema
    expected_catalog = {
        str(target_count): sha256_json(
            schema_builder(target_count=target_count, slot_count=slot_count)
        )
        for target_count in range(1, slot_count + 1)
    }
    if manifest.get("schema_catalog") != expected_catalog:
        raise ValueError("manifest schema catalog differs from round-four source")
    if manifest.get("schema_catalog_sha256") != sha256_json(expected_catalog):
        raise ValueError("manifest schema catalog SHA-256 differs")

    chunk_size = manifest.get("chunk_size")
    batch_count = manifest.get("batch_count")
    if type(chunk_size) is not int or chunk_size < 1:
        raise ValueError("manifest chunk size is invalid")
    if type(batch_count) is not int or batch_count < 1:
        raise ValueError("manifest batch count is invalid")
    expected_batch_ids = manifest.get("expected_batch_ids")
    if (
        type(expected_batch_ids) is not list
        or len(expected_batch_ids) != batch_count
        or any(not _is_sha256(value) for value in expected_batch_ids)
        or len(expected_batch_ids) != len(set(expected_batch_ids))
    ):
        raise ValueError("manifest expected batch IDs are invalid")
    if manifest.get("expected_batch_ids_sha256") != sha256_json(
        expected_batch_ids
    ):
        raise ValueError("manifest expected batch IDs SHA-256 differs")
    selected_parent_ids = manifest.get("selected_parent_ids")
    selected_parent_count = manifest.get("selected_parent_count")
    if (
        type(selected_parent_ids) is not list
        or type(selected_parent_count) is not int
        or selected_parent_count < 1
        or len(selected_parent_ids) != selected_parent_count
        or any(type(value) is not str or not value for value in selected_parent_ids)
        or len(selected_parent_ids) != len(set(selected_parent_ids))
    ):
        raise ValueError("manifest selected parent inventory is invalid")
    if manifest.get("selected_parent_ids_sha256") != sha256_json(
        selected_parent_ids
    ):
        raise ValueError("manifest selected parent IDs SHA-256 differs")
    if type(manifest.get("provenance")) is not dict:
        raise ValueError("manifest provenance must be an object")

    entries = manifest.get("chunks")
    if type(entries) is not list or not entries:
        raise ValueError("manifest chunk inventory must be a nonempty list")
    if any(type(entry) is not dict or set(entry) != _CHUNK_ENTRY_FIELDS for entry in entries):
        raise ValueError("manifest chunk entries have the wrong fields")

    expected_names = [f"chunk_{index:05d}.jsonl.gz" for index in range(len(entries))]
    names = [entry.get("file") for entry in entries]
    if names != expected_names:
        raise ValueError("manifest chunks must be ordered, contiguous, and canonical")
    actual_names = sorted(path.name for path in chunks_dir.iterdir())
    if actual_names != expected_names:
        raise ValueError("chunks directory differs from manifest inventory")

    paths: list[Path] = []
    all_batch_ids: list[str] = []
    row_parent_ids: set[str] = set()
    for index, entry in enumerate(entries):
        name = entry["file"]
        if type(name) is not str or _CHUNK_RE.fullmatch(name) is None:
            raise ValueError(f"chunk entry {index} has an invalid filename")
        if type(entry["bytes"]) is not int or entry["bytes"] < 1:
            raise ValueError(f"chunk entry {index} has an invalid byte count")
        if type(entry["records"]) is not int or entry["records"] < 1:
            raise ValueError(f"chunk entry {index} has an invalid record count")
        if not _is_sha256(entry["sha256"]):
            raise ValueError(f"chunk entry {index} has an invalid SHA-256")
        if not _is_sha256(entry["batch_ids_sha256"]):
            raise ValueError(
                f"chunk entry {index} has an invalid batch ID SHA-256"
            )
        path = chunks_dir / name
        _require_regular_file(path, f"chunk entry {index}")
        chunk_bytes = path.read_bytes()
        if len(chunk_bytes) != entry["bytes"]:
            raise ValueError(f"{name}: byte count differs from manifest")
        if sha256_bytes(chunk_bytes) != entry["sha256"]:
            raise ValueError(f"{name}: bytes differ from manifest SHA-256")
        rows = read_gzip_jsonl_bytes(chunk_bytes, label=name)
        if len(rows) != entry["records"]:
            raise ValueError(f"{name}: record count differs from manifest")
        if len(rows) > chunk_size or (index < len(entries) - 1 and len(rows) != chunk_size):
            raise ValueError(f"{name}: record count differs from canonical chunking")
        chunk_batch_ids = [str(batch.get("batch_id")) for batch in rows]
        if entry["batch_ids_sha256"] != sha256_json(chunk_batch_ids):
            raise ValueError(f"{name}: batch ID inventory SHA-256 differs")
        for row_index, batch in enumerate(rows):
            _validate_batch(
                batch,
                stage=stage,
                label=f"{name}[{row_index}]",
            )
            all_batch_ids.append(str(batch["batch_id"]))
            row_parent_ids.add(str(batch["parent_document_id"]))
            if sample_source_pairs is not None and sample_source_pairs.get(str(batch['parent_document_id'])) != batch['extraction_source_text_sha256']:
                raise ValueError('Sample four batch source differs from its frozen selection.')
        paths.append(path)
    if all_batch_ids != expected_batch_ids:
        raise ValueError("round-four batch order differs from manifest")
    if len(all_batch_ids) != batch_count:
        raise ValueError("round-four batch count differs from manifest")
    selected_parent_set = set(selected_parent_ids)
    if not row_parent_ids <= selected_parent_set:
        raise ValueError("a round-four batch parent is outside the selected sample")
    return manifest, manifest_hash, paths


def stage_prompt(stage: str) -> str:
    if stage == LINE_DECISION_STAGE:
        return ROUND4_STAGE_A_PROMPT
    if stage == RECORD_EXTRACTION_STAGE:
        return ROUND4_STAGE_B_PROMPT
    raise ValueError(f"unsupported round-four stage: {stage}")


def stage_prompt_sha256(stage: str) -> str:
    if stage == LINE_DECISION_STAGE:
        return ROUND4_STAGE_A_PROMPT_SHA256
    if stage == RECORD_EXTRACTION_STAGE:
        return ROUND4_STAGE_B_PROMPT_SHA256
    raise ValueError(f"unsupported round-four stage: {stage}")


def user_message_rendering(stage: str) -> str:
    if stage == LINE_DECISION_STAGE:
        return LINE_DECISION_USER_MESSAGE_RENDERING
    if stage == RECORD_EXTRACTION_STAGE:
        return RECORD_EXTRACTION_USER_MESSAGE_RENDERING
    raise ValueError(f"unsupported round-four stage: {stage}")


def schema_for_batch(batch: Mapping[str, Any]) -> dict[str, Any]:
    stage = batch.get("stage")
    target_slots = batch.get("target_slots")
    padding_slots = batch.get("padding_slots")
    targets = batch.get("targets")
    if (
        type(target_slots) is not list
        or type(padding_slots) is not list
        or type(targets) is not list
    ):
        raise ValueError("round-four batch slot inventories are invalid")
    target_count = len(targets)
    if len(target_slots) != target_count:
        raise ValueError("round-four target slot and target counts differ")
    slot_count = target_count + len(padding_slots)
    if slot_count != _stage_slot_count(str(stage)):
        raise ValueError("round-four batch slot count differs from its stage")
    schema = (
        stage_a_schema(target_count=target_count, slot_count=slot_count)
        if stage == LINE_DECISION_STAGE
        else stage_b_schema(target_count=target_count, slot_count=slot_count)
        if stage == RECORD_EXTRACTION_STAGE
        else None
    )
    if type(schema) is not dict:
        raise ValueError("round-four schema builder did not return an object")
    canonical_json(schema)
    return schema


def _json_values_equal(left: Any, right: Any) -> bool:
    return type(left) is type(right) and left == right


def _json_schema_errors(
    value: Any, schema: Mapping[str, Any], *, path: str = "$"
) -> list[str]:
    """Validate the JSON-schema subset emitted by the round-four builders."""
    one_of = schema.get("oneOf")
    if one_of is not None:
        if type(one_of) is not list or not one_of:
            return [f"{path}: invalid oneOf schema"]
        branch_errors = [
            _json_schema_errors(value, branch, path=path) for branch in one_of
        ]
        matches = sum(not errors for errors in branch_errors)
        return [] if matches == 1 else [f"{path}: matched {matches} oneOf branches"]

    expected_type = schema.get("type")
    type_matches = {
        "object": type(value) is dict,
        "array": type(value) is list,
        "string": type(value) is str,
        "boolean": type(value) is bool,
        "null": value is None,
    }
    if expected_type is not None:
        if expected_type not in type_matches:
            return [f"{path}: unsupported schema type {expected_type!r}"]
        if not type_matches[expected_type]:
            return [f"{path}: expected {expected_type}"]

    if "const" in schema and not _json_values_equal(value, schema["const"]):
        return [f"{path}: value differs from const"]
    if "enum" in schema and not any(
        _json_values_equal(value, allowed) for allowed in schema["enum"]
    ):
        return [f"{path}: value is outside enum"]
    if type(value) is str and "minLength" in schema:
        if len(value) < int(schema["minLength"]):
            return [f"{path}: string is shorter than minLength"]

    errors: list[str] = []
    if type(value) is dict:
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        if type(properties) is not dict or type(required) is not list:
            return [f"{path}: invalid object schema"]
        for key in required:
            if key not in value:
                errors.append(f"{path}: missing required property {key}")
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in properties:
                    errors.append(f"{path}: additional property {key}")
        for key, child in value.items():
            if key in properties:
                errors.extend(
                    _json_schema_errors(
                        child, properties[key], path=f"{path}.{key}"
                    )
                )
    if type(value) is list:
        if "minItems" in schema and len(value) < int(schema["minItems"]):
            errors.append(f"{path}: array is shorter than minItems")
        if "maxItems" in schema and len(value) > int(schema["maxItems"]):
            errors.append(f"{path}: array is longer than maxItems")
        item_schema = schema.get("items")
        if item_schema is not None:
            if type(item_schema) is not dict:
                errors.append(f"{path}: invalid items schema")
            else:
                for index, child in enumerate(value):
                    errors.extend(
                        _json_schema_errors(
                            child, item_schema, path=f"{path}[{index}]"
                        )
                    )
    return errors


def _visible_record_block(target: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for line in target["record_block_lines"]:
        parts.append(str(line["line_text"]))
        parts.append(str(line.get("line_terminator", "")))
    return "".join(parts)


def _spans_overlap(
    left: Sequence[tuple[int, int]], right: Sequence[tuple[int, int]]
) -> bool:
    return any(
        max(left_start, right_start) < min(left_end, right_end)
        for left_start, left_end in left
        for right_start, right_end in right
    )


def validate_stage_output(
    batch: Mapping[str, Any], model_output: Any
) -> dict[str, Any]:
    """Replay schema and deterministic source checks for one model output."""
    stage = batch.get("stage")
    if stage not in STAGES:
        raise ValueError("round-four output stage is invalid")
    schema = schema_for_batch(batch)
    errors = _json_schema_errors(model_output, schema)
    schema_valid = errors == []
    expected_slots = [
        *batch["target_slots"],
        *batch["padding_slots"],
    ]
    coverage_complete = type(model_output) is dict and set(model_output) == set(
        expected_slots
    )
    if not coverage_complete:
        errors.append("$: slot coverage differs")

    source_projection: dict[str, list[dict[str, Any]]] = {}
    source_valid = schema_valid and coverage_complete
    if source_valid and stage == RECORD_EXTRACTION_STAGE:
        targets = {
            str(target["slot"]): target for target in batch["targets"]
        }
        for slot in batch["target_slots"]:
            decision = model_output[slot]
            projections: list[dict[str, Any]] = []
            projected_identities: list[
                tuple[str, list[tuple[int, int]]]
            ] = []
            visible_text = _visible_record_block(targets[slot])
            for record_index, record in enumerate(decision["records"]):
                record_path = f"$.{slot}.records[{record_index}]"
                name_parts = record["name_parts"]
                spans = resolve_ordered_name_parts(name_parts, visible_text)
                sanitized, optional_changes = sanitize_optional_fields(
                    record, visible_text
                )
                if not spans:
                    source_valid = False
                    errors.append(
                        f"{record_path}.name_parts: no ordered source projection"
                    )
                if optional_changes:
                    source_valid = False
                    for change in optional_changes:
                        errors.append(
                            f"{record_path}.{change['field']}: "
                            "not an exact contiguous visible substring"
                        )
                canonical_name = canonical_name_from_parts(name_parts)
                normalized_name = normalize_name_for_match(canonical_name)
                if spans and any(
                    normalized_name == prior_name
                    and _spans_overlap(spans, prior_spans)
                    for prior_name, prior_spans in projected_identities
                ):
                    source_valid = False
                    errors.append(
                        f"{record_path}.name_parts: duplicate overlapping "
                        "normalized identity"
                    )
                if spans:
                    projected_identities.append((normalized_name, spans))
                projections.append(
                    {
                        "record_index": record_index,
                        "canonical_name": canonical_name,
                        "name_part_spans": [
                            {
                                "part_index": part_index,
                                "start_char": start,
                                "end_char": end,
                                "source_text": visible_text[start:end],
                            }
                            for part_index, (start, end) in enumerate(spans)
                        ],
                        "optional_field_changes": optional_changes,
                        "sanitized_record_sha256": sha256_json(sanitized),
                    }
                )
            source_projection[slot] = projections
    elif stage == RECORD_EXTRACTION_STAGE:
        source_valid = False

    return {
        "schema_valid": schema_valid,
        "coverage_complete": coverage_complete,
        "source_projection_valid": source_valid,
        "source_projection": source_projection,
        "errors": errors,
    }


def _line_index(line: Mapping[str, Any], *, label: str) -> int:
    value = line.get("parent_line_index")
    if type(value) is not int or value < 0:
        raise ValueError(f"{label}.parent_line_index must be a nonnegative integer")
    return value


def compact_line_decision_model_view(
    batch: Mapping[str, Any],
) -> dict[str, Any]:
    """Project an authenticated Stage-A batch to its minimal model-facing view.

    The complete batch remains the authenticated input and is still hashed into
    every output row.  This projection removes byte offsets, content hashes, and
    construction provenance that the model does not need for classification.
    """
    if batch.get("stage") != LINE_DECISION_STAGE:
        raise ValueError("compact line-decision rendering requires a Stage-A batch")

    visible_by_id: dict[str, Mapping[str, Any]] = {}
    visible_lines: list[dict[str, Any]] = []
    for index, line in enumerate(batch["visible_lines"]):
        source_line_id = str(line["source_line_id"])
        visible_by_id[source_line_id] = line
        visible_lines.append(
            {
                "line_index": _line_index(
                    line, label=f"batch prompt.visible_lines[{index}]"
                ),
                "line_text": str(line["line_text"]),
            }
        )

    target_lines: list[dict[str, Any]] = []
    for index, target in enumerate(batch["targets"]):
        source_line_id = str(target["source_line_id"])
        visible = visible_by_id[source_line_id]
        target_line_index = _line_index(
            target, label=f"batch prompt.targets[{index}]"
        )
        if (
            target_line_index
            != _line_index(
                visible,
                label=f"batch prompt.target_visible_lines[{index}]",
            )
            or target["line_text"] != visible["line_text"]
        ):
            raise ValueError(
                "batch prompt target line differs from its visible-line projection"
            )
        target_lines.append(
            {
                "slot": str(target["slot"]),
                "source_line_id": source_line_id,
                "line_index": target_line_index,
                "line_text": str(target["line_text"]),
            }
        )

    candidate: dict[str, Any] = {
        "candidate_cycle_id": str(batch["candidate_cycle_id"]),
    }
    for source_field, rendered_field in (
        ("candidate_icpsr", "candidate_name"),
        ("state", "state"),
        ("office", "office"),
        ("year", "year"),
        ("party", "party"),
    ):
        value = batch.get(source_field)
        if value is not None:
            candidate[rendered_field] = value

    page: dict[str, Any] = {
        "parent_document_id": str(batch["parent_document_id"]),
    }
    context = batch.get("endorsement_line_context")
    if isinstance(context, Mapping) and type(context.get("context_text")) is str:
        page["endorsement_context"] = context["context_text"]

    return {
        "candidate": candidate,
        "page": page,
        "target_lines": target_lines,
        "padding_slots": list(batch["padding_slots"]),
        "visible_lines": visible_lines,
    }


def render_batch_prompt(batch: Mapping[str, Any]) -> str:
    """Render a deterministic, byte-stable user message from one batch."""
    stage = str(batch.get("stage"))
    _validate_batch(
        batch,
        stage=stage,
        label="batch prompt",
    )
    rendered = (
        compact_line_decision_model_view(batch)
        if stage == LINE_DECISION_STAGE
        else batch
    )
    instructions = ""
    if stage == RECORD_EXTRACTION_STAGE and "read_only_context_lines" in batch:
        # Full source bytes authenticate projections but are not model context.
        rendered = {key: value for key, value in batch.items()
                    if key != "parent_source_text"}
        instructions = (
            "READ_ONLY_CONTEXT_INSTRUCTIONS\n"
            "read_only_context_lines are evidence only, not extraction targets. "
            "Use these lines and headings to interpret endorsement context. "
            "Every record's name_parts must originate exclusively from that "
            "slot's owned target.record_block_lines. Never extract names from "
            "read-only context or another slot's block.\n"
        )
    return instructions + "ROUND4_DEVELOPMENT_BATCH_JSON\n" + canonical_json(rendered)


def build_conversation(batch: Mapping[str, Any]) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": stage_prompt(str(batch["stage"]))},
        {"role": "user", "content": render_batch_prompt(batch)},
    ]


def generation_config_payload(
    args: argparse.Namespace, *, input_manifest_sha256: str
) -> dict[str, Any]:
    if args.stage not in STAGES:
        raise ValueError("generation stage is invalid")
    if not _is_sha256(input_manifest_sha256):
        raise ValueError("generation input manifest SHA-256 is invalid")
    return {
        "version": "candidate_endorsement_round4_development_generation_v1",
        "runner_version": RUNNER_VERSION,
        "round": 4,
        "artifact_scope": ARTIFACT_SCOPE,
        "development_only": True,
        "authorizations": DEVELOPMENT_AUTHORIZATIONS,
        "round4_development_binding": ROUND4_DEVELOPMENT_BINDING,
        "round4_development_binding_sha256": (
            ROUND4_DEVELOPMENT_BINDING_SHA256
        ),
        "stage": args.stage,
        "input_manifest_sha256": input_manifest_sha256,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "system_prompt_sha256": stage_prompt_sha256(args.stage),
        "user_message_rendering": user_message_rendering(args.stage),
        "schema_binding": "exact_round4_schema_per_authenticated_batch",
        "seed": int(args.seed),
        "max_model_len": int(args.max_model_len),
        "max_output_tokens": int(args.max_output_tokens),
        "gpu_memory_utilization": float(args.gpu_memory_utilization),
        "temperature": 0.0,
        "thinking_enabled": False,
        "chat_template_add_generation_prompt": True,
        "context_safety_margin_tokens": CONTEXT_SAFETY_MARGIN_TOKENS,
        "structured_decoding": {
            "mode": "json_schema",
            "backend": STRUCTURED_DECODING_BACKEND,
            "disable_any_whitespace": STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        },
    }


def generation_config_sha256(
    args: argparse.Namespace, *, input_manifest_sha256: str
) -> str:
    return sha256_json(
        generation_config_payload(
            args, input_manifest_sha256=input_manifest_sha256
        )
    )


def llm_engine_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "model": str(args.model_path),
        "tokenizer": str(args.model_path),
        "max_model_len": int(args.max_model_len),
        "gpu_memory_utilization": float(args.gpu_memory_utilization),
        "seed": int(args.seed),
        "trust_remote_code": False,
        "language_model_only": True,
        "structured_outputs_config": {
            "backend": STRUCTURED_DECODING_BACKEND,
            "disable_any_whitespace": STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        },
    }


def structured_parameters(schema: dict[str, Any]) -> dict[str, Any]:
    """Use vLLM JSON-schema decoding with its pinned compatibility fallback."""
    try:
        from vllm.sampling_params import StructuredOutputsParams

        return {"structured_outputs": StructuredOutputsParams(json=schema)}
    except ImportError:
        from vllm.sampling_params import GuidedDecodingParams

        return {"guided_decoding": GuidedDecodingParams(json=schema)}


def _single_prompt_token_ids(value: Any) -> list[int]:
    if isinstance(value, Mapping):
        if "input_ids" not in value:
            raise ValueError("chat-template mapping lacks input_ids")
        value = value["input_ids"]
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, tuple):
        value = list(value)
    if (
        isinstance(value, list)
        and len(value) == 1
        and isinstance(value[0], (list, tuple))
    ):
        value = list(value[0])
    if not isinstance(value, list) or not value:
        raise ValueError("chat-template tokens have the wrong shape")
    if any(
        isinstance(token, bool)
        or not isinstance(token, Integral)
        or int(token) < 0
        for token in value
    ):
        raise ValueError("chat-template tokens must be nonnegative integers")
    return [int(token) for token in value]


def prompt_token_count(tokenizer: Any, conversation: Any) -> int:
    try:
        encoded = tokenizer.apply_chat_template(
            conversation,
            tokenize=True,
            add_generation_prompt=True,
            enable_thinking=False,
        )
        count = len(_single_prompt_token_ids(encoded))
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise RuntimeError(
            "the tokenizer cannot attest the deterministic prompt budget"
        ) from error
    if count < 1:
        raise RuntimeError("the deterministic prompt has no tokens")
    return count


def _validation_failure(error: Exception) -> dict[str, Any]:
    return {
        "schema_valid": False,
        "coverage_complete": False,
        "source_projection_valid": False,
        "source_projection": {},
        "errors": [f"{type(error).__name__}: {error}"],
    }


def derive_output_row(
    *,
    batch: Mapping[str, Any],
    raw_output: str,
    finish_reason: str,
    stop_reason: Any,
    output_tokens: int,
    prompt_tokens: int,
    args: argparse.Namespace,
    input_manifest_sha256: str,
    input_chunk_sha256: str,
    generation_config_sha256_value: str,
) -> dict[str, Any]:
    """Parse and validate one completion while retaining its exact raw bytes."""
    if type(raw_output) is not str:
        raise TypeError("raw model output must be a string")
    if type(finish_reason) is not str:
        raise TypeError("finish reason must be a string")
    if type(output_tokens) is not int or output_tokens < 0:
        raise TypeError("output token count must be a nonnegative integer")
    if type(prompt_tokens) is not int or prompt_tokens < 1:
        raise TypeError("prompt token count must be a positive integer")
    _validate_batch(batch, stage=args.stage, label="output batch")
    if (
        not _is_sha256(input_manifest_sha256)
        or not _is_sha256(input_chunk_sha256)
        or not _is_sha256(generation_config_sha256_value)
    ):
        raise ValueError("output provenance SHA-256 is invalid")

    model_output: Any = None
    model_output_sha256: str | None = None
    validation: dict[str, Any] | None = None
    parse_error: str | None = None
    parse_status = "parsed"
    if finish_reason != "stop":
        parse_status = "generation_incomplete"
    else:
        try:
            model_output = strict_json_loads(raw_output)
            model_output_sha256 = sha256_json(model_output)
        except StrictJSONError as error:
            parse_status = "strict_json_parse_error"
            parse_error = f"{type(error).__name__}: {error}"
        if parse_status == "parsed":
            try:
                validation = validate_stage_output(batch, model_output)
            except (KeyError, TypeError, ValueError) as error:
                validation = _validation_failure(error)
                parse_error = f"{type(error).__name__}: {error}"
            if (
                validation.get("schema_valid") is not True
                or validation.get("coverage_complete") is not True
                or validation.get("errors")
            ):
                parse_status = "invalid_stage_output"

    technical_pass = bool(
        parse_status == "parsed"
        and finish_reason == "stop"
        and validation is not None
        and validation.get("schema_valid") is True
        and validation.get("coverage_complete") is True
        and validation.get("source_projection_valid") is True
        and validation.get("errors") == []
    )
    schema = schema_for_batch(batch)
    margin = int(args.max_model_len) - prompt_tokens - int(args.max_output_tokens)
    return {
        "version": OUTPUT_VERSION,
        "runner_version": RUNNER_VERSION,
        "round": 4,
        "artifact_scope": ARTIFACT_SCOPE,
        "development_only": True,
        "authorizations": DEVELOPMENT_AUTHORIZATIONS,
        "document_id": batch["document_id"],
        "batch_id": batch["batch_id"],
        "parent_document_id": batch["parent_document_id"],
        "candidate_cycle_id": batch["candidate_cycle_id"],
        "stage": batch["stage"],
        "input_batch_sha256": sha256_json(batch),
        "input_manifest_sha256": input_manifest_sha256,
        "input_chunk_sha256": input_chunk_sha256,
        "generation_config_sha256": generation_config_sha256_value,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "system_prompt_sha256": stage_prompt_sha256(args.stage),
        "user_message_sha256": sha256_text(render_batch_prompt(batch)),
        "json_schema_sha256": sha256_json(schema),
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "temperature": 0.0,
        "thinking_enabled": False,
        "seed": int(args.seed),
        "max_model_len": int(args.max_model_len),
        "max_output_tokens": int(args.max_output_tokens),
        "prompt_tokens_used": prompt_tokens,
        "output_tokens": output_tokens,
        "context_margin_tokens": margin,
        "input_truncated": False,
        "finish_reason": finish_reason,
        "stop_reason": stop_reason,
        "raw_output": raw_output,
        "raw_output_sha256": sha256_text(raw_output),
        "raw_output_n_chars": len(raw_output),
        "raw_output_utf8_n_bytes": len(raw_output.encode("utf-8")),
        "model_output": model_output,
        "model_output_sha256": model_output_sha256,
        "parse_status": parse_status,
        "parse_error": parse_error,
        "stage_validation": validation,
        "technical_pass": technical_pass,
    }


def _gzip_jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    plain = "".join(canonical_json(row) + "\n" for row in rows).encode("utf-8")
    output = io.BytesIO()
    with gzip.GzipFile(
        filename="", mode="wb", fileobj=output, mtime=0
    ) as handle:
        handle.write(plain)
    return output.getvalue()


def _exclusive_publish_bytes(path: Path, payload: bytes) -> None:
    """Publish bytes atomically and fail if the destination already exists."""
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to overwrite existing artifact: {path}")
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def select_shard(
    chunks: Sequence[Path], shard_index: int | None, shard_count: int | None
) -> list[Path]:
    if shard_index is None and shard_count is None:
        return list(chunks)
    if shard_index is None or shard_count is None:
        raise ValueError("shard-index and shard-count must be supplied together")
    if (
        type(shard_index) is not int
        or type(shard_count) is not int
        or shard_count < 1
        or shard_index < 0
        or shard_index >= shard_count
    ):
        raise ValueError("invalid shard index/count")
    return [
        chunk
        for index, chunk in enumerate(chunks)
        if index % shard_count == shard_index
    ]


def _completion_fields(output: Any) -> tuple[str, str, Any, int]:
    choices = getattr(output, "outputs", None)
    if not isinstance(choices, Sequence) or len(choices) != 1:
        raise RuntimeError("vLLM must return exactly one completion per batch")
    completion = choices[0]
    text = getattr(completion, "text", None)
    if type(text) is not str:
        raise RuntimeError("vLLM completion text is not a string")
    finish_reason = str(getattr(completion, "finish_reason", "") or "")
    stop_reason = getattr(completion, "stop_reason", None)
    token_ids = getattr(completion, "token_ids", None)
    if token_ids is None:
        token_ids = []
    if not isinstance(token_ids, Sequence) or any(type(x) is not int for x in token_ids):
        raise RuntimeError("vLLM completion token IDs are invalid")
    return text, finish_reason, stop_reason, len(token_ids)


def process_chunk(
    *,
    input_path: Path,
    output_path: Path,
    llm: Any,
    tokenizer: Any,
    sampling_params_factory: Callable[..., Any],
    args: argparse.Namespace,
    input_manifest_sha256: str,
    expected_input_chunk_sha256: str,
    generation_config_sha256_value: str,
) -> list[dict[str, Any]]:
    """Infer and exclusively publish one previously absent authenticated chunk."""
    if output_path.exists() or output_path.is_symlink():
        raise FileExistsError(f"refusing to overwrite existing output: {output_path}")
    if not _is_sha256(expected_input_chunk_sha256):
        raise ValueError("expected input chunk SHA-256 is invalid")
    _require_regular_file(input_path, "input chunk")
    input_bytes = input_path.read_bytes()
    observed_input_chunk_sha256 = sha256_bytes(input_bytes)
    if observed_input_chunk_sha256 != expected_input_chunk_sha256:
        raise ValueError("input chunk bytes differ from authenticated manifest")
    batches = read_gzip_jsonl_bytes(input_bytes, label=input_path.name)
    conversations: list[list[dict[str, str]]] = []
    token_counts: list[int] = []
    sampling_params: list[Any] = []
    sampling_cache: dict[str, Any] = {}
    for index, batch in enumerate(batches):
        _validate_batch(batch, stage=args.stage, label=f"batch[{index}]")
        conversation = build_conversation(batch)
        tokens = prompt_token_count(tokenizer, conversation)
        margin = int(args.max_model_len) - tokens - int(args.max_output_tokens)
        if margin < CONTEXT_SAFETY_MARGIN_TOKENS:
            raise ValueError(
                f"{batch['batch_id']}: context margin {margin} is below "
                f"{CONTEXT_SAFETY_MARGIN_TOKENS}; input truncation is forbidden"
            )
        schema = schema_for_batch(batch)
        schema_hash = sha256_json(schema)
        if schema_hash not in sampling_cache:
            sampling_cache[schema_hash] = sampling_params_factory(
                temperature=0.0,
                max_tokens=int(args.max_output_tokens),
                seed=int(args.seed),
                **structured_parameters(schema),
            )
        conversations.append(conversation)
        token_counts.append(tokens)
        sampling_params.append(sampling_cache[schema_hash])

    outputs = llm.chat(
        conversations,
        sampling_params,
        use_tqdm=True,
        chat_template_kwargs={"enable_thinking": False},
    )
    if not isinstance(outputs, Sequence) or len(outputs) != len(batches):
        raise RuntimeError("vLLM returned the wrong number of outputs")
    rows: list[dict[str, Any]] = []
    for batch, output, tokens in zip(batches, outputs, token_counts):
        raw, finish, stop, output_tokens = _completion_fields(output)
        rows.append(
            derive_output_row(
                batch=batch,
                raw_output=raw,
                finish_reason=finish,
                stop_reason=stop,
                output_tokens=output_tokens,
                prompt_tokens=tokens,
                args=args,
                input_manifest_sha256=input_manifest_sha256,
                input_chunk_sha256=observed_input_chunk_sha256,
                generation_config_sha256_value=(
                    generation_config_sha256_value
                ),
            )
        )
    _exclusive_publish_bytes(output_path, _gzip_jsonl_bytes(rows))
    return rows


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run_metadata(
    *,
    args: argparse.Namespace,
    input_manifest_sha256: str,
    generation_config_sha256_value: str,
    selected_chunks: Sequence[Path],
    rows: Sequence[Mapping[str, Any]],
    started_at: str,
) -> dict[str, Any]:
    return {
        "version": RUN_METADATA_VERSION,
        "runner_version": RUNNER_VERSION,
        "round": 4,
        "artifact_scope": ARTIFACT_SCOPE,
        "development_only": True,
        "authorizations": DEVELOPMENT_AUTHORIZATIONS,
        "stage": args.stage,
        "input_manifest_sha256": input_manifest_sha256,
        "generation_config_sha256": generation_config_sha256_value,
        "generation_config": generation_config_payload(
            args, input_manifest_sha256=input_manifest_sha256
        ),
        "selected_chunk_names": [path.name for path in selected_chunks],
        "selected_chunk_count": len(selected_chunks),
        "processed_output_count": len(rows),
        "technical_pass_count": sum(row.get("technical_pass") is True for row in rows),
        "technical_fail_count": sum(row.get("technical_pass") is not True for row in rows),
        "parse_status_counts": dict(
            sorted(Counter(str(row.get("parse_status")) for row in rows).items())
        ),
        "started_at": started_at,
        "completed_at": utc_now(),
        "host": socket.gethostname(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "slurm_array_task_id": os.environ.get("SLURM_ARRAY_TASK_ID"),
    }


def _metadata_name(shard_index: int | None) -> str:
    return (
        "run_metadata.json"
        if shard_index is None
        else f"run_metadata_shard_{shard_index:05d}.json"
    )


def _validate_fixed_generation_args(args: argparse.Namespace) -> None:
    if args.max_model_len != MAX_MODEL_LEN:
        raise ValueError(f"max-model-len must remain {MAX_MODEL_LEN}")
    if args.max_output_tokens != MAX_OUTPUT_TOKENS[args.stage]:
        raise ValueError(
            f"max-output-tokens must remain {MAX_OUTPUT_TOKENS[args.stage]}"
        )
    if args.seed != GENERATION_SEED:
        raise ValueError(f"seed must remain {GENERATION_SEED}")
    if args.model_path.name.casefold() != MODEL_REVISION.casefold():
        raise ValueError("model path does not end in the pinned model revision")
    if (
        not math.isfinite(args.gpu_memory_utilization)
        or args.gpu_memory_utilization != GPU_MEMORY_UTILIZATION
    ):
        raise ValueError(
            f"gpu-memory-utilization must remain {GPU_MEMORY_UTILIZATION}"
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks-dir", type=Path, required=True)
    parser.add_argument("--input-manifest", type=Path, required=True)
    parser.add_argument("--expected-input-manifest-sha256", required=True)
    parser.add_argument("--expected-generation-config-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--stage", choices=STAGES, required=True)
    parser.add_argument("--max-model-len", type=int, default=MAX_MODEL_LEN)
    parser.add_argument("--max-output-tokens", type=int)
    parser.add_argument(
        "--gpu-memory-utilization", type=float, default=GPU_MEMORY_UTILIZATION
    )
    parser.add_argument("--seed", type=int, default=GENERATION_SEED)
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--shard-count", type=int)
    args = parser.parse_args(argv)
    if args.max_output_tokens is None:
        args.max_output_tokens = MAX_OUTPUT_TOKENS[args.stage]
    _validate_fixed_generation_args(args)
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    manifest, manifest_sha, chunks = validate_input_manifest(
        args.input_manifest,
        args.chunks_dir,
        stage=args.stage,
        expected_manifest_sha256=args.expected_input_manifest_sha256,
    )
    generation_sha = generation_config_sha256(
        args, input_manifest_sha256=manifest_sha
    )
    if not _is_sha256(args.expected_generation_config_sha256):
        raise ValueError("expected generation configuration SHA-256 is invalid")
    if args.expected_generation_config_sha256 != generation_sha:
        raise ValueError("generation configuration differs from expectation")
    selected_chunks = select_shard(chunks, args.shard_index, args.shard_count)
    if not selected_chunks:
        raise ValueError("selected shard contains no chunks")

    input_root = args.input_manifest.parent.resolve()
    output_root = args.output_dir.resolve()
    if (
        output_root == input_root
        or output_root in input_root.parents
        or input_root in output_root.parents
    ):
        raise ValueError("input and output artifact roots must not overlap")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.output_dir.is_symlink():
        raise ValueError("output directory must not be a symlink")
    metadata_path = args.output_dir / _metadata_name(args.shard_index)
    output_paths = [
        args.output_dir / path.name.replace(".jsonl.gz", ".out.jsonl.gz")
        for path in selected_chunks
    ]
    existing = [path for path in [*output_paths, metadata_path] if path.exists() or path.is_symlink()]
    if existing:
        raise FileExistsError(
            "refusing to overwrite existing round-four artifacts: "
            + ", ".join(str(path) for path in existing)
        )
    stale = sorted(args.output_dir.glob(".*.tmp-*"))
    if stale:
        raise ValueError("stale temporary artifacts require manual audit")

    from vllm import LLM, SamplingParams

    started = utc_now()
    llm = LLM(**llm_engine_kwargs(args))
    tokenizer = llm.get_tokenizer()
    entries_by_name = {
        str(entry["file"]): entry for entry in manifest["chunks"]
    }
    rows: list[dict[str, Any]] = []
    for input_path, output_path in zip(selected_chunks, output_paths):
        rows.extend(
            process_chunk(
                input_path=input_path,
                output_path=output_path,
                llm=llm,
                tokenizer=tokenizer,
                sampling_params_factory=SamplingParams,
                args=args,
                input_manifest_sha256=manifest_sha,
                expected_input_chunk_sha256=str(
                    entries_by_name[input_path.name]["sha256"]
                ),
                generation_config_sha256_value=generation_sha,
            )
        )
    metadata = run_metadata(
        args=args,
        input_manifest_sha256=manifest_sha,
        generation_config_sha256_value=generation_sha,
        selected_chunks=selected_chunks,
        rows=rows,
        started_at=started,
    )
    _exclusive_publish_bytes(
        metadata_path,
        (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    failures = sum(row["technical_pass"] is not True for row in rows)
    print(
        f"processed={len(rows)} technical_failures={failures} generation={generation_sha}",
        flush=True,
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
