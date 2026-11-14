#!/usr/bin/env python3
"""Build the private round-four endorsement human-review webapp.

The builder deliberately accepts two narrow inputs:

1. an authenticated fresh round-four Stage-A input artifact, which is the
   authoritative source for the exact 40 parent texts; and
2. an authenticated prepared provisional-draft JSON, which contains only the
   AI first-cut child rows and hashes binding those rows to the completed
   Stage-A and Stage-B runs.

Raw model output never enters the browser task.  Every AI parent decision and
child row starts unconfirmed.  A browser export is possible only after every
parent decision and every retained child have been explicitly confirmed.
Building this app never authorizes development inference, production
integration, full expansion, release, or human-truth promotion.
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import gzip
import hashlib
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
BUILDER_VERSION = "candidate_endorsement_round4_human_review_builder_v1"
CONVERTER_VERSION = "candidate_endorsement_round4_provisional_draft_converter_v1"
TASK_VERSION = "candidate_endorsement_round4_human_review_task_v1"
AUTH_VERSION = "candidate_endorsement_round4_human_review_auth_v1"
DRAFT_VERSION = "candidate_endorsement_round4_provisional_review_draft_v1"
RESULT_FORMAT = "candidate-endorsement-round4-human-review-results-v1"
COMPLETED_REVIEW_STATUS = (
    "all_parents_children_and_unresolved_blocks_explicitly_confirmed"
)
DEFAULT_TASK_ID = "candidate_endorsement_recall40_round4_human_review_20260907"
DEFAULT_TASK_REVISION = "candidate_endorsement_recall40_round4_human_review_v1"
EXPECTED_PARENT_COUNT = 40
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
TASK_ID_RE = re.compile(r"[a-z0-9][a-z0-9_]{7,127}\Z")
STAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\Z")
CHUNK_NAME_RE = re.compile(r"chunk_\d{5}\.jsonl\.gz\Z")
OPTIONAL_ONLY_FAILURE_RE = re.compile(
    r"^\$\.(slot_\d{2})\.records\[(\d+)\]\."
    r"(title|affiliation|jurisdiction): "
    r"not an exact contiguous visible substring\Z"
)

SOURCE_AUTHORIZATIONS = {
    "bounded_development_inference": True,
    "development_labels_authoritative": False,
    "human_truth": False,
    "production_integration": False,
    "full_expansion": False,
    "release": False,
}
REVIEW_AUTHORIZATIONS = {
    "development": False,
    "human_truth": False,
    "production_integration": False,
    "full_expansion": False,
    "release": False,
}
RESULT_AUTHORIZATIONS = {
    "human_review_complete": True,
    "development": False,
    "human_truth": False,
    "production_integration": False,
    "full_expansion": False,
    "release": False,
}

MANIFEST_FIELDS = {
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
CHUNK_FIELDS = {"file", "bytes", "sha256", "records", "batch_ids_sha256"}
SOURCE_PROVENANCE_FIELDS = {
    "sample_selection_sha256",
    "sample_selection_spec_sha256",
    "source_export_manifest_sha256",
    "sealed_reserve_source_text_reconstructed",
    "ai_draft_used_in_prompt",
}
DRAFT_FIELDS = {
    "version",
    "sample_round",
    "pipeline_revision",
    "development_only",
    "parent_count",
    "selected_parent_ids_sha256",
    "source_bundle_sha256",
    "provenance",
    "conversion",
    "authorizations",
    "parents",
}
DRAFT_PROVENANCE_FIELDS = {
    "stage_a_input_manifest_sha256",
    "stage_a_output_inventory_sha256",
    "stage_b_input_manifest_sha256",
    "stage_b_output_inventory_sha256",
}
DRAFT_PARENT_FIELDS = {
    "parent_document_id",
    "source_text_sha256",
    "provisional_parent_disposition",
    "children",
    "unresolved_blocks",
}
DRAFT_CHILD_FIELDS = {
    "provisional_child_id",
    "entity_type",
    "name_spans",
    "support_spans",
}
DRAFT_UNRESOLVED_BLOCK_FIELDS = {
    "unresolved_block_id",
    "stage_b_batch_id",
    "stage_b_slot",
    "source_line_id",
    "block_span",
    "parse_status",
    "raw_output_sha256",
    "model_output_sha256",
    "stage_validation_sha256",
}
DRAFT_CONVERSION_FIELDS = {
    "converter_version",
    "name_projection_rule",
    "support_projection_rule",
    "optional_field_policy",
    "optional_field_change_count",
    "optional_field_changes_sha256",
    "issue_count",
    "issues_sha256",
}
RESULT_FIELDS = {
    "task_id",
    "task_revision",
    "task_sha256",
    "format",
    "exported_at",
    "parent_count",
    "review_status",
    "authorizations",
    "responses",
}
RESULT_RESPONSE_FIELDS = {
    "parent_document_id",
    "parent_confirmed",
    "true_zero",
    "children",
    "reviewed_unresolved_block_ids",
    "notes",
}
RESULT_CHILD_FIELDS = {
    "human_confirmed",
    "entity_type",
    "name_spans",
    "support_spans",
}
RUN_METADATA_FIELDS = {
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
}


class NonrepairableStageBOutput(ValueError):
    """A validly authenticated batch whose model records cannot seed review."""

    def __init__(
        self,
        message: str,
        *,
        isolated_validation: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.isolated_validation = (
            copy.deepcopy(isolated_validation)
            if isolated_validation is not None
            else None
        )


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def canonical_bytes(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def strict_json_bytes(raw: bytes, *, label: str) -> Any:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{label} is not UTF-8") from error

    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, child in pairs:
            if key in value:
                raise ValueError(f"{label} has duplicate JSON key: {key}")
            value[key] = child
        return value

    return json.loads(
        text,
        object_pairs_hook=hook,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"{label} has nonfinite JSON value: {value}")
        ),
    )


def _require_sha256(value: Any, label: str) -> str:
    if type(value) is not str or SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be lowercase SHA-256 hex")
    return value


def _require_task_identity(value: Any, label: str) -> str:
    if type(value) is not str or TASK_ID_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a stable snake-case identifier")
    return value


def _canonical_stamp(value: Any, label: str) -> str:
    if type(value) is not str or STAMP_RE.fullmatch(value) is None:
        raise ValueError(f"{label} timestamp is invalid")
    parsed = dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    if parsed.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" != value:
        raise ValueError(f"{label} timestamp is not canonical")
    return value


def _typed_int(value: Any) -> bool:
    return type(value) is int


def _strict_gzip_jsonl(raw: bytes, *, label: str) -> list[dict[str, Any]]:
    try:
        decompressed = gzip.decompress(raw)
    except (OSError, EOFError) as error:
        raise ValueError(f"{label} is not valid gzip") from error
    if not decompressed or not decompressed.endswith(b"\n"):
        raise ValueError(f"{label} must be nonempty newline-terminated JSONL")
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(decompressed.splitlines(), start=1):
        if not line:
            raise ValueError(f"{label}:{line_number} is blank")
        value = strict_json_bytes(line, label=f"{label}:{line_number}")
        if type(value) is not dict:
            raise ValueError(f"{label}:{line_number} is not an object")
        rows.append(value)
    return rows


def _validate_source_manifest(manifest: Any, *, expected_sha256: str) -> None:
    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as runner,
    )
    from scripts.build_candidate_endorsement_round4_development_inputs import (
        BUILDER_VERSION as SOURCE_BUILDER_VERSION,
        STAGE_A_SLOT_COUNT,
    )
    from src.candidate_endorsement_round4 import (
        ROUND4_STAGE_A_PROMPT,
        stage_a_schema,
    )

    if type(manifest) is not dict or set(manifest) != MANIFEST_FIELDS:
        raise ValueError("fresh Stage-A input manifest field set differs")
    payload = dict(manifest)
    payload_hash = payload.pop("manifest_payload_sha256")
    if _require_sha256(payload_hash, "manifest payload hash") != sha256_json(payload):
        raise ValueError("fresh Stage-A input manifest payload hash differs")
    selected_ids = manifest["selected_parent_ids"]
    expected_batch_ids = manifest["expected_batch_ids"]
    expected_schema_catalog = {
        str(target_count): sha256_json(
            stage_a_schema(
                target_count=target_count, slot_count=STAGE_A_SLOT_COUNT
            )
        )
        for target_count in range(1, STAGE_A_SLOT_COUNT + 1)
    }
    if (
        manifest["version"] != SOURCE_BUILDER_VERSION
        or manifest["stage"] != "line_decision"
        or manifest["pipeline_revision"] != 4
        or manifest["prompt_sha256"]
        != sha256_bytes(ROUND4_STAGE_A_PROMPT.encode("utf-8"))
        or manifest["schema_catalog"] != expected_schema_catalog
        or manifest["schema_catalog_sha256"]
        != sha256_json(expected_schema_catalog)
        or manifest["slot_count"] != STAGE_A_SLOT_COUNT
        or not _typed_int(manifest["chunk_size"])
        or manifest["chunk_size"] < 1
        or not _typed_int(manifest["batch_count"])
        or manifest["batch_count"] < 1
        or type(expected_batch_ids) is not list
        or len(expected_batch_ids) != manifest["batch_count"]
        or any(type(value) is not str or not value for value in expected_batch_ids)
        or len(expected_batch_ids) != len(set(expected_batch_ids))
        or manifest["expected_batch_ids_sha256"]
        != sha256_json(expected_batch_ids)
        or manifest["selected_parent_count"] != EXPECTED_PARENT_COUNT
        or type(selected_ids) is not list
        or len(selected_ids) != EXPECTED_PARENT_COUNT
        or len(selected_ids) != len(set(selected_ids))
        or any(type(value) is not str or not value for value in selected_ids)
        or manifest["selected_parent_ids_sha256"] != sha256_json(selected_ids)
        or type(manifest["chunks"]) is not list
        or not manifest["chunks"]
        or manifest["authorizations"] != SOURCE_AUTHORIZATIONS
    ):
        raise ValueError("fresh Stage-A input manifest identity or census differs")
    provenance = manifest["provenance"]
    runner.validate_sample_round_binding(
        manifest["sample_round"], provenance, selected_ids
    )
    expected_provenance_fields = set(SOURCE_PROVENANCE_FIELDS)
    if manifest["sample_round"] == 4:
        expected_provenance_fields.update(
            {"independent_sample_binding", "independent_sample_source_pairs"}
        )
    if (
        type(provenance) is not dict
        or set(provenance) != expected_provenance_fields
        or any(
            SHA256_RE.fullmatch(str(provenance[field])) is None
            for field in (
                "sample_selection_sha256",
                "sample_selection_spec_sha256",
                "source_export_manifest_sha256",
            )
        )
        or provenance["sealed_reserve_source_text_reconstructed"] is not False
        or provenance["ai_draft_used_in_prompt"] is not False
    ):
        raise ValueError("fresh Stage-A input provenance differs")
    _require_sha256(expected_sha256, "expected Stage-A manifest hash")


def load_stage_a_source_artifact(
    input_dir: Path,
    *,
    expected_manifest_sha256: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    """Authenticate a fresh Stage-A input and reconstruct its 40 parent texts."""
    input_dir = input_dir.resolve()
    manifest_path = input_dir / "manifest.json"
    chunks_dir = input_dir / "chunks"
    if (
        input_dir.is_symlink()
        or not input_dir.is_dir()
        or manifest_path.is_symlink()
        or not manifest_path.is_file()
        or chunks_dir.is_symlink()
        or not chunks_dir.is_dir()
    ):
        raise ValueError("fresh Stage-A input paths are absent or symlinked")
    raw_manifest = manifest_path.read_bytes()
    observed_manifest_sha256 = sha256_bytes(raw_manifest)
    if observed_manifest_sha256 != _require_sha256(
        expected_manifest_sha256, "expected Stage-A manifest hash"
    ):
        raise ValueError("fresh Stage-A input manifest raw bytes differ")
    manifest = strict_json_bytes(raw_manifest, label="fresh Stage-A input manifest")
    _validate_source_manifest(manifest, expected_sha256=expected_manifest_sha256)

    declared_names: list[str] = []
    rows: list[dict[str, Any]] = []
    for index, entry in enumerate(manifest["chunks"]):
        if type(entry) is not dict or set(entry) != CHUNK_FIELDS:
            raise ValueError(f"fresh Stage-A chunk entry {index} fields differ")
        name = entry["file"]
        if type(name) is not str or CHUNK_NAME_RE.fullmatch(name) is None:
            raise ValueError(f"fresh Stage-A chunk entry {index} name differs")
        if name in declared_names:
            raise ValueError("fresh Stage-A chunk entry names duplicate")
        declared_names.append(name)
        path = chunks_dir / name
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"fresh Stage-A chunk is absent or symlinked: {name}")
        raw_chunk = path.read_bytes()
        if (
            not _typed_int(entry["bytes"])
            or entry["bytes"] != len(raw_chunk)
            or _require_sha256(entry["sha256"], f"chunk {name} hash")
            != sha256_bytes(raw_chunk)
        ):
            raise ValueError(f"fresh Stage-A chunk bytes differ: {name}")
        chunk_rows = _strict_gzip_jsonl(raw_chunk, label=f"Stage-A chunk {name}")
        batch_ids = [row.get("batch_id") for row in chunk_rows]
        if (
            not _typed_int(entry["records"])
            or entry["records"] != len(chunk_rows)
            or entry["batch_ids_sha256"] != sha256_json(batch_ids)
        ):
            raise ValueError(f"fresh Stage-A chunk inventory differs: {name}")
        rows.extend(chunk_rows)
    observed_names = sorted(path.name for path in chunks_dir.glob("*.jsonl.gz"))
    if observed_names != sorted(declared_names):
        raise ValueError("fresh Stage-A chunk directory has missing or extra JSONL files")
    if (
        len(rows) != manifest["batch_count"]
        or [row.get("batch_id") for row in rows]
        != manifest["expected_batch_ids"]
    ):
        raise ValueError("fresh Stage-A chunks do not match the batch manifest")

    from scripts.build_candidate_endorsement_round4_development_inputs import (
        _validated_fresh_stage_a_parents,
    )

    validated = _validated_fresh_stage_a_parents(rows, manifest)
    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as runner,
    )

    source_pairs = runner.validate_sample_round_binding(
        manifest["sample_round"], manifest["provenance"], manifest["selected_parent_ids"]
    )
    if source_pairs is not None and any(
        source_pairs[parent["parent_document_id"]]
        != parent["extraction_source_text_sha256"]
        for parent in validated
    ):
        raise ValueError("fresh Stage-A source bytes differ from independent sample binding")
    parents_by_id = {parent["parent_document_id"]: parent for parent in validated}
    selected_ids = manifest["selected_parent_ids"]
    if set(parents_by_id) != set(selected_ids):
        raise ValueError("fresh Stage-A parent reconstruction differs")
    parents = [parents_by_id[parent_id] for parent_id in selected_ids]
    return parents, manifest, observed_manifest_sha256


def source_bundle_for(parents: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "parent_document_id": parent["parent_document_id"],
            "source_n_chars": len(parent["parent_source_text"]),
            "source_text_sha256": parent["extraction_source_text_sha256"],
        }
        for parent in parents
    ]


def _validate_spans(
    spans: Any,
    source: str,
    *,
    label: str,
    allow_empty: bool,
) -> list[list[int]]:
    if type(spans) is not list or (not allow_empty and not spans):
        raise ValueError(f"{label} must be a {'possibly empty' if allow_empty else 'nonempty'} list")
    prior_end = -1
    checked: list[list[int]] = []
    for index, pair in enumerate(spans):
        if (
            type(pair) is not list
            or len(pair) != 2
            or any(type(value) is not int for value in pair)
        ):
            raise ValueError(f"{label} span {index} schema differs")
        start, end = pair
        if (
            not 0 <= start < end <= len(source)
            or start < prior_end
            or not source[start:end].strip()
        ):
            raise ValueError(f"{label} span {index} is invalid")
        checked.append([start, end])
        prior_end = end
    return checked


def provisional_child_id(
    *,
    stage_b_output_inventory_sha256: str,
    parent_document_id: str,
    provisional_child_index: int,
    entity_type: str,
    name_spans: Sequence[Sequence[int]],
    support_spans: Sequence[Sequence[int]],
) -> str:
    return sha256_json(
        {
            "version": DRAFT_VERSION,
            "stage_b_output_inventory_sha256": stage_b_output_inventory_sha256,
            "parent_document_id": parent_document_id,
            "provisional_child_index": provisional_child_index,
            "entity_type": entity_type,
            "name_spans": [list(pair) for pair in name_spans],
            "support_spans": [list(pair) for pair in support_spans],
        }
    )


def load_provisional_draft(
    path: Path,
    *,
    expected_sha256: str,
    parents: Sequence[Mapping[str, Any]],
    stage_a_manifest: Mapping[str, Any],
    stage_a_manifest_sha256: str,
) -> tuple[dict[str, Any], str]:
    """Authenticate the prepared AI first cut against exact source parents."""
    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as runner,
    )

    runner.validate_sample_round_binding(
        stage_a_manifest.get("sample_round"),
        stage_a_manifest.get("provenance"),
        stage_a_manifest.get("selected_parent_ids"),
    )
    path = path.resolve()
    if path.is_symlink() or not path.is_file():
        raise ValueError("provisional draft is absent or symlinked")
    raw = path.read_bytes()
    observed_sha256 = sha256_bytes(raw)
    if observed_sha256 != _require_sha256(
        expected_sha256, "expected provisional draft hash"
    ):
        raise ValueError("provisional draft raw bytes differ")
    draft = strict_json_bytes(raw, label="provisional draft")
    if type(draft) is not dict or set(draft) != DRAFT_FIELDS:
        raise ValueError("provisional draft field set differs")
    if (
        draft["version"] != DRAFT_VERSION
        or type(draft["sample_round"]) is not int
        or draft["sample_round"] != stage_a_manifest["sample_round"]
        or draft["pipeline_revision"] != 4
        or draft["development_only"] is not True
        or draft["parent_count"] != EXPECTED_PARENT_COUNT
        or draft["authorizations"] != SOURCE_AUTHORIZATIONS
        or type(draft["parents"]) is not list
        or len(draft["parents"]) != EXPECTED_PARENT_COUNT
    ):
        raise ValueError("provisional draft identity or census differs")
    provenance = draft["provenance"]
    if type(provenance) is not dict or set(provenance) != DRAFT_PROVENANCE_FIELDS:
        raise ValueError("provisional draft provenance field set differs")
    for field in DRAFT_PROVENANCE_FIELDS:
        _require_sha256(provenance[field], f"provisional draft {field}")
    if provenance["stage_a_input_manifest_sha256"] != stage_a_manifest_sha256:
        raise ValueError("provisional draft is bound to a different Stage-A input")
    conversion = draft["conversion"]
    if (
        type(conversion) is not dict
        or set(conversion) != DRAFT_CONVERSION_FIELDS
        or conversion["converter_version"] != CONVERTER_VERSION
        or conversion["name_projection_rule"]
        != "ordered_name_parts_projected_to_exact_parent_codepoint_spans"
        or conversion["support_projection_rule"]
        != "trimmed_authenticated_record_block_projected_to_parent_span"
        or conversion["optional_field_policy"]
        != "sanitize_against_record_block_then_omit_from_human_truth_seed"
        or not _typed_int(conversion["optional_field_change_count"])
        or conversion["optional_field_change_count"] < 0
        or not _typed_int(conversion["issue_count"])
        or conversion["issue_count"] < 0
    ):
        raise ValueError("provisional draft conversion contract differs")
    _require_sha256(
        conversion["optional_field_changes_sha256"],
        "provisional draft optional-field ledger hash",
    )
    _require_sha256(
        conversion["issues_sha256"], "provisional draft issue ledger hash"
    )

    selected_ids = [parent["parent_document_id"] for parent in parents]
    bundle_sha256 = sha256_json(source_bundle_for(parents))
    if (
        draft["selected_parent_ids_sha256"] != sha256_json(selected_ids)
        or draft["selected_parent_ids_sha256"]
        != stage_a_manifest["selected_parent_ids_sha256"]
        or draft["source_bundle_sha256"] != bundle_sha256
    ):
        raise ValueError("provisional draft source binding differs")

    draft_ids: list[str] = []
    seen_child_ids: set[str] = set()
    seen_unresolved_ids: set[str] = set()
    for parent_index, (draft_parent, source_parent) in enumerate(
        zip(draft["parents"], parents, strict=True)
    ):
        if type(draft_parent) is not dict or set(draft_parent) != DRAFT_PARENT_FIELDS:
            raise ValueError(f"provisional parent {parent_index} fields differ")
        parent_id = draft_parent["parent_document_id"]
        source = source_parent["parent_source_text"]
        draft_ids.append(parent_id)
        if (
            parent_id != source_parent["parent_document_id"]
            or draft_parent["source_text_sha256"]
            != source_parent["extraction_source_text_sha256"]
            or draft_parent["provisional_parent_disposition"]
            not in {"true_zero", "contains_children", "needs_review"}
            or type(draft_parent["children"]) is not list
            or type(draft_parent["unresolved_blocks"]) is not list
        ):
            raise ValueError(f"provisional parent {parent_index} source or schema differs")
        children = draft_parent["children"]
        disposition = draft_parent["provisional_parent_disposition"]
        if (disposition == "true_zero" and children) or (
            disposition == "contains_children" and not children
        ) or (draft_parent["unresolved_blocks"] and disposition != "needs_review"):
            raise ValueError(f"provisional parent {parent_index} disposition is not closed")
        for child_index, child in enumerate(children):
            if type(child) is not dict or set(child) != DRAFT_CHILD_FIELDS:
                raise ValueError(
                    f"provisional child {parent_index}:{child_index} fields differ"
                )
            if child["entity_type"] not in {
                "individual",
                "organization",
                "unclear",
            }:
                raise ValueError(
                    f"provisional child {parent_index}:{child_index} type differs"
                )
            name_spans = _validate_spans(
                child["name_spans"],
                source,
                label=f"provisional child {parent_index}:{child_index} name",
                allow_empty=False,
            )
            support_spans = _validate_spans(
                child["support_spans"],
                source,
                label=f"provisional child {parent_index}:{child_index} support",
                allow_empty=True,
            )
            child_id = _require_sha256(
                child["provisional_child_id"],
                f"provisional child {parent_index}:{child_index} ID",
            )
            expected_child_id = provisional_child_id(
                stage_b_output_inventory_sha256=provenance[
                    "stage_b_output_inventory_sha256"
                ],
                parent_document_id=parent_id,
                provisional_child_index=child_index,
                entity_type=child["entity_type"],
                name_spans=name_spans,
                support_spans=support_spans,
            )
            if child_id != expected_child_id or child_id in seen_child_ids:
                raise ValueError(
                    f"provisional child {parent_index}:{child_index} identity differs"
                )
            seen_child_ids.add(child_id)
        for unresolved_index, unresolved in enumerate(
            draft_parent["unresolved_blocks"]
        ):
            label = f"provisional unresolved block {parent_index}:{unresolved_index}"
            if (
                type(unresolved) is not dict
                or set(unresolved) != DRAFT_UNRESOLVED_BLOCK_FIELDS
            ):
                raise ValueError(f"{label} fields differ")
            for field in (
                "unresolved_block_id",
                "stage_b_batch_id",
                "raw_output_sha256",
                "stage_validation_sha256",
            ):
                _require_sha256(unresolved[field], f"{label} {field}")
            if unresolved["model_output_sha256"] is not None:
                _require_sha256(
                    unresolved["model_output_sha256"],
                    f"{label} model_output_sha256",
                )
            if (
                type(unresolved["stage_b_slot"]) is not str
                or re.fullmatch(r"slot_\d{2}", unresolved["stage_b_slot"])
                is None
                or type(unresolved["source_line_id"]) is not str
                or not unresolved["source_line_id"]
                or type(unresolved["parse_status"]) is not str
                or not unresolved["parse_status"]
            ):
                raise ValueError(f"{label} identity fields differ")
            block_span = _validate_spans(
                [unresolved["block_span"]],
                source,
                label=f"{label} source",
                allow_empty=False,
            )[0]
            expected_unresolved_id = sha256_json(
                {
                    "version": CONVERTER_VERSION,
                    "stage_b_batch_id": unresolved["stage_b_batch_id"],
                    "stage_b_slot": unresolved["stage_b_slot"],
                    "source_line_id": unresolved["source_line_id"],
                    "block_span": block_span,
                    "raw_output_sha256": unresolved["raw_output_sha256"],
                }
            )
            unresolved_id = unresolved["unresolved_block_id"]
            if (
                unresolved_id != expected_unresolved_id
                or unresolved_id in seen_unresolved_ids
            ):
                raise ValueError(f"{label} identity differs")
            seen_unresolved_ids.add(unresolved_id)
    if draft_ids != selected_ids:
        raise ValueError("provisional draft parent order differs")
    return draft, observed_sha256


def authenticate_fresh_stage_b_run(
    stage_b_input_dir: Path,
    stage_b_output_dir: Path,
    *,
    expected_input_manifest_sha256: str,
    expected_output_inventory_sha256: str | None = None,
    stage_a_input_manifest_sha256: str,
    stage_a_output_provenance: Mapping[str, Any],
    selected_parent_ids: Sequence[str],
    stage_a_sample_round: int = 3,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
]:
    """Authenticate a complete fresh Stage-B run and its exact output bytes."""
    from collections import Counter

    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as runner,
    )

    stage_b_input_dir = stage_b_input_dir.resolve()
    stage_b_output_dir = stage_b_output_dir.resolve()
    manifest, manifest_sha256, input_paths = runner.validate_input_manifest(
        stage_b_input_dir / "manifest.json",
        stage_b_input_dir / "chunks",
        stage="record_extraction",
        expected_manifest_sha256=expected_input_manifest_sha256,
    )
    if (
        type(stage_a_sample_round) is not int
        or manifest["sample_round"] != stage_a_sample_round
        or manifest["selected_parent_ids"] != list(selected_parent_ids)
        or manifest["selected_parent_ids_sha256"]
        != sha256_json(list(selected_parent_ids))
    ):
        raise ValueError("fresh Stage-B sample round or selected parents differ from Stage A")
    provenance = manifest.get("provenance")
    expected_provenance_fields = {
        "handoff_version",
        "stage_a_input_manifest_sha256",
        "stage_a_output",
        "stage_a_roles_are_advisory",
        "handoff_diagnostics",
        "ai_draft_used_in_prompt",
        "semantic_inference_performed_by_handoff",
    }
    if manifest["sample_round"] == 4:
        expected_provenance_fields.update(
            {"independent_sample_binding", "independent_sample_source_pairs"}
        )
    if (
        type(provenance) is not dict
        or set(provenance) != expected_provenance_fields
        or provenance["stage_a_input_manifest_sha256"]
        != stage_a_input_manifest_sha256
        or provenance["stage_a_output"] != stage_a_output_provenance
        or provenance["stage_a_roles_are_advisory"] is not True
        or provenance["ai_draft_used_in_prompt"] is not False
        or provenance["semantic_inference_performed_by_handoff"] is not False
    ):
        raise ValueError("fresh Stage-B handoff provenance differs")
    diagnostics = provenance["handoff_diagnostics"]
    if type(diagnostics) is not dict:
        raise ValueError("fresh Stage-B handoff diagnostics are absent")
    diagnostics_payload = dict(diagnostics)
    recorded_diagnostics_hash = diagnostics_payload.pop(
        "handoff_diagnostics_sha256", None
    )
    if (
        provenance["handoff_version"] != diagnostics.get("version")
        or recorded_diagnostics_hash != sha256_json(diagnostics_payload)
    ):
        raise ValueError("fresh Stage-B handoff diagnostics hash differs")

    if stage_b_output_dir.is_symlink() or not stage_b_output_dir.is_dir():
        raise ValueError("fresh Stage-B output directory is absent or symlinked")
    expected_output_names = [
        path.name.replace(".jsonl.gz", ".out.jsonl.gz") for path in input_paths
    ]
    observed_output_names = sorted(
        path.name for path in stage_b_output_dir.glob("chunk_*.out.jsonl.gz")
    )
    if observed_output_names != expected_output_names:
        raise ValueError("fresh Stage-B output chunks are incomplete or unexpected")
    metadata_paths = sorted(stage_b_output_dir.glob("run_metadata*.json"))
    if not metadata_paths or any(
        path.is_symlink() or not path.is_file() for path in metadata_paths
    ):
        raise ValueError("fresh Stage-B run metadata is absent or non-regular")
    metadata_names = [path.name for path in metadata_paths]
    if "run_metadata.json" in metadata_names:
        if metadata_names != ["run_metadata.json"]:
            raise ValueError("fresh Stage-B mixes sharded and unsharded metadata")
    elif metadata_names != [
        f"run_metadata_shard_{index:05d}.json"
        for index in range(len(metadata_names))
    ]:
        raise ValueError("fresh Stage-B shard metadata names are not canonical")

    args = argparse.Namespace(
        stage="record_extraction",
        seed=runner.GENERATION_SEED,
        max_model_len=runner.MAX_MODEL_LEN,
        max_output_tokens=runner.MAX_OUTPUT_TOKENS["record_extraction"],
        gpu_memory_utilization=runner.GPU_MEMORY_UTILIZATION,
    )
    input_rows_by_name = {
        path.name: runner.read_gzip_jsonl(path) for path in input_paths
    }
    metadata_rows = [
        strict_json_bytes(path.read_bytes(), label=f"Stage-B metadata {path.name}")
        for path in metadata_paths
    ]
    generation_hashes: set[str] = set()
    selected_chunk_names: list[str] = []
    metadata_by_chunk: dict[str, Mapping[str, Any]] = {}
    for index, metadata in enumerate(metadata_rows):
        if type(metadata) is not dict or set(metadata) != RUN_METADATA_FIELDS:
            raise ValueError(f"fresh Stage-B metadata {index} fields differ")
        expected_generation = runner.generation_config_payload(
            args, input_manifest_sha256=manifest_sha256
        )
        generation_hash = metadata.get("generation_config_sha256")
        if (
            metadata.get("version") != runner.RUN_METADATA_VERSION
            or metadata.get("runner_version") != runner.RUNNER_VERSION
            or metadata.get("round") != 4
            or metadata.get("artifact_scope") != runner.ARTIFACT_SCOPE
            or metadata.get("development_only") is not True
            or metadata.get("authorizations")
            != runner.DEVELOPMENT_AUTHORIZATIONS
            or metadata.get("stage") != "record_extraction"
            or metadata.get("input_manifest_sha256") != manifest_sha256
            or metadata.get("generation_config") != expected_generation
            or generation_hash != sha256_json(expected_generation)
        ):
            raise ValueError(f"fresh Stage-B metadata {index} provenance differs")
        names = metadata.get("selected_chunk_names")
        if (
            type(names) is not list
            or any(type(name) is not str for name in names)
            or metadata.get("selected_chunk_count") != len(names)
        ):
            raise ValueError(f"fresh Stage-B metadata {index} chunk census differs")
        for name in names:
            if name in metadata_by_chunk:
                raise ValueError("fresh Stage-B metadata selects a chunk twice")
            metadata_by_chunk[name] = metadata
        selected_chunk_names.extend(names)
        generation_hashes.add(generation_hash)
    if len(generation_hashes) != 1:
        raise ValueError("fresh Stage-B shards use different generation settings")
    if sorted(selected_chunk_names) != [path.name for path in input_paths]:
        raise ValueError("fresh Stage-B metadata does not cover every input chunk")
    generation_hash = next(iter(generation_hashes))

    batches: list[dict[str, Any]] = []
    output_rows: list[dict[str, Any]] = []
    output_rows_by_input_name: dict[str, list[dict[str, Any]]] = {}
    output_entries: list[dict[str, Any]] = []
    for input_path in input_paths:
        input_rows = input_rows_by_name[input_path.name]
        output_path = stage_b_output_dir / input_path.name.replace(
            ".jsonl.gz", ".out.jsonl.gz"
        )
        if output_path.is_symlink() or not output_path.is_file():
            raise ValueError("fresh Stage-B output chunk is not a regular file")
        rows = runner.read_gzip_jsonl(output_path)
        if len(rows) != len(input_rows) or [
            row.get("batch_id") for row in rows
        ] != [batch["batch_id"] for batch in input_rows]:
            raise ValueError("fresh Stage-B input/output row order differs")
        input_chunk_sha256 = sha256_file(input_path)
        for batch, row in zip(input_rows, rows, strict=True):
            try:
                expected_row = runner.derive_output_row(
                    batch=batch,
                    raw_output=row["raw_output"],
                    finish_reason=row["finish_reason"],
                    stop_reason=row["stop_reason"],
                    output_tokens=row["output_tokens"],
                    prompt_tokens=row["prompt_tokens_used"],
                    args=args,
                    input_manifest_sha256=manifest_sha256,
                    input_chunk_sha256=input_chunk_sha256,
                    generation_config_sha256_value=generation_hash,
                )
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    f"fresh Stage-B output wrapper cannot be replayed: {batch['batch_id']}"
                ) from error
            if row != expected_row:
                raise ValueError(
                    f"fresh Stage-B output wrapper differs: {batch['batch_id']}"
                )
        batches.extend(input_rows)
        output_rows.extend(rows)
        output_rows_by_input_name[input_path.name] = rows
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
    if [row["batch_id"] for row in output_rows] != manifest["expected_batch_ids"]:
        raise ValueError("fresh Stage-B completed output order differs")
    for metadata in metadata_rows:
        selected = list(metadata["selected_chunk_names"])
        rows = [row for name in selected for row in output_rows_by_input_name[name]]
        counts = dict(sorted(Counter(str(row["parse_status"]) for row in rows).items()))
        if (
            metadata["processed_output_count"] != len(rows)
            or metadata["technical_pass_count"]
            != sum(row["technical_pass"] is True for row in rows)
            or metadata["technical_fail_count"]
            != sum(row["technical_pass"] is not True for row in rows)
            or metadata["parse_status_counts"] != counts
        ):
            raise ValueError("fresh Stage-B run metadata counts differ")
    metadata_entries = [
        {
            "file": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in metadata_paths
    ]
    inventory_payload = {
        "version": "candidate_endorsement_round4_stage_b_output_inventory_v1",
        "input_manifest_sha256": manifest_sha256,
        "generation_config_sha256": generation_hash,
        "ordered_output_batch_ids_sha256": sha256_json(
            [row["batch_id"] for row in output_rows]
        ),
        "output_files": output_entries,
        "run_metadata_files": metadata_entries,
    }
    inventory_sha256 = sha256_json(inventory_payload)
    if expected_output_inventory_sha256 is not None and (
        inventory_sha256
        != _require_sha256(
            expected_output_inventory_sha256,
            "expected Stage-B output inventory hash",
        )
    ):
        raise ValueError("fresh Stage-B output inventory differs from expectation")
    return (
        batches,
        output_rows,
        manifest,
        {**inventory_payload, "output_inventory_sha256": inventory_sha256},
    )


def _trimmed_parent_block_span(
    *,
    target: Mapping[str, Any],
    parent_source: str,
) -> tuple[str, list[int]]:
    block_lines = target["record_block_lines"]
    block_start = block_lines[0]["parent_start_char"]
    block_end = block_lines[-1]["parent_full_end_char"]
    block_text = "".join(
        str(line["line_text"]) + str(line["line_terminator"])
        for line in block_lines
    )
    if (
        target["record_block_text"] != block_text
        or target["record_block_text_sha256"]
        != sha256_bytes(block_text.encode("utf-8"))
        or parent_source[block_start:block_end] != block_text
    ):
        raise ValueError("Stage-B record block does not project to its parent source")
    leading = len(block_text) - len(block_text.lstrip())
    trailing = len(block_text.rstrip())
    if leading >= trailing:
        raise ValueError("Stage-B record block has no non-whitespace support")
    return block_text, [block_start + leading, block_start + trailing]


def _isolated_stage_b_model_output(
    *,
    batch: Mapping[str, Any],
    slot: str,
    decision: Any,
) -> dict[str, Any]:
    """Place one raw target decision in an otherwise canonical batch output."""
    isolated: dict[str, Any] = {}
    for target_slot in batch["target_slots"]:
        isolated[target_slot] = (
            copy.deepcopy(decision)
            if target_slot == slot
            else {
                "disposition": "not_endorsement",
                "source_mentions_fully_accounted": True,
                "records": [],
            }
        )
    for padding_slot in batch["padding_slots"]:
        isolated[padding_slot] = {
            "disposition": "padding",
            "source_mentions_fully_accounted": True,
            "records": [],
        }
    return isolated


def _validation_is_clean(validation: Mapping[str, Any]) -> bool:
    return (
        validation.get("schema_valid") is True
        and validation.get("coverage_complete") is True
        and validation.get("source_projection_valid") is True
        and validation.get("errors") == []
    )


def _sanitized_stage_b_slot_decision(
    *,
    runner: Any,
    batch: Mapping[str, Any],
    output_row: Mapping[str, Any],
    slot: str,
    target: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Replay one target slot and repair only its invalid optional fields."""
    model_output = copy.deepcopy(output_row.get("model_output"))
    if type(model_output) is not dict:
        raise NonrepairableStageBOutput(
            f"fresh Stage-B {slot} is nonrepairable because parsed JSON is absent"
        )
    from src.candidate_endorsement_round4 import sanitize_optional_fields

    if slot not in model_output:
        raise NonrepairableStageBOutput(
            f"fresh Stage-B {slot} is nonrepairable because its decision is absent"
        )
    decision = copy.deepcopy(model_output[slot])
    isolated_output = _isolated_stage_b_model_output(
        batch=batch, slot=slot, decision=decision
    )
    isolated_validation = runner.validate_stage_output(batch, isolated_output)
    if _validation_is_clean(isolated_validation):
        return decision, [], isolated_validation

    errors = isolated_validation.get("errors")
    if (
        isolated_validation.get("schema_valid") is not True
        or isolated_validation.get("coverage_complete") is not True
        or type(errors) is not list
        or not errors
        or any(
            type(error) is not str
            or (match := OPTIONAL_ONLY_FAILURE_RE.fullmatch(error)) is None
            or match.group(1) != slot
            for error in errors
        )
        or type(decision) is not dict
        or type(decision.get("records")) is not list
        or any(type(record) is not dict for record in decision["records"])
    ):
        raise NonrepairableStageBOutput(
            f"fresh Stage-B {slot} has a nonrepairable isolated failure",
            isolated_validation=isolated_validation,
        )

    block_text = "".join(
        str(line["line_text"]) + str(line["line_terminator"])
        for line in target["record_block_lines"]
    )
    if (
        target.get("record_block_text") != block_text
        or target.get("record_block_text_sha256")
        != sha256_bytes(block_text.encode("utf-8"))
    ):
        raise ValueError("fresh Stage-B record-block bytes differ")
    changes: list[dict[str, Any]] = []
    expected_error_messages: list[str] = []
    for record_index, record in enumerate(decision["records"]):
        sanitized, record_changes = sanitize_optional_fields(record, block_text)
        decision["records"][record_index] = sanitized
        for change in record_changes:
            path = (
                f"$.{slot}.records[{record_index}].{change['field']}: "
                "not an exact contiguous visible substring"
            )
            expected_error_messages.append(path)
            changes.append(
                {
                    "parent_document_id": batch["parent_document_id"],
                    "stage_b_batch_id": batch["batch_id"],
                    "stage_b_slot": slot,
                    "record_index": record_index,
                    "raw_output_sha256": output_row["raw_output_sha256"],
                    "model_output_sha256": output_row["model_output_sha256"],
                    **copy.deepcopy(change),
                }
            )
    if sorted(errors) != sorted(expected_error_messages) or not changes:
        raise NonrepairableStageBOutput(
            f"fresh Stage-B {slot} optional failure ledger differs",
            isolated_validation=isolated_validation,
        )
    repaired_output = _isolated_stage_b_model_output(
        batch=batch, slot=slot, decision=decision
    )
    repaired_validation = runner.validate_stage_output(batch, repaired_output)
    if not _validation_is_clean(repaired_validation):
        raise NonrepairableStageBOutput(
            f"fresh Stage-B {slot} optional repair fails isolated replay",
            isolated_validation=repaired_validation,
        )
    return decision, changes, repaired_validation


def prepare_provisional_draft_from_authenticated_runs(
    *,
    stage_a_input_dir: Path,
    stage_a_output_dir: Path,
    expected_stage_a_input_manifest_sha256: str,
    stage_b_input_dir: Path,
    stage_b_output_dir: Path,
    expected_stage_b_input_manifest_sha256: str,
    expected_stage_a_output_inventory_sha256: str | None = None,
    expected_stage_b_output_inventory_sha256: str | None = None,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, Any],
]:
    """Convert authenticated Stage-B results into editable, unconfirmed seeds."""
    from scripts.build_candidate_endorsement_round4_development_inputs import (
        authenticate_fresh_stage_a_run,
    )
    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as runner,
    )
    from src.candidate_endorsement_round4 import resolve_ordered_name_parts

    (
        _stage_a_batches,
        _stage_a_output_rows,
        stage_a_authenticated,
        stage_a_output_provenance,
    ) = authenticate_fresh_stage_a_run(
        stage_a_input_dir.resolve(),
        stage_a_output_dir.resolve(),
        expected_input_manifest_sha256=expected_stage_a_input_manifest_sha256,
        expected_output_inventory_sha256=expected_stage_a_output_inventory_sha256,
    )
    stage_a_manifest = stage_a_authenticated["manifest"]
    selected_ids = list(stage_a_manifest["selected_parent_ids"])
    runner.validate_sample_round_binding(
        stage_a_manifest["sample_round"], stage_a_manifest["provenance"], selected_ids
    )
    source_by_id = {
        parent["parent_document_id"]: parent
        for parent in stage_a_authenticated["parents"]
    }
    parents = [source_by_id[parent_id] for parent_id in selected_ids]
    (
        stage_b_batches,
        stage_b_output_rows,
        stage_b_manifest,
        stage_b_output_provenance,
    ) = authenticate_fresh_stage_b_run(
        stage_b_input_dir,
        stage_b_output_dir,
        expected_input_manifest_sha256=expected_stage_b_input_manifest_sha256,
        expected_output_inventory_sha256=expected_stage_b_output_inventory_sha256,
        stage_a_input_manifest_sha256=expected_stage_a_input_manifest_sha256,
        stage_a_output_provenance=stage_a_output_provenance,
        selected_parent_ids=selected_ids,
        stage_a_sample_round=stage_a_manifest["sample_round"],
    )

    provisional_by_parent: dict[str, list[dict[str, Any]]] = {
        parent_id: [] for parent_id in selected_ids
    }
    unresolved_by_parent: dict[str, list[dict[str, Any]]] = {
        parent_id: [] for parent_id in selected_ids
    }
    issues: list[dict[str, Any]] = []
    optional_field_changes: list[dict[str, Any]] = []
    issue_parent_ids: set[str] = set()
    handoff = stage_b_manifest["provenance"]["handoff_diagnostics"]
    for field, issue_type in (
        ("unclear_lines", "stage_a_unclear_line"),
        ("unowned_continuation_lines", "stage_a_unowned_continuation"),
        ("ambiguous_ownership_lines", "stage_a_ambiguous_ownership"),
    ):
        for row in handoff[field]:
            parent_id = row["parent_document_id"]
            issue_parent_ids.add(parent_id)
            issues.append({"issue_type": issue_type, **copy.deepcopy(row)})

    for batch, output_row in zip(stage_b_batches, stage_b_output_rows, strict=True):
        parent_id = batch["parent_document_id"]
        parent_source = source_by_id[parent_id]["parent_source_text"]
        targets_by_slot = {target["slot"]: target for target in batch["targets"]}
        accepted_slots: list[str] = []
        unresolved_slots: list[str] = []
        optional_repaired_slots: list[str] = []
        for slot in batch["target_slots"]:
            target = targets_by_slot[slot]
            try:
                decision, slot_optional_changes, _isolated_validation = (
                    _sanitized_stage_b_slot_decision(
                        runner=runner,
                        batch=batch,
                        output_row=output_row,
                        slot=slot,
                        target=target,
                    )
                )
            except NonrepairableStageBOutput as error:
                issue_parent_ids.add(parent_id)
                unresolved_slots.append(slot)
                _block_text, block_span = _trimmed_parent_block_span(
                    target=target, parent_source=parent_source
                )
                unresolved_id = sha256_json(
                    {
                        "version": CONVERTER_VERSION,
                        "stage_b_batch_id": batch["batch_id"],
                        "stage_b_slot": slot,
                        "source_line_id": target["source_line_id"],
                        "block_span": block_span,
                        "raw_output_sha256": output_row["raw_output_sha256"],
                    }
                )
                unresolved = {
                    "unresolved_block_id": unresolved_id,
                    "stage_b_batch_id": batch["batch_id"],
                    "stage_b_slot": slot,
                    "source_line_id": target["source_line_id"],
                    "block_span": block_span,
                    "parse_status": output_row["parse_status"],
                    "raw_output_sha256": output_row["raw_output_sha256"],
                    "model_output_sha256": output_row["model_output_sha256"],
                    "stage_validation_sha256": sha256_json(
                        output_row["stage_validation"]
                    ),
                }
                unresolved_by_parent[parent_id].append(unresolved)
                issues.append(
                    {
                        "issue_type": "stage_b_nonrepairable_target_block",
                        "parent_document_id": parent_id,
                        **copy.deepcopy(unresolved),
                        "failure": str(error),
                        "stage_validation_errors": copy.deepcopy(
                            output_row["stage_validation"].get("errors", [])
                            if type(output_row["stage_validation"]) is dict
                            else []
                        ),
                        "isolated_stage_validation": copy.deepcopy(
                            error.isolated_validation
                        ),
                    }
                )
                continue
            accepted_slots.append(slot)
            if slot_optional_changes:
                optional_repaired_slots.append(slot)
            optional_field_changes.extend(slot_optional_changes)
            if decision["disposition"] == "unclear":
                issue_parent_ids.add(parent_id)
                issues.append(
                    {
                        "issue_type": "stage_b_unclear_target",
                        "parent_document_id": parent_id,
                        "stage_b_batch_id": batch["batch_id"],
                        "stage_b_slot": slot,
                        "source_line_id": target["source_line_id"],
                    }
                )
                continue
            if decision["disposition"] == "not_endorsement":
                continue
            if decision["disposition"] != "records":
                raise ValueError("technical-pass Stage-B target disposition differs")
            block_text, support_span = _trimmed_parent_block_span(
                target=target, parent_source=parent_source
            )
            block_start = target["record_block_lines"][0]["parent_start_char"]
            for record_index, record in enumerate(decision["records"]):
                projected = resolve_ordered_name_parts(
                    record["name_parts"], block_text
                )
                if not projected:
                    raise ValueError(
                        "technical-pass Stage-B name has no exact block projection"
                    )
                name_spans = [
                    [block_start + start, block_start + end]
                    for start, end in projected
                ]
                _validate_spans(
                    name_spans,
                    parent_source,
                    label="converted Stage-B name",
                    allow_empty=False,
                )
                if record["entity_type"] == "unclear":
                    issue_parent_ids.add(parent_id)
                    issues.append(
                        {
                            "issue_type": "stage_b_unclear_entity_type",
                            "parent_document_id": parent_id,
                            "stage_b_batch_id": batch["batch_id"],
                            "stage_b_slot": slot,
                            "record_index": record_index,
                        }
                    )
                provisional_by_parent[parent_id].append(
                    {
                        "entity_type": record["entity_type"],
                        "name_spans": name_spans,
                        "support_spans": [support_span],
                    }
                )
        if output_row.get("technical_pass") is not True:
            issues.append(
                {
                    "issue_type": "stage_b_failed_batch_slot_salvage",
                    "parent_document_id": parent_id,
                    "stage_b_batch_id": batch["batch_id"],
                    "parse_status": output_row["parse_status"],
                    "raw_output_sha256": output_row["raw_output_sha256"],
                    "model_output_sha256": output_row["model_output_sha256"],
                    "stage_validation_sha256": sha256_json(
                        output_row["stage_validation"]
                    ),
                    "accepted_target_slots": accepted_slots,
                    "optional_repaired_target_slots": optional_repaired_slots,
                    "unresolved_target_slots": unresolved_slots,
                    "stage_validation_errors": copy.deepcopy(
                        output_row["stage_validation"].get("errors", [])
                        if type(output_row["stage_validation"]) is dict
                        else []
                    ),
                }
            )

    for parent_id in selected_ids:
        source = source_by_id[parent_id]["parent_source_text"]
        reason = duplicate_child_reason(source, provisional_by_parent[parent_id])
        if reason is not None:
            issue_parent_ids.add(parent_id)
            issues.append(
                {
                    "issue_type": "cross_target_provisional_duplicate",
                    "parent_document_id": parent_id,
                    "reason": reason,
                }
            )

    optional_hash = sha256_json(optional_field_changes)
    issues_hash = sha256_json(issues)
    stage_b_output_inventory_sha256 = stage_b_output_provenance[
        "output_inventory_sha256"
    ]
    draft_parents: list[dict[str, Any]] = []
    for parent in parents:
        parent_id = parent["parent_document_id"]
        provisional_children = []
        for child_index, child in enumerate(provisional_by_parent[parent_id]):
            provisional_children.append(
                {
                    "provisional_child_id": provisional_child_id(
                        stage_b_output_inventory_sha256=(
                            stage_b_output_inventory_sha256
                        ),
                        parent_document_id=parent_id,
                        provisional_child_index=child_index,
                        entity_type=child["entity_type"],
                        name_spans=child["name_spans"],
                        support_spans=child["support_spans"],
                    ),
                    **child,
                }
            )
        disposition = (
            "needs_review"
            if parent_id in issue_parent_ids
            else "contains_children"
            if provisional_children
            else "true_zero"
        )
        draft_parents.append(
            {
                "parent_document_id": parent_id,
                "source_text_sha256": parent[
                    "extraction_source_text_sha256"
                ],
                "provisional_parent_disposition": disposition,
                "children": provisional_children,
                "unresolved_blocks": copy.deepcopy(
                    unresolved_by_parent[parent_id]
                ),
            }
        )
    conversion = {
        "converter_version": CONVERTER_VERSION,
        "name_projection_rule": (
            "ordered_name_parts_projected_to_exact_parent_codepoint_spans"
        ),
        "support_projection_rule": (
            "trimmed_authenticated_record_block_projected_to_parent_span"
        ),
        "optional_field_policy": (
            "sanitize_against_record_block_then_omit_from_human_truth_seed"
        ),
        "optional_field_change_count": len(optional_field_changes),
        "optional_field_changes_sha256": optional_hash,
        "issue_count": len(issues),
        "issues_sha256": issues_hash,
    }
    draft = {
        "version": DRAFT_VERSION,
        "sample_round": stage_a_manifest["sample_round"],
        "pipeline_revision": 4,
        "development_only": True,
        "parent_count": EXPECTED_PARENT_COUNT,
        "selected_parent_ids_sha256": stage_a_manifest[
            "selected_parent_ids_sha256"
        ],
        "source_bundle_sha256": sha256_json(source_bundle_for(parents)),
        "provenance": {
            "stage_a_input_manifest_sha256": (
                expected_stage_a_input_manifest_sha256
            ),
            "stage_a_output_inventory_sha256": stage_a_output_provenance[
                "output_inventory_sha256"
            ],
            "stage_b_input_manifest_sha256": expected_stage_b_input_manifest_sha256,
            "stage_b_output_inventory_sha256": stage_b_output_inventory_sha256,
        },
        "conversion": conversion,
        "authorizations": copy.deepcopy(SOURCE_AUTHORIZATIONS),
        "parents": draft_parents,
    }
    report = {
        "version": "candidate_endorsement_round4_provisional_conversion_report_v1",
        "conversion": conversion,
        "parent_count": EXPECTED_PARENT_COUNT,
        "provisional_child_count": sum(
            len(parent["children"]) for parent in draft_parents
        ),
        "unresolved_block_count": sum(
            len(parent["unresolved_blocks"]) for parent in draft_parents
        ),
        "needs_review_parent_count": sum(
            parent["provisional_parent_disposition"] == "needs_review"
            for parent in draft_parents
        ),
        "optional_field_changes": optional_field_changes,
        "issues": issues,
        "authorizations": copy.deepcopy(REVIEW_AUTHORIZATIONS),
    }
    return draft, report, parents, stage_a_manifest


def _normalize_name(source: str, child: Mapping[str, Any]) -> str:
    value = " ".join(source[start:end] for start, end in child["name_spans"])
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _spans_overlap(
    left: Sequence[Sequence[int]], right: Sequence[Sequence[int]]
) -> bool:
    return any(
        max(a_start, b_start) < min(a_end, b_end)
        for a_start, a_end in left
        for b_start, b_end in right
    )


def _span_union_contains(
    outer: Sequence[Sequence[int]], inner: Sequence[Sequence[int]]
) -> bool:
    for start, end in inner:
        cursor = start
        for left, right in outer:
            if right <= cursor:
                continue
            if left > cursor:
                break
            cursor = max(cursor, right)
            if cursor >= end:
                break
        if cursor < end:
            return False
    return True


def _token_sequence_contains(longer: str, shorter: str) -> bool:
    left = longer.split()
    right = shorter.split()
    if not right or len(right) > len(left):
        return False
    return any(
        left[index : index + len(right)] == right
        for index in range(len(left) - len(right) + 1)
    )


def duplicate_child_reason(
    source: str, children: Sequence[Mapping[str, Any]]
) -> str | None:
    entries = [
        {
            "index": index,
            "spans": child["name_spans"],
            "name": _normalize_name(source, child),
        }
        for index, child in enumerate(children)
    ]
    for left_index, left in enumerate(entries):
        for right in entries[left_index + 1 :]:
            if not _spans_overlap(left["spans"], right["spans"]):
                continue
            if left["name"] == right["name"]:
                return "overlapping children encode the same normalized name"
            if _token_sequence_contains(left["name"], right["name"]) or (
                _token_sequence_contains(right["name"], left["name"])
            ):
                return "overlapping children encode nested normalized names"
            if _span_union_contains(left["spans"], right["spans"]) or (
                _span_union_contains(right["spans"], left["spans"])
            ):
                return "overlapping children encode nested name coordinates"
    return None


def _validate_final_child(child: Any, source: str, *, label: str) -> None:
    if type(child) is not dict or set(child) != RESULT_CHILD_FIELDS:
        raise ValueError(f"{label} fields differ")
    if child["human_confirmed"] is not True:
        raise ValueError(f"{label} was not explicitly confirmed")
    if child["entity_type"] not in {"individual", "organization"}:
        raise ValueError(f"{label} entity type differs")
    _validate_spans(
        child["name_spans"], source, label=f"{label} name", allow_empty=False
    )
    _validate_spans(
        child["support_spans"],
        source,
        label=f"{label} support",
        allow_empty=False,
    )


def validate_completed_results(
    results: Any,
    *,
    task: Mapping[str, Any],
    task_sha256: str,
) -> dict[str, Any]:
    """Validate a complete export; incomplete state has no valid result form."""
    if type(results) is not dict or set(results) != RESULT_FIELDS:
        raise ValueError("review results root fields differ")
    if (
        results["task_id"] != task["task_id"]
        or results["task_revision"] != task["task_revision"]
        or results["task_sha256"] != task_sha256
        or results["format"] != RESULT_FORMAT
        or results["parent_count"] != EXPECTED_PARENT_COUNT
        or results["review_status"] != COMPLETED_REVIEW_STATUS
        or results["authorizations"] != RESULT_AUTHORIZATIONS
        or type(results["responses"]) is not list
        or len(results["responses"]) != EXPECTED_PARENT_COUNT
    ):
        raise ValueError("review results identity or completion status differs")
    _canonical_stamp(results["exported_at"], "review results")
    for index, (response, parent) in enumerate(
        zip(results["responses"], task["parents"], strict=True)
    ):
        if type(response) is not dict or set(response) != RESULT_RESPONSE_FIELDS:
            raise ValueError(f"review response {index} fields differ")
        source = parent["parent_source_text"]
        if (
            response["parent_document_id"] != parent["parent_document_id"]
            or response["parent_confirmed"] is not True
            or type(response["true_zero"]) is not bool
            or type(response["children"]) is not list
            or type(response["reviewed_unresolved_block_ids"]) is not list
            or type(response["notes"]) is not str
            or response["true_zero"] is not (len(response["children"]) == 0)
            or response["reviewed_unresolved_block_ids"]
            != [
                block["unresolved_block_id"]
                for block in parent["ai_unresolved_blocks"]
            ]
        ):
            raise ValueError(f"review response {index} is incomplete or inconsistent")
        for child_index, child in enumerate(response["children"]):
            _validate_final_child(
                child, source, label=f"review child {index}:{child_index}"
            )
        reason = duplicate_child_reason(source, response["children"])
        if reason is not None:
            raise ValueError(f"review response {index} duplicates children: {reason}")
    return copy.deepcopy(results)


def build_task(
    *,
    parents: Sequence[Mapping[str, Any]],
    stage_a_manifest: Mapping[str, Any],
    stage_a_manifest_sha256: str,
    draft: Mapping[str, Any],
    draft_sha256: str,
    task_id: str = DEFAULT_TASK_ID,
    task_revision: str = DEFAULT_TASK_REVISION,
) -> dict[str, Any]:
    task_id = _require_task_identity(task_id, "task ID")
    task_revision = _require_task_identity(task_revision, "task revision")
    draft_by_id = {
        parent["parent_document_id"]: parent for parent in draft["parents"]
    }
    task_parents: list[dict[str, Any]] = []
    for source_parent in parents:
        parent_id = source_parent["parent_document_id"]
        draft_parent = draft_by_id[parent_id]
        task_parents.append(
            {
                "parent_document_id": parent_id,
                "source_text_sha256": source_parent[
                    "extraction_source_text_sha256"
                ],
                "source_n_chars": len(source_parent["parent_source_text"]),
                "parent_source_text": source_parent["parent_source_text"],
                "metadata": {
                    "candidate_cycle_id": source_parent["candidate_cycle_id"],
                    "candidate_icpsr": source_parent["candidate_icpsr"],
                    "state": source_parent["state"],
                    "office": source_parent["office"],
                    "year": source_parent["year"],
                    "party": source_parent["party"],
                },
                "ai_parent_disposition": draft_parent[
                    "provisional_parent_disposition"
                ],
                "human_parent_confirmed": False,
                "ai_children": [
                    {
                        **copy.deepcopy(child),
                        "human_confirmed": False,
                    }
                    for child in draft_parent["children"]
                ],
                "ai_unresolved_blocks": [
                    {
                        **copy.deepcopy(block),
                        "human_reviewed": False,
                    }
                    for block in draft_parent["unresolved_blocks"]
                ],
            }
        )
    ai_child_count = sum(len(parent["ai_children"]) for parent in task_parents)
    task = {
        "version": TASK_VERSION,
        "task_id": task_id,
        "task_revision": task_revision,
        "title": "Private endorsement review: round-four AI first cut",
        "parent_count": EXPECTED_PARENT_COUNT,
        "banner": "AI first cut—human review pending",
        "instructions": (
            "### Purpose\n"
            "Review the AI first cut against the complete displayed source. The "
            "AI suggestions are provisional and may omit, combine, split, or invent "
            "endorsers. They are not human truth.\n\n"
            "### Parent decision\n"
            "Every parent begins unconfirmed. Press `C` to explicitly confirm that "
            "the parent contains one or more endorsement children, or `Z` to "
            "explicitly confirm a true zero. Correct the AI decision when needed.\n\n"
            "### Child rows\n"
            "Every AI child begins unconfirmed. Keep one row per distinct endorser "
            "identity occurrence. Add, delete, or split rows to correct multiplicity. "
            "A split copies the current spans into a new unconfirmed row so both rows "
            "can be edited. Repeated names at disjoint coordinates remain distinct.\n\n"
            "### Unresolved source blocks\n"
            "A failed model target slot contributes no child seeds. Each affected target "
            "block appears separately under Unresolved source blocks. Inspect the "
            "block, add any children it contains, then explicitly mark the block "
            "reviewed. Independently valid sibling slots remain as AI seeds. Parent "
            "confirmation alone does not resolve these blocks.\n\n"
            "### Entity type and exact name spans\n"
            "Choose `individual` for a person and `organization` for a group. Name "
            "spans use zero-based half-open Unicode code-point offsets into the "
            "complete displayed source. Select every component needed for the exact "
            "source name. Distinct people may share a surname span: encode each first "
            "name plus the shared surname in a separate child row.\n\n"
            "### Support spans\n"
            "Each confirmed child needs at least one exact support span showing the "
            "endorsement statement or list context. The AI may leave support blank. "
            "Select and add the missing source span before confirming the child.\n\n"
            "### Keyboard workflow\n"
            "Use `C` for Contains, `Z` for True zero, `1` for Individual, `2` for "
            "Organization, and `Enter` to confirm the current child. Use `U` for the "
            "next unresolved judgment, `J`/`]` and `K`/`[` to move among children, "
            "`A` to add, `S` to split, `R` to mark the next unresolved block reviewed, "
            "and left/right arrows to change parent. `I` "
            "toggles these instructions.\n\n"
            "### Completion and authority\n"
            "Export stays blocked until all 40 parent decisions, every retained "
            "child, and every unresolved source block are explicitly confirmed or "
            "reviewed. Progress autosaves. Import accepts only "
            "a complete export bound to these exact task bytes. This review task does "
            "not authorize development inference, production integration, full "
            "expansion, release, or automatic human-truth promotion."
        ),
        "privacy": "private_source_with_bounded_ai_first_cut",
        "predictions_included": True,
        "human_truth_status": "pending_explicit_human_review",
        "authorizations": copy.deepcopy(REVIEW_AUTHORIZATIONS),
        "contract": {
            "source_parent_count": EXPECTED_PARENT_COUNT,
            "truth_child_fields": ["entity_type", "name_spans", "support_spans"],
            "entity_types": ["individual", "organization"],
            "true_zero_rule": "explicit_parent_confirmation_and_zero_children",
            "nonzero_rule": "explicit_parent_confirmation_and_at_least_one_confirmed_child",
            "seed_rule": "all_ai_parent_decisions_and_children_begin_unconfirmed",
            "span_rule": (
                "ordered_nonempty_nonoverlapping_zero_based_half_open_python_"
                "unicode_codepoint_spans_within_complete_source"
            ),
            "support_rule": "support_may_be_absent_in_ai_seed_but_is_required_for_human_confirmation",
            "multiplicity_rule": "retained_child_row_count_defines_truth_multiplicity",
            "promotion_rule": "authenticated_complete_export_required_before_any_human_truth_claim",
            "unresolved_block_rule": (
                "each_nonrepairable_model_slot_seeds_no_records_and_its_target_"
                "block_requires_separate_explicit_human_review_after_isolated_replay"
            ),
        },
        "input_provenance": {
            "stage_a_input_manifest_sha256": stage_a_manifest_sha256,
            "stage_a_output_inventory_sha256": draft["provenance"][
                "stage_a_output_inventory_sha256"
            ],
            "stage_b_input_manifest_sha256": draft["provenance"][
                "stage_b_input_manifest_sha256"
            ],
            "stage_b_output_inventory_sha256": draft["provenance"][
                "stage_b_output_inventory_sha256"
            ],
            "provisional_draft_sha256": draft_sha256,
            "selected_parent_ids_sha256": stage_a_manifest[
                "selected_parent_ids_sha256"
            ],
            "source_bundle_sha256": sha256_json(source_bundle_for(parents)),
            "parent_count": EXPECTED_PARENT_COUNT,
            "ai_child_count": ai_child_count,
            "ai_true_zero_count": sum(
                parent["ai_parent_disposition"] == "true_zero"
                for parent in task_parents
            ),
            "ai_needs_review_count": sum(
                parent["ai_parent_disposition"] == "needs_review"
                for parent in task_parents
            ),
            "ai_unresolved_block_count": sum(
                len(parent["ai_unresolved_blocks"]) for parent in task_parents
            ),
        },
        "result_contract": {
            "format": RESULT_FORMAT,
            "review_status": COMPLETED_REVIEW_STATUS,
            "root_fields": sorted(RESULT_FIELDS),
            "response_fields": sorted(RESULT_RESPONSE_FIELDS),
            "child_fields": sorted(RESULT_CHILD_FIELDS),
            "authorizations": copy.deepcopy(RESULT_AUTHORIZATIONS),
        },
        "parents": task_parents,
    }
    if len(task_parents) != EXPECTED_PARENT_COUNT:
        raise ValueError("review task parent census differs")
    return task


def render_html(task: Mapping[str, Any], task_sha256: str) -> str:
    payload = json.dumps(task, ensure_ascii=False, sort_keys=True).replace(
        "</", "<\\/"
    )
    auth = json.dumps(
        {
            "task_id": task["task_id"],
            "task_revision": task["task_revision"],
            "task_sha256": task_sha256,
            "result_format": RESULT_FORMAT,
            "result_authorizations": RESULT_AUTHORIZATIONS,
        },
        sort_keys=True,
    ).replace("</", "<\\/")
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="icon" href="data:,">
<title>Round-four endorsement human review</title>
<style>
:root{{--ink:#17202a;--muted:#59636e;--line:#b7c0c8;--blue:#174f86;--pale:#eef5fb;--ok:#17643b;--bad:#9b1c1c;--warn:#7a5200;--warnbg:#fff3cd}}
*{{box-sizing:border-box}}body{{margin:0;color:var(--ink);font:14px system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f5f6f7}}
header{{position:sticky;top:0;z-index:5;background:#fff;border-bottom:1px solid var(--line);padding:8px 12px;display:flex;gap:7px;align-items:center;flex-wrap:wrap}}
.review-banner{{flex:1 0 100%;padding:8px 12px;border:2px solid #b98500;background:var(--warnbg);color:var(--warn);font-size:16px;font-weight:750;text-align:center}}
button{{border:1px solid #84909b;background:#fff;border-radius:4px;padding:6px 9px;cursor:pointer}}button:hover{{background:var(--pale)}}button.primary{{background:var(--blue);color:#fff;border-color:var(--blue)}}button.selected{{outline:3px solid #78aeda}}button.suggested{{outline:3px dashed #c28a00}}button.danger{{color:var(--bad)}}
main{{padding:10px;display:grid;grid-template-columns:minmax(420px,1.4fr) minmax(360px,1fr);gap:10px}}.panel{{background:#fff;border:1px solid var(--line);border-radius:5px;padding:10px;min-width:0}}
#instructions{{white-space:pre-wrap;max-height:28vh;overflow:auto;color:#27313b}}.source{{height:58vh;overflow:auto;padding:10px;border:1px solid #7c8792;background:#fff;font:13px ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;user-select:text;-webkit-user-select:text;cursor:text;line-height:1.35}}::selection{{background:#7ebcf0;color:#07131f}}
.decision,.toolbar{{display:flex;gap:6px;flex-wrap:wrap;margin:7px 0}}.status{{font-weight:650}}.muted{{color:var(--muted)}}.error{{color:var(--bad);font-weight:650}}.ok{{color:var(--ok)}}.warning{{color:var(--warn);font-weight:650}}
#unresolvedBlocks{{margin:8px 0}}.unresolved-block{{padding:7px;margin:5px 0;border:2px solid #d7a30d;background:var(--warnbg)}}.unresolved-block.reviewed{{border-color:#74a987;background:#eef8f1}}.unresolved-block code{{overflow-wrap:anywhere}}
#children{{max-height:26vh;overflow:auto;border:1px solid #ccd2d8}}.childrow{{padding:6px;border-bottom:1px solid #e1e5e8;display:grid;grid-template-columns:3.5em 7em 1fr auto;gap:6px;align-items:center}}.childrow.current{{background:var(--pale)}}
.editor{{border-top:2px solid #8da0b1;margin-top:9px;padding-top:9px}}.spans{{margin:6px 0;padding:0;list-style:none}}.spans li{{padding:5px;background:#f3f5f6;margin:3px 0;overflow-wrap:anywhere}}textarea{{width:100%;min-height:62px}}#message{{min-height:1.3em}}dialog{{width:min(850px,92vw)}}dialog textarea{{height:45vh;font:12px ui-monospace,monospace}}
.instruction-gap{{height:.7em}}#instructions h3{{margin:.8em 0 .25em}}#shortcutHelp{{line-height:1.45}}
@media(max-width:900px){{main{{grid-template-columns:1fr}}.source{{height:45vh}}}}
</style></head><body>
<header><div id="reviewBanner" class="review-banner">AI first cut—human review pending</div><strong id="position"></strong><button id="prev">Previous parent</button><button id="next">Next parent</button><button id="nextUnresolved" class="primary">Next unresolved (U)</button><span id="progress"></span><button id="import">Import</button><button id="export" class="primary">Export completed review</button><input id="importFile" type="file" accept="application/json,.json" hidden><span id="message"></span></header>
<main><section class="panel"><details id="instructionBox" open><summary><strong>Instructions (press I)</strong></summary><div id="instructions"></div></details><div class="toolbar"><button id="showCurrent">Show current child in source</button><span id="selectionMessage" class="muted">Select exact text in the source.</span></div><pre id="source" class="source" tabindex="0" aria-label="Complete parent source text"></pre></section>
<section class="panel"><div id="parentSummary"></div><p id="shortcutHelp" class="muted"><strong>Keys:</strong> C confirm contains · Z confirm zero · 1 individual · 2 organization · Enter confirm child · U next unresolved · R review failed block · J/] next child · K/[ previous child · A add · S split · ←/→ parent · I instructions</p><div class="decision"><button id="nonzero">Confirm contains children (C)</button><button id="zero">Confirm true zero (Z)</button></div><div class="toolbar"><button id="addChild">Add blank child (A)</button><button id="splitChild">Split current child (S)</button><button id="deleteChild" class="danger">Delete current child</button><button id="resetParent">Reset parent to AI first cut</button></div><div id="unresolvedBlocks"></div><div id="children"></div><div id="editor" class="editor"></div><label>Parent notes<textarea id="notes"></textarea></label></section></main>
<dialog id="exportDialog"><h2>Completed human-review JSON</h2><textarea id="exportText" readonly></textarea><p><button id="download">Download file</button><button id="copy">Copy to clipboard</button><button id="closeDialog">Close</button></p></dialog>
<script id="taskData" type="application/json">{payload}</script><script id="taskAuth" type="application/json">{auth}</script>
<script>
"use strict";
const T=JSON.parse(document.getElementById('taskData').textContent),A=JSON.parse(document.getElementById('taskAuth').textContent);
const storageKey=['handcode',T.task_id,T.task_revision,T.input_provenance.source_bundle_sha256,T.input_provenance.provisional_draft_sha256].join(':');
let parentIndex=0,childIndex=0,lastSelection=null,exportObject=null;
const el=id=>document.getElementById(id),chars=s=>Array.from(s),slice=(s,a,b)=>chars(s).slice(a,b).join(''),curParent=()=>T.parents[parentIndex];
function exactKeys(x,keys){{return x&&typeof x==='object'&&!Array.isArray(x)&&Object.keys(x).sort().join('|')===[...keys].sort().join('|')}}
function cloneSpans(spans){{return spans.map(pair=>[...pair])}}
function unresolvedIds(parent){{return parent.ai_unresolved_blocks.map(block=>block.unresolved_block_id)}}
function seedState(parent){{const disposition=parent.ai_parent_disposition;return{{parent_confirmed:false,true_zero:disposition==='true_zero'?true:(disposition==='contains_children'||parent.ai_children.length?false:null),children:parent.ai_children.map(seed=>({{entity_type:seed.entity_type,name_spans:cloneSpans(seed.name_spans),support_spans:cloneSpans(seed.support_spans),confirmed:false,origin_seed_id:seed.provisional_child_id}})),reviewed_unresolved_block_ids:[],notes:''}}}}
function freshState(){{return Object.fromEntries(T.parents.map(parent=>[parent.parent_document_id,seedState(parent)]))}}
function internalStateValid(value){{return value&&typeof value==='object'&&!Array.isArray(value)&&typeof value.parent_confirmed==='boolean'&&(value.true_zero===null||typeof value.true_zero==='boolean')&&Array.isArray(value.children)&&Array.isArray(value.reviewed_unresolved_block_ids)&&value.reviewed_unresolved_block_ids.every(id=>typeof id==='string')&&typeof value.notes==='string'}}
let S;try{{const parsed=JSON.parse(localStorage.getItem(storageKey)||'null');S=parsed&&typeof parsed==='object'&&!Array.isArray(parsed)?parsed:freshState()}}catch(error){{S=freshState()}}
for(const parent of T.parents)if(!internalStateValid(S[parent.parent_document_id]))S[parent.parent_document_id]=seedState(parent);
function state(){{return S[curParent().parent_document_id]}}function save(){{localStorage.setItem(storageKey,JSON.stringify(S))}}
function esc(value){{return String(value).replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]))}}
function inlineMarkdown(value){{return esc(value).replace(/`([^`]+)`/g,'<code>$1</code>').replace(/\\*\\*([^*]+)\\*\\*/g,'<strong>$1</strong>')}}
function renderInstructions(value){{return value.split('\\n').map(line=>{{if(line.startsWith('### '))return`<h3>${{inlineMarkdown(line.slice(4))}}</h3>`;if(line.startsWith('- '))return`<div>• ${{inlineMarkdown(line.slice(2))}}</div>`;if(!line.trim())return'<div class="instruction-gap"></div>';return`<div>${{inlineMarkdown(line)}}</div>`}}).join('')}}
function selectionSpan(){{const root=el('source'),selection=window.getSelection();if(!selection||selection.rangeCount!==1||selection.isCollapsed)return null;const range=selection.getRangeAt(0);if(!root.contains(range.commonAncestorContainer))return null;const before=document.createRange(),through=document.createRange();before.selectNodeContents(root);before.setEnd(range.startContainer,range.startOffset);through.selectNodeContents(root);through.setEnd(range.endContainer,range.endOffset);return[chars(before.toString()).length,chars(through.toString()).length]}}
function rememberSelection(){{const span=selectionSpan();if(span&&span[0]<span[1]){{lastSelection=span;el('selectionMessage').textContent=`Selected ${{span[0]}}:${{span[1]}} “${{slice(curParent().parent_source_text,...span)}}”`}}}}
function takeSelection(){{const span=selectionSpan()||lastSelection;if(!span||!Number.isInteger(span[0])||span[0]>=span[1])throw Error('Select a nonempty exact source span first.');return span}}
function spanArrayValid(spans,source,allowEmpty=false){{if(!Array.isArray(spans)||(!allowEmpty&&!spans.length))return false;let end=-1;for(const pair of spans){{if(!Array.isArray(pair)||pair.length!==2||!pair.every(Number.isInteger))return false;const[start,stop]=pair;if(!(0<=start&&start<stop&&stop<=chars(source).length&&start>=end&&slice(source,start,stop).trim()))return false;end=stop}}return true}}
function childDataValid(child,source){{return child&&['individual','organization'].includes(child.entity_type)&&spanArrayValid(child.name_spans,source)&&spanArrayValid(child.support_spans,source)}}
function childComplete(child,source){{return childDataValid(child,source)&&child.confirmed===true}}
function normalizedNameFor(parent,child){{return child.name_spans.map(pair=>slice(parent.parent_source_text,...pair)).join(' ').normalize('NFKC').replace(/\\s+/gu,' ').trim().toLowerCase()}}
function spansOverlap(left,right){{return left.some(a=>right.some(b=>Math.max(a[0],b[0])<Math.min(a[1],b[1])))}}
function spanUnionContains(outer,inner){{for(const[start,stop]of inner){{let cursor=start;for(const[left,right]of outer){{if(right<=cursor)continue;if(left>cursor)break;cursor=Math.max(cursor,right);if(cursor>=stop)break}}if(cursor<stop)return false}}return true}}
function tokenSequenceContains(longer,shorter){{const a=longer.split(' '),b=shorter.split(' ');if(!b.length||b.length>a.length)return false;for(let i=0;i<=a.length-b.length;i++)if(b.every((token,j)=>token===a[i+j]))return true;return false}}
function duplicateChildReason(parent,children){{const entries=children.map((child,index)=>spanArrayValid(child.name_spans,parent.parent_source_text)?{{index,spans:child.name_spans,name:normalizedNameFor(parent,child)}}:null).filter(Boolean);for(let i=0;i<entries.length;i++){{const left=entries[i];for(let j=i+1;j<entries.length;j++){{const right=entries[j];if(!spansOverlap(left.spans,right.spans))continue;if(left.name===right.name)return`Children ${{left.index+1}} and ${{right.index+1}} encode the same overlapping normalized name.`;if(tokenSequenceContains(left.name,right.name)||tokenSequenceContains(right.name,left.name))return`Children ${{left.index+1}} and ${{right.index+1}} encode nested overlapping normalized names.`;if(spanUnionContains(left.spans,right.spans)||spanUnionContains(right.spans,left.spans))return`Children ${{left.index+1}} and ${{right.index+1}} encode nested overlapping name coordinates.`}}}}return null}}
function parentComplete(parent,response){{if(!response||response.parent_confirmed!==true||typeof response.true_zero!=='boolean'||typeof response.notes!=='string'||!Array.isArray(response.children)||!Array.isArray(response.reviewed_unresolved_block_ids))return false;if(JSON.stringify(response.reviewed_unresolved_block_ids)!==JSON.stringify(unresolvedIds(parent)))return false;if(response.true_zero)return response.children.length===0;if(!response.children.length)return false;if(response.children.some(child=>!childComplete(child,parent.parent_source_text)))return false;return duplicateChildReason(parent,response.children)===null}}
function confirmContains(){{const p=curParent(),r=state();if(r.true_zero===true&&r.parent_confirmed&&r.children.length&&!confirm('Replace the confirmed true-zero decision?'))return;r.true_zero=false;r.parent_confirmed=true;if(!r.children.length)r.children.push({{entity_type:null,name_spans:[],support_spans:[],confirmed:false,origin_seed_id:null}});save();draw();showMessage('Contains decision explicitly confirmed.')}}
function confirmZero(){{const p=curParent(),r=state();if(r.children.length&&!confirm('Confirm true zero and remove all current child rows?'))return;r.true_zero=true;r.parent_confirmed=true;r.children=[];childIndex=0;save();draw();showMessage('True-zero decision explicitly confirmed.')}}
function resetParent(){{if(!confirm("Discard this parent's edits and restore the unconfirmed AI first cut?"))return;S[curParent().parent_document_id]=seedState(curParent());childIndex=0;save();draw();showMessage('Parent reset to the unconfirmed AI first cut.')}}
function currentChild(){{return state().children[childIndex]||null}}function dirtyChild(child){{child.confirmed=false}}
function addSpan(kind){{try{{const child=currentChild();if(!child)throw Error('Confirm Contains and select or add a child first.');const span=takeSelection(),field=kind==='name'?'name_spans':'support_spans';child[field].push(span);child[field].sort((a,b)=>a[0]-b[0]||a[1]-b[1]);dirtyChild(child);save();draw()}}catch(error){{showMessage(error.message,true)}}}}
function removeSpan(kind,index){{const child=currentChild(),field=kind==='name'?'name_spans':'support_spans';child[field].splice(index,1);dirtyChild(child);save();draw()}}
function setType(value){{const child=currentChild();if(!child)return;child.entity_type=value;dirtyChild(child);save();draw()}}
function confirmChild(){{const child=currentChild(),p=curParent(),r=state();if(!child)return false;if(r.true_zero!==false){{showMessage('Confirm Contains before confirming a child.',true);return false}}if(!childDataValid(child,p.parent_source_text)){{showMessage('Set a valid type plus nonempty ordered name and support spans first.',true);return false}}child.confirmed=true;const duplicate=duplicateChildReason(p,r.children);if(duplicate){{child.confirmed=false;save();draw();showMessage(duplicate,true);return false}}save();draw();return true}}
function addChild(){{const r=state();if(r.true_zero!==false)return showMessage('Confirm Contains before adding a child.',true);r.children.push({{entity_type:null,name_spans:[],support_spans:[],confirmed:false,origin_seed_id:null}});childIndex=r.children.length-1;save();draw()}}
function splitChild(){{const r=state(),child=currentChild();if(!child)return showMessage('There is no current child to split.',true);child.confirmed=false;r.children.splice(childIndex+1,0,{{entity_type:child.entity_type,name_spans:cloneSpans(child.name_spans),support_spans:cloneSpans(child.support_spans),confirmed:false,origin_seed_id:child.origin_seed_id}});childIndex++;save();draw();showMessage('Copied spans into a second unconfirmed child. Edit both rows.')}}
function deleteChild(){{const r=state();if(!currentChild())return;if(!confirm('Delete this child row? Child row count defines multiplicity.'))return;r.children.splice(childIndex,1);if(!r.children.length)r.parent_confirmed=false;childIndex=Math.max(0,Math.min(childIndex,r.children.length-1));save();draw()}}
function canonicalName(child){{return child.name_spans.map(pair=>slice(curParent().parent_source_text,...pair)).join(' ').replace(/\\s+/g,' ').trim()}}
function moveChild(delta){{const r=state();if(!r.children.length)return showMessage('This parent has no child rows.',true);childIndex=Math.max(0,Math.min(r.children.length-1,childIndex+delta));draw()}}
function navigateNextUnresolved(includeCurrent=true){{const order=[];T.parents.forEach((parent,pi)=>{{const response=S[parent.parent_document_id];if(!response.parent_confirmed)order.push([pi,Math.max(0,response.children.findIndex(child=>!childComplete(child,parent.parent_source_text))),'parent',null]);for(const block of parent.ai_unresolved_blocks)if(!response.reviewed_unresolved_block_ids.includes(block.unresolved_block_id))order.push([pi,0,'block',block.unresolved_block_id]);if(response.true_zero===false)response.children.forEach((child,ci)=>{{if(!childComplete(child,parent.parent_source_text))order.push([pi,ci,'child',null])}})}});let target=null;if(includeCurrent)target=order.find(value=>value[0]===parentIndex&&(value[2]!=='child'||value[1]===childIndex))||null;if(!target)target=order.find(value=>value[0]>parentIndex||(value[0]===parentIndex&&value[1]>childIndex))||order[0]||null;if(target){{parentIndex=target[0];childIndex=Math.max(0,target[1]);draw();if(target[2]==='block'){{const block=curParent().ai_unresolved_blocks.find(value=>value.unresolved_block_id===target[3]);showSpan(block&&block.block_span)}}showMessage(`${{target[2]==='parent'?'Parent decision':target[2]==='block'?'Failed model source block':'Child '+(childIndex+1)}} needs explicit review.`);return true}}showMessage('All parents, children, and failed-model source blocks are explicitly reviewed.');return false}}
function confirmAndNext(){{if(confirmChild())navigateNextUnresolved(false)}}
function showSpan(pair){{if(!pair)return;const root=el('source'),text=root.textContent,node=root.firstChild;if(!node)return;const startUtf16=chars(text).slice(0,pair[0]).join('').length,endUtf16=chars(text).slice(0,pair[1]).join('').length,range=document.createRange(),selection=window.getSelection();range.setStart(node,startUtf16);range.setEnd(node,endUtf16);selection.removeAllRanges();selection.addRange(range);root.focus();const linesBefore=chars(text).slice(0,pair[0]).join('').split('\\n').length-1,total=Math.max(1,text.split('\\n').length);root.scrollTop=Math.max(0,root.scrollHeight*(linesBefore/total)-root.clientHeight/3);rememberSelection()}}
function showCurrent(){{const child=currentChild();showSpan(child&&(child.name_spans[0]||child.support_spans[0]))}}
function showMessage(message,bad=false){{el('message').textContent=message||'';el('message').className=bad?'error':'ok'}}
function setBlockReviewed(id,reviewed){{const p=curParent(),r=state(),allowed=unresolvedIds(p);if(!allowed.includes(id))return showMessage('Unknown unresolved block.',true);const chosen=new Set(r.reviewed_unresolved_block_ids);if(reviewed)chosen.add(id);else chosen.delete(id);r.reviewed_unresolved_block_ids=allowed.filter(value=>chosen.has(value));save();draw();showMessage(reviewed?'Failed-model source block explicitly reviewed.':'Failed-model source block returned to pending review.')}}
function reviewNextBlock(){{const p=curParent(),r=state(),block=p.ai_unresolved_blocks.find(value=>!r.reviewed_unresolved_block_ids.includes(value.unresolved_block_id));if(!block)return showMessage('This parent has no pending failed-model source block.',true);setBlockReviewed(block.unresolved_block_id,true)}}
function drawUnresolvedBlocks(){{const p=curParent(),r=state();if(!p.ai_unresolved_blocks.length){{el('unresolvedBlocks').innerHTML='';return}}el('unresolvedBlocks').innerHTML=`<h3 class="error">Unresolved source blocks from failed model slots</h3><p class="warning">No model record from each flagged slot was accepted. Independently valid sibling slots may remain as AI seeds. Inspect each source block, add any children it contains, and explicitly mark it reviewed.</p>`+p.ai_unresolved_blocks.map((block,index)=>{{const reviewed=r.reviewed_unresolved_block_ids.includes(block.unresolved_block_id),excerpt=slice(p.parent_source_text,...block.block_span).trim();return`<div class="unresolved-block ${{reviewed?'reviewed':''}}"><strong>Block ${{index+1}} · ${{reviewed?'human reviewed':'review pending'}}</strong><br><code>${{block.block_span[0]}}:${{block.block_span[1]}} · ${{esc(excerpt)}}</code><div class="toolbar"><button data-show-block="${{index}}">Show in source</button><button data-review-block="${{index}}" class="${{reviewed?'':'primary'}}">${{reviewed?'Return to pending':'Mark explicitly reviewed'}}</button></div></div>`}}).join('');for(const button of el('unresolvedBlocks').querySelectorAll('[data-show-block]'))button.onclick=()=>showSpan(p.ai_unresolved_blocks[Number(button.dataset.showBlock)].block_span);for(const button of el('unresolvedBlocks').querySelectorAll('[data-review-block]'))button.onclick=()=>{{const block=p.ai_unresolved_blocks[Number(button.dataset.reviewBlock)];setBlockReviewed(block.unresolved_block_id,!r.reviewed_unresolved_block_ids.includes(block.unresolved_block_id))}}}}
function drawChildren(){{const p=curParent(),r=state();if(r.true_zero===true){{el('children').innerHTML=`<p class="${{r.parent_confirmed?'ok':'warning'}}">${{r.parent_confirmed?'Human-confirmed true zero.':'AI suggests true zero; human confirmation pending.'}}</p>`;return}}if(r.true_zero===null){{el('children').innerHTML='<p class="warning">Parent decision needs review.</p>';return}}el('children').innerHTML=r.children.map((child,index)=>`<div class="childrow ${{index===childIndex?'current':''}}"><button data-edit="${{index}}">${{index+1}}</button><span>${{child.entity_type||'TYPE NEEDED'}}</span><span>${{esc(canonicalName(child)||'NAME SPANS NEEDED')}}</span><span class="${{childComplete(child,p.parent_source_text)?'ok':'error'}}">${{childComplete(child,p.parent_source_text)?'human confirmed':'unconfirmed'}}</span></div>`).join('')||'<p class="error">Add at least one child or confirm True zero.</p>';for(const button of el('children').querySelectorAll('[data-edit]'))button.onclick=()=>{{childIndex=Number(button.dataset.edit);draw()}}}}
function spanList(kind,spans){{const p=curParent();return `<ul class="spans">${{spans.map((pair,index)=>`<li><button data-show-${{kind}}="${{index}}">Show</button> ${{pair[0]}}:${{pair[1]}} “${{esc(slice(p.parent_source_text,...pair))}}” <button class="danger" data-drop-${{kind}}="${{index}}">Remove</button></li>`).join('')}}</ul>`}}
function preserveSelection(button){{button.addEventListener('pointerdown',rememberSelection);button.addEventListener('mousedown',event=>{{rememberSelection();event.preventDefault()}})}}
function drawEditor(){{const p=curParent(),r=state(),child=currentChild();if(r.true_zero!==false||!child){{el('editor').innerHTML='<p class="muted">No child selected.</p>';return}}el('editor').innerHTML=`<h3>Child ${{childIndex+1}} of ${{r.children.length}}</h3><div class="decision"><button id="individualType" class="${{child.entity_type==='individual'?'selected':''}}">Individual (1)</button><button id="organizationType" class="${{child.entity_type==='organization'?'selected':''}}">Organization (2)</button><button id="confirmChild" class="primary">${{child.confirmed?'Human confirmed · Enter advances':'Confirm child + next unresolved (Enter)'}}</button></div><strong>Name spans</strong>${{spanList('name',child.name_spans)}}<button id="addName">Add selection as name component</button><strong><br>Support spans</strong>${{spanList('support',child.support_spans)}}<button id="addSupport">Add selection as support component</button>`;el('individualType').onclick=()=>setType('individual');el('organizationType').onclick=()=>setType('organization');el('confirmChild').onclick=confirmAndNext;el('addName').onclick=()=>addSpan('name');el('addSupport').onclick=()=>addSpan('support');for(const button of el('editor').querySelectorAll('[data-show-name]'))button.onclick=()=>showSpan(child.name_spans[Number(button.dataset.showName)]);for(const button of el('editor').querySelectorAll('[data-show-support]'))button.onclick=()=>showSpan(child.support_spans[Number(button.dataset.showSupport)]);for(const button of el('editor').querySelectorAll('[data-drop-name]'))button.onclick=()=>removeSpan('name',Number(button.dataset.dropName));for(const button of el('editor').querySelectorAll('[data-drop-support]'))button.onclick=()=>removeSpan('support',Number(button.dataset.dropSupport));preserveSelection(el('addName'));preserveSelection(el('addSupport'))}}
function draw(){{const p=curParent(),r=state(),source=el('source');if(source.dataset.parentId!==p.parent_document_id){{source.textContent=p.parent_source_text;source.dataset.parentId=p.parent_document_id;source.scrollTop=0}}el('instructions').innerHTML=renderInstructions(T.instructions);el('position').textContent=`Parent ${{parentIndex+1}}/${{T.parents.length}} · ${{p.metadata.candidate_icpsr}} · ${{p.metadata.year}}`;const complete=T.parents.filter(parent=>parentComplete(parent,S[parent.parent_document_id])).length,parentConfirmed=T.parents.filter(parent=>S[parent.parent_document_id].parent_confirmed).length,totalChildren=T.parents.reduce((count,parent)=>count+S[parent.parent_document_id].children.length,0),childConfirmed=T.parents.reduce((count,parent)=>count+S[parent.parent_document_id].children.filter(child=>childComplete(child,parent.parent_source_text)).length,0),totalBlocks=T.parents.reduce((count,parent)=>count+parent.ai_unresolved_blocks.length,0),blocksReviewed=T.parents.reduce((count,parent)=>count+S[parent.parent_document_id].reviewed_unresolved_block_ids.length,0),duplicate=duplicateChildReason(p,r.children||[]);el('progress').textContent=`${{complete}}/${{T.parents.length}} complete · ${{parentConfirmed}} parent decisions · ${{childConfirmed}}/${{totalChildren}} children confirmed · ${{blocksReviewed}}/${{totalBlocks}} failed blocks reviewed`;el('parentSummary').innerHTML=`<span class="status">Human decision: ${{r.parent_confirmed?(r.true_zero?'TRUE ZERO':'CONTAINS CHILDREN'):'UNCONFIRMED'}}</span><br><span class="warning">AI first cut: ${{p.ai_parent_disposition.replaceAll('_',' ')}} · ${{p.ai_children.length}} child row(s). Every AI seed started unconfirmed.</span>${{p.ai_unresolved_blocks.length?`<br><span class="error">${{p.ai_unresolved_blocks.length}} source block(s) received no model seeds and require separate review.</span>`:''}}${{duplicate?`<br><span class="error">${{esc(duplicate)}}</span>`:''}}`;el('nonzero').classList.toggle('selected',r.true_zero===false&&r.parent_confirmed);el('zero').classList.toggle('selected',r.true_zero===true&&r.parent_confirmed);el('nonzero').classList.toggle('suggested',r.true_zero===false&&!r.parent_confirmed);el('zero').classList.toggle('suggested',r.true_zero===true&&!r.parent_confirmed);el('notes').value=r.notes||'';childIndex=Math.max(0,Math.min(childIndex,Math.max(0,r.children.length-1)));drawUnresolvedBlocks();drawChildren();drawEditor();lastSelection=null;el('selectionMessage').textContent='Select exact text in the source.';showMessage('')}}
function exportResponse(parent,response){{return{{parent_document_id:parent.parent_document_id,parent_confirmed:true,true_zero:response.true_zero,children:response.children.map(child=>({{human_confirmed:true,entity_type:child.entity_type,name_spans:cloneSpans(child.name_spans),support_spans:cloneSpans(child.support_spans)}})),reviewed_unresolved_block_ids:unresolvedIds(parent),notes:response.notes||''}}}}
function buildExport(){{for(let index=0;index<T.parents.length;index++){{const parent=T.parents[index],response=S[parent.parent_document_id];if(!parentComplete(parent,response))throw Error(`Parent ${{index+1}} is not fully and explicitly confirmed.`)}}return{{task_id:T.task_id,task_revision:T.task_revision,task_sha256:A.task_sha256,format:A.result_format,exported_at:new Date().toISOString(),parent_count:T.parents.length,review_status:'all_parents_children_and_unresolved_blocks_explicitly_confirmed',authorizations:A.result_authorizations,responses:T.parents.map(parent=>exportResponse(parent,S[parent.parent_document_id]))}}}}
function validateImported(out){{if(!exactKeys(out,['task_id','task_revision','task_sha256','format','exported_at','parent_count','review_status','authorizations','responses'])||out.task_id!==T.task_id||out.task_revision!==T.task_revision||out.task_sha256!==A.task_sha256||out.format!==A.result_format||out.parent_count!==T.parents.length||out.review_status!=='all_parents_children_and_unresolved_blocks_explicitly_confirmed'||JSON.stringify(out.authorizations)!==JSON.stringify(A.result_authorizations)||!Array.isArray(out.responses)||out.responses.length!==T.parents.length)throw Error('Imported result identity or root schema differs.');const next={{}};out.responses.forEach((response,index)=>{{const parent=T.parents[index];if(!exactKeys(response,['parent_document_id','parent_confirmed','true_zero','children','reviewed_unresolved_block_ids','notes'])||response.parent_document_id!==parent.parent_document_id||response.parent_confirmed!==true||typeof response.true_zero!=='boolean'||!Array.isArray(response.children)||!Array.isArray(response.reviewed_unresolved_block_ids)||JSON.stringify(response.reviewed_unresolved_block_ids)!==JSON.stringify(unresolvedIds(parent))||typeof response.notes!=='string')throw Error(`Imported parent ${{index+1}} schema differs.`);const internal={{parent_confirmed:true,true_zero:response.true_zero,reviewed_unresolved_block_ids:[...response.reviewed_unresolved_block_ids],notes:response.notes,children:response.children.map(child=>{{if(!exactKeys(child,['human_confirmed','entity_type','name_spans','support_spans'])||child.human_confirmed!==true)throw Error(`Imported child schema differs at parent ${{index+1}}.`);return{{entity_type:child.entity_type,name_spans:child.name_spans,support_spans:child.support_spans,confirmed:true,origin_seed_id:null}}}})}};if(!parentComplete(parent,internal))throw Error(`Imported parent ${{index+1}} is incomplete, inconsistent, or duplicated.`);next[parent.parent_document_id]=internal}});return next}}
function openExport(out){{exportObject=out;el('exportText').value=JSON.stringify(out,null,2);el('exportDialog').showModal()}}
el('source').addEventListener('mouseup',rememberSelection);el('source').addEventListener('keyup',rememberSelection);document.addEventListener('selectionchange',()=>{{if(document.activeElement===el('source'))rememberSelection()}});el('nonzero').onclick=confirmContains;el('zero').onclick=confirmZero;el('addChild').onclick=addChild;el('splitChild').onclick=splitChild;el('deleteChild').onclick=deleteChild;el('resetParent').onclick=resetParent;el('showCurrent').onclick=showCurrent;el('nextUnresolved').onclick=()=>navigateNextUnresolved(true);
el('notes').oninput=()=>{{const r=state();r.notes=el('notes').value;save()}};el('prev').onclick=()=>{{parentIndex=Math.max(0,parentIndex-1);childIndex=0;draw()}};el('next').onclick=()=>{{parentIndex=Math.min(T.parents.length-1,parentIndex+1);childIndex=0;draw()}};
el('export').onclick=()=>{{try{{openExport(buildExport())}}catch(error){{showMessage(error.message,true)}}}};el('download').onclick=()=>{{const blob=new Blob([JSON.stringify(exportObject,null,2)+'\\n'],{{type:'application/json'}}),link=document.createElement('a');link.href=URL.createObjectURL(blob);link.download=T.task_id+'_results.json';link.click();setTimeout(()=>URL.revokeObjectURL(link.href),1000)}};el('copy').onclick=async()=>{{const text=el('exportText').value;try{{await navigator.clipboard.writeText(text)}}catch(error){{el('exportText').select();document.execCommand('copy')}}}};el('closeDialog').onclick=()=>el('exportDialog').close();
el('import').onclick=()=>el('importFile').click();el('importFile').onchange=async event=>{{try{{const file=event.target.files[0];if(!file)return;const imported=JSON.parse(await file.text());S=validateImported(imported);save();parentIndex=0;childIndex=0;draw();showMessage('Complete results imported and authenticated to this task.')}}catch(error){{showMessage(error.message,true)}}finally{{event.target.value=''}}}};
document.addEventListener('keydown',event=>{{const key=event.key.toLowerCase();if(['TEXTAREA','INPUT'].includes(event.target.tagName)||(event.target===el('source')&&!['r','u'].includes(key)))return;if(event.key==='ArrowRight')el('next').click();else if(event.key==='ArrowLeft')el('prev').click();else if(key==='i')el('instructionBox').open=!el('instructionBox').open;else if(key==='c')confirmContains();else if(key==='z')confirmZero();else if(key==='1')setType('individual');else if(key==='2')setType('organization');else if(event.key==='Enter')confirmAndNext();else if(key==='u')navigateNextUnresolved(true);else if(key==='r')reviewNextBlock();else if(key==='j'||event.key===']')moveChild(1);else if(key==='k'||event.key==='[')moveChild(-1);else if(key==='a')addChild();else if(key==='s')splitChild();else return;event.preventDefault()}});
save();draw();window.__round4ReviewApp={{buildExport,validateImported,selectionSpan,rememberSelection,takeSelection,parentComplete,duplicateChildReason,seedState,setBlockReviewed,reviewNextBlock,getState:()=>S,getCurrent:()=>({{parentIndex,childIndex}})}};
</script></body></html>'''


def build_files(
    *,
    stage_a_input_dir: Path,
    expected_stage_a_manifest_sha256: str,
    provisional_draft_path: Path,
    expected_provisional_draft_sha256: str,
    output_dir: Path,
    task_id: str = DEFAULT_TASK_ID,
    task_revision: str = DEFAULT_TASK_REVISION,
) -> dict[str, Any]:
    """Build task.json, auth.json, and self-contained HTML into a new directory."""
    output_dir = output_dir.resolve()
    if output_dir.exists():
        raise ValueError("output directory must not already exist")
    parents, manifest, manifest_sha256 = load_stage_a_source_artifact(
        stage_a_input_dir,
        expected_manifest_sha256=expected_stage_a_manifest_sha256,
    )
    draft, draft_sha256 = load_provisional_draft(
        provisional_draft_path,
        expected_sha256=expected_provisional_draft_sha256,
        parents=parents,
        stage_a_manifest=manifest,
        stage_a_manifest_sha256=manifest_sha256,
    )
    task = build_task(
        parents=parents,
        stage_a_manifest=manifest,
        stage_a_manifest_sha256=manifest_sha256,
        draft=draft,
        draft_sha256=draft_sha256,
        task_id=task_id,
        task_revision=task_revision,
    )
    task_raw = canonical_bytes(task)
    task_sha256 = sha256_bytes(task_raw)
    html_raw = render_html(task, task_sha256).encode("utf-8")
    auth = {
        "version": AUTH_VERSION,
        "builder_version": BUILDER_VERSION,
        "task_id": task["task_id"],
        "task_revision": task["task_revision"],
        "task_sha256": task_sha256,
        "html_sha256": sha256_bytes(html_raw),
        "stage_a_input_manifest_sha256": manifest_sha256,
        "provisional_draft_sha256": draft_sha256,
        "stage_a_output_inventory_sha256": draft["provenance"][
            "stage_a_output_inventory_sha256"
        ],
        "stage_b_input_manifest_sha256": draft["provenance"][
            "stage_b_input_manifest_sha256"
        ],
        "stage_b_output_inventory_sha256": draft["provenance"][
            "stage_b_output_inventory_sha256"
        ],
        "selected_parent_ids_sha256": manifest["selected_parent_ids_sha256"],
        "source_bundle_sha256": task["input_provenance"]["source_bundle_sha256"],
        "generator_sha256": sha256_file(Path(__file__).resolve()),
        "conversion": copy.deepcopy(draft["conversion"]),
        "authorizations": copy.deepcopy(REVIEW_AUTHORIZATIONS),
    }
    output_dir.mkdir(parents=True)
    (output_dir / "task.json").write_bytes(task_raw)
    (output_dir / "index.html").write_bytes(html_raw)
    (output_dir / "auth.json").write_bytes(canonical_bytes(auth))
    for filename in ("task.json", "index.html", "auth.json"):
        path = output_dir / filename
        if not path.is_file() or path.stat().st_size < 100:
            raise ValueError(f"built review artifact is missing or trivial: {filename}")
    return auth


def build_files_from_authenticated_runs(
    *,
    stage_a_input_dir: Path,
    stage_a_output_dir: Path,
    expected_stage_a_manifest_sha256: str,
    stage_b_input_dir: Path,
    stage_b_output_dir: Path,
    expected_stage_b_manifest_sha256: str,
    output_dir: Path,
    expected_stage_a_output_inventory_sha256: str | None = None,
    expected_stage_b_output_inventory_sha256: str | None = None,
    task_id: str = DEFAULT_TASK_ID,
    task_revision: str = DEFAULT_TASK_REVISION,
) -> dict[str, Any]:
    """Authenticate both fresh stages, prepare seeds, and build in one call."""
    output_dir = output_dir.resolve()
    if output_dir.exists():
        raise ValueError("output directory must not already exist")
    draft, report, parents, stage_a_manifest = (
        prepare_provisional_draft_from_authenticated_runs(
            stage_a_input_dir=stage_a_input_dir,
            stage_a_output_dir=stage_a_output_dir,
            expected_stage_a_input_manifest_sha256=(
                expected_stage_a_manifest_sha256
            ),
            stage_b_input_dir=stage_b_input_dir,
            stage_b_output_dir=stage_b_output_dir,
            expected_stage_b_input_manifest_sha256=(
                expected_stage_b_manifest_sha256
            ),
            expected_stage_a_output_inventory_sha256=(
                expected_stage_a_output_inventory_sha256
            ),
            expected_stage_b_output_inventory_sha256=(
                expected_stage_b_output_inventory_sha256
            ),
        )
    )
    draft_raw = canonical_bytes(draft)
    draft_sha256 = sha256_bytes(draft_raw)
    report_raw = canonical_bytes(report)
    report_sha256 = sha256_bytes(report_raw)
    task = build_task(
        parents=parents,
        stage_a_manifest=stage_a_manifest,
        stage_a_manifest_sha256=expected_stage_a_manifest_sha256,
        draft=draft,
        draft_sha256=draft_sha256,
        task_id=task_id,
        task_revision=task_revision,
    )
    task_raw = canonical_bytes(task)
    task_sha256 = sha256_bytes(task_raw)
    html_raw = render_html(task, task_sha256).encode("utf-8")
    auth = {
        "version": AUTH_VERSION,
        "builder_version": BUILDER_VERSION,
        "task_id": task["task_id"],
        "task_revision": task["task_revision"],
        "task_sha256": task_sha256,
        "html_sha256": sha256_bytes(html_raw),
        "stage_a_input_manifest_sha256": expected_stage_a_manifest_sha256,
        "provisional_draft_sha256": draft_sha256,
        "conversion_report_sha256": report_sha256,
        "stage_a_output_inventory_sha256": draft["provenance"][
            "stage_a_output_inventory_sha256"
        ],
        "stage_b_input_manifest_sha256": draft["provenance"][
            "stage_b_input_manifest_sha256"
        ],
        "stage_b_output_inventory_sha256": draft["provenance"][
            "stage_b_output_inventory_sha256"
        ],
        "selected_parent_ids_sha256": stage_a_manifest[
            "selected_parent_ids_sha256"
        ],
        "source_bundle_sha256": task["input_provenance"]["source_bundle_sha256"],
        "generator_sha256": sha256_file(Path(__file__).resolve()),
        "conversion": copy.deepcopy(draft["conversion"]),
        "authorizations": copy.deepcopy(REVIEW_AUTHORIZATIONS),
    }
    output_dir.mkdir(parents=True)
    files = {
        "task.json": task_raw,
        "index.html": html_raw,
        "auth.json": canonical_bytes(auth),
        "provisional_draft.json": draft_raw,
        "provisional_conversion_report.json": report_raw,
    }
    for filename, payload in files.items():
        (output_dir / filename).write_bytes(payload)
    for filename in files:
        path = output_dir / filename
        if not path.is_file() or path.stat().st_size < 100:
            raise ValueError(f"built review artifact is missing or trivial: {filename}")
    return auth


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage-a-input-dir", type=Path, required=True)
    parser.add_argument("--expected-stage-a-manifest-sha256", required=True)
    parser.add_argument("--provisional-draft", type=Path)
    parser.add_argument("--expected-provisional-draft-sha256")
    parser.add_argument("--stage-a-output-dir", type=Path)
    parser.add_argument("--stage-b-input-dir", type=Path)
    parser.add_argument("--expected-stage-b-manifest-sha256")
    parser.add_argument("--stage-b-output-dir", type=Path)
    parser.add_argument("--expected-stage-a-output-inventory-sha256")
    parser.add_argument("--expected-stage-b-output-inventory-sha256")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--task-id", default=DEFAULT_TASK_ID)
    parser.add_argument("--task-revision", default=DEFAULT_TASK_REVISION)
    args = parser.parse_args()
    run_values = (
        args.stage_a_output_dir,
        args.expected_stage_a_output_inventory_sha256,
        args.stage_b_input_dir,
        args.expected_stage_b_manifest_sha256,
        args.stage_b_output_dir,
        args.expected_stage_b_output_inventory_sha256,
    )
    if args.provisional_draft is not None:
        if args.expected_provisional_draft_sha256 is None:
            parser.error(
                "--expected-provisional-draft-sha256 is required with "
                "--provisional-draft"
            )
        if any(value is not None for value in run_values):
            parser.error(
                "prepared-draft mode cannot be combined with Stage-A/Stage-B "
                "run directories"
            )
        auth = build_files(
            stage_a_input_dir=args.stage_a_input_dir,
            expected_stage_a_manifest_sha256=(
                args.expected_stage_a_manifest_sha256
            ),
            provisional_draft_path=args.provisional_draft,
            expected_provisional_draft_sha256=(
                args.expected_provisional_draft_sha256
            ),
            output_dir=args.output_dir,
            task_id=args.task_id,
            task_revision=args.task_revision,
        )
    else:
        if any(value is None for value in run_values):
            parser.error(
                "direct-run mode requires --stage-a-output-dir, "
                "--expected-stage-a-output-inventory-sha256, "
                "--stage-b-input-dir, --expected-stage-b-manifest-sha256, "
                "--stage-b-output-dir, and "
                "--expected-stage-b-output-inventory-sha256"
            )
        auth = build_files_from_authenticated_runs(
            stage_a_input_dir=args.stage_a_input_dir,
            stage_a_output_dir=args.stage_a_output_dir,
            expected_stage_a_manifest_sha256=(
                args.expected_stage_a_manifest_sha256
            ),
            stage_b_input_dir=args.stage_b_input_dir,
            stage_b_output_dir=args.stage_b_output_dir,
            expected_stage_b_manifest_sha256=(
                args.expected_stage_b_manifest_sha256
            ),
            expected_stage_a_output_inventory_sha256=(
                args.expected_stage_a_output_inventory_sha256
            ),
            expected_stage_b_output_inventory_sha256=(
                args.expected_stage_b_output_inventory_sha256
            ),
            output_dir=args.output_dir,
            task_id=args.task_id,
            task_revision=args.task_revision,
        )
    print(canonical_json(auth))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
