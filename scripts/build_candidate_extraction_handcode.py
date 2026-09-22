#!/usr/bin/env python3
"""Build the independent human review apps for candidate extraction v2.

The input is a frozen v2 bundle. ``bundle_manifest.json`` must name one
SHA-256-verified JSONL or JSONL.GZ segment file. Each segment row must include
the v2 extraction, exact source text, parent and segment identifiers, segment
bounds, processing status, and candidate metadata. This script samples only
the importer's canonical resolved records. It never calls a model.

The output contains deterministic task JSON files, self-contained HTML files,
and ``handcode_manifest.json``. The HTML files use the shared handcode template.
Existing output files are never overwritten.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.candidate_extraction import (
    BIOGRAPHY_FIELDS_V2,
    LIST_KEY_BY_VERSION,
    ROUTES,
    align_extraction_exact_spans,
    get_extraction_contract,
    validate_extraction,
)
from scripts.import_candidate_extraction_v2 import (
    EXPECTED_GENERATION_CONFIG,
    EXPECTED_GENERATION_CONFIG_SHA256,
    FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256,
    FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256,
    KNOWN_OMISSION_MATCHING_SPEC,
    KNOWN_OMISSION_MATCHING_SPEC_SHA256,
    all_output_aggregate,
    build_record_variants,
    output_provenance_summary,
    load_endorsement_line_ledger_bundle,
    resolve_parent_variants,
    run_metadata_aggregate,
    validate_generation_config_identity,
    validate_endorsement_context_manifest,
    validate_private_output_provenance,
    validate_run_metadata_accounting,
    validate_run_metadata_against_all_output,
)
from src.candidate_endorsement_line_ledger import get_line_ledger_contract
from src.candidate_endorsement_round4_release import (
    ROUND4_RECORD_RESOLUTION_METHOD,
    ROUND4_RECORD_RESOLUTION_STATUS,
)
from scripts.import_candidate_extraction_v2 import (  # noqa: E402
    TRUNCATED_PARSE_STATUS,
)
from scripts.hive.extract_candidate_pages_vllm import (
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
)


DEFAULT_SEED = 20260824
DEFAULT_BATCH_SIZE = 60
BIOGRAPHY_PER_FIELD = 10
ISSUE_PRECISION_N = 60
ENDORSEMENT_PRECISION_N = 60
RECALL_PER_ROUTE = 20
RECALL_ZERO_TARGET = 5
RECALL_MULTI_TARGET = 5
OPTIONAL_ENDORSEMENT_TARGET = 20
HEX64 = set("0123456789abcdef")
V2_1_CONTRACT = get_extraction_contract("2.1")

HANDCODE_BUILDER = (
    Path.home() / ".agents" / "skills" / "handcode" / "scripts" / "build_task.py"
)


class HandcodeInputError(ValueError):
    """The frozen bundle cannot support the declared human review."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def handcode_renderer_provenance(
    builder: Path = HANDCODE_BUILDER,
) -> dict[str, str]:
    """Identify the exact shared builder and template used for coding apps."""
    template = builder.parent.parent / "template.html"
    _require(builder.is_file(), f"shared handcode builder is absent: {builder}")
    _require(template.is_file(), f"shared handcode template is absent: {template}")
    return {
        "renderer_id": "shared_handcode_builder_and_template_v1",
        "builder_file": builder.name,
        "builder_sha256": sha256_file(builder),
        "template_file": template.name,
        "template_sha256": sha256_file(template),
    }


def stable_rank(seed: int, *values: Any) -> str:
    payload = "\n".join([str(seed), *(str(value) for value in values)])
    return sha256_text(payload)


def _require(condition: Any, message: str) -> None:
    if not bool(condition):
        raise HandcodeInputError(message)


def _repaired_window_identities(manifest: Mapping[str, Any]) -> dict[str, str]:
    """Map each window a repair recovered to that attempt's generation identity."""
    identities: dict[str, str] = {}
    for attempt in manifest.get("seed_vary_repair") or []:
        for window in attempt.get("windows") or []:
            if window.get("repaired") is not True:
                continue
            document_id = str(window.get("document_id"))
            _require(
                document_id not in identities,
                f"{document_id} is recorded as repaired by two attempts",
            )
            identities[document_id] = str(
                attempt.get("repair_generation_config_sha256")
            )
    return identities


def _typed_equal(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            _typed_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, (list, tuple)):
        return len(left) == len(right) and all(
            _typed_equal(left_item, right_item)
            for left_item, right_item in zip(left, right, strict=True)
        )
    return left == right


def validate_bundle_generation_config(manifest: Mapping[str, Any]) -> None:
    """Translate the shared frozen-generation failure into a bundle error."""
    try:
        observed = validate_generation_config_identity(
            manifest.get("generation_config"),
            manifest.get("generation_config_sha256"),
            "bundle manifest",
        )
    except ValueError as error:
        raise HandcodeInputError(str(error)) from error
    _require(
        _typed_equal(observed, EXPECTED_GENERATION_CONFIG),
        "bundle complete generation config differs",
    )


def _require_sha256(value: Any, label: str) -> str:
    text = str(value or "")
    _require(len(text) == 64 and set(text) <= HEX64, f"{label} is not a lower-case SHA-256")
    return text


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    rows: list[dict[str, Any]] = []
    with opener(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            _require(isinstance(value, dict), f"{path.name}:{line_number} is not an object")
            rows.append(value)
    return rows


def _safe_bundle_file(bundle_dir: Path, spec: Any) -> tuple[Path, dict[str, Any]]:
    _require(isinstance(spec, dict), "bundle segments_file must be an object")
    relative = Path(str(spec.get("file") or ""))
    _require(str(relative) not in {"", "."}, "bundle segments_file has no file")
    _require(not relative.is_absolute(), "bundle segment path must be relative")
    _require(".." not in relative.parts, "bundle segment path leaves the bundle")
    path = (bundle_dir / relative).resolve()
    _require(bundle_dir.resolve() in path.parents, "bundle segment path leaves the bundle")
    _require(path.is_file(), f"bundle segment file is absent: {relative}")
    expected = _require_sha256(spec.get("sha256"), "segments_file.sha256")
    _require(sha256_file(path) == expected, "bundle segment file hash differs")
    return path, spec


def _route_value(value: Any, route: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(route)
    return value


def _segment_id(row: Mapping[str, Any]) -> str:
    return _require_sha256(row.get("segment_id") or row.get("document_id"), "segment_id")


def _is_cap_limited(row: Mapping[str, Any]) -> bool:
    """Report whether this window reached the output cap and was never finished.

    Such a window has no parsed output and no extraction, so every replay that
    reads a complete answer has to skip it. Its page is ineligible for sampling.
    """
    return (
        row.get("parse_status") == TRUNCATED_PARSE_STATUS
        and row.get("finish_reason") == "length"
    )


def _processing_is_valid(row: Mapping[str, Any]) -> bool:
    validation = row.get("validation")
    schema_valid = row.get("schema_valid") is True or (
        isinstance(validation, Mapping) and validation.get("schema_valid") is True
    )
    return (
        row.get("parse_status") == "parsed"
        and row.get("processing_status")
        in {
            "processed_success",
            "processed_partial_withheld",
            "processed_all_withheld",
        }
        and schema_valid
        and row.get("input_truncated") is False
        and row.get("finish_reason") == "stop"
    )


def _record_highlights(route: str, payload: Mapping[str, Any]) -> list[str]:
    fields = {
        "issues": ("source_heading", "evidence"),
        "biography": ("evidence",),
        "endorsements": ("record_evidence", "endorsement_context_evidence"),
    }[route]
    result: list[str] = []
    for field in fields:
        value = payload.get(field)
        if isinstance(value, str) and value and value not in result:
            result.append(value)
    return result


def _subtype(route: str, payload: Mapping[str, Any]) -> str:
    return str({
        "issues": payload.get("statement_kind"),
        "biography": payload.get("field"),
        "endorsements": payload.get("endorser_category"),
    }[route])


def build_parent_pages(
    segment_rows: Iterable[dict[str, Any]],
    manifest: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Validate segment rows and collect the importer's resolved records."""
    rows = list(segment_rows)
    _require(bool(rows), "bundle has no segment rows")
    manifest = manifest or {}
    strict_private_provenance = (
        manifest.get("extraction_contract_sha256") is not None
    )
    repaired_identities = _repaired_window_identities(manifest)
    round4_ledger = manifest.get("_validated_endorsement_round4_ledger")
    round4_enabled = isinstance(round4_ledger, Mapping)
    validated_ledger = (
        round4_ledger
        if round4_enabled
        else manifest.get("_validated_endorsement_line_ledger")
    )
    ledger_enabled = isinstance(validated_ledger, Mapping)
    if round4_enabled:
        ledger_parent_summaries = {
            str(item["parent_document_id"]): item
            for item in round4_ledger.get("parents", [])
        } or round4_ledger.get("parent_summaries", {})
    else:
        ledger_parent_summaries = (
            validated_ledger.get("parent_summaries", {}) if ledger_enabled else {}
        )
    endorsement_architecture = (
        "endorsement_round4_record_ledger_v1"
        if round4_enabled
        else "endorsement_parent_line_ledger_v1"
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    cap_withheld_parents: set[str] = set()
    seen_segments: set[str] = set()
    parsed_by_segment: dict[str, Any] = {}
    for row in rows:
        route = str(row.get("route") or "")
        _require(route in ROUTES, f"segment has an invalid route: {route}")
        segment_id = _segment_id(row)
        _require(segment_id not in seen_segments, f"duplicate segment ID: {segment_id}")
        seen_segments.add(segment_id)
        parent_id = _require_sha256(row.get("parent_document_id"), f"{segment_id}.parent")
        raw_text_sha256 = _require_sha256(
            row.get("raw_text_sha256"), f"{segment_id}.raw_text_sha256"
        )
        expected_parent_id = sha256_text(
            f"{row.get('candidate_cycle_id')}\n{route}\n{raw_text_sha256}"
        )
        _require(parent_id == expected_parent_id, f"{parent_id} is not deterministic")
        if _is_cap_limited(row):
            # A window that reached the output cap was never read to the end.
            # It is a recorded terminal outcome, not a processing error, so the
            # page stays in the bundle and every count check stays exact. The
            # page is marked here and removed at the sampling step: in a recall
            # task its lost text would count as a model miss, which measures a
            # known gap rather than model behaviour.
            cap_withheld_parents.add(parent_id)
        else:
            _require(
                _processing_is_valid(row),
                f"{segment_id} did not process successfully",
            )
        source_text = row.get("source_text")
        _require(isinstance(source_text, str) and bool(source_text), f"{segment_id} lacks source text")
        segment_hash = _require_sha256(
            row.get("segment_text_sha256"), f"{segment_id}.segment_text_sha256"
        )
        _require(sha256_text(source_text) == segment_hash, f"{segment_id} text hash differs")
        start = row.get("segment_start_char")
        end = row.get("segment_end_char")
        _require(
            isinstance(start, int) and not isinstance(start, bool),
            f"{segment_id} start is not an integer",
        )
        _require(
            isinstance(end, int) and not isinstance(end, bool),
            f"{segment_id} end is not an integer",
        )
        _require(0 <= start < end and end - start == len(source_text), f"{segment_id} bounds differ")
        expected_segment_id = sha256_text(
            f"{parent_id}\n{row.get('segment_index')}\n{start}\n{end}\n{segment_hash}"
        )
        _require(segment_id == expected_segment_id, f"{segment_id} is not deterministic")
        if strict_private_provenance and not _is_cap_limited(row):
            parsed, _ = validate_private_output_provenance(
                row, source_text, segment_id
            )
            _require(parsed is not None, f"{segment_id} lacks parsed raw output")
            parsed_by_segment[segment_id] = parsed
        for field in (
            "model_id",
            "model_revision",
            "runtime_lock_sha256",
            "prompt_sha256",
            "json_schema_sha256",
            "extraction_contract_sha256",
            "endorsement_context_provenance_spec_sha256",
            "user_message_template_id",
            "user_message_template_sha256",
            "inference_display_escape_id",
            "inference_display_escape_spec_sha256",
            "json_string_control_canonicalizer_id",
            "json_string_control_canonicalizer_spec_sha256",
            "array_item_cap_spec_id",
            "array_item_cap_spec_sha256",
            "model_item_repetition_spec_id",
            "model_item_repetition_spec_sha256",
            "generation_config_sha256",
            "structured_decoding_backend",
            "structured_decoding_disable_any_whitespace",
        ):
            expected = _route_value(manifest.get(field), route)
            if field == "generation_config_sha256" and segment_id in repaired_identities:
                # A window a seed-vary repair recovered carries its attempt's
                # identity, which differs from the primary one only in the seed.
                # The expected value comes from the bundle's repair attestation,
                # not from the row, so a row cannot claim an unrecorded repair.
                expected = repaired_identities[segment_id]
            if expected is not None:
                _require(
                    _typed_equal(row.get(field), expected),
                    f"{segment_id} differs on {field}",
                )
        if strict_private_provenance:
            _require(
                row.get("route_array_item_cap")
                == (manifest.get("route_array_item_caps") or {}).get(route),
                f"{segment_id} differs on route_array_item_cap",
            )
        grouped[parent_id].append(row)

    if strict_private_provenance:
        private_output_summary = output_provenance_summary(rows)
        _require(
            _typed_equal(
                manifest.get("private_output_provenance_summary"),
                private_output_summary,
            ),
            "bundle private-output provenance summary differs",
        )
        _require(
            private_output_summary.get(
                "raw_structural_whitespace_validator_id"
            )
            == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
            and private_output_summary.get(
                "raw_structural_whitespace_validator_spec_sha256"
            )
            == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
            and private_output_summary.get(
                "raw_structural_whitespace_gate_passed"
            )
            is True,
            "bundle raw structural-whitespace gate did not pass",
        )
        recomputed_all_output = all_output_aggregate(rows)
        _require(
            _typed_equal(
                manifest.get("all_output_aggregate"),
                recomputed_all_output,
            ),
            "bundle all-output aggregate differs",
        )
        _require(
            type(recomputed_all_output["array_item_cap_reached_outputs"])
            is int
            and recomputed_all_output["array_item_cap_reached_outputs"] == 0
            and type(recomputed_all_output[
                "model_item_repetition_error_outputs"
            ])
            is int
            and recomputed_all_output[
                "model_item_repetition_error_outputs"
            ]
            == 0,
            "bundle contains a pre-alignment model-item guard failure",
        )

    pages: list[dict[str, Any]] = []
    route_parent_counts: Counter[str] = Counter()
    route_segment_counts: Counter[str] = Counter()
    replayed_conflict_groups: set[str] = set()
    silently_selected_conflict_groups: set[str] = set()
    for parent_id, parent_rows in sorted(grouped.items()):
        parent_rows.sort(key=lambda row: (int(row["segment_index"]), _segment_id(row)))
        first = parent_rows[0]
        route = str(first["route"])
        expected_count = len(parent_rows)
        _require(
            [int(row["segment_index"]) for row in parent_rows] == list(range(expected_count)),
            f"{parent_id} has incomplete segment indexes",
        )
        _require(
            all(int(row.get("segment_count", -1)) == expected_count for row in parent_rows),
            f"{parent_id} has inconsistent segment counts",
        )
        for row in parent_rows:
            for field in (
                "route",
                "candidate_cycle_id",
                "candidate_icpsr",
                "state",
                "office",
                "year",
                "party",
                "raw_text_sha256",
            ):
                _require(row.get(field) == first.get(field), f"{parent_id} differs on {field}")

        reconstructed = ""
        prior_end = 0
        prior_text = ""
        resolved_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for index, row in enumerate(parent_rows):
            start = int(row["segment_start_char"])
            end = int(row["segment_end_char"])
            text = str(row["source_text"])
            if index == 0:
                _require(start == 0, f"{parent_id} does not start at zero")
                reconstructed = text
            else:
                _require(start <= prior_end, f"{parent_id} has a gap between segments")
                overlap = prior_end - start
                if overlap:
                    _require(prior_text[-overlap:] == text[:overlap], f"{parent_id} overlap differs")
                reconstructed += text[overlap:]
            prior_end = end
            prior_text = text
            resolved = row.get("resolved_records")
            _require(isinstance(resolved, list), f"{_segment_id(row)} lacks resolved_records")
            for record in resolved:
                _require(isinstance(record, dict), f"{_segment_id(row)} has an invalid resolved record")
                resolved_rows.append((row, record))
        _require(prior_end == len(reconstructed), f"{parent_id} segments do not reach the page end")
        extraction_hash = first.get("extraction_source_text_sha256")
        if extraction_hash is not None:
            _require(
                sha256_text(reconstructed) == extraction_hash,
                f"{parent_id} reconstructed source hash differs",
            )
        for row in parent_rows:
            segment_id = _segment_id(row)
            extraction = row.get("extraction")
            if strict_private_provenance and not _is_cap_limited(row):
                recomputed_extraction, recomputed_withheld, recomputed_alignment = (
                    align_extraction_exact_spans(
                        route,
                        parsed_by_segment[segment_id],
                        str(row["source_text"]),
                        endorsement_context_provenance=row.get(
                            "endorsement_context_provenance"
                        ),
                        segment_start_char=int(row["segment_start_char"]),
                    )
                )
                expected_replay = {
                    "extraction": recomputed_extraction,
                    "withheld_model_items": recomputed_withheld,
                    "exact_alignment": recomputed_alignment,
                    "model_extraction_sha256": recomputed_alignment[
                        "original_model_extraction_sha256"
                    ],
                    "aligned_extraction_sha256": recomputed_alignment[
                        "aligned_extraction_sha256"
                    ],
                }
                for field, expected_value in expected_replay.items():
                    _require(
                        _typed_equal(row.get(field), expected_value),
                        f"{segment_id} {field} differs from raw-output replay",
                    )
                replayed_processing_status = {
                    "complete": "processed_success",
                    "partial_withheld": "processed_partial_withheld",
                    "all_withheld": "processed_all_withheld",
                    "invalid_schema": "failed_schema",
                }[str(recomputed_alignment["status"])]
                if route == "endorsements" and ledger_enabled:
                    _require(
                        row.get("superseded_one_stage_processing_status")
                        == replayed_processing_status,
                        f"{segment_id} superseded one-stage status differs "
                        "from raw-output replay",
                    )
                    _require(
                        row.get("extraction_architecture")
                        == endorsement_architecture,
                        f"{segment_id} endorsement architecture differs",
                    )
                else:
                    _require(
                        row.get("processing_status")
                        == replayed_processing_status,
                        f"{segment_id} processing status differs from "
                        "raw-output replay",
                    )
                extraction = recomputed_extraction
            if _is_cap_limited(row):
                # The answer was cut off, so there is no extraction to validate.
                # The stored validation records that outcome and the page is
                # already ineligible for sampling.
                _require(
                    (row.get("validation") or {}).get("status")
                    == TRUNCATED_PARSE_STATUS
                    and row.get("extraction") is None,
                    f"{segment_id} is cap limited but carries an extraction",
                )
            else:
                validation = validate_extraction(
                    route,
                    extraction,
                    str(row["source_text"]),
                    "2.1",
                    endorsement_context_provenance=row.get(
                        "endorsement_context_provenance"
                    ),
                    segment_start_char=int(row["segment_start_char"]),
                    parent_source_text=reconstructed,
                )
                if row.get("validation") is not None:
                    _require(
                        _typed_equal(row["validation"], validation),
                        f"{segment_id} validation differs from replay",
                    )
                _require(
                    validation["schema_valid"] is True,
                    f"{segment_id} has an invalid v2.1 schema",
                )
                _require(
                    validation["n_supported"] == validation["n_items"],
                    f"{segment_id} has a non-exact evidence span",
                )

        if strict_private_provenance and not (
            route == "endorsements" and ledger_enabled
        ):
            replayed_variants = build_record_variants(parent_rows)
            replayed_annotated, replayed_resolved = resolve_parent_variants(
                parent_id, route, replayed_variants
            )
            expected_variants: dict[str, list[dict[str, Any]]] = defaultdict(list)
            expected_resolved: dict[str, list[dict[str, Any]]] = defaultdict(list)
            conflict_variant_ids: set[str] = set()
            for variant in replayed_annotated:
                expected_variants[str(variant["segment_id"])].append(variant)
                if variant["overlap_resolution_status"] == "conflict_withheld":
                    replayed_conflict_groups.add(
                        str(variant["overlap_group_id"])
                    )
                    conflict_variant_ids.add(str(variant["variant_id"]))
            for record in replayed_resolved:
                expected_resolved[str(record["canonical_segment_id"])].append(
                    record
                )
                if conflict_variant_ids & {
                    str(value) for value in record["all_variant_ids"]
                }:
                    silently_selected_conflict_groups.add(
                        str(record["record_id"])
                    )
            for row in parent_rows:
                segment_id = _segment_id(row)
                expected_variants[segment_id].sort(
                    key=lambda value: value["variant_id"]
                )
                expected_resolved[segment_id].sort(
                    key=lambda value: value["record_id"]
                )
                _require(
                    _typed_equal(
                        row.get("record_variants"),
                        expected_variants[segment_id],
                    ),
                    f"{segment_id} record variants differ from replay",
                )
                _require(
                    _typed_equal(
                        row.get("resolved_records"),
                        expected_resolved[segment_id],
                    ),
                    f"{segment_id} resolved records differ from replay",
                )
        elif strict_private_provenance:
            ledger_parent = ledger_parent_summaries.get(parent_id)
            if round4_enabled:
                # Round four reports coverage per parent, and 277 of 2,583
                # parents are publicly incomplete for a typed reason. They are
                # still real pages with real records, so they stay eligible for
                # the validation sample. Excluding them would bias precision
                # upward and would hide from recall exactly the pages where the
                # extraction misses the most.
                _require(
                    isinstance(ledger_parent, Mapping)
                    and not ledger_parent.get("retry_required"),
                    f"{parent_id} lacks a settled round-four parent summary",
                )
            else:
                _require(
                    isinstance(ledger_parent, Mapping)
                    and ledger_parent.get("public_coverage_complete") is True,
                    f"{parent_id} lacks complete endorsement ledger coverage",
                )
            _require(
                all(row.get("record_variants") == [] for row in parent_rows),
                f"{parent_id} exposes superseded endorsement variants",
            )
            _require(
                sum(len(row.get("resolved_records") or []) for row in parent_rows)
                == int(ledger_parent["aligned_record_count"]),
                f"{parent_id} endorsement ledger record count differs",
            )

        records: list[dict[str, Any]] = []
        seen_record_ids: set[str] = set()
        anchor_fields = {
            "issues": ("source_heading", "evidence"),
            "biography": ("evidence",),
            "endorsements": ("record_evidence", "endorsement_context_evidence"),
        }[route]
        for row, resolved in sorted(resolved_rows, key=lambda pair: str(pair[1].get("record_id"))):
            record_id = _require_sha256(resolved.get("record_id"), "resolved record ID")
            _require(record_id not in seen_record_ids, f"duplicate resolved record: {record_id}")
            seen_record_ids.add(record_id)
            _require(resolved.get("parent_document_id") == parent_id, f"{record_id} parent differs")
            _require(resolved.get("route") == route, f"{record_id} route differs")
            _require(
                resolved.get("canonical_segment_id") == _segment_id(row),
                f"{record_id} is stored on a noncanonical segment",
            )
            _require(
                resolved.get("resolution_status") in {
                    "single_variant",
                    "equivalent_overlap",
                    "equivalent_semantics_distinct_exact_evidence",
                    "line_ledger_single_record",
                    ROUND4_RECORD_RESOLUTION_STATUS,
                },
                f"{record_id} has an invalid resolution status",
            )
            payload = resolved.get("item")
            _require(isinstance(payload, dict), f"{record_id} lacks its resolved item")
            key = LIST_KEY_BY_VERSION["2.1"][route]
            if route == "endorsements" and ledger_enabled:
                expected_status, expected_method = (
                    (ROUND4_RECORD_RESOLUTION_STATUS, ROUND4_RECORD_RESOLUTION_METHOD)
                    if round4_enabled
                    else ("line_ledger_single_record", "two_stage_parent_line_ledger_v1")
                )
                _require(
                    resolved.get("resolution_status") == expected_status
                    and resolved.get("resolution_method") == expected_method
                    and payload.get("record_evidence_carrier_segment_id")
                    == _segment_id(row),
                    f"{record_id} ledger record identity differs",
                )
                _require_sha256(
                    payload.get("line_inventory_id"),
                    f"{record_id}.line_inventory_id",
                )
                _require_sha256(
                    payload.get("source_line_id"),
                    f"{record_id}.source_line_id",
                )
                _require_sha256(
                    payload.get("inventory_owner_segment_id"),
                    f"{record_id}.inventory_owner_segment_id",
                )
            else:
                _require(
                    canonical_json(payload) in {
                        canonical_json(item) for item in row["extraction"][key]
                    },
                    f"{record_id} is not in its canonical segment extraction",
                )
            parent_offsets = resolved.get("parent_offsets")
            _require(isinstance(parent_offsets, dict), f"{record_id} lacks parent offsets")
            for text_field in anchor_fields:
                value = payload.get(text_field)
                start = parent_offsets.get(f"{text_field}_parent_start_char")
                end = parent_offsets.get(f"{text_field}_parent_end_char")
                if value is None:
                    _require(start is None and end is None, f"{record_id} has offsets without text")
                    continue
                _require(
                    isinstance(start, int) and not isinstance(start, bool)
                    and isinstance(end, int) and not isinstance(end, bool),
                    f"{record_id} parent offsets are invalid",
                )
                _require(
                    0 <= start < end <= len(reconstructed)
                    and reconstructed[start:end] == value,
                    f"{record_id} parent evidence is not source-exact",
                )
            records.append({
                "record_id": record_id,
                "parent_document_id": parent_id,
                "route": route,
                "subtype": _subtype(route, payload),
                "model_record": payload,
                "parent_offsets": parent_offsets,
                "source_segment_ids": sorted(str(value) for value in resolved["all_source_segment_ids"]),
                "resolution_status": resolved["resolution_status"],
                "highlights": _record_highlights(route, payload),
            })
        records.sort(key=lambda value: value["record_id"])
        pages.append({
            "has_cap_limited_window": parent_id in cap_withheld_parents,
            "parent_document_id": parent_id,
            "candidate_cycle_id": str(first.get("candidate_cycle_id") or ""),
            "candidate_icpsr": str(first.get("candidate_icpsr") or ""),
            "state": str(first.get("state") or ""),
            "office": str(first.get("office") or ""),
            "year": first.get("year"),
            "party": str(first.get("party") or ""),
            "route": route,
            "raw_text_sha256": str(first.get("raw_text_sha256") or ""),
            "source_text": reconstructed,
            "segment_count": expected_count,
            "records": records,
        })
        route_parent_counts[route] += 1
        route_segment_counts[route] += expected_count

    if manifest.get("segments") is not None:
        _require(int(manifest["segments"]) == len(rows), "manifest segment count differs")
    if manifest.get("parent_documents") is not None:
        _require(int(manifest["parent_documents"]) == len(pages), "manifest parent count differs")
    for field, actual in (
        ("segments_by_route", route_segment_counts),
        ("parent_documents_by_route", route_parent_counts),
    ):
        if manifest.get(field) is not None:
            expected = {str(key): int(value) for key, value in manifest[field].items()}
            _require(dict(actual) == expected, f"manifest {field} differs")
    resolved_count = sum(len(page["records"]) for page in pages)
    if manifest.get("resolved_records") is not None:
        _require(int(manifest["resolved_records"]) == resolved_count, "manifest record count differs")
    if strict_private_provenance:
        _require(
            int(manifest.get("withheld_overlap_conflict_groups", -1))
            == len(replayed_conflict_groups),
            "manifest overlap-conflict count differs from replay",
        )
        _require(
            not silently_selected_conflict_groups,
            "a replayed overlap conflict entered resolved records",
        )
    technical = {
        "passed": True,
        "segment_rows": len(rows),
        "parent_pages": len(pages),
        "records": resolved_count,
        "segments_by_route": dict(sorted(route_segment_counts.items())),
        "parents_by_route": dict(sorted(route_parent_counts.items())),
        "schema_valid_rate": 1.0,
        "exact_span_rate": 1.0,
        "silently_selected_overlap_conflicts": len(
            silently_selected_conflict_groups
        ),
        "withheld_overlap_conflicts": (
            len(replayed_conflict_groups)
            if strict_private_provenance
            else int(manifest.get("withheld_overlap_conflict_groups", 0))
        ),
        "truncated_segments": 0,
        "user_message_hash_replay_rate": 1.0,
        "extraction_contract_sha256": manifest.get(
            "extraction_contract_sha256"
        ),
        "endorsement_context_provenance_spec_sha256": manifest.get(
            "endorsement_context_provenance_spec_sha256"
        ),
        "user_message_template_id": manifest.get(
            "user_message_template_id"
        ),
        "user_message_template_sha256": manifest.get(
            "user_message_template_sha256"
        ),
        "endorsement_architecture": (
            endorsement_architecture if ledger_enabled else None
        ),
        "endorsement_line_ledger_contract_sha256": (
            None
            if round4_enabled or not ledger_enabled
            else get_line_ledger_contract()["line_ledger_contract_sha256"]
        ),
        "endorsement_round4_production_contract_sha256": (
            validated_ledger["summary"]["round4_production_contract_sha256"]
            if round4_enabled else None
        ),
        # The line ledger reports one coverage boolean for the corpus. Round
        # four reports an accounting gate instead, because a parent can be
        # publicly incomplete and still fully accounted with a typed reason.
        "endorsement_line_ledger_public_coverage_complete": (
            None
            if round4_enabled or not ledger_enabled
            else validated_ledger["summary"]["public_coverage_complete"]
        ),
        "endorsement_round4_all_model_records_accounted": (
            validated_ledger["summary"]["all_model_records_accounted"]
            if round4_enabled else None
        ),
        "raw_structural_whitespace_validator_id": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        ),
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
        "raw_structural_whitespace_gate_passed": (
            not strict_private_provenance
            or manifest["private_output_provenance_summary"][
                "raw_structural_whitespace_gate_passed"
            ]
            is True
        ),
        "array_item_cap_spec_id": manifest.get("array_item_cap_spec_id"),
        "array_item_cap_spec_sha256": manifest.get(
            "array_item_cap_spec_sha256"
        ),
        "route_array_item_caps": manifest.get("route_array_item_caps"),
        "model_item_repetition_spec_id": manifest.get(
            "model_item_repetition_spec_id"
        ),
        "model_item_repetition_spec_sha256": manifest.get(
            "model_item_repetition_spec_sha256"
        ),
        "array_item_cap_reached_outputs": (
            0
            if not strict_private_provenance
            else manifest["all_output_aggregate"][
                "array_item_cap_reached_outputs"
            ]
        ),
        "model_item_repetition_error_outputs": (
            0
            if not strict_private_provenance
            else manifest["all_output_aggregate"][
                "model_item_repetition_error_outputs"
            ]
        ),
        "pre_alignment_model_item_guard_gate_passed": (
            not strict_private_provenance
            or (
                manifest["all_output_aggregate"][
                    "array_item_cap_reached_outputs"
                ]
                == 0
                and manifest["all_output_aggregate"][
                    "model_item_repetition_error_outputs"
                ]
                == 0
            )
        ),
    }
    return pages, technical


def load_known_omission_recovery(
    bundle_dir: Path,
    pages: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = bundle_dir / "known_omission_recovery.json"
    _require(path.is_file(), "known_omission_recovery.json is absent")
    report = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(report, dict), "known-omission report is not an object")
    items = report.get("items")
    _require(isinstance(items, list), "known-omission report has no items")
    _require(report.get("known_omissions") == 11, "known-omission report does not declare 11 items")
    _require(report.get("recovered") == 11, "known-omission report does not recover all 11 items")
    _require(report.get("all_recovered") is True, "known-omission automated gate did not pass")
    _require(
        report.get("matching_spec") == KNOWN_OMISSION_MATCHING_SPEC
        and report.get("matching_spec_sha256")
        == KNOWN_OMISSION_MATCHING_SPEC_SHA256,
        "known-omission matching policy provenance differs",
    )
    _require(len(items) == 11, "known-omission report does not contain 11 items")

    pages_by_id = {page["parent_document_id"]: page for page in pages}
    records_by_id = {
        record["record_id"]: (page, record)
        for page in pages
        for record in page["records"]
    }
    omission_ids: set[str] = set()
    omissions: list[dict[str, Any]] = []
    for item in items:
        _require(isinstance(item, dict), "known-omission item is not an object")
        omission_id = _require_sha256(item.get("omission_id"), "known omission ID")
        _require(omission_id not in omission_ids, f"duplicate known omission: {omission_id}")
        omission_ids.add(omission_id)
        parent_id = _require_sha256(item.get("parent_document_id"), f"{omission_id}.parent")
        route = str(item.get("route") or "")
        _require(route in ROUTES, f"{omission_id} has an invalid route")
        _require(parent_id in pages_by_id, f"{omission_id} links to an absent parent")
        page = pages_by_id[parent_id]
        _require(page["route"] == route, f"{omission_id} parent route differs")
        evidence = str(item.get("evidence") or "")
        _require(bool(evidence) and evidence in page["source_text"], f"{omission_id} evidence is not exact")
        matched_ids = item.get("matched_record_ids")
        _require(isinstance(matched_ids, list) and bool(matched_ids), f"{omission_id} has no matched record")
        _require(len(matched_ids) == len(set(matched_ids)), f"{omission_id} repeats a matched record")
        matches: list[dict[str, Any]] = []
        for record_id_value in matched_ids:
            record_id = _require_sha256(record_id_value, f"{omission_id}.matched record")
            _require(record_id in records_by_id, f"{omission_id} matched record is absent")
            record_page, record = records_by_id[record_id]
            _require(
                record_page["parent_document_id"] == parent_id and record["route"] == route,
                f"{omission_id}/{record_id} linkage differs",
            )
            matches.append(record)
        omissions.append({
            "omission_id": omission_id,
            "parent_document_id": parent_id,
            "candidate_cycle_id": str(item.get("candidate_cycle_id") or page["candidate_cycle_id"]),
            "route": route,
            "expected_type": str(item.get("expected_type") or ""),
            "expected_value": str(item.get("expected_value") or ""),
            "evidence": evidence,
            "matched_record_ids": sorted(str(value) for value in matched_ids),
            "page": page,
            "matches": matches,
        })
    omissions.sort(key=lambda value: value["omission_id"])
    return omissions, {
        "file": path.name,
        "sha256": sha256_file(path),
        "known_omissions": 11,
        "automated_recovered": 11,
    }


def load_frozen_bundle(bundle_dir: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    manifest_path = bundle_dir / "bundle_manifest.json"
    _require(manifest_path.is_file(), "bundle_manifest.json is absent")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _require(isinstance(manifest, dict), "bundle manifest is not an object")
    _require(str(manifest.get("schema_version")) == "2.1", "bundle schema is not v2.1")
    _require(
        manifest.get("source_corpus_sha256")
        == FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256,
        "bundle source corpus is not the frozen suffix-p corpus",
    )
    _require(
        manifest.get("source_export_manifest_sha256")
        == FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256,
        "bundle source export is not the frozen suffix-p full export",
    )
    _require(
        manifest.get("source_export_upstream_full_manifest_sha256") is None,
        "handcoding bundle must come directly from the frozen full export",
    )
    _require(
        manifest.get("extraction_contract_sha256")
        == V2_1_CONTRACT["extraction_contract_sha256"],
        "bundle extraction contract is not the frozen v2.1 contract",
    )
    _require(
        manifest.get("generation_config_sha256")
        == EXPECTED_GENERATION_CONFIG_SHA256,
        "bundle generation config is not the frozen v2.1 identity",
    )
    _require(
        manifest.get("structured_decoding_backend")
        == STRUCTURED_DECODING_BACKEND,
        "bundle structured-decoding backend differs",
    )
    _require(
        manifest.get("structured_decoding_disable_any_whitespace")
        is STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        "bundle structured-decoding whitespace policy differs",
    )
    validate_bundle_generation_config(manifest)
    for field in (
        "endorsement_context_provenance_spec_sha256",
        "user_message_template_id",
        "user_message_template_sha256",
        "inference_display_escape_id",
        "inference_display_escape_spec_sha256",
        "json_string_control_canonicalizer_id",
        "json_string_control_canonicalizer_spec_sha256",
        "array_item_cap_spec_id",
        "array_item_cap_spec_sha256",
        "route_array_item_caps",
        "model_item_repetition_spec_id",
        "model_item_repetition_spec_sha256",
    ):
        _require(
            _typed_equal(manifest.get(field), V2_1_CONTRACT[field]),
            f"bundle {field} differs from the frozen v2.1 contract",
        )
    guard_aggregate = manifest.get("all_output_aggregate")
    _require(
        isinstance(guard_aggregate, dict)
        and type(guard_aggregate.get("array_item_cap_reached_outputs")) is int
        and guard_aggregate["array_item_cap_reached_outputs"] == 0
        and type(guard_aggregate.get("model_item_repetition_error_outputs"))
        is int
        and guard_aggregate["model_item_repetition_error_outputs"] == 0,
        "bundle contains a pre-alignment model-item guard failure",
    )
    _require(
        manifest.get("technical_segment_gate_passed") is True,
        "bundle technical segment gate did not pass",
    )
    _require(
        manifest.get("known_omission_gate") is True,
        "the revised model did not recover all known omissions",
    )
    segment_path, segment_spec = _safe_bundle_file(bundle_dir, manifest.get("segments_file"))
    rows = _read_jsonl(segment_path)
    _require(int(segment_spec.get("rows", -1)) == len(rows), "segment row count differs")
    # One bundle carries one endorsement architecture. Round four replaces the
    # line ledger and reports an accounting gate instead of a coverage boolean:
    # a parent can be publicly incomplete and still fully accounted, and every
    # gap carries a typed reason.
    if manifest.get("endorsement_round4_record_ledger") is not None:
        from scripts.import_candidate_extraction_v2 import (
            load_endorsement_round4_ledger_bundle,
        )

        try:
            validated_ledger = load_endorsement_round4_ledger_bundle(
                bundle_dir, manifest, rows
            )
        except (OSError, EOFError, json.JSONDecodeError, ValueError) as error:
            raise HandcodeInputError(
                f"bundle endorsement round-four replay differs: {error}"
            ) from error
        _require(
            validated_ledger["summary"].get("all_model_records_accounted") is True
            and int(validated_ledger["summary"].get("unaccounted_outputs", -1)) == 0,
            "bundle endorsement round-four accounting is incomplete",
        )
        manifest["_validated_endorsement_round4_ledger"] = validated_ledger
    else:
        try:
            validated_ledger = load_endorsement_line_ledger_bundle(
                bundle_dir, manifest, rows
            )
        except (OSError, EOFError, json.JSONDecodeError, ValueError) as error:
            raise HandcodeInputError(
                f"bundle endorsement line-ledger replay differs: {error}"
            ) from error
        _require(
            validated_ledger["summary"].get("public_coverage_complete") is True,
            "bundle endorsement line-ledger coverage is incomplete",
        )
        manifest["_validated_endorsement_line_ledger"] = validated_ledger
    try:
        selected_context_census = validate_endorsement_context_manifest(
            manifest.get("endorsement_context_provenance"),
            rows,
        )
    except ValueError as error:
        raise HandcodeInputError(
            f"bundle endorsement-context provenance differs: {error}"
        ) from error
    _require(
        _typed_equal(
            manifest.get(
                "endorsement_context_selected_segment_complete_line_census"
            ),
            selected_context_census,
        ),
        "bundle selected endorsement complete-line census differs",
    )
    upstream_source_manifest = manifest.get(
        "source_export_upstream_full_manifest_sha256"
    )
    if upstream_source_manifest is not None:
        _require_sha256(
            upstream_source_manifest,
            "bundle upstream full source-manifest SHA-256",
        )
    metadata_entries = manifest.get("run_metadata")
    _require(
        isinstance(metadata_entries, list)
        and bool(metadata_entries)
        and all(isinstance(entry, dict) for entry in metadata_entries),
        "bundle run metadata is absent",
    )
    try:
        for index, entry in enumerate(metadata_entries):
            for field in (
                "array_item_cap_spec_id",
                "array_item_cap_spec_sha256",
                "route_array_item_caps",
                "model_item_repetition_spec_id",
                "model_item_repetition_spec_sha256",
            ):
                if not _typed_equal(entry.get(field), manifest.get(field)):
                    raise ValueError(
                        f"bundle run metadata {index}: {field} differs"
                    )
            validate_run_metadata_accounting(
                entry, f"bundle run metadata {index}"
            )
        recomputed_metadata = run_metadata_aggregate(metadata_entries)
        if not _typed_equal(
            manifest.get("run_metadata_aggregate"), recomputed_metadata
        ):
            raise ValueError("bundle run-metadata aggregate differs")
        # A window a seed-vary repair recovered shows its repair row here and
        # its primary row in the primary run metadata. The metadata is
        # therefore compared against what the primary run alone produced. A
        # bundle with no repair carries the same numbers in both.
        recomputed_all_output = all_output_aggregate(rows)
        primary_equivalent = manifest.get("primary_equivalent_output_aggregate")
        if primary_equivalent is None and not manifest.get("seed_vary_repair"):
            primary_equivalent = recomputed_all_output
        _require(
            isinstance(primary_equivalent, Mapping),
            "bundle primary-equivalent output aggregate is absent",
        )
        validate_run_metadata_against_all_output(
            recomputed_metadata,
            primary_equivalent,
            "bundle run metadata aggregate",
        )
    except ValueError as error:
        raise HandcodeInputError(str(error)) from error
    pages, technical = build_parent_pages(rows, manifest)
    omissions, omission_file = load_known_omission_recovery(bundle_dir, pages)
    return pages, {
        "bundle_manifest": manifest,
        "bundle_manifest_sha256": sha256_file(manifest_path),
        "segments_file": str(
            segment_path.resolve().relative_to(Path(bundle_dir).resolve())
        ),
        "segments_file_sha256": sha256_file(segment_path),
        "technical_checks": technical,
        "known_omissions": omissions,
        "known_omission_recovery_file": omission_file,
    }


def load_excluded_parent_ids(
    paths: Iterable[Path] = (),
    inline_ids: Iterable[str] = (),
) -> set[str]:
    result = {str(value).strip() for value in inline_ids if str(value).strip()}
    for path in paths:
        _require(path.is_file(), f"exclusion file is absent: {path}")
        suffix = path.suffix.lower()
        if suffix == ".csv":
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                _require(
                    reader.fieldnames is not None and "parent_document_id" in reader.fieldnames,
                    f"{path} lacks parent_document_id",
                )
                result.update(
                    str(row["parent_document_id"]).strip()
                    for row in reader
                    if str(row.get("parent_document_id") or "").strip()
                )
        elif suffix == ".json":
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                value = value.get("parent_document_ids") or value.get("items")
            _require(isinstance(value, list), f"{path} JSON must contain a list")
            for item in value:
                if isinstance(item, dict):
                    item = item.get("parent_document_id")
                if item is not None and str(item).strip():
                    result.add(str(item).strip())
        else:
            result.update(
                line.strip()
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            )
    return result


def _eligible_pages(
    pages: Iterable[dict[str, Any]], route: str, excluded: set[str]
) -> list[dict[str, Any]]:
    return [
        page for page in pages
        if page["route"] == route and page["parent_document_id"] not in excluded
    ]


def _all_records(pages: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for page in pages:
        for record in page["records"]:
            rows.append({**record, "page": page})
    return rows


def _page_length_bin(length: int) -> str:
    if length < 3_000:
        return "short"
    if length < 8_000:
        return "medium"
    return "long"


def _issue_precision_strata(row: Mapping[str, Any]) -> dict[str, str]:
    page = row["page"]
    model = row["model_record"]
    return {
        "year": str(page.get("year") or "missing"),
        "party": str(page.get("party") or "missing").casefold(),
        "page_length": _page_length_bin(len(page["source_text"])),
        "segmentation": "multi" if page["segment_count"] > 1 else "single",
        "heading": "present" if model.get("source_heading") else "absent",
        "statement_kind": str(row["subtype"]),
    }


def _endorsement_precision_strata(
    row: Mapping[str, Any]
) -> dict[str, str]:
    model = row["model_record"]
    return {
        "entity_type": str(model["entity_type"]),
        "endorser_category": str(model["endorser_category"]),
        "support_mode": str(model["support_mode"]),
        "context_scope": str(
            model["endorsement_context_evidence_source_scope"]
        ),
    }


def precision_pool_observed_levels(
    pages: Iterable[dict[str, Any]], excluded_parent_ids: set[str]
) -> dict[str, dict[str, list[str]]]:
    pages = list(pages)
    result: dict[str, dict[str, list[str]]] = {}
    for route, strata_getter in (
        ("issues", _issue_precision_strata),
        ("endorsements", _endorsement_precision_strata),
    ):
        pool = _all_records(_eligible_pages(pages, route, excluded_parent_ids))
        _require(bool(pool), f"{route} precision pool is empty")
        names = strata_getter(pool[0]).keys()
        result[route] = {
            name: sorted({strata_getter(row)[name] for row in pool})
            for name in names
        }
    return result


def _balanced_select(
    rows: list[dict[str, Any]],
    count: int,
    dimensions: Mapping[str, Any],
    seed: int,
) -> list[dict[str, Any]]:
    _require(len(rows) >= count, f"balanced sample needs {count} rows; found {len(rows)}")
    remaining = list(rows)
    selected: list[dict[str, Any]] = []
    category_counts: dict[str, Counter[Any]] = {name: Counter() for name in dimensions}
    observed = {
        name: {getter(row) for row in rows}
        for name, getter in dimensions.items()
    }
    while len(selected) < count:
        def score(row: dict[str, Any]) -> tuple[float, str]:
            balance = 0.0
            missing_bonus = 0.0
            for name, getter in dimensions.items():
                value = getter(row)
                current = category_counts[name][value]
                balance += 1.0 / (1.0 + current)
                if current == 0 and value in observed[name]:
                    missing_bonus += 100.0
            return missing_bonus + balance, stable_rank(seed, row["record_id"])

        choice = max(remaining, key=score)
        remaining.remove(choice)
        selected.append(choice)
        for name, getter in dimensions.items():
            category_counts[name][getter(choice)] += 1
    return selected


def select_precision_records(
    pages: Iterable[dict[str, Any]],
    excluded_parent_ids: set[str],
    seed: int = DEFAULT_SEED,
) -> dict[str, list[dict[str, Any]]]:
    pages = list(pages)
    selected: dict[str, list[dict[str, Any]]] = {}

    biography_pool = _all_records(_eligible_pages(pages, "biography", excluded_parent_ids))
    biography: list[dict[str, Any]] = []
    for field in BIOGRAPHY_FIELDS_V2:
        pool = [row for row in biography_pool if row["subtype"] == field]
        _require(
            len(pool) >= BIOGRAPHY_PER_FIELD,
            f"biography {field} needs {BIOGRAPHY_PER_FIELD} records; found {len(pool)}",
        )
        pool.sort(key=lambda row: stable_rank(seed + 10, field, row["record_id"]))
        biography.extend(pool[:BIOGRAPHY_PER_FIELD])
    biography.sort(key=lambda row: stable_rank(seed + 11, row["record_id"]))
    selected["biography"] = biography

    issue_pool = _all_records(_eligible_pages(pages, "issues", excluded_parent_ids))
    _require(bool(issue_pool), "issue precision pool is empty")
    issue_dimensions = {
        name: (lambda row, field=name: _issue_precision_strata(row)[field])
        for name in _issue_precision_strata(issue_pool[0])
    }
    issues = _balanced_select(issue_pool, ISSUE_PRECISION_N, issue_dimensions, seed + 20)
    for name in issue_dimensions:
        observed = {_issue_precision_strata(row)[name] for row in issue_pool}
        sampled = {_issue_precision_strata(row)[name] for row in issues}
        _require(
            sampled == observed,
            f"issue sample does not cover every observed {name}",
        )
    issues.sort(key=lambda row: stable_rank(seed + 21, row["record_id"]))
    selected["issues"] = issues

    endorsement_pool = _all_records(
        _eligible_pages(pages, "endorsements", excluded_parent_ids)
    )
    _require(
        len(endorsement_pool) >= ENDORSEMENT_PRECISION_N,
        f"endorsement sample needs {ENDORSEMENT_PRECISION_N} records; found {len(endorsement_pool)}",
    )
    dimensions = {
        "entity_type": lambda row: row["model_record"]["entity_type"],
        "endorser_category": lambda row: row["model_record"]["endorser_category"],
        "support_mode": lambda row: row["model_record"]["support_mode"],
        "context_scope": lambda row: row["model_record"][
            "endorsement_context_evidence_source_scope"
        ],
        "title": lambda row: "present" if row["model_record"].get("title") else "null",
        "affiliation": lambda row: "present" if row["model_record"].get("affiliation") else "null",
        "jurisdiction": lambda row: "present" if row["model_record"].get("jurisdiction") else "null",
    }
    enum_requirements = {
        (field, value)
        for field in (
            "entity_type", "endorser_category", "support_mode",
            "endorsement_context_evidence_source_scope",
        )
        for value in {row["model_record"][field] for row in endorsement_pool}
    }
    optional_remaining = {
        field: OPTIONAL_ENDORSEMENT_TARGET
        for field in ("title", "affiliation", "jurisdiction")
    }
    endorsements: list[dict[str, Any]] = []
    remaining = list(endorsement_pool)
    while enum_requirements or any(optional_remaining.values()):
        def coverage_score(row: dict[str, Any]) -> tuple[int, str]:
            model = row["model_record"]
            enum_gain = sum(
                (field, model[field]) in enum_requirements
                for field in (
                    "entity_type", "endorser_category", "support_mode",
                    "endorsement_context_evidence_source_scope",
                )
            )
            optional_gain = sum(
                bool(model.get(field)) and optional_remaining[field] > 0
                for field in optional_remaining
            )
            return enum_gain * 100 + optional_gain, stable_rank(
                seed + 30, row["record_id"]
            )

        _require(bool(remaining), "endorsement coverage requirements are infeasible")
        choice = max(remaining, key=coverage_score)
        gain, _ = coverage_score(choice)
        _require(gain > 0, "endorsement coverage requirements are infeasible")
        remaining.remove(choice)
        endorsements.append(choice)
        model = choice["model_record"]
        for field in (
            "entity_type", "endorser_category", "support_mode",
            "endorsement_context_evidence_source_scope",
        ):
            enum_requirements.discard((field, model[field]))
        for field in optional_remaining:
            if model.get(field) and optional_remaining[field] > 0:
                optional_remaining[field] -= 1
    _require(
        len(endorsements) <= ENDORSEMENT_PRECISION_N,
        "endorsement coverage requirements exceed the sample size",
    )
    selected_ids = {row["record_id"] for row in endorsements}
    fill_count = ENDORSEMENT_PRECISION_N - len(endorsements)
    if fill_count:
        endorsements.extend(
            _balanced_select(
                [row for row in endorsement_pool if row["record_id"] not in selected_ids],
                fill_count,
                dimensions,
                seed + 31,
            )
        )
    for field in ("title", "affiliation", "jurisdiction"):
        nonnull = sum(bool(row["model_record"].get(field)) for row in endorsements)
        _require(
            nonnull >= OPTIONAL_ENDORSEMENT_TARGET,
            f"endorsement sample has fewer than {OPTIONAL_ENDORSEMENT_TARGET} non-null {field} values",
        )
    for field in (
        "entity_type", "endorser_category", "support_mode",
        "endorsement_context_evidence_source_scope",
    ):
        observed = {row["model_record"][field] for row in endorsement_pool}
        sampled = {row["model_record"][field] for row in endorsements}
        _require(sampled == observed, f"endorsement sample does not cover every observed {field}")
    _require(
        len({row["record_id"] for row in endorsements}) == ENDORSEMENT_PRECISION_N,
        "endorsement sample contains duplicate records",
    )
    endorsements.sort(key=lambda row: stable_rank(seed + 32, row["record_id"]))
    selected["endorsements"] = endorsements
    return selected


def select_recall_pages(
    pages: Iterable[dict[str, Any]],
    excluded_parent_ids: set[str],
    seed: int = DEFAULT_SEED,
) -> dict[str, list[dict[str, Any]]]:
    pages = list(pages)
    selected: dict[str, list[dict[str, Any]]] = {}
    for route_index, route in enumerate(ROUTES):
        pool = _eligible_pages(pages, route, excluded_parent_ids)
        _require(len(pool) >= RECALL_PER_ROUTE, f"{route} recall needs {RECALL_PER_ROUTE} pages")
        zero = sorted(
            [page for page in pool if not page["records"]],
            key=lambda page: stable_rank(seed + 100 + route_index, page["parent_document_id"]),
        )
        _require(
            len(zero) >= RECALL_ZERO_TARGET,
            f"{route} recall needs {RECALL_ZERO_TARGET} zero-result pages; found {len(zero)}",
        )
        route_selected = zero[:RECALL_ZERO_TARGET]
        selected_ids = {page["parent_document_id"] for page in route_selected}
        multi = sorted(
            [
                page for page in pool
                if page["segment_count"] > 1
                and page["records"]
                and page["parent_document_id"] not in selected_ids
            ],
            key=lambda page: stable_rank(seed + 110 + route_index, page["parent_document_id"]),
        )
        _require(
            len(multi) >= RECALL_MULTI_TARGET,
            f"{route} recall needs {RECALL_MULTI_TARGET} nonzero multi-window pages; found {len(multi)}",
        )
        route_selected.extend(multi[:RECALL_MULTI_TARGET])
        selected_ids = {page["parent_document_id"] for page in route_selected}
        general = sorted(
            [page for page in pool if page["parent_document_id"] not in selected_ids],
            key=lambda page: stable_rank(seed + 120 + route_index, page["parent_document_id"]),
        )
        route_selected.extend(general[: RECALL_PER_ROUTE - len(route_selected)])
        _require(len(route_selected) == RECALL_PER_ROUTE, f"{route} recall sample is incomplete")
        route_selected.sort(
            key=lambda page: stable_rank(seed + 130 + route_index, page["parent_document_id"])
        )
        selected[route] = route_selected
    return selected


def _candidate_badges(page: Mapping[str, Any], **extra: Any) -> dict[str, Any]:
    badges = {
        "Candidate": page.get("candidate_icpsr") or page.get("candidate_cycle_id"),
        "Candidate-year ID": page.get("candidate_cycle_id"),
        "State": page.get("state"),
        "Office": page.get("office"),
        "Year": page.get("year"),
        "Party": page.get("party"),
        "Route": page.get("route"),
        "Segments": page.get("segment_count"),
    }
    badges.update(extra)
    return badges


def _resolved_record_display(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "record": record["model_record"],
        "parent_page_offsets": record.get("parent_offsets"),
        "overlap_resolution": record.get("resolution_status"),
    }


def _precision_questions(route: str) -> list[dict[str, Any]]:
    if route == "biography":
        return [
            {"id": "fact_present", "label": "Is this fact stated on the page?", "type": "yesno"},
            {"id": "value_faithful", "label": "Does the value faithfully describe the stated fact?", "type": "yesno"},
            {"id": "field_correct", "label": "Is the biography field correct?", "type": "yesno"},
            {"id": "timing_correct", "label": "Is the timing label correct?", "type": "yesno"},
            {"id": "relationship_correct", "label": "Is the local-roots relationship correct or correctly null?", "type": "yesno"},
            {"id": "evidence_supports_fact", "label": "Does the highlighted evidence support the full fact?", "type": "yesno"},
        ]
    if route == "issues":
        return [
            {"id": "statement_present", "label": "Is this policy statement present on the page?", "type": "yesno"},
            {"id": "claim_faithful", "label": "Does the claim faithfully describe the page statement?", "type": "yesno"},
            {"id": "kind_correct", "label": "Is the statement-kind label correct?", "type": "yesno"},
            {"id": "heading_correct", "label": "Is the source heading exact, or correctly null?", "type": "yesno"},
            {"id": "evidence_supports_statement", "label": "Does the highlighted evidence support the claim?", "type": "yesno"},
        ]
    return [
        {"id": "endorser_present", "label": "Is the named endorser present on the page?", "type": "yesno"},
        {"id": "name_correct", "label": "Is the endorser name copied and separated correctly?", "type": "yesno"},
        {"id": "entity_category_correct", "label": "Are entity type and endorser category correct?", "type": "yesno"},
        {"id": "optional_fields_correct", "label": "Are title, affiliation, and jurisdiction correct or correctly null?", "type": "yesno"},
        {"id": "record_evidence_supports_identity", "label": "Does record evidence support the endorser identity?", "type": "yesno"},
        {"id": "combined_record_context_support", "label": "Do record and context evidence together establish this endorsement?", "type": "yesno"},
    ]


def _precision_instructions(route: str) -> str:
    special = {
        "biography": (
            "- Timing describes the claim at the archived capture date.\n"
            "- Local roots require the stated relationship. Do not infer current residence."
        ),
        "issues": (
            "- Check the claim, statement kind, nearest heading, and evidence separately.\n"
            "- Do not infer a topic or ideology label."
        ),
        "endorsements": (
            "- A bare name needs support from exact segment or parent-page context, "
            "or from the explicitly labeled classifier-qualified historical "
            "endorsement-page URL context.\n"
            "- Do not infer title, affiliation, jurisdiction, or organization."
        ),
    }[route]
    return (
        "### Review rule\n"
        "- Judge only the archived page shown here.\n"
        "- Answer each dimension independently.\n"
        "- Use Notes when a no answer needs clarification.\n"
        f"{special}"
    )


def precision_task(route: str, task_id: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for record in records:
        page = record["page"]
        items.append({
            "id": record["record_id"],
            "label": f"{page.get('candidate_icpsr') or page['candidate_cycle_id']} | {record['subtype']}",
            "display": [
                {
                    "label": "Candidate and sample",
                    "format": "badges",
                    "content": _candidate_badges(
                        page,
                        **{"Record ID": record["record_id"], "Subtype": record["subtype"]},
                    ),
                },
                {
                    "label": "Resolved model record",
                    "format": "json",
                    "content": _resolved_record_display(record),
                },
                {
                    "label": "Archived page text",
                    "format": "text",
                    "content": page["source_text"],
                    "highlight": record["highlights"],
                },
            ],
        })
    return {
        "task_id": task_id,
        "title": f"Candidate extraction v2: {route} precision",
        "instructions": _precision_instructions(route),
        "items": items,
        "questions": _precision_questions(route),
    }


def recall_task(route: str, task_id: str, pages: list[dict[str, Any]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for page in pages:
        highlights: list[str] = []
        for record in page["records"]:
            for value in record["highlights"]:
                if value not in highlights:
                    highlights.append(value)
        items.append({
            "id": page["parent_document_id"],
            "label": f"{page.get('candidate_icpsr') or page['candidate_cycle_id']} | {len(page['records'])} records",
            "display": [
                {
                    "label": "Candidate and page",
                    "format": "badges",
                    "content": _candidate_badges(
                        page,
                        **{
                            "Parent ID": page["parent_document_id"],
                            "Model records": len(page["records"]),
                        },
                    ),
                },
                {
                    "label": "All model records",
                    "format": "json",
                    "content": [
                        _resolved_record_display(record) for record in page["records"]
                    ],
                },
                {
                    "label": "Complete archived page text",
                    "format": "text",
                    "content": page["source_text"],
                    "highlight": highlights,
                },
            ],
        })
    return {
        "task_id": task_id,
        "title": f"Candidate extraction v2: {route} complete-page recall",
        "instructions": (
            "### Review rule\n"
            "- Read the complete archived page.\n"
            "- Compare every eligible fact with all model records.\n"
            "- Answer yes only when no eligible record is missing.\n"
            "- If no, add each missed record with exact evidence.\n"
            "- Empty model output is valid only when the page has no eligible record."
        ),
        "items": items,
        "questions": [
            {
                "id": "all_requested_records_captured",
                "label": "Did the model capture every eligible record on this page?",
                "type": "yesno",
            },
            {
                "id": "missed_records",
                "label": "Missed eligible records",
                "type": "structured",
                "required": True,
                "show_if": {
                    "question": "all_requested_records_captured",
                    "equals": "no",
                },
                "fields": [
                    {"id": "type", "label": "Type or category"},
                    {"id": "value", "label": "Value or name"},
                    {"id": "evidence", "label": "Exact page evidence"},
                ],
            },
        ],
    }


def known_omission_pairs(
    omissions: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for omission in omissions:
        for record in omission["matches"]:
            pair_id = sha256_text(canonical_json({
                "omission_id": omission["omission_id"],
                "record_id": record["record_id"],
            }))
            pairs.append({
                "item_id": pair_id,
                "omission": omission,
                "record": record,
            })
    pairs.sort(key=lambda value: value["item_id"])
    return pairs


def known_omission_task(
    task_id: str,
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for pair in pairs:
        omission = pair["omission"]
        record = pair["record"]
        page = omission["page"]
        highlights = [omission["evidence"], *record["highlights"]]
        items.append({
            "id": pair["item_id"],
            "label": f"{page.get('candidate_icpsr') or page['candidate_cycle_id']} | {omission['route']}",
            "display": [
                {
                    "label": "Known omission and linked record",
                    "format": "badges",
                    "content": _candidate_badges(
                        page,
                        **{
                            "Omission ID": omission["omission_id"],
                            "Linked record ID": record["record_id"],
                        },
                    ),
                },
                {
                    "label": "Known omitted fact",
                    "format": "json",
                    "content": {
                        "expected_type": omission["expected_type"],
                        "expected_value": omission["expected_value"],
                        "known_evidence": omission["evidence"],
                    },
                },
                {
                    "label": "Linked resolved model record",
                    "format": "json",
                    "content": _resolved_record_display(record),
                },
                {
                    "label": "Complete archived page text",
                    "format": "text",
                    "content": page["source_text"],
                    "highlight": list(dict.fromkeys(highlights)),
                },
            ],
        })
    return {
        "task_id": task_id,
        "title": "Candidate extraction v2: known-omission recovery",
        "instructions": (
            "### Review rule\n"
            "- This is a tuning-regression check, not a holdout estimate.\n"
            "- Compare the known omitted fact with the linked model record.\n"
            "- Confirm semantics only when the linked record recovers the same fact.\n"
            "- Confirm evidence only when exact page text supports that recovery."
        ),
        "items": items,
        "questions": [
            {
                "id": "semantic_recovery_confirmed",
                "label": "Does this linked model record recover the known omitted fact?",
                "type": "yesno",
            },
            {
                "id": "recovery_evidence_confirmed",
                "label": "Does exact page evidence support this semantic recovery?",
                "type": "yesno",
            },
        ],
    }


def _batch(values: list[Any], batch_size: int) -> list[list[Any]]:
    _require(batch_size > 0, "batch size must be positive")
    return [values[start : start + batch_size] for start in range(0, len(values), batch_size)]


def _sample_metadata(kind: str, route: str, value: dict[str, Any]) -> dict[str, Any]:
    if kind == "precision":
        page = value["page"]
        model = value["model_record"]
        result = {
            "item_id": value["record_id"],
            "parent_document_id": page["parent_document_id"],
            "route": route,
            "kind": kind,
            "subtype": value["subtype"],
            "entity_type": model.get("entity_type"),
            "endorser_category": model.get("endorser_category"),
            "support_mode": model.get("support_mode"),
            "context_scope": model.get(
                "endorsement_context_evidence_source_scope"
            ),
            "title_nonnull": bool(model.get("title")),
            "affiliation_nonnull": bool(model.get("affiliation")),
            "jurisdiction_nonnull": bool(model.get("jurisdiction")),
        }
        if route == "issues":
            result.update(_issue_precision_strata(value))
        return result
    return {
        "item_id": value["parent_document_id"],
        "parent_document_id": value["parent_document_id"],
        "route": route,
        "kind": kind,
        "subtype": "complete_page",
        "zero_result": not value["records"],
        "multi_window": value["segment_count"] > 1,
        "model_record_count": len(value["records"]),
    }


def build_task_payloads(
    precision: Mapping[str, list[dict[str, Any]]],
    recall: Mapping[str, list[dict[str, Any]]],
    bundle_hash: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    known_omissions: Iterable[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    run_key = bundle_hash[:12]
    for kind, selection in (("precision", precision), ("recall", recall)):
        for route in ROUTES:
            values = selection[route]
            for batch_index, batch_values in enumerate(_batch(values, batch_size), start=1):
                task_id = (
                    f"candidate_extraction_v2_{kind}_{route}_{run_key}_b{batch_index:03d}"
                )
                payload = (
                    precision_task(route, task_id, batch_values)
                    if kind == "precision"
                    else recall_task(route, task_id, batch_values)
                )
                tasks.append({
                    "task_id": task_id,
                    "kind": kind,
                    "route": route,
                    "batch_index": batch_index,
                    "payload": payload,
                    "sample_items": [
                        _sample_metadata(kind, route, value) for value in batch_values
                    ],
                })
    pairs = known_omission_pairs(known_omissions)
    for batch_index, batch_pairs in enumerate(_batch(pairs, batch_size), start=1):
        task_id = (
            f"candidate_extraction_v2_known_omissions_{run_key}_b{batch_index:03d}"
        )
        payload = known_omission_task(task_id, batch_pairs)
        tasks.append({
            "task_id": task_id,
            "kind": "known_omission",
            "route": "all",
            "batch_index": batch_index,
            "payload": payload,
            "sample_items": [
                {
                    "item_id": pair["item_id"],
                    "parent_document_id": pair["omission"]["parent_document_id"],
                    "route": pair["omission"]["route"],
                    "kind": "known_omission",
                    "subtype": pair["record"]["subtype"],
                    "omission_id": pair["omission"]["omission_id"],
                    "record_id": pair["record"]["record_id"],
                    "matched_record_ids": pair["omission"]["matched_record_ids"],
                }
                for pair in batch_pairs
            ],
        })
    return tasks


def build_task_html(task_json: Path, output_html: Path, builder: Path = HANDCODE_BUILDER) -> None:
    _require(builder.is_file(), f"shared handcode builder is absent: {builder}")
    completed = subprocess.run(
        [sys.executable, str(builder), str(task_json), str(output_html)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        raise HandcodeInputError(
            f"shared handcode builder failed for {task_json.name}: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )
    _require(output_html.is_file(), f"shared builder did not create {output_html}")
    # The app must carry the whole task, not a truncated copy. The task file is
    # indented and the app embeds the same data compactly, so the app can be the
    # smaller file. Compare against the compact form the app actually holds.
    compact = json.dumps(
        json.loads(task_json.read_text(encoding="utf-8")),
        separators=(",", ":"),
        ensure_ascii=False,
    )
    _require(
        output_html.stat().st_size > len(compact.encode("utf-8")),
        f"{output_html} is too small for its task data",
    )


def _selection_summary(
    precision: Mapping[str, list[dict[str, Any]]],
    recall: Mapping[str, list[dict[str, Any]]],
    known_omissions: Iterable[dict[str, Any]] = (),
    pool_observed_levels: Mapping[str, Mapping[str, list[str]]] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "precision": {},
        "recall": {},
        "pool_observed_levels": dict(pool_observed_levels or {}),
    }
    for route in ROUTES:
        records = precision[route]
        result["precision"][route] = {
            "records": len(records),
            "subtypes": dict(sorted(Counter(row["subtype"] for row in records).items())),
        }
        if route == "issues":
            result["precision"][route]["strata"] = {
                "year": dict(sorted(Counter(
                    str(row["page"].get("year") or "missing") for row in records
                ).items())),
                "party": dict(sorted(Counter(
                    str(row["page"].get("party") or "missing").casefold()
                    for row in records
                ).items())),
                "page_length": dict(sorted(Counter(
                    _page_length_bin(len(row["page"]["source_text"])) for row in records
                ).items())),
                "segmentation": dict(sorted(Counter(
                    "multi" if row["page"]["segment_count"] > 1 else "single"
                    for row in records
                ).items())),
                "heading": dict(sorted(Counter(
                    "present" if row["model_record"].get("source_heading") else "absent"
                    for row in records
                ).items())),
            }
        if route == "endorsements":
            result["precision"][route].update({
                "entity_types": dict(sorted(Counter(
                    row["model_record"]["entity_type"] for row in records
                ).items())),
                "support_modes": dict(sorted(Counter(
                    row["model_record"]["support_mode"] for row in records
                ).items())),
                "context_scopes": dict(sorted(Counter(
                    row["model_record"][
                        "endorsement_context_evidence_source_scope"
                    ]
                    for row in records
                ).items())),
                "nonnull_optional_fields": {
                    field: sum(bool(row["model_record"].get(field)) for row in records)
                    for field in ("title", "affiliation", "jurisdiction")
                },
            })
        pages = recall[route]
        result["recall"][route] = {
            "pages": len(pages),
            "zero_result_pages": sum(not page["records"] for page in pages),
            "multi_window_pages": sum(page["segment_count"] > 1 for page in pages),
        }
    omissions = list(known_omissions)
    result["known_omission_confirmation"] = {
        "omissions": len(omissions),
        "record_links": sum(len(item["matches"]) for item in omissions),
        "routes": dict(sorted(Counter(item["route"] for item in omissions).items())),
    }
    return result


def build_assignment_batches(
    task_entries: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Group the 240 holdout items into four resumable assignments."""
    entries = list(task_entries)
    assignment_specs = (
        ("batch_001", "precision", "biography", "timed"),
        ("batch_002", "precision", "issues", "untimed"),
        ("batch_003", "precision", "endorsements", "untimed"),
        ("batch_004", "recall", None, "untimed"),
    )
    assignment_batches: list[dict[str, Any]] = []
    assigned_task_ids: set[str] = set()
    for assignment_id, kind, route, timing in assignment_specs:
        selected_tasks = [
            entry for entry in entries
            if entry.get("kind") == kind
            and (route is None or entry.get("route") == route)
        ]
        item_count = sum(int(entry["item_count"]) for entry in selected_tasks)
        _require(
            item_count == DEFAULT_BATCH_SIZE,
            f"{assignment_id} must contain exactly {DEFAULT_BATCH_SIZE} items",
        )
        task_ids = [str(entry["task_id"]) for entry in selected_tasks]
        _require(
            not (assigned_task_ids & set(task_ids)),
            f"{assignment_id} duplicates a holdout task",
        )
        assigned_task_ids.update(task_ids)
        assignment_batches.append({
            "assignment_batch_id": assignment_id,
            "kind": kind,
            "route": route,
            "item_count": item_count,
            "task_ids": task_ids,
            "timing_protocol": timing,
        })
    holdout_task_ids = {
        str(entry["task_id"]) for entry in entries
        if entry.get("kind") in {"precision", "recall"}
    }
    _require(
        assigned_task_ids == holdout_task_ids,
        "four assignment batches do not cover the complete holdout",
    )
    return assignment_batches


def write_handcode_package(
    output_dir: Path,
    tasks: list[dict[str, Any]],
    bundle_info: Mapping[str, Any],
    excluded_parent_ids: set[str],
    precision: Mapping[str, list[dict[str, Any]]],
    recall: Mapping[str, list[dict[str, Any]]],
    known_omissions: list[dict[str, Any]],
    pool_observed_levels: Mapping[str, Mapping[str, list[str]]],
    seed: int,
    batch_size: int,
    builder: Path = HANDCODE_BUILDER,
    build_html: bool = True,
) -> dict[str, Any]:
    if output_dir.exists():
        _require(not any(output_dir.iterdir()), f"output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    task_entries: list[dict[str, Any]] = []
    for task in tasks:
        task_id = task["task_id"]
        task_path = output_dir / f"{task_id}.json"
        html_path = output_dir / f"{task_id}.html"
        task_path.write_text(
            json.dumps(task["payload"], indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        entry = {
            "task_id": task_id,
            "kind": task["kind"],
            "route": task["route"],
            "batch_index": task["batch_index"],
            "item_count": len(task["payload"]["items"]),
            "task_json": {"file": task_path.name, "sha256": sha256_file(task_path)},
            "expected_results_file": f"{task_id}_results.json",
            "sample_items": task["sample_items"],
        }
        if build_html:
            build_task_html(task_path, html_path, builder)
            entry["html"] = {
                "file": html_path.name,
                "bytes": html_path.stat().st_size,
                "sha256": sha256_file(html_path),
            }
        task_entries.append(entry)

    sampled_rows: dict[tuple[str, str], set[str]] = defaultdict(set)
    for task in task_entries:
        for item in task["sample_items"]:
            sampled_rows[
                (str(item["parent_document_id"]), str(item["route"]))
            ].add(str(item["kind"]))
    sampled_path = output_dir / "sampled_parent_ids.csv"
    with sampled_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("parent_document_id", "route", "sample_kinds"),
        )
        writer.writeheader()
        for (parent_id, route), kinds in sorted(sampled_rows.items()):
            writer.writerow({
                "parent_document_id": parent_id,
                "route": route,
                "sample_kinds": "|".join(sorted(kinds)),
            })

    excluded_sorted = sorted(excluded_parent_ids)
    assignment_batches = build_assignment_batches(task_entries)
    manifest = {
        "format": "candidate-extraction-handcode-v2",
        "schema_version": "2.1",
        "seed": seed,
        "batch_size": batch_size,
        "bundle_manifest_sha256": bundle_info["bundle_manifest_sha256"],
        "segments_file": bundle_info["segments_file"],
        "segments_file_sha256": bundle_info["segments_file_sha256"],
        "source_corpus_sha256": bundle_info["bundle_manifest"].get("source_corpus_sha256"),
        "extraction_contract_sha256": bundle_info["bundle_manifest"].get(
            "extraction_contract_sha256"
        ),
        "endorsement_context_provenance_spec_sha256": bundle_info[
            "bundle_manifest"
        ].get("endorsement_context_provenance_spec_sha256"),
        # The manifest must name the architecture that produced these records.
        # A batch drawn from a round-four bundle carries the round-four contract,
        # never the line-ledger one.
        "endorsement_architecture": bundle_info["bundle_manifest"].get(
            "endorsement_round4_record_ledger"
        ) is not None and "endorsement_round4_record_ledger_v1"
        or "endorsement_parent_line_ledger_v1",
        "endorsement_line_ledger_contract_sha256": (
            None
            if bundle_info["bundle_manifest"].get("endorsement_round4_record_ledger")
            is not None
            else get_line_ledger_contract()["line_ledger_contract_sha256"]
        ),
        "endorsement_round4_production_contract_sha256": (
            (
                bundle_info["bundle_manifest"]["endorsement_round4_record_ledger"]
                ["summary"]["round4_production_contract_sha256"]
            )
            if bundle_info["bundle_manifest"].get("endorsement_round4_record_ledger")
            is not None
            else None
        ),
        "user_message_template_id": bundle_info["bundle_manifest"].get(
            "user_message_template_id"
        ),
        "user_message_template_sha256": bundle_info[
            "bundle_manifest"
        ].get("user_message_template_sha256"),
        "generation_config_sha256": bundle_info["bundle_manifest"].get(
            "generation_config_sha256"
        ),
        "array_item_cap_spec_id": bundle_info["bundle_manifest"].get(
            "array_item_cap_spec_id"
        ),
        "array_item_cap_spec_sha256": bundle_info["bundle_manifest"].get(
            "array_item_cap_spec_sha256"
        ),
        "route_array_item_caps": bundle_info["bundle_manifest"].get(
            "route_array_item_caps"
        ),
        "model_item_repetition_spec_id": bundle_info["bundle_manifest"].get(
            "model_item_repetition_spec_id"
        ),
        "model_item_repetition_spec_sha256": bundle_info[
            "bundle_manifest"
        ].get("model_item_repetition_spec_sha256"),
        "array_item_cap_reached_outputs": bundle_info["technical_checks"][
            "array_item_cap_reached_outputs"
        ],
        "model_item_repetition_error_outputs": bundle_info[
            "technical_checks"
        ]["model_item_repetition_error_outputs"],
        "pre_alignment_model_item_guard_gate_passed": bundle_info[
            "technical_checks"
        ]["pre_alignment_model_item_guard_gate_passed"],
        "structured_decoding_backend": bundle_info["bundle_manifest"].get(
            "structured_decoding_backend"
        ),
        "structured_decoding_disable_any_whitespace": bundle_info[
            "bundle_manifest"
        ].get("structured_decoding_disable_any_whitespace"),
        "raw_structural_whitespace_validator_id": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        ),
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
        "raw_structural_whitespace_gate_passed": bundle_info[
            "technical_checks"
        ]["raw_structural_whitespace_gate_passed"],
        "handcode_renderer": handcode_renderer_provenance(builder),
        "technical_checks": bundle_info["technical_checks"],
        "prior_ai_assisted_audit_role": "tuning_only",
        "excluded_parent_ids": {
            "count": len(excluded_sorted),
            "sha256": sha256_text(canonical_json(excluded_sorted)),
            "values": excluded_sorted,
        },
        "known_omission_recovery_file": bundle_info["known_omission_recovery_file"],
        "selection": _selection_summary(
            precision, recall, known_omissions, pool_observed_levels
        ),
        "assignment_batches": assignment_batches,
        "known_omission_tasks_separate_from_holdout": True,
        "sampled_parent_ids_file": {
            "file": sampled_path.name,
            "rows": len(sampled_rows),
            "sha256": sha256_file(sampled_path),
        },
        "tasks": task_entries,
    }
    manifest_path = output_dir / "handcode_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def build_handcode_package(
    bundle_dir: Path,
    output_dir: Path,
    excluded_parent_ids: set[str],
    *,
    seed: int = DEFAULT_SEED,
    batch_size: int = DEFAULT_BATCH_SIZE,
    builder: Path = HANDCODE_BUILDER,
    build_html: bool = True,
) -> dict[str, Any]:
    prepared = prepare_handcode_selection(
        bundle_dir,
        excluded_parent_ids,
        seed=seed,
        batch_size=batch_size,
    )
    return write_handcode_package(
        output_dir,
        prepared["tasks"],
        prepared["bundle_info"],
        excluded_parent_ids,
        prepared["precision"],
        prepared["recall"],
        prepared["known_omissions"],
        prepared["pool_observed_levels"],
        seed,
        batch_size,
        builder,
        build_html,
    )


def prepare_handcode_selection(
    bundle_dir: Path,
    excluded_parent_ids: set[str],
    *,
    seed: int = DEFAULT_SEED,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, Any]:
    """Replay the complete deterministic holdout selection without writing files."""
    _require(
        type(seed) is int and seed >= 0,
        "handcode seed must be a nonnegative integer",
    )
    _require(
        type(batch_size) is int and batch_size > 0,
        "handcode batch size must be a positive integer",
    )
    _require(bool(excluded_parent_ids), "at least one prior tuning parent ID is required")
    pages, bundle_info = load_frozen_bundle(bundle_dir)
    # A page holding a window that reached the output cap is ineligible. Part of
    # its text was never read, so a recall task on it would count a known gap as
    # a model miss. This is separate from the tuning exclusions, and the
    # manifest records it separately.
    cap_limited_parent_ids = {
        str(page["parent_document_id"])
        for page in pages
        if page.get("has_cap_limited_window")
    }
    sampling_excluded_ids = set(excluded_parent_ids) | cap_limited_parent_ids
    available_ids = {page["parent_document_id"] for page in pages}
    _require(
        bool(available_ids & excluded_parent_ids),
        "none of the supplied tuning parent IDs occur in the bundle",
    )
    pool_observed_levels = precision_pool_observed_levels(
        pages, sampling_excluded_ids
    )
    precision = select_precision_records(pages, sampling_excluded_ids, seed)
    precision_parent_ids = {
        record["page"]["parent_document_id"]
        for route_records in precision.values()
        for record in route_records
    }
    recall = select_recall_pages(
        pages, sampling_excluded_ids | precision_parent_ids, seed
    )
    sampled_parent_ids = {
        item["page"]["parent_document_id"]
        for route_records in precision.values()
        for item in route_records
    } | {
        page["parent_document_id"]
        for route_pages in recall.values()
        for page in route_pages
    }
    _require(not (sampled_parent_ids & excluded_parent_ids), "sample includes a tuning parent")
    tasks = build_task_payloads(
        precision,
        recall,
        bundle_info["bundle_manifest_sha256"],
        batch_size,
        bundle_info["known_omissions"],
    )
    return {
        "bundle_info": bundle_info,
        "precision": precision,
        "recall": recall,
        "known_omissions": bundle_info["known_omissions"],
        "pool_observed_levels": pool_observed_levels,
        "selection": _selection_summary(
            precision,
            recall,
            bundle_info["known_omissions"],
            pool_observed_levels,
        ),
        "tasks": tasks,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--exclude-parent-file",
        type=Path,
        action="append",
        default=[],
        help="CSV, JSON, or text file with prior tuning parent IDs. Repeat as needed.",
    )
    parser.add_argument("--exclude-parent-id", action="append", default=[])
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--task-builder", type=Path, default=HANDCODE_BUILDER)
    parser.add_argument(
        "--skip-html",
        action="store_true",
        help="Write task JSON only. This is for tests and inspection, not handcoding.",
    )
    args = parser.parse_args(argv)
    excluded = load_excluded_parent_ids(args.exclude_parent_file, args.exclude_parent_id)
    manifest = build_handcode_package(
        args.bundle_dir,
        args.output_dir,
        excluded,
        seed=args.seed,
        batch_size=args.batch_size,
        builder=args.task_builder,
        build_html=not args.skip_html,
    )
    print(canonical_json({
        "output_dir": str(args.output_dir),
        "tasks": len(manifest["tasks"]),
        "items": sum(task["item_count"] for task in manifest["tasks"]),
        "selection": manifest["selection"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
