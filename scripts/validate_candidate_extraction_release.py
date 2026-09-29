#!/usr/bin/env python3
"""Validate the six candidate-extraction files against frozen v2 inputs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.build_candidate_extraction_release import (
    EXTRACTION_STATUSES,
    PUBLIC_FILES,
    SOURCE_OBSERVABILITY,
    STATUS_COLUMNS,
    ReleaseInputError,
    extension_manifest_entries,
    prepare_release_tables,
)
from scripts.hive.extract_candidate_pages_vllm import (
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
)
from scripts.import_candidate_extraction_v2 import (
    CONTRACT as V2_CONTRACT,
    EXPECTED_GENERATION_CONFIG_SHA256,
    _typed_equal,
)
from src.candidate_endorsement_line_ledger import get_line_ledger_contract
from src.candidate_endorsement_round4_production import (
    get_round4_production_contract,
)
from src.candidate_endorsement_round4_release import (
    ENDORSEMENT_ARCHITECTURE as ROUND4_ENDORSEMENT_ARCHITECTURE,
)

LINE_LEDGER_ENDORSEMENT_ARCHITECTURE = "endorsement_parent_line_ledger_v1"
ENDORSEMENT_ROUTE_CONTRACTS = {
    LINE_LEDGER_ENDORSEMENT_ARCHITECTURE: (
        get_line_ledger_contract, "line_ledger_contract_sha256",
    ),
    ROUND4_ENDORSEMENT_ARCHITECTURE: (
        get_round4_production_contract, "round4_production_contract_sha256",
    ),
}


def _endorsement_route_contract(architecture: str) -> dict:
    """Return the frozen contract of one endorsement architecture."""
    getter, digest_key = ENDORSEMENT_ROUTE_CONTRACTS[architecture]
    contract = getter()
    return {**contract, "route_contract_sha256": contract[digest_key]}


def _require(condition: Any, message: str) -> None:
    if not bool(condition):
        raise AssertionError(message)


def _validate_manifest(manifest_path: Path, release_dir: Path) -> None:
    _require(manifest_path.is_file(), f"release manifest is absent: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    _require(isinstance(payload, list), "release manifest must contain a list")
    _require(
        len(payload) == len(PUBLIC_FILES)
        and all(isinstance(row, dict) for row in payload),
        "release manifest inventory is not exactly the six extension files",
    )
    names = [row.get("file") for row in payload if isinstance(row, dict)]
    _require(len(names) == len(set(names)), "release manifest contains duplicate files")
    by_name = {str(row["file"]): row for row in payload if isinstance(row, dict) and "file" in row}
    _require(
        set(by_name) == set(PUBLIC_FILES),
        "release manifest inventory differs from the six extension files",
    )
    actual = {row["file"]: row for row in extension_manifest_entries(release_dir)}
    for name in PUBLIC_FILES:
        recorded = by_name[name]
        _require(
            set(recorded) == {"file", "rows", "columns", "bytes", "sha256"},
            f"manifest fields differ for {name}",
        )
        for field in ("rows", "columns", "bytes"):
            _require(
                type(recorded.get(field)) is int
                and recorded[field] == actual[name][field],
                f"manifest {field} differs for {name}",
            )
        _require(recorded.get("sha256") == actual[name]["sha256"], f"manifest hash differs for {name}")


def _validate_status(frame: pd.DataFrame, expected: pd.DataFrame) -> None:
    _require(tuple(frame.columns) == STATUS_COLUMNS, "status table columns differ")
    _require(frame[["candidate_cycle_id", "route"]].notna().all().all(), "status key has missing values")
    _require(not frame.duplicated(["candidate_cycle_id", "route"]).any(), "status key is not unique")
    _require(set(frame["source_observability"]) <= SOURCE_OBSERVABILITY, "status table has invalid source observability")
    _require(set(frame["extraction_status"]) <= EXTRACTION_STATUSES, "status table has invalid extraction status")
    numeric = [
        "n_raw_pages", "n_documents", "n_segments", "n_records",
        "n_withheld_overlap_conflict_groups",
    ]
    for column in numeric:
        values = pd.to_numeric(frame[column], errors="coerce")
        _require(values.notna().all() and values.ge(0).all(), f"status {column} is invalid")
        _require(values.mod(1).eq(0).all(), f"status {column} is not integral")
    left = frame.astype({column: "int64" for column in numeric}).reset_index(drop=True)
    right = expected.astype({column: "int64" for column in numeric}).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(left, right, check_dtype=False)
    except AssertionError as error:
        raise AssertionError(
            "status table differs from the frozen bundle and core corpus"
        ) from error


def _validate_arrow_table(
    path: Path,
    expected: pa.Table,
    *,
    two_stage_endorsements: bool = False,
    endorsement_architecture: str = LINE_LEDGER_ENDORSEMENT_ARCHITECTURE,
) -> None:
    _require(path.is_file(), f"release file is absent: {path.name}")
    actual = pq.read_table(path)
    _require(actual.schema.equals(expected.schema, check_metadata=False), f"{path.name} schema differs")
    _require(actual.equals(expected, check_metadata=False), f"{path.name} values differ from the frozen bundle")
    forbidden = {
        "source_text",
        "segment_text",
        "raw_output",
        "raw_output_sha256",
        "raw_output_n_chars",
        "raw_output_utf8_n_bytes",
        "canonical_output_sha256",
        "canonical_output_n_chars",
        "canonical_output_utf8_n_bytes",
        "inference_display_source_text_sha256",
        "inference_display_source_n_chars",
        "inference_display_text_sha256",
        "inference_display_chars_used",
        "inference_display_escape_count",
        "inference_display_escape_codepoint_counts",
        "inference_display_interior_boundary_count",
        "inference_display_boundary_map_sha256",
        "inference_display_roundtrip_valid",
        "json_control_canonicalization_codepoint_counts",
        "json_control_unrepaired_after_escape_count",
        "json_control_unrepaired_after_escape_codepoint_counts",
        "json_control_outside_string_permitted_whitespace_count",
        "json_control_outside_string_permitted_whitespace_codepoint_counts",
        "json_control_outside_string_invalid_count",
        "json_control_outside_string_invalid_codepoint_counts",
        "raw_structural_whitespace_validator_id",
        "raw_structural_whitespace_validator_spec_sha256",
        "raw_structural_whitespace_validation_passed",
        "raw_structural_separator_count",
        "raw_structural_separator_space_count",
        "raw_structural_whitespace_violation_count",
        "raw_structural_whitespace_violation_counts",
        "user_message_sha256",
        "model_extraction",
        "model_output",
        "stage_validation",
        "stage_alignment",
        "input_batch",
        "line_decisions",
        "target_blocks",
        "withheld_records",
        "carrier_alignment_report",
        "carrier_withheld",
        "model_extraction_sha256",
        "pre_alignment_model_item_guard",
        "array_item_cap_report",
        "model_item_repetition_report",
        "array_item_cap_spec_id",
        "array_item_cap_spec_sha256",
        "route_array_item_cap",
        "route_array_item_caps",
        "model_item_repetition_spec_id",
        "model_item_repetition_spec_sha256",
        "aligned_extraction_sha256",
        "withheld_model_items",
        "exact_alignment",
        "display_anchor_adjustment_count",
        "display_anchor_adjustments",
        "endorsement_context_extension_count",
        "endorsement_context_extensions",
        "model_excerpt",
        "model_context_excerpt",
        "candidate_name",
        "organization",
        "family",
        "other",
    }
    _require(not (forbidden & set(actual.column_names)), f"{path.name} exposes a forbidden field")
    if path.name in {
        "candidate_extraction_documents.parquet",
        "candidate_extraction_segments.parquet",
    }:
        _validate_public_route_provenance(
            actual.to_pylist(),
            table_name=path.name,
            two_stage_endorsements=two_stage_endorsements,
            endorsement_architecture=endorsement_architecture,
        )
    if path.name == "candidate_extraction_segments.parquet":
        _validate_public_segment_boundaries(actual.to_pylist())


def _validate_public_route_provenance(
    rows: list[dict[str, Any]],
    *,
    table_name: str,
    two_stage_endorsements: bool,
    endorsement_architecture: str = LINE_LEDGER_ENDORSEMENT_ARCHITECTURE,
) -> None:
    """Reject singular one-stage identities on two-stage endorsement rows."""
    ledger_contract = _endorsement_route_contract(endorsement_architecture)
    singular_fields = (
        "prompt_sha256",
        "json_schema_sha256",
        "generation_config_sha256",
        "alignment_id",
        "alignment_spec_sha256",
        "user_message_template_id",
        "user_message_template_sha256",
    )
    for row in rows:
        route = row.get("route")
        row_id = str(
            row.get("segment_id") or row.get("parent_document_id") or "row"
        )
        _require(
            route in {"issues", "biography", "endorsements"},
            f"{row_id} has an invalid route",
        )
        if route == "endorsements" and two_stage_endorsements:
            _require(
                all(row.get(field) is None for field in singular_fields),
                f"{row_id} exposes superseded singular endorsement provenance",
            )
            _require(
                row.get("extraction_contract_sha256")
                == ledger_contract["route_contract_sha256"],
                f"{row_id} does not bind the two-stage endorsement contract",
            )
            for field in ("model_id", "model_revision", "runtime_lock_sha256"):
                _require(
                    row.get(field) == ledger_contract[field],
                    f"{row_id} shared model/runtime provenance differs",
                )
            if table_name == "candidate_extraction_segments.parquet":
                _require(
                    all(
                        row.get(field) is None
                        for field in (
                            "output_tokens",
                            "max_output_tokens",
                            "json_control_canonicalization_applied",
                            "json_control_canonicalization_count",
                        )
                    ),
                    f"{row_id} exposes superseded singular output provenance",
                )
            continue

        exact = {
            "prompt_sha256": V2_CONTRACT["prompt_sha256"][route],
            "json_schema_sha256": V2_CONTRACT["json_schema_sha256"][route],
            "extraction_contract_sha256": V2_CONTRACT[
                "extraction_contract_sha256"
            ],
            "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
            "alignment_id": V2_CONTRACT["alignment_id"],
            "alignment_spec_sha256": V2_CONTRACT[
                "alignment_spec_sha256"
            ],
            "user_message_template_id": V2_CONTRACT[
                "user_message_template_id"
            ],
            "user_message_template_sha256": V2_CONTRACT[
                "user_message_template_sha256"
            ],
            "model_id": V2_CONTRACT["model_id"],
            "model_revision": V2_CONTRACT["model_revision"],
            "runtime_lock_sha256": V2_CONTRACT["runtime_lock_sha256"],
        }
        _require(
            all(_typed_equal(row.get(field), value) for field, value in exact.items()),
            f"{row_id} one-stage route provenance differs",
        )
        if table_name == "candidate_extraction_segments.parquet":
            _require(
                type(row.get("output_tokens")) is int
                and row["output_tokens"] > 0
                and type(row.get("max_output_tokens")) is int
                and row["max_output_tokens"] > 0
                and row["output_tokens"] <= row["max_output_tokens"]
                and type(row.get("json_control_canonicalization_applied"))
                is bool
                and type(row.get("json_control_canonicalization_count")) is int
                and row["json_control_canonicalization_count"] >= 0,
                f"{row_id} one-stage output provenance is null or invalid",
            )


def _validate_public_segment_boundaries(rows: list[dict[str, Any]]) -> None:
    """Validate text-free complete-line provenance in the public segment table."""
    boolean_fields = (
        "segment_starts_mid_line",
        "segment_has_complete_line",
        "segment_ends_mid_line",
    )
    bound_fields = (
        "segment_first_complete_line_start_char",
        "segment_first_complete_line_parent_start_char",
        "segment_first_complete_line_inference_display_start_char",
        "segment_last_complete_line_end_char",
        "segment_last_complete_line_parent_end_char",
        "segment_last_complete_line_inference_display_end_char",
    )
    for row in rows:
        segment_id = str(row.get("segment_id") or "segment")
        if row.get("route") != "endorsements":
            _require(
                all(row.get(field) is None for field in (*boolean_fields, *bound_fields)),
                f"{segment_id} exposes endorsement boundary provenance on another route",
            )
            continue
        _require(
            all(type(row.get(field)) is bool for field in boolean_fields),
            f"{segment_id} has invalid complete-line boundary flags",
        )
        has_complete = row["segment_has_complete_line"]
        if not has_complete:
            _require(
                all(row.get(field) is None for field in bound_fields),
                f"{segment_id} has complete-line bounds while has_complete_line is false",
            )
            continue
        _require(
            all(
                isinstance(row.get(field), int)
                and not isinstance(row.get(field), bool)
                and int(row[field]) >= 0
                for field in bound_fields
            ),
            f"{segment_id} has null or invalid complete-line bounds",
        )
        start = int(row["segment_start_char"])
        end = int(row["segment_end_char"])
        first_local = int(row["segment_first_complete_line_start_char"])
        last_local = int(row["segment_last_complete_line_end_char"])
        first_parent = int(
            row["segment_first_complete_line_parent_start_char"]
        )
        last_parent = int(row["segment_last_complete_line_parent_end_char"])
        first_display = int(
            row[
                "segment_first_complete_line_inference_display_start_char"
            ]
        )
        last_display = int(
            row["segment_last_complete_line_inference_display_end_char"]
        )
        _require(
            0 <= first_local < last_local <= end - start
            and first_parent == start + first_local
            and last_parent == start + last_local
            and first_display < last_display,
            f"{segment_id} complete-line bounds do not reconcile",
        )
        if row["segment_starts_mid_line"] is False:
            _require(first_local == 0, f"{segment_id} complete-line start differs")
        if row["segment_ends_mid_line"] is False:
            _require(
                last_local == end - start,
                f"{segment_id} complete-line end differs",
            )


def validate_candidate_extraction_release(
    release_dir: Path,
    core_release_dir: Path,
    bundle_dir: Path,
    manifest_path: Path,
    *,
    production: bool = True,
    model_coded_validation: Path | None = None,
) -> dict[str, Any]:
    _require(release_dir.is_dir(), f"release directory is absent: {release_dir}")
    allowed_inventory = set(PUBLIC_FILES)
    if manifest_path.parent.resolve() == release_dir.resolve():
        allowed_inventory.add(manifest_path.name)
    disk_inventory = {path.name for path in release_dir.iterdir()}
    _require(
        disk_inventory == allowed_inventory,
        "release directory inventory differs from the six extension files"
        + (" plus its manifest" if manifest_path.name in allowed_inventory else ""),
    )
    missing = [name for name in PUBLIC_FILES if not (release_dir / name).is_file()]
    _require(not missing, f"release files are absent: {missing}")
    expected, build_summary = prepare_release_tables(
        core_release_dir,
        bundle_dir,
        production=production,
        model_coded_validation=model_coded_validation,
    )
    status = pd.read_csv(
        release_dir / "candidate_extraction_status.csv",
        dtype={"candidate_cycle_id": "string", "route": "string"},
        low_memory=False,
    )
    _validate_status(status, expected["candidate_extraction_status.csv"])
    for name in PUBLIC_FILES:
        if name.endswith(".parquet"):
            _validate_arrow_table(
                release_dir / name,
                expected[name],
                two_stage_endorsements=production,
                endorsement_architecture=(
                    build_summary.get("endorsement_architecture")
                    or LINE_LEDGER_ENDORSEMENT_ARCHITECTURE
                ),
            )
    _validate_manifest(manifest_path, release_dir)
    _require(
        build_summary.get("raw_structural_whitespace_validator_id")
        == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        and build_summary.get(
            "raw_structural_whitespace_validator_spec_sha256"
        )
        == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        and build_summary.get("raw_structural_whitespace_gate_passed") is True,
        "raw structural-whitespace release gate did not pass",
    )
    if production:
        architecture = build_summary.get("endorsement_architecture")
        _require(
            architecture in ENDORSEMENT_ROUTE_CONTRACTS,
            "release names an unknown endorsement architecture",
        )
        _require(
            build_summary.get("endorsement_route_contract_sha256")
            == _endorsement_route_contract(architecture)[
                "route_contract_sha256"
            ],
            "endorsement route contract differs",
        )
    _require(
        build_summary.get("array_item_cap_spec_id")
        == V2_CONTRACT["array_item_cap_spec_id"]
        and build_summary.get("array_item_cap_spec_sha256")
        == V2_CONTRACT["array_item_cap_spec_sha256"]
        and _typed_equal(
            build_summary.get("route_array_item_caps"),
            V2_CONTRACT["route_array_item_caps"],
        )
        and build_summary.get("model_item_repetition_spec_id")
        == V2_CONTRACT["model_item_repetition_spec_id"]
        and build_summary.get("model_item_repetition_spec_sha256")
        == V2_CONTRACT["model_item_repetition_spec_sha256"]
        and type(build_summary.get("array_item_cap_reached_outputs")) is int
        and build_summary["array_item_cap_reached_outputs"] == 0
        and type(build_summary.get("model_item_repetition_error_outputs"))
        is int
        and build_summary["model_item_repetition_error_outputs"] == 0
        and build_summary.get("pre_alignment_model_item_guard_gate_passed")
        is True,
        "pre-alignment model-item release gate did not pass",
    )
    statuses = set(status["extraction_status"].astype(str))
    _require("failed" not in statuses, "release contains failed routes")
    _require(build_summary["release_ready"] is True, "candidate extraction is not release-ready")
    return {
        "schema_version": "2.1",
        "validated_files": list(PUBLIC_FILES),
        "rows": {name: int(len(expected[name])) for name in PUBLIC_FILES},
        "source_corpus_sha256": build_summary["source_corpus_sha256"],
        "generation_config_sha256": build_summary[
            "generation_config_sha256"
        ],
        "endorsement_architecture": build_summary["endorsement_architecture"],
        "endorsement_route_contract_sha256": build_summary[
            "endorsement_route_contract_sha256"
        ],
        "array_item_cap_spec_id": build_summary["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": build_summary[
            "array_item_cap_spec_sha256"
        ],
        "route_array_item_caps": build_summary["route_array_item_caps"],
        "model_item_repetition_spec_id": build_summary[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": build_summary[
            "model_item_repetition_spec_sha256"
        ],
        "array_item_cap_reached_outputs": 0,
        "model_item_repetition_error_outputs": 0,
        "pre_alignment_model_item_guard_gate_passed": True,
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
        "raw_structural_whitespace_gate_passed": True,
        "route_validation_passed": build_summary["route_validation_passed"],
        "release_ready": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release_dir", type=Path)
    parser.add_argument("--core-release-dir", type=Path, required=True)
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--model-coded-validation",
        type=Path,
        help="Validate a release gated on this model-coded report.",
    )
    args = parser.parse_args(argv)
    try:
        summary = validate_candidate_extraction_release(
            args.release_dir,
            args.core_release_dir,
            args.bundle_dir,
            args.manifest,
            model_coded_validation=args.model_coded_validation,
        )
    except (AssertionError, ReleaseInputError) as error:
        raise SystemExit(f"candidate extraction validation failed: {error}") from error
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
