#!/usr/bin/env python3
"""Mechanically gate the adjudicated endorsement line-ledger pilot.

The evaluator accepts only the exact frozen suffix-b Stage-A run and complete,
restart-replayable Stage-A2 and Stage-B exports. It replays both classification
passes, derives the final ownership ledger, and then applies the eight frozen
endorsement controls plus the unchanged-route issue conflict control. It never
repairs or backfills a model output.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.build_candidate_endorsement_line_ledger import (  # noqa: E402
    CHAMPION_INTERFACE_LINE_TEXT,
    CORRECTED_ISSUE_AUDIT_GATE,
    DEFAULT_FULL_EXPORT_DIR,
    FROZEN_STAGE_A2_EXCEPTION_COUNT,
    LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR,
    PILOT_CONTENT_GATES,
    PILOT_PARENT_IDS,
    STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES,
    STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS,
    STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS,
    TANAKA_INTERFACE_LINE_TEXT,
    derive_recall40_stage_a2_exception_inventory,
    load_frozen_stage_a_suffix_b,
    recall40_post_a2_evaluation_report,
    replay_recall40_stage_a_and_a2,
    read_jsonl_gz,
    reconstruct_selected_parents,
    replay_frozen_source,
    sha256_file,
    validate_two_pass_stage_b_manifest_provenance,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (  # noqa: E402
    FROZEN_GENERATION_SEED,
    LINE_DECISION_STAGE,
    RECORD_EXTRACTION_STAGE,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    PROMPT_TOKEN_COUNT_SPEC_SHA256,
    generation_config_payload,
    line_ledger_chunk_is_complete,
    load_validated_stage_run,
    schema_variant_inventory,
    validate_input_manifest,
)
from src.candidate_endorsement_line_ledger import (  # noqa: E402
    ANCHOR_ROLES,
    CONTINUATION_ROLES,
    FROZEN_STAGE_A3_AUTHENTICATION,
    LINE_ADJUDICATION_STAGE,
    LINE_BATCH_SPEC_SHA256,
    LINE_INVENTORY_SPEC_SHA256,
    build_line_adjudication_batches,
    build_record_extraction_batches_from_adjudicated_merge,
    get_line_ledger_contract,
    get_frozen_stage_a2_v4_contract,
    line_adjudication_ledger,
    merge_line_adjudication_ledger,
    sha256_json,
)
from scripts.hive.extract_candidate_pages_vllm import (  # noqa: E402
    json_value_equal_typed,
)


PILOT_EVALUATION_VERSION = "candidate_endorsement_line_ledger_pilot_gate_v1"
FROZEN_STAGE_B_PILOT_MANIFEST_SHA256 = (
    "aef6d6263d51873854cfa7fb1e4a1d2be6a7c3bf5f1244d0be93d57931dec178"
)
FROZEN_ISSUE_CONTROL_BUNDLE_MANIFEST_SHA256 = (
    "5f92e2467cfc6ab4ccbf5d21318bdc480790cb55e86c8c44985bccb77d823d24"
)
FROZEN_ISSUE_CONTROL_SEGMENTS_SHA256 = (
    "38fb639935389b573ebb10cfbf63fcd2224f413b5c964da978070363efdda17b"
)
FROZEN_ISSUE_CONTROL_DOCUMENT_IDS = (
    "029290d53febc0f3c06a8e63fdce60ef6bdf722d5e7a991ceda3c980626f163f",
    "23f764b67dd8e0e7cb6e8d67432dce90f9abe63841f2772de5543b87cf47e729",
    "5fa65c8ef604e192c08138fd14334f4145feb557f26bb97ed6a30157dda2e436",
)


def _require_equal(actual: Any, expected: Any, label: str) -> None:
    if not json_value_equal_typed(actual, expected):
        raise ValueError(f"{label} differs: {actual!r} != {expected!r}")


def _metadata_paths(output_dir: Path) -> list[Path]:
    unsharded = output_dir / "run_metadata.json"
    sharded = sorted(output_dir.glob("run_metadata_shard_*.json"))
    if unsharded.is_file() and sharded:
        raise ValueError("run output mixes sharded and unsharded metadata")
    paths = [unsharded] if unsharded.is_file() else sharded
    if not paths:
        raise FileNotFoundError(f"run metadata is absent: {output_dir}")
    return paths


def load_validated_run(
    input_export_dir: Path,
    output_dir: Path,
    *,
    stage: str,
    contract_override: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Use the runner's single authenticated replay boundary."""
    if stage == LINE_DECISION_STAGE:
        manifest_path = input_export_dir / "manifest.json"
        if manifest_path.is_file():
            candidate = json.loads(manifest_path.read_text(encoding="utf-8"))
            if (
                isinstance(candidate, Mapping)
                and candidate.get("line_ledger_contract_sha256")
                != get_line_ledger_contract()["line_ledger_contract_sha256"]
            ):
                manifest, batches, rows, _decisions, artifacts = (
                    load_frozen_stage_a_suffix_b(
                        input_export_dir, output_dir
                    )
                )
                return manifest, batches, rows, artifacts
    return load_validated_stage_run(
        input_export_dir, output_dir, stage=stage,
        contract_override=(
            contract_override
            if contract_override is not None
            else (
                get_frozen_stage_a2_v4_contract()
                if stage == LINE_ADJUDICATION_STAGE else None
            )
        ),
    )


def _source_line_maps(
    stage_a_batches: Sequence[Mapping[str, Any]],
    stage_a_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    roles: dict[str, str] = {}
    lines: dict[str, dict[str, Any]] = {}
    for batch, row in zip(stage_a_batches, stage_a_rows):
        targets = {str(item["slot"]): item for item in batch["targets"]}
        for slot, target in targets.items():
            line_id = str(target["source_line_id"])
            if line_id in roles:
                raise ValueError("duplicate Stage-A line decision")
            roles[line_id] = str(row["model_output"][slot]["role"])
            lines[line_id] = dict(target)
    return roles, lines


def _field_observability_census(
    all_model_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    model_record_count = len(all_model_records)
    def _rate(count: int) -> float | None:
        return count / model_record_count if model_record_count else None
    unclear_entity_count = sum(
        record.get("entity_type") == "unclear" for record in all_model_records
    )
    unclear_category_count = sum(
        record.get("endorser_category") == "unclear"
        for record in all_model_records
    )
    optional_nonnull_counts = {
        field: sum(record.get(field) is not None for record in all_model_records)
        for field in ("title", "affiliation", "jurisdiction")
    }
    return {
        "gate_status": "NON_GATING_DIAGNOSTIC_ONLY",
        "model_record_count": model_record_count,
        "entity_type_unclear_count": unclear_entity_count,
        "entity_type_unclear_rate": _rate(unclear_entity_count),
        "endorser_category_unclear_count": unclear_category_count,
        "endorser_category_unclear_rate": _rate(unclear_category_count),
        "optional_field_nonnull_counts": optional_nonnull_counts,
        "optional_field_nonnull_rates": {
            field: _rate(count) for field, count in optional_nonnull_counts.items()
        },
        "validation_status": (
            "entity_type,endorser_category,title,affiliation,and_"
            "jurisdiction_remain_unvalidated_until_later_human_validation"
        ),
        "full_expansion_or_release_authorized_by_this_census": False,
    }


def collect_pilot_observations(
    stage_a_batches: Sequence[Mapping[str, Any]],
    stage_a_rows: Sequence[Mapping[str, Any]],
    stage_b_batches: Sequence[Mapping[str, Any]],
    stage_b_rows: Sequence[Mapping[str, Any]],
    *,
    final_decisions: Mapping[str, Mapping[str, Any]] | None = None,
    stage_a2_merge: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    first_pass_roles, source_lines = _source_line_maps(
        stage_a_batches, stage_a_rows
    )
    roles = (
        dict(first_pass_roles)
        if final_decisions is None
        else {
            str(line_id): str(decision["role"])
            for line_id, decision in final_decisions.items()
        }
    )
    if set(roles) != set(first_pass_roles):
        raise ValueError("final Stage-A2 decisions do not cover Stage-A lines")
    model_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    aligned_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    target_by_line: dict[str, dict[str, Any]] = {}
    continuation_uses: Counter[str] = Counter()
    continuation_anchor_by_line: dict[str, str] = {}
    stage_b_dispositions: dict[str, str] = {}
    parent_segment_bounds: dict[str, dict[str, tuple[int, int]]] = {}
    for batch, row in zip(stage_b_batches, stage_b_rows):
        parent_id = str(batch["parent_document_id"])
        parent_segment_bounds[parent_id] = {
            str(item["segment_id"]): (
                int(item["segment_start_char"]), int(item["segment_end_char"])
            )
            for item in batch["segment_descriptors"]
        }
        targets = {str(item["slot"]): item for item in batch["targets"]}
        for slot, target in targets.items():
            line_id = str(target["source_line_id"])
            target_by_line[line_id] = dict(target)
            decision = row["model_output"][slot]
            stage_b_dispositions[line_id] = "records"
            for block_line in target["record_block_lines"]:
                block_id = str(block_line["source_line_id"])
                if block_id != line_id:
                    continuation_uses[block_id] += 1
                    if (
                        block_id in continuation_anchor_by_line
                        and continuation_anchor_by_line[block_id] != line_id
                    ):
                        raise ValueError("one continuation line feeds two anchors")
                    continuation_anchor_by_line[block_id] = line_id
            for record in decision["records"]:
                model_by_parent[parent_id].append({
                    **dict(record),
                    "source_line_id": line_id,
                    "anchor_parent_start_char": int(target["parent_start_char"]),
                    "anchor_parent_end_char": int(target["parent_end_char"]),
                })
        alignment = row["stage_alignment"]
        if not isinstance(alignment, Mapping):
            raise ValueError("Stage-B output lacks alignment report")
        for record in alignment["aligned_records"]:
            aligned_by_parent[parent_id].append(dict(record))
    parent_observations: dict[str, Any] = {}
    for parent_id in PILOT_PARENT_IDS:
        model = model_by_parent.get(parent_id, [])
        aligned = aligned_by_parent.get(parent_id, [])
        segment_model_names: dict[str, list[str]] = {}
        segment_aligned_names: dict[str, list[str]] = {}
        for segment_id, (start, end) in parent_segment_bounds.get(parent_id, {}).items():
            segment_model_names[segment_id] = sorted({
                str(record["endorser_name"])
                for record in model
                if int(record["anchor_parent_start_char"]) >= start
                and int(record["anchor_parent_end_char"]) <= end
            })
            segment_aligned_names[segment_id] = sorted({
                str(record["endorser_name"])
                for record in aligned
                if (
                    (target := target_by_line.get(str(record["source_line_id"])))
                    is not None
                )
                and int(target["parent_start_char"]) >= start
                and int(target["parent_end_char"]) <= end
            })
        parent_observations[parent_id] = {
            "model_unique_names": sorted({str(row["endorser_name"]) for row in model}),
            "aligned_unique_names": sorted({
                str(row["endorser_name"]) for row in aligned
            }),
            "model_records": model,
            "aligned_records": aligned,
            "model_unique_names_by_source_segment": segment_model_names,
            "aligned_unique_names_by_source_segment": segment_aligned_names,
        }
    all_model_records = [
        record for records in model_by_parent.values() for record in records
    ]
    return {
        "stage_a_unresolved_line_count": sum(role == "unclear" for role in roles.values()),
        "stage_a_first_pass_roles": first_pass_roles,
        "stage_a_roles": roles,
        "stage_a2_candidate_self_exception_line_ids": sorted(
            line_id for line_id, role in first_pass_roles.items()
            if role == "candidate_self"
        ),
        "stage_a2_merge_validation": (
            dict(stage_a2_merge.get("validation", {}))
            if isinstance(stage_a2_merge, Mapping) else None
        ),
        "source_lines": source_lines,
        "stage_b_dispositions": stage_b_dispositions,
        "stage_b_continuation_use_counts": dict(continuation_uses),
        "stage_b_continuation_anchor_by_line": continuation_anchor_by_line,
        "stage_b_withheld_record_count": sum(
            int(row["stage_alignment"]["withheld_record_count"])
            for row in stage_b_rows
        ),
        "stage_b_public_coverage_complete": all(
            row["stage_alignment"]["public_coverage_complete"] is True
            for row in stage_b_rows
        ),
        "stage_b_field_observability_non_gating": (
            _field_observability_census(all_model_records)
        ),
        "parents": parent_observations,
    }


def _check(
    checks: list[dict[str, Any]], name: str, passed: bool,
    observed: Any, expected: Any,
) -> None:
    checks.append({
        "gate": name, "passed": bool(passed),
        "observed": observed, "expected": expected,
    })


def evaluate_pilot_observations(
    observations: Mapping[str, Any], issue_control: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    parents = observations["parents"]
    _check(
        checks, "all_stage_a_lines_decided_without_unclear",
        observations["stage_a_unresolved_line_count"] == 0,
        observations["stage_a_unresolved_line_count"], 0,
    )
    _check(
        checks, "all_stage_b_public_coverage_complete",
        observations["stage_b_public_coverage_complete"] is True
        and observations["stage_b_withheld_record_count"] == 0,
        {
            "coverage": observations["stage_b_public_coverage_complete"],
            "withheld": observations["stage_b_withheld_record_count"],
        },
        {"coverage": True, "withheld": 0},
    )

    tanaka_id, champion_id, quote_id, compound_id, testimonial_id, json_id, magaziner_id, vets_id = PILOT_PARENT_IDS
    tanaka = parents[tanaka_id]
    tanaka_names = set(tanaka["model_unique_names"])
    _check(checks, "tanaka_full_parent_unique", len(tanaka_names) >= 390, len(tanaka_names), ">=390")
    for segment_id, minimum in PILOT_CONTENT_GATES[tanaka_id]["required_segment_counts"].items():
        count = len(tanaka["model_unique_names_by_source_segment"].get(segment_id, []))
        _check(checks, f"tanaka_model_segment_{segment_id[:12]}", count >= minimum, count, f">={minimum}")
    aligned_aa0 = len(tanaka["aligned_unique_names_by_source_segment"].get(
        "aa0b99eb1331464dce28db32d73a6f53bddbf7e4aa10f46f4c17c48b12720433", []
    ))
    _check(checks, "tanaka_aa0_uniquely_aligned", aligned_aa0 >= 224, aligned_aa0, ">=224")
    aligned_2e9 = len(tanaka["aligned_unique_names_by_source_segment"].get(
        "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73", []
    ))
    _check(checks, "tanaka_2e9_uniquely_aligned", aligned_2e9 >= 84, aligned_2e9, ">=84")
    excluded_tanaka = sorted(tanaka_names & {"Greg Tanaka", "ena Song"})
    _check(checks, "tanaka_exclusions", not excluded_tanaka, excluded_tanaka, [])

    def interface_failures(
        parent: Mapping[str, Any], expected: Mapping[str, str]
    ) -> dict[str, Any]:
        model_records = parent["model_records"]
        aligned_records = parent["aligned_records"]
        return {
            "wrong_roles": {
                line_id: observations["stage_a_roles"].get(line_id)
                for line_id in expected
                if observations["stage_a_roles"].get(line_id)
                != "navigation_form_footer"
            },
            "stage_b_anchor_line_ids": sorted(
                line_id for line_id in expected
                if line_id in observations["stage_b_dispositions"]
            ),
            "model_record_line_ids": sorted({
                str(row["source_line_id"]) for row in model_records
                if str(row["source_line_id"]) in expected
            }),
            "aligned_record_line_ids": sorted({
                str(row["source_line_id"]) for row in aligned_records
                if str(row["source_line_id"]) in expected
            }),
            "forbidden_exact_evidence": sorted({
                str(row.get("record_evidence"))
                for row in [*model_records, *aligned_records]
                if str(row.get("record_evidence")) in set(expected.values())
            }),
        }

    tanaka_interface = interface_failures(tanaka, TANAKA_INTERFACE_LINE_TEXT)
    _check(
        checks, "tanaka_interface_exclusion",
        not any(tanaka_interface.values()), tanaka_interface,
        {
            "wrong_roles": {}, "stage_b_anchor_line_ids": [],
            "model_record_line_ids": [], "aligned_record_line_ids": [],
            "forbidden_exact_evidence": [],
        },
    )

    champion = parents[champion_id]
    champion_model = set(champion["model_unique_names"])
    champion_aligned = set(champion["aligned_unique_names"])
    _check(checks, "champion_model_unique", len(champion_model) >= 230, len(champion_model), ">=230")
    _check(checks, "champion_aligned_unique", len(champion_aligned) >= 222, len(champion_aligned), ">=222")
    _check(checks, "champion_clipped_exclusion", "cy Ruffeno" not in champion_model | champion_aligned, "cy Ruffeno" in champion_model | champion_aligned, False)
    champion_interface = interface_failures(
        champion, CHAMPION_INTERFACE_LINE_TEXT
    )
    _check(
        checks, "champion_interface_exclusion",
        not any(champion_interface.values()), champion_interface,
        {
            "wrong_roles": {}, "stage_b_anchor_line_ids": [],
            "model_record_line_ids": [], "aligned_record_line_ids": [],
            "forbidden_exact_evidence": [],
        },
    )

    quote_gate = PILOT_CONTENT_GATES[quote_id]
    quote_records = [
        row for row in parents[quote_id]["aligned_records"]
        if row.get("source_line_id") == quote_gate["required_source_line_id"]
        and row.get("record_evidence") == quote_gate["must_resolve_quote_bearing_record_evidence"]
    ]
    _check(checks, "quote_bearing_exact_record", bool(quote_records), len(quote_records), ">=1")

    compound_gate = PILOT_CONTENT_GATES[compound_id]
    compound_names = {
        str(row["endorser_name"])
        for row in parents[compound_id]["aligned_records"]
        if row.get("source_line_id") == compound_gate["required_compound_source_line_id"]
    }
    required_compound = set(compound_gate["exact_compound_names"])
    _check(checks, "compound_names_same_anchor", required_compound == compound_names, sorted(compound_names), sorted(required_compound))

    testimonial_gate = PILOT_CONTENT_GATES[testimonial_id]
    required_continuations = set(testimonial_gate[
        "required_continuation_source_line_ids"
    ])
    roles = observations["stage_a_roles"]
    use_counts = observations["stage_b_continuation_use_counts"]
    continuation_roles_ok = all(
        roles.get(line_id) == "record_continuation_previous"
        for line_id in required_continuations
    )
    continuation_uses_ok = all(use_counts.get(line_id) == 1 for line_id in required_continuations)
    observed_continuation_map = {
        line_id: observations["stage_b_continuation_anchor_by_line"].get(line_id)
        for line_id in sorted(required_continuations)
    }
    expected_continuation_map = dict(
        LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR
    )
    continuation_map_ok = observed_continuation_map == expected_continuation_map
    _check(
        checks, "long_testimonial_continuations",
        continuation_roles_ok and continuation_uses_ok and continuation_map_ok,
        {
            "role_failures": sorted(line_id for line_id in required_continuations if roles.get(line_id) != "record_continuation_previous"),
            "use_count_failures": {line_id: use_counts.get(line_id, 0) for line_id in sorted(required_continuations) if use_counts.get(line_id) != 1},
            "mapping_failures": {
                line_id: {
                    "observed": observed_continuation_map.get(line_id),
                    "expected": expected_continuation_map[line_id],
                }
                for line_id in sorted(required_continuations)
                if observed_continuation_map.get(line_id)
                != expected_continuation_map[line_id]
            },
        },
        {"role_failures": [], "use_count_failures": {}, "mapping_failures": {}},
    )
    testimonial_names_by_anchor: dict[str, set[str]] = defaultdict(set)
    aligned_testimonial = parents[testimonial_id]["aligned_records"]
    for record in aligned_testimonial:
        testimonial_names_by_anchor[str(record["source_line_id"])].add(
            str(record["endorser_name"])
        )
    continuation_anchor_ids = set(expected_continuation_map.values())
    name_sets = [
        tuple(sorted(testimonial_names_by_anchor[line_id]))
        for line_id in sorted(continuation_anchor_ids)
        if testimonial_names_by_anchor[line_id]
    ]
    _check(
        checks, "long_testimonial_speakers_distinct",
        len(name_sets) == len(continuation_anchor_ids)
        and len(name_sets) == len(set(name_sets)),
        {"resolved_anchor_count": len(name_sets), "unique_name_sets": len(set(name_sets))},
        "all resolved adjacent anchors have distinct nonempty name sets",
    )

    json_gate = PILOT_CONTENT_GATES[json_id]
    json_line_id = json_gate["required_source_line_id"]
    _check(
        checks, "long_json_stage_a_nonrecord",
        roles.get(json_line_id) == "nonrecord_content"
        and json_line_id not in observations["stage_b_dispositions"],
        {"role": roles.get(json_line_id), "stage_b": observations["stage_b_dispositions"].get(json_line_id)},
        {"role": "nonrecord_content", "stage_b": None},
    )

    magaziner = parents[magaziner_id]
    magaziner_records = [
        row for row in magaziner["aligned_records"]
        if row.get("endorsement_context_evidence_source_scope")
        == "page_or_section_route_context"
    ]
    magaziner_names = set(magaziner["model_unique_names"]) | set(
        magaziner["aligned_unique_names"]
    )
    _check(checks, "magaziner_url_context", bool(magaziner_records) and "Seth Magaziner" not in magaziner_names, {"records": len(magaziner_records), "candidate_self": "Seth Magaziner" in magaziner_names}, {"records": ">=1", "candidate_self": False})

    vets_gate = PILOT_CONTENT_GATES[vets_id]
    vets = parents[vets_id]
    vets_line_id = vets_gate["required_source_line_id"]
    vets_phrase = vets_gate["must_include_exact"][0]
    vets_records = [
        row for row in vets["aligned_records"]
        if row.get("source_line_id") == vets_line_id
        and vets_phrase in str(row.get("record_evidence", ""))
    ]
    _check(checks, "explicit_testimonial_count", len(vets["aligned_unique_names"]) >= 11, len(vets["aligned_unique_names"]), ">=11")
    _check(checks, "explicit_vets_record", bool(vets_records), len(vets_records), ">=1")

    issue_gate = CORRECTED_ISSUE_AUDIT_GATE
    _require_equal(issue_control.get("route"), "issues", "issue control route")
    _require_equal(issue_control.get("parent_document_id"), issue_gate["parent_document_id"], "issue control parent")
    resolved = issue_control.get("quote_bearing_resolved_issue_records")
    conflict_groups = issue_control.get("fully_accounted_private_conflict_groups")
    if not isinstance(conflict_groups, list):
        raise ValueError("issue control conflict groups must be a list")
    qualifying_groups = [
        group for group in conflict_groups
        if isinstance(group, Mapping)
        and group.get("disposition") == "conflict_withheld_private"
        and {"proposal", "oppose"} <= set(group.get("statement_kinds", []))
        and group.get("fully_accounted") is True
    ]
    _check(checks, "issue_quote_resolved", isinstance(resolved, int) and not isinstance(resolved, bool) and resolved >= 2, resolved, ">=2")
    _check(checks, "issue_private_conflict_accounted", len(qualifying_groups) >= 1, len(qualifying_groups), ">=1")
    return {
        "evaluation_version": PILOT_EVALUATION_VERSION,
        "checks": checks,
        "passed_checks": sum(check["passed"] for check in checks),
        "failed_checks": sum(not check["passed"] for check in checks),
        "gate_decision": "GO" if all(check["passed"] for check in checks) else "NO_GO",
    }


def evaluate_stage_a2_pilot_observations(
    observations: Mapping[str, Any], issue_control: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply mandatory adjudication/ownership gates before content gates."""
    checks: list[dict[str, Any]] = []
    validation = observations.get("stage_a2_merge_validation")
    if not isinstance(validation, Mapping):
        validation = {}
    _check(
        checks,
        "stage_a2_exception_coverage",
        validation.get("expected_exception_count")
        == FROZEN_STAGE_A2_EXCEPTION_COUNT
        and validation.get("adjudicated_exception_count")
        == FROZEN_STAGE_A2_EXCEPTION_COUNT
        and validation.get("coverage_complete") is True,
        {
            "expected": validation.get("expected_exception_count"),
            "adjudicated": validation.get("adjudicated_exception_count"),
            "coverage_complete": validation.get("coverage_complete"),
        },
        {
            "expected": FROZEN_STAGE_A2_EXCEPTION_COUNT,
            "adjudicated": FROZEN_STAGE_A2_EXCEPTION_COUNT,
            "coverage_complete": True,
        },
    )
    _check(
        checks,
        "stage_a2_zero_unresolved_or_withheld",
        validation.get("unresolved_line_count") == 0
        and validation.get("withheld_line_count") == 0,
        {
            "unresolved": validation.get("unresolved_line_count"),
            "withheld": validation.get("withheld_line_count"),
        },
        {"unresolved": 0, "withheld": 0},
    )
    _check(
        checks,
        "stage_a2_continuation_ownership_complete",
        validation.get("unowned_continuation_count") == 0
        and validation.get("duplicate_continuation_owner_count") == 0
        and validation.get("ownership_conflict_count") == 0,
        {
            "unowned": validation.get("unowned_continuation_count"),
            "duplicate_owner": validation.get(
                "duplicate_continuation_owner_count"
            ),
            "ownership_conflict": validation.get("ownership_conflict_count"),
        },
        {"unowned": 0, "duplicate_owner": 0, "ownership_conflict": 0},
    )
    _check(
        checks,
        "stage_a2_final_validation_passed",
        validation.get("validation_passed") is True,
        validation.get("validation_passed"),
        True,
    )

    roles = observations.get("stage_a_roles", {})
    candidate_expected = dict(STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES)
    candidate_line_ids = set(candidate_expected)
    stage_b_dispositions = observations.get("stage_b_dispositions", {})
    record_line_ids = {
        str(record.get("source_line_id"))
        for parent in observations.get("parents", {}).values()
        for record in [
            *parent.get("model_records", []),
            *parent.get("aligned_records", []),
        ]
    }
    candidate_failures = {
        "final_roles": {
            line_id: roles.get(line_id) for line_id in candidate_expected
        },
        "stage_b_anchor_line_ids": sorted(
            candidate_line_ids & set(stage_b_dispositions)
        ),
        "record_line_ids": sorted(candidate_line_ids & record_line_ids),
    }
    _check(
        checks,
        "stage_a2_candidate_controls_excluded",
        candidate_failures == {
            "final_roles": candidate_expected,
            "stage_b_anchor_line_ids": [],
            "record_line_ids": [],
        },
        candidate_failures,
        {
            "final_roles": candidate_expected,
            "stage_b_anchor_line_ids": [],
            "record_line_ids": [],
        },
    )

    content = evaluate_pilot_observations(observations, issue_control)
    all_checks = [*checks, *content["checks"]]
    return {
        "evaluation_version": (
            "candidate_endorsement_line_ledger_adjudicated_pilot_gate_v1"
        ),
        "checks": all_checks,
        "passed_checks": sum(check["passed"] for check in all_checks),
        "failed_checks": sum(not check["passed"] for check in all_checks),
        "gate_decision": (
            "GO" if all(check["passed"] for check in all_checks) else "NO_GO"
        ),
    }


def evaluate_stage_a2_only_observations(
    observations: Mapping[str, Any],
) -> dict[str, Any]:
    """Gate the approved A2-only run without requiring any Stage-B artifact."""
    checks: list[dict[str, Any]] = []
    validation = observations.get("stage_a2_merge_validation")
    if not isinstance(validation, Mapping):
        validation = {}
    _check(
        checks,
        "stage_a2_exception_coverage",
        validation.get("expected_exception_count")
        == FROZEN_STAGE_A2_EXCEPTION_COUNT
        and validation.get("adjudicated_exception_count")
        == FROZEN_STAGE_A2_EXCEPTION_COUNT
        and validation.get("coverage_complete") is True,
        {
            "expected": validation.get("expected_exception_count"),
            "adjudicated": validation.get("adjudicated_exception_count"),
            "coverage_complete": validation.get("coverage_complete"),
        },
        {
            "expected": FROZEN_STAGE_A2_EXCEPTION_COUNT,
            "adjudicated": FROZEN_STAGE_A2_EXCEPTION_COUNT,
            "coverage_complete": True,
        },
    )
    _check(
        checks,
        "stage_a2_zero_unresolved_or_withheld",
        validation.get("unresolved_line_count") == 0
        and validation.get("withheld_line_count") == 0,
        {
            "unresolved": validation.get("unresolved_line_count"),
            "withheld": validation.get("withheld_line_count"),
        },
        {"unresolved": 0, "withheld": 0},
    )
    _check(
        checks,
        "stage_a2_continuation_ownership_complete",
        validation.get("unowned_continuation_count") == 0
        and validation.get("duplicate_continuation_owner_count") == 0
        and validation.get("ownership_conflict_count") == 0,
        {
            "unowned": validation.get("unowned_continuation_count"),
            "duplicate_owner": validation.get(
                "duplicate_continuation_owner_count"
            ),
            "ownership_conflict": validation.get("ownership_conflict_count"),
        },
        {"unowned": 0, "duplicate_owner": 0, "ownership_conflict": 0},
    )
    _check(
        checks,
        "stage_a2_final_validation_passed",
        validation.get("validation_passed") is True,
        validation.get("validation_passed"),
        True,
    )

    roles = observations.get("final_roles", {})
    ownership = observations.get("ownership_by_continuation", {})
    attachment_expected = dict(STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS)
    terminal_expected = dict(STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS)
    attachment_observed = {
        line_id: ownership.get(line_id) for line_id in attachment_expected
    }
    attachment_roles = {
        line_id: roles.get(line_id) for line_id in attachment_expected
    }
    terminal_observed = {
        line_id: roles.get(line_id) for line_id in terminal_expected
    }
    terminal_ownership = {
        line_id: ownership.get(line_id)
        for line_id in terminal_expected if line_id in ownership
    }
    _check(
        checks,
        "stage_a2_exact_nine_unowned_resolutions",
        len(attachment_expected) == 3
        and len(terminal_expected) == 6
        and attachment_observed == attachment_expected
        and set(attachment_roles.values()) == {"record_continuation_next"}
        and terminal_observed == terminal_expected
        and not terminal_ownership
        and not any(
            roles.get(line_id) in ANCHOR_ROLES
            for line_id in {*attachment_expected, *terminal_expected}
        ),
        {
            "attachment_owners": attachment_observed,
            "attachment_roles": attachment_roles,
            "terminal_roles": terminal_observed,
            "terminal_ownership": terminal_ownership,
        },
        {
            "attachment_owners": attachment_expected,
            "attachment_roles": {
                line_id: "record_continuation_next"
                for line_id in attachment_expected
            },
            "terminal_roles": terminal_expected,
            "terminal_ownership": {},
        },
    )

    interface_expected = {
        **TANAKA_INTERFACE_LINE_TEXT,
        **CHAMPION_INTERFACE_LINE_TEXT,
    }
    interface_wrong = {
        line_id: roles.get(line_id)
        for line_id in interface_expected
        if roles.get(line_id) != "navigation_form_footer"
    }
    _check(
        checks,
        "stage_a2_all_33_interface_lines_excluded",
        len(interface_expected) == 33 and not interface_wrong,
        {"expected_line_count": len(interface_expected), "wrong_roles": interface_wrong},
        {"expected_line_count": 33, "wrong_roles": {}},
    )

    json_parent_id = PILOT_PARENT_IDS[5]
    json_line_id = PILOT_CONTENT_GATES[json_parent_id][
        "required_source_line_id"
    ]
    _check(
        checks,
        "stage_a2_long_json_nonrecord",
        roles.get(json_line_id) == "nonrecord_content",
        roles.get(json_line_id),
        "nonrecord_content",
    )

    candidate_expected = dict(STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES)
    candidate_ids = set(candidate_expected)
    candidate_observed = {
        line_id: roles.get(line_id) for line_id in candidate_expected
    }
    _check(
        checks,
        "stage_a2_exact_candidate_controls",
        candidate_observed == candidate_expected
        and not (candidate_ids & set(ownership)),
        {
            "final_roles": candidate_observed,
            "continuation_owner_entries": sorted(candidate_ids & set(ownership)),
            "stage_b_anchor_eligible_line_ids": sorted(
                line_id for line_id, role in candidate_observed.items()
                if role in ANCHOR_ROLES
            ),
        },
        {
            "final_roles": candidate_expected,
            "continuation_owner_entries": [],
            "stage_b_anchor_eligible_line_ids": [],
        },
    )

    expected_testimonial_ownership = dict(
        LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR
    )
    observed_testimonial_ownership = {
        line_id: ownership.get(line_id)
        for line_id in expected_testimonial_ownership
    }
    testimonial_anchor_ids = set(expected_testimonial_ownership.values())
    _check(
        checks,
        "stage_a2_exact_testimonial_ownership",
        len(expected_testimonial_ownership) == 38
        and len(testimonial_anchor_ids) == 27
        and observed_testimonial_ownership == expected_testimonial_ownership
        and all(roles.get(line_id) in ANCHOR_ROLES for line_id in testimonial_anchor_ids),
        {
            "continuation_count": len(expected_testimonial_ownership),
            "anchor_count": len(testimonial_anchor_ids),
            "mapping_failures": {
                line_id: {
                    "observed": observed_testimonial_ownership.get(line_id),
                    "expected": expected_testimonial_ownership[line_id],
                }
                for line_id in expected_testimonial_ownership
                if observed_testimonial_ownership.get(line_id)
                != expected_testimonial_ownership[line_id]
            },
            "nonanchor_owner_roles": {
                line_id: roles.get(line_id)
                for line_id in sorted(testimonial_anchor_ids)
                if roles.get(line_id) not in ANCHOR_ROLES
            },
        },
        {
            "continuation_count": 38,
            "anchor_count": 27,
            "mapping_failures": {},
            "nonanchor_owner_roles": {},
        },
    )

    quote_line_id = PILOT_CONTENT_GATES[PILOT_PARENT_IDS[2]][
        "required_source_line_id"
    ]
    compound_line_id = PILOT_CONTENT_GATES[PILOT_PARENT_IDS[3]][
        "required_compound_source_line_id"
    ]
    vets_line_id = PILOT_CONTENT_GATES[PILOT_PARENT_IDS[7]][
        "required_source_line_id"
    ]
    required_anchor_roles = {
        quote_line_id: "record_anchor",
        compound_line_id: "compound_record_anchor",
        vets_line_id: "record_anchor",
    }
    observed_anchor_roles = {
        line_id: roles.get(line_id) for line_id in required_anchor_roles
    }
    _check(
        checks,
        "stage_a2_quote_compound_vets_anchor_roles",
        observed_anchor_roles == required_anchor_roles,
        observed_anchor_roles,
        required_anchor_roles,
    )

    anchor_counts = observations.get("anchor_counts_by_parent", {})
    dense_observed = {
        PILOT_PARENT_IDS[0]: anchor_counts.get(PILOT_PARENT_IDS[0]),
        PILOT_PARENT_IDS[1]: anchor_counts.get(PILOT_PARENT_IDS[1]),
    }
    _check(
        checks,
        "stage_a2_dense_parent_anchor_counts",
        isinstance(dense_observed[PILOT_PARENT_IDS[0]], int)
        and dense_observed[PILOT_PARENT_IDS[0]] >= 390
        and isinstance(dense_observed[PILOT_PARENT_IDS[1]], int)
        and dense_observed[PILOT_PARENT_IDS[1]] >= 230,
        dense_observed,
        {PILOT_PARENT_IDS[0]: ">=390", PILOT_PARENT_IDS[1]: ">=230"},
    )
    return {
        "evaluation_version": (
            "candidate_endorsement_line_adjudication_preflight_gate_v1"
        ),
        "checks": checks,
        "passed_checks": sum(check["passed"] for check in checks),
        "failed_checks": sum(not check["passed"] for check in checks),
        "gate_decision": (
            "GO" if all(check["passed"] for check in checks) else "NO_GO"
        ),
    }


def derive_issue_control(bundle_dir: Path) -> dict[str, Any]:
    """Derive the unchanged-route control from one frozen authenticated bundle."""
    manifest_path = bundle_dir / "bundle_manifest.json"
    if sha256_file(manifest_path) != FROZEN_ISSUE_CONTROL_BUNDLE_MANIFEST_SHA256:
        raise ValueError("issue-control bundle manifest differs from frozen bytes")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, Mapping):
        raise TypeError("issue-control bundle manifest is not an object")
    contract = get_line_ledger_contract()
    _require_equal(
        manifest.get("source_corpus_sha256"),
        "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3",
        "issue-control source corpus",
    )
    _require_equal(
        manifest.get("model_id"), contract["model_id"],
        "issue-control model",
    )
    _require_equal(
        manifest.get("model_revision"), contract["model_revision"],
        "issue-control model revision",
    )
    _require_equal(
        manifest.get("runtime_lock_sha256"), contract["runtime_lock_sha256"],
        "issue-control runtime",
    )
    _require_equal(
        manifest.get("prompt_sha256", {}).get("issues"),
        contract["unchanged_issue_prompt_sha256"],
        "issue-control prompt",
    )
    _require_equal(
        manifest.get("json_schema_sha256", {}).get("issues"),
        contract["unchanged_issue_schema_sha256"],
        "issue-control schema",
    )
    segments_entry = manifest.get("segments_file")
    if not isinstance(segments_entry, Mapping):
        raise ValueError("issue-control bundle lacks segment-file inventory")
    name = segments_entry.get("file")
    if not isinstance(name, str) or Path(name).name != name:
        raise ValueError("issue-control segment filename is unsafe")
    segments_path = bundle_dir / name
    actual_segments_sha256 = sha256_file(segments_path)
    _require_equal(
        actual_segments_sha256, FROZEN_ISSUE_CONTROL_SEGMENTS_SHA256,
        "issue-control frozen segment bytes",
    )
    _require_equal(
        actual_segments_sha256, segments_entry.get("sha256"),
        "issue-control manifest segment hash",
    )
    with gzip.open(segments_path, "rt", encoding="utf-8") as handle:
        all_rows = [json.loads(line) for line in handle if line.strip()]
    _require_equal(len(all_rows), segments_entry.get("rows"), "issue-control rows")
    rows = [
        row for row in all_rows
        if row.get("parent_document_id")
        == CORRECTED_ISSUE_AUDIT_GATE["parent_document_id"]
    ]
    _require_equal(
        [str(row.get("document_id")) for row in rows],
        list(FROZEN_ISSUE_CONTROL_DOCUMENT_IDS),
        "issue-control complete parent segments",
    )
    if any(row.get("route") != "issues" for row in rows):
        raise ValueError("issue-control parent contains a non-issue route")
    resolved_by_id: dict[str, Mapping[str, Any]] = {}
    variants_by_group: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in rows:
        for record in row.get("resolved_records", []):
            record_id = str(record.get("record_id"))
            if record_id in resolved_by_id and not json_value_equal_typed(
                resolved_by_id[record_id], record
            ):
                raise ValueError("issue-control resolved record ID conflicts")
            resolved_by_id[record_id] = record
        for variant in row.get("record_variants", []):
            group_id = str(variant.get("overlap_group_id"))
            variant_id = str(variant.get("variant_id"))
            existing = variants_by_group[group_id].get(variant_id)
            if existing is not None and not json_value_equal_typed(existing, variant):
                raise ValueError("issue-control variant ID conflicts")
            variants_by_group[group_id][variant_id] = variant
    quote_resolved = [
        record for record in resolved_by_id.values()
        if '"' in str(record.get("item", {}).get("evidence", ""))
    ]
    conflict_groups = []
    for group_id, by_variant in sorted(variants_by_group.items()):
        variants = list(by_variant.values())
        kinds = sorted({
            str(variant.get("item", {}).get("statement_kind"))
            for variant in variants
        })
        if not {"proposal", "oppose"} <= set(kinds):
            continue
        fully_accounted = bool(
            len(variants) >= 2
            and all(
                variant.get("overlap_resolution_status") == "conflict_withheld"
                and variant.get("overlap_resolution_method")
                == "withheld_distinct_semantic_payloads"
                and variant.get("group_release_eligible") is False
                and variant.get("selected_as_canonical") is False
                for variant in variants
            )
        )
        conflict_groups.append({
            "overlap_group_id": group_id,
            "fully_accounted": fully_accounted,
            "disposition": "conflict_withheld_private",
            "statement_kinds": kinds,
            "variant_ids": sorted(by_variant),
        })
    return {
        "route": "issues",
        "parent_document_id": CORRECTED_ISSUE_AUDIT_GATE[
            "parent_document_id"
        ],
        "quote_bearing_resolved_issue_records": len(quote_resolved),
        "fully_accounted_private_conflict_groups": conflict_groups,
        "artifact_provenance": {
            "bundle_dir": str(bundle_dir),
            "bundle_manifest_sha256": sha256_file(manifest_path),
            "segments_file": name,
            "segments_sha256": actual_segments_sha256,
            "parent_document_ids": list(FROZEN_ISSUE_CONTROL_DOCUMENT_IDS),
            "derived_without_self_reported_counts": True,
        },
    }


def replay_stage_a2_merge(
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    full_export_dir: Path = DEFAULT_FULL_EXPORT_DIR,
) -> dict[str, Any]:
    """Authenticate both passes and replay the deterministic final merge."""
    (
        a_manifest,
        a_batches,
        a_rows,
        stage_a_decisions,
        a_artifacts,
    ) = load_frozen_stage_a_suffix_b(
        stage_a_export_dir, stage_a_output_dir
    )
    a2_manifest, a2_batches, a2_rows, a2_artifacts = load_validated_run(
        stage_a2_export_dir,
        stage_a2_output_dir,
        stage=LINE_ADJUDICATION_STAGE,
    )
    _require_equal(
        a_manifest.get("selected_parent_ids"),
        list(PILOT_PARENT_IDS),
        "Stage-A pilot parents",
    )
    _require_equal(
        a2_manifest.get("selected_parent_ids"),
        list(PILOT_PARENT_IDS),
        "Stage-A2 pilot parents",
    )
    _require_equal(
        a_manifest.get("complete_parent_closure"), True,
        "Stage-A parent closure",
    )
    _require_equal(
        a2_manifest.get("complete_parent_closure"), True,
        "Stage-A2 parent closure",
    )
    _require_equal(
        a2_manifest.get("stage_a_provenance"),
        a_artifacts,
        "Stage-A2 exact frozen Stage-A provenance",
    )
    _require_equal(
        a2_manifest.get("stage_a_exception_count"),
        FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "Stage-A2 deterministic exception count",
    )
    replay = replay_frozen_source(full_export_dir)
    _require_equal(
        a_manifest.get("source_export", {}).get(
            "source_export_manifest_sha256"
        ),
        replay["manifest_sha256"],
        "pilot source-export binding",
    )
    parents = reconstruct_selected_parents(
        replay, [str(value) for value in a_manifest["selected_parent_ids"]]
    )
    regenerated_a2 = build_line_adjudication_batches(
        parents,
        stage_a_decisions,
        stage_a_provenance=a_artifacts,
        contract_override=get_frozen_stage_a2_v4_contract(),
    )
    _require_equal(
        [sha256_json(batch) for batch in a2_batches],
        [sha256_json(batch) for batch in regenerated_a2],
        "regenerated Stage-A2 inputs",
    )
    stage_a2_decisions = line_adjudication_ledger(a2_batches, a2_rows)
    stage_a2_merge = merge_line_adjudication_ledger(
        parents, stage_a_decisions, stage_a2_decisions
    )
    return {
        "stage_a_manifest": a_manifest,
        "stage_a_batches": a_batches,
        "stage_a_rows": a_rows,
        "stage_a_decisions": stage_a_decisions,
        "stage_a_artifacts": a_artifacts,
        "stage_a2_manifest": a2_manifest,
        "stage_a2_batches": a2_batches,
        "stage_a2_rows": a2_rows,
        "stage_a2_decisions": stage_a2_decisions,
        "stage_a2_artifacts": {
            **a2_artifacts,
            "stage_a2_decision_ledger_sha256": sha256_json(
                stage_a2_decisions
            ),
        },
        "parents": parents,
        "stage_a2_merge": stage_a2_merge,
    }


def evaluate_stage_a2_only(
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    full_export_dir: Path = DEFAULT_FULL_EXPORT_DIR,
) -> dict[str, Any]:
    replayed = replay_stage_a2_merge(
        stage_a_export_dir,
        stage_a_output_dir,
        stage_a2_export_dir,
        stage_a2_output_dir,
        full_export_dir,
    )
    stage_a_decisions = replayed["stage_a_decisions"]
    merge = replayed["stage_a2_merge"]
    final_roles = {
        str(line_id): str(decision["role"])
        for line_id, decision in merge["final_decisions"].items()
    }
    anchor_counts_by_parent: dict[str, int] = {}
    for parent in replayed["parents"]:
        parent_id = str(parent["parent_document_id"])
        anchor_counts_by_parent[parent_id] = sum(
            final_roles.get(str(line["source_line_id"])) in ANCHOR_ROLES
            for line in parent["lines"]
        )
    observations = {
        "stage_a2_merge_validation": dict(merge["validation"]),
        "candidate_self_exception_line_ids": sorted(
            line_id for line_id, decision in stage_a_decisions.items()
            if decision.get("role") == "candidate_self"
        ),
        "final_roles": final_roles,
        "ownership_by_continuation": dict(
            merge["ownership_by_continuation"]
        ),
        "anchor_counts_by_parent": anchor_counts_by_parent,
        "final_role_counts": dict(sorted(Counter(final_roles.values()).items())),
    }
    evaluation = evaluate_stage_a2_only_observations(observations)
    return {
        **evaluation,
        "line_ledger_contract_sha256": get_frozen_stage_a2_v4_contract()[
            "line_ledger_contract_sha256"
        ],
        "stage_a_artifacts": replayed["stage_a_artifacts"],
        "stage_a2_artifacts": replayed["stage_a2_artifacts"],
        "stage_a2_merge": {
            "validation": merge["validation"],
            "final_decisions_sha256": merge["final_decisions_sha256"],
            "per_line_provenance_sha256": merge[
                "per_line_provenance_sha256"
            ],
            "ownership_by_continuation_sha256": sha256_json(
                merge["ownership_by_continuation"]
            ),
        },
        "observations": observations,
        "stage_b_required": False,
    }


def evaluate_pilot(
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    stage_b_export_dir: Path,
    stage_b_output_dir: Path,
    issue_control_bundle_dir: Path,
    stage_a2_evaluation_report: Path,
    stage_a3_task_json: Path,
    stage_a3_html: Path,
    stage_a3_results_json: Path,
    stage_a3_import_artifact: Path,
    full_export_dir: Path = DEFAULT_FULL_EXPORT_DIR,
    stage_b_residual_task_json: Path | None = None,
    stage_b_residual_html: Path | None = None,
    stage_b_residual_results_json: Path | None = None,
    stage_b_residual_import_artifact: Path | None = None,
) -> dict[str, Any]:
    from scripts.build_candidate_endorsement_line_ledger_a3_handcode import (
        FROZEN_A3_HTML_SHA256, FROZEN_A3_IMPORT_ARTIFACT_SHA256,
        FROZEN_A3_RESULTS_SHA256, FROZEN_A3_TASK_SHA256,
        build_a3_provenance, derive_a3_residuals, sha256_file,
    )
    from scripts.import_candidate_endorsement_line_ledger_a3_handcode import (
        human_a3_import_artifact_payload, merge_human_a3,
        unchanged_gate_replay, validate_results, validate_task_provenance,
    )
    replayed = replay_stage_a2_merge(
        stage_a_export_dir, stage_a_output_dir,
        stage_a2_export_dir, stage_a2_output_dir, full_export_dir,
    )
    a_manifest = replayed["stage_a_manifest"]
    a_batches = replayed["stage_a_batches"]
    a_rows = replayed["stage_a_rows"]
    stage_a_decisions = replayed["stage_a_decisions"]
    a_artifacts = replayed["stage_a_artifacts"]
    a2_manifest = replayed["stage_a2_manifest"]
    a2_batches = replayed["stage_a2_batches"]
    a2_rows = replayed["stage_a2_rows"]
    a2_artifacts = replayed["stage_a2_artifacts"]
    stage_a2_decisions = replayed["stage_a2_decisions"]
    parents = replayed["parents"]
    stage_a2_merge = replayed["stage_a2_merge"]
    stage_a2_evaluation = evaluate_stage_a2_only(
        stage_a_export_dir, stage_a_output_dir,
        stage_a2_export_dir, stage_a2_output_dir, full_export_dir,
    )
    _require_equal(
        json.loads(stage_a2_evaluation_report.read_text()),
        stage_a2_evaluation, "stored Stage-A2 evaluation",
    )
    task_sha = sha256_file(stage_a3_task_json)
    html_sha = sha256_file(stage_a3_html)
    results_sha = sha256_file(stage_a3_results_json)
    import_sha = sha256_file(stage_a3_import_artifact)
    _require_equal(task_sha, FROZEN_A3_TASK_SHA256, "frozen A3 task")
    _require_equal(html_sha, FROZEN_A3_HTML_SHA256, "frozen A3 HTML")
    _require_equal(results_sha, FROZEN_A3_RESULTS_SHA256, "frozen A3 results")
    _require_equal(import_sha, FROZEN_A3_IMPORT_ARTIFACT_SHA256, "frozen A3 import")
    residuals = derive_a3_residuals(replayed, stage_a2_evaluation)
    task = json.loads(stage_a3_task_json.read_text())
    validate_task_provenance(task, build_a3_provenance(
        replayed, residuals, stage_a2_export_dir=stage_a2_export_dir,
        stage_a2_evaluation_report=stage_a2_evaluation_report,
    ))
    results = json.loads(stage_a3_results_json.read_text())
    decisions = validate_results(task, results)
    stage_a3_merge = merge_human_a3(
        parents, stage_a2_merge, residuals, decisions,
        task_sha256=task_sha, results_sha256=results_sha,
    )
    gate_replay = unchanged_gate_replay(replayed, stage_a3_merge)["evaluation"]
    expected_import = human_a3_import_artifact_payload(
        task_sha256=task_sha, results_sha256=results_sha,
        results_exported_at=results["exported_at"], residual_count=len(residuals),
        decisions=decisions, stage_a2_merge=stage_a2_merge,
        stage_a3_merge=stage_a3_merge, gate_replay=gate_replay,
    )
    _require_equal(
        json.loads(stage_a3_import_artifact.read_text()), expected_import,
        "Stage-A3 imported authorization",
    )
    stage_b_manifest_sha256 = sha256_file(stage_b_export_dir / "manifest.json")
    _require_equal(
        stage_b_manifest_sha256,
        FROZEN_STAGE_B_PILOT_MANIFEST_SHA256,
        "frozen Stage-B pilot manifest bytes",
    )
    frozen_stage_b_manifest = json.loads(
        (stage_b_export_dir / "manifest.json").read_text(encoding="utf-8")
    )
    frozen_stage_b_contract = frozen_stage_b_manifest.get("line_ledger_contract")
    if not isinstance(frozen_stage_b_contract, Mapping):
        raise ValueError("frozen Stage-B pilot contract is absent")
    residual_paths = (
        stage_b_residual_task_json, stage_b_residual_html,
        stage_b_residual_results_json, stage_b_residual_import_artifact,
    )
    if any(path is not None for path in residual_paths):
        if any(path is None for path in residual_paths):
            raise ValueError("all Stage-B residual replay artifacts are required")
        from scripts.build_candidate_endorsement_stage_b_residual_handcode import (
            load_frozen_failed_run,
        )
        from scripts.import_candidate_endorsement_stage_b_residual_handcode import (
            replay_import_artifact,
        )
        b_manifest, _, _ = load_frozen_failed_run(
            stage_b_export_dir, stage_b_output_dir
        )
        b_batches, b_rows, residual_artifact = replay_import_artifact(
            export_dir=stage_b_export_dir, output_dir=stage_b_output_dir,
            task_path=stage_b_residual_task_json,
            html_path=stage_b_residual_html,
            results_path=stage_b_residual_results_json,
            artifact_path=stage_b_residual_import_artifact,
        )
        b_artifacts = {
            "replay_mode": "authenticated_human_exact_source_residual_merge",
            "residual_import_artifact": residual_artifact,
        }
    else:
        b_manifest, b_batches, b_rows, b_artifacts = load_validated_run(
            stage_b_export_dir, stage_b_output_dir, stage=RECORD_EXTRACTION_STAGE,
            contract_override=frozen_stage_b_contract,
        )
    _require_equal(a_manifest.get("selected_parent_ids"), list(PILOT_PARENT_IDS), "Stage-A pilot parents")
    _require_equal(a2_manifest.get("selected_parent_ids"), list(PILOT_PARENT_IDS), "Stage-A2 pilot parents")
    _require_equal(b_manifest.get("selected_parent_ids"), list(PILOT_PARENT_IDS), "Stage-B pilot parents")
    _require_equal(a_manifest.get("complete_parent_closure"), True, "Stage-A parent closure")
    _require_equal(a2_manifest.get("complete_parent_closure"), True, "Stage-A2 parent closure")
    _require_equal(b_manifest.get("complete_parent_closure"), True, "Stage-B parent closure")
    _require_equal(
        a2_manifest.get("stage_a_provenance"),
        a_artifacts,
        "Stage-A2 exact frozen Stage-A provenance",
    )
    _require_equal(
        a2_manifest.get("stage_a_exception_count"),
        FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "Stage-A2 deterministic exception count",
    )

    replay = replay_frozen_source(full_export_dir)
    _require_equal(
        a_manifest.get("source_export", {}).get(
            "source_export_manifest_sha256"
        ),
        replay["manifest_sha256"],
        "pilot source-export binding",
    )
    authorization = dict(FROZEN_STAGE_A3_AUTHENTICATION)
    regenerated_b = build_record_extraction_batches_from_adjudicated_merge(
        parents, stage_a3_merge, three_pass_authorization=authorization,
        contract_override=frozen_stage_b_contract,
    )
    _require_equal(
        [sha256_json(batch) for batch in b_batches],
        [sha256_json(batch) for batch in regenerated_b],
        "Stage-B inputs regenerated from exact A1+A2+A3 merge",
    )
    _require_equal(
        b_manifest.get("upstream_stage_a3"), authorization,
        "Stage-B exact upstream A3 authorization",
    )
    issue_control = derive_issue_control(issue_control_bundle_dir)
    observations = collect_pilot_observations(
        a_batches,
        a_rows,
        b_batches,
        b_rows,
        final_decisions=stage_a3_merge["final_decisions"],
        stage_a2_merge=stage_a3_merge,
    )
    evaluation = evaluate_stage_a2_pilot_observations(
        observations, issue_control
    )
    return {
        **evaluation,
        "line_ledger_contract_sha256": get_line_ledger_contract()[
            "line_ledger_contract_sha256"
        ],
        "stage_a_artifacts": a_artifacts,
        "stage_a2_artifacts": a2_artifacts,
        "stage_a2_merge": {
            "validation": stage_a2_merge["validation"],
            "final_decisions_sha256": stage_a2_merge[
                "final_decisions_sha256"
            ],
            "per_line_provenance_sha256": stage_a2_merge[
                "per_line_provenance_sha256"
            ],
            "ownership_by_continuation_sha256": sha256_json(
                stage_a2_merge["ownership_by_continuation"]
            ),
        },
        "stage_a3_merge": {
            "validation": stage_a3_merge["validation"],
            "final_decisions_sha256": stage_a3_merge["final_decisions_sha256"],
            "per_line_provenance_sha256": stage_a3_merge["per_line_provenance_sha256"],
            "ownership_by_continuation_sha256": sha256_json(
                stage_a3_merge["ownership_by_continuation"]
            ),
        },
        "stage_a3_artifacts": authorization,
        "stage_b_artifacts": b_artifacts,
        "stage_b_manifest_sha256": stage_b_manifest_sha256,
        "issue_control_artifact": issue_control.pop("artifact_provenance"),
        "observations": observations,
    }


def evaluate_recall40_stage_a_for_a2(
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    full_export_dir: Path = DEFAULT_FULL_EXPORT_DIR,
) -> dict[str, Any]:
    """Fail-closed technical gate from the private Stage-A40 run to A2 build."""
    inventory = derive_recall40_stage_a2_exception_inventory(
        full_export_dir, stage_a_export_dir, stage_a_output_dir
    )
    checks = [
        {
            "check_id": "stage_a40_exact_authenticated_complete_run",
            "passed": inventory["selected_parent_count"] == 40,
            "observed": inventory["selected_parent_count"],
            "expected": 40,
        },
        {
            "check_id": "stage_a40_decision_coverage_nonempty",
            "passed": inventory["stage_a_decision_count"] > 0,
            "observed": inventory["stage_a_decision_count"],
            "expected": ">0",
        },
        {
            "check_id": "stage_a40_distinct_a2_contract_required",
            "passed": inventory["requires_distinct_upstream_contract_rotation"] is True,
            "observed": inventory["requires_distinct_upstream_contract_rotation"],
            "expected": True,
        },
    ]
    passed = sum(check["passed"] is True for check in checks)
    return {
        "evaluation_scope": "private_recall40_stage_a_to_a2_only",
        "gate_decision": "GO_BUILD_FROZEN_A2_INPUTS" if passed == len(checks) else "NO_GO",
        "passed_checks": passed,
        "failed_checks": len(checks) - passed,
        "checks": checks,
        "stage_a2_exception_inventory": inventory,
        "a2_input_build_authorized": passed == len(checks),
        "a2_hive_submission_authorized": False,
        "stage_b_authorized": False,
        "full_expansion_or_release_authorized": False,
    }


def evaluate_recall40_stage_a2_for_stage_b(
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    full_export_dir: Path = DEFAULT_FULL_EXPORT_DIR,
) -> dict[str, Any]:
    """Fail closed at the private recall40 A1+A2 to bounded-B boundary."""
    replay = replay_recall40_stage_a_and_a2(
        full_export_dir,
        stage_a_export_dir,
        stage_a_output_dir,
        stage_a2_export_dir,
        stage_a2_output_dir,
    )
    return recall40_post_a2_evaluation_report(replay)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evaluation-mode",
        choices=(
            "recall40-stage-a-for-a2",
            "recall40-stage-a2-for-stage-b",
            "stage-a2-only",
            "full-pilot",
        ),
        default="stage-a2-only",
    )
    parser.add_argument("--stage-a-export-dir", type=Path, required=True)
    parser.add_argument("--stage-a-output-dir", type=Path, required=True)
    parser.add_argument("--stage-a2-export-dir", type=Path)
    parser.add_argument("--stage-a2-output-dir", type=Path)
    parser.add_argument("--stage-b-export-dir", type=Path)
    parser.add_argument("--stage-b-output-dir", type=Path)
    parser.add_argument("--stage-a2-evaluation-report", type=Path)
    parser.add_argument("--stage-a3-task-json", type=Path)
    parser.add_argument("--stage-a3-html", type=Path)
    parser.add_argument("--stage-a3-results-json", type=Path)
    parser.add_argument("--stage-a3-import-artifact", type=Path)
    parser.add_argument("--stage-b-residual-task-json", type=Path)
    parser.add_argument("--stage-b-residual-html", type=Path)
    parser.add_argument("--stage-b-residual-results-json", type=Path)
    parser.add_argument("--stage-b-residual-import-artifact", type=Path)
    parser.add_argument(
        "--full-export-dir", type=Path, default=DEFAULT_FULL_EXPORT_DIR
    )
    parser.add_argument("--issue-control-bundle-dir", type=Path)
    parser.add_argument("--output-report", type=Path, required=True)
    args = parser.parse_args()
    if args.output_report.exists():
        raise FileExistsError(f"refusing to overwrite pilot report: {args.output_report}")
    if args.evaluation_mode == "recall40-stage-a-for-a2":
        report = evaluate_recall40_stage_a_for_a2(
            args.stage_a_export_dir, args.stage_a_output_dir,
            args.full_export_dir,
        )
    elif args.evaluation_mode == "recall40-stage-a2-for-stage-b":
        if args.stage_a2_export_dir is None or args.stage_a2_output_dir is None:
            raise SystemExit(
                "recall40-stage-a2-for-stage-b requires both --stage-a2-* directories"
            )
        report = evaluate_recall40_stage_a2_for_stage_b(
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.stage_a2_export_dir,
            args.stage_a2_output_dir,
            args.full_export_dir,
        )
    elif args.evaluation_mode == "stage-a2-only":
        if args.stage_a2_export_dir is None or args.stage_a2_output_dir is None:
            raise SystemExit("stage-a2-only requires both --stage-a2-* directories")
        report = evaluate_stage_a2_only(
            args.stage_a_export_dir,
            args.stage_a_output_dir,
            args.stage_a2_export_dir,
            args.stage_a2_output_dir,
            args.full_export_dir,
        )
    else:
        if (
            args.stage_a2_export_dir is None
            or args.stage_a2_output_dir is None
            or args.stage_b_export_dir is None
            or args.stage_b_output_dir is None
            or args.issue_control_bundle_dir is None
            or args.stage_a2_evaluation_report is None
            or args.stage_a3_task_json is None
            or args.stage_a3_html is None
            or args.stage_a3_results_json is None
            or args.stage_a3_import_artifact is None
        ):
            raise SystemExit(
                "full-pilot requires --stage-b-export-dir, "
                "--stage-b-output-dir, and --issue-control-bundle-dir"
                ", plus all --stage-a2-evaluation-report/--stage-a3-* inputs"
            )
        report = evaluate_pilot(
            args.stage_a_export_dir, args.stage_a_output_dir,
            args.stage_a2_export_dir, args.stage_a2_output_dir,
            args.stage_b_export_dir, args.stage_b_output_dir,
            args.issue_control_bundle_dir,
            args.stage_a2_evaluation_report,
            args.stage_a3_task_json,
            args.stage_a3_html,
            args.stage_a3_results_json,
            args.stage_a3_import_artifact,
            args.full_export_dir,
            args.stage_b_residual_task_json,
            args.stage_b_residual_html,
            args.stage_b_residual_results_json,
            args.stage_b_residual_import_artifact,
        )
    args.output_report.parent.mkdir(parents=True, exist_ok=True)
    args.output_report.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "gate_decision": report["gate_decision"],
        "passed_checks": report["passed_checks"],
        "failed_checks": report["failed_checks"],
        "output_report": str(args.output_report),
    }, indent=2, sort_keys=True))
    return 0 if report["gate_decision"] in {
        "GO", "GO_BUILD_FROZEN_A2_INPUTS", "GO_BUILD_BOUNDED_STAGE_B_INPUTS"
    } else 2


if __name__ == "__main__":
    raise SystemExit(main())
