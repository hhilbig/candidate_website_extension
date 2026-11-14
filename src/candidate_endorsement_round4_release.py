"""Project round-four endorsement output into the public release schema.

The round-four runner returns one extraction decision for every Stage-B target
block.  Those decisions carry model records that use ``name_parts`` instead of
a rewritten name.  This module converts each decision into the exact public
endorsement record shape that the release packager already consumes, and it
keeps the complete lineage of every record.

Deterministic code performs only source reconstruction, exact-span projection,
optional-field sanitization, and coverage bookkeeping.  It never creates,
repairs, or relabels a semantic endorsement record.  A record that cannot be
projected to its exact source becomes a withheld record with a typed reason.
It never becomes a measured zero.
"""
from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Any

from src.candidate_endorsement_line_ledger import (
    reconstruct_parent_inventory,
)
from src.candidate_endorsement_round4 import (
    CATEGORY_ALLOWED_ENTITY_TYPES,
    ENDORSEMENT_CATEGORIES,
    ENTITY_TYPES,
    MAX_RECORDS_PER_STAGE_B_SLOT,
    OPTIONAL_FIELDS,
    STAGE_B_DISPOSITIONS,
    canonical_name_from_parts,
    resolve_ordered_name_parts,
    sanitize_optional_fields,
    sha256_json,
)
from src.candidate_endorsement_round4_production import (
    ANCHOR_ROLES,
    ROUND4_FAILURE_CLASSES,
    ROUND4_PRODUCTION_ARTIFACT_SCOPE,
    STAGE_B,
    get_round4_production_contract,
    validate_scope_authorizations,
)
from src.candidate_extraction import align_extraction_exact_spans


RECORD_LEDGER_VERSION = "candidate_endorsement_round4_record_ledger_bundle_v1"
ENDORSEMENT_ARCHITECTURE = "endorsement_round4_record_ledger_v1"
TARGET_BLOCK_ALIGNMENT_METHOD = (
    "round4_deterministic_trimmed_record_block_projection_v1"
)

# A released round-four record labels itself with the architecture that produced
# it. The construction matches the line ledger's, but reusing that vocabulary
# would make a round-four record read as a line-ledger record in the released
# table, where the architecture is not otherwise visible.
ROUND4_RECORD_IDENTITY = "candidate_endorsement_round4_record_ledger_v1"
ROUND4_RECORD_RESOLUTION_STATUS = "round4_record_ledger_single_record"
ROUND4_RECORD_RESOLUTION_METHOD = "two_stage_parent_record_ledger_round4_v1"

# The optional-field errors that a slot-level replay can repair.  Any other
# error leaves the target block unresolved.
_OPTIONAL_FIELD_ERROR_MARKERS = tuple(
    f"{field}:" for field in OPTIONAL_FIELDS
)


class Round4ProjectionError(ValueError):
    """Raised when a batch and its output cannot be paired at all."""


def _require(condition: Any, message: str) -> None:
    if not condition:
        raise Round4ProjectionError(message)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parent_inventory_from_export_rows(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Rebuild one endorsement parent in the frozen base inventory scope.

    Round four reads the inventory in its base scope.  The production path
    must never apply a route revision, because the reviewed development runs
    did not apply one either.
    """
    return reconstruct_parent_inventory(rows)


def classify_stage_b_response(
    output_row: Mapping[str, Any],
) -> tuple[str, list[str]]:
    """Return the typed failure class of one Stage-B response.

    The five classes are frozen in the production failure-accounting spec.
    ``complete`` and ``optional_field_repaired`` need no re-inference.  The
    two remaining classes are infrastructure problems and are the only ones
    that a bounded retry may repeat.
    """
    if not isinstance(output_row, Mapping):
        raise TypeError("output_row must be a mapping")
    parse_status = output_row.get("parse_status")
    finish_reason = output_row.get("finish_reason")
    validation = output_row.get("stage_validation")
    if output_row.get("input_truncated") is True:
        return "infrastructure_failure", ["input_truncated"]
    if finish_reason is not None and finish_reason != "stop":
        return "infrastructure_failure", [f"finish_reason:{finish_reason}"]
    if parse_status in {"strict_json_parse_error", "generation_incomplete"}:
        return "infrastructure_failure", [str(parse_status)]
    if output_row.get("technical_pass") is True:
        return "complete", []
    if parse_status != "invalid_stage_output":
        return "infrastructure_failure", [f"parse_status:{parse_status}"]
    errors = []
    if isinstance(validation, Mapping):
        errors = [str(value) for value in validation.get("errors", [])]
    if not errors:
        return "unlocalizable_schema_failure", ["no_reported_error"]
    if isinstance(validation, Mapping) and validation.get("schema_valid") is False:
        return "unlocalizable_schema_failure", errors
    repairable = all(
        any(marker in error for marker in _OPTIONAL_FIELD_ERROR_MARKERS)
        for error in errors
    )
    if repairable:
        return "optional_field_repaired", errors
    return "unresolved_target_block", errors


def _validated_decision(decision: Any, slot: str) -> dict[str, Any]:
    _require(isinstance(decision, Mapping), f"{slot}: decision is not an object")
    disposition = decision.get("disposition")
    _require(
        disposition in STAGE_B_DISPOSITIONS,
        f"{slot}: unknown disposition {disposition!r}",
    )
    records = decision.get("records")
    _require(isinstance(records, list), f"{slot}: records is not a list")
    _require(
        len(records) <= MAX_RECORDS_PER_STAGE_B_SLOT,
        f"{slot}: records exceed the guided maximum",
    )
    if disposition != "records":
        _require(not records, f"{slot}: {disposition} must carry no record")
    else:
        _require(records, f"{slot}: records disposition carries no record")
    return dict(decision)


def _v2_shape_from_round4_record(
    model_record: Mapping[str, Any], visible_text: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Convert one round-four record into the v2.1 endorsement item fields."""
    _require(
        isinstance(model_record, Mapping), "model record is not an object"
    )
    entity_type = model_record.get("entity_type")
    category = model_record.get("endorser_category")
    _require(entity_type in ENTITY_TYPES, f"unknown entity type {entity_type!r}")
    _require(
        category in ENDORSEMENT_CATEGORIES,
        f"unknown endorser category {category!r}",
    )
    _require(
        entity_type in CATEGORY_ALLOWED_ENTITY_TYPES[category],
        "entity type is not allowed for the endorser category",
    )
    sanitized, changes = sanitize_optional_fields(model_record, visible_text)
    item = {
        "endorser_name": canonical_name_from_parts(model_record["name_parts"]),
        "entity_type": entity_type,
        "endorser_category": category,
    }
    for field in OPTIONAL_FIELDS:
        item[field] = sanitized.get(field)
    return item, changes


def _trimmed_block_span(
    parent_text: str, target: Mapping[str, Any],
) -> tuple[str, int, int]:
    """Return the trimmed record-block text and its exact parent offsets."""
    lines = list(target["record_block_lines"])
    _require(lines, "record-anchor target has no source line")
    start = min(int(line["parent_start_char"]) for line in lines)
    end = max(int(line["parent_full_end_char"]) for line in lines)
    _require(
        0 <= start < end <= len(parent_text),
        "record block span is outside the parent text",
    )
    block = parent_text[start:end]
    stripped = block.strip()
    _require(stripped, "record block text is empty after trimming")
    offset = block.index(stripped)
    start += offset
    end = start + len(stripped)
    return parent_text[start:end], start, end


def _support_mode(evidence: str, item: Mapping[str, Any]) -> str:
    from src.candidate_endorsement_line_ledger import _projected_support_mode

    return _projected_support_mode(evidence, item)


def project_round4_stage_b_batch(
    batch: Mapping[str, Any],
    output_row: Mapping[str, Any],
    parent: Mapping[str, Any],
    *,
    contract_sha256: str | None = None,
    artifact_scope: str = ROUND4_PRODUCTION_ARTIFACT_SCOPE,
) -> dict[str, Any]:
    """Project one Stage-B batch and its output into public endorsement records.

    ``parent`` is the reconstructed inventory for the batch parent.  It
    supplies the exact parent text, the segment descriptors, the line owners,
    and the inventory identity, none of which the model ever sees.
    """
    _require(isinstance(batch, Mapping), "batch is not an object")
    _require(isinstance(output_row, Mapping), "output row is not an object")
    _require(isinstance(parent, Mapping), "parent inventory is not an object")
    _require(batch.get("stage") == STAGE_B, "batch is not a Stage-B batch")
    _require(
        output_row.get("stage") == STAGE_B, "output row is not a Stage-B row"
    )
    _require(
        str(batch["batch_id"]) == str(output_row["batch_id"]),
        "batch and output identities differ",
    )
    _require(
        str(batch["parent_document_id"]) == str(parent["parent_document_id"]),
        "batch and parent inventory identities differ",
    )
    validate_scope_authorizations(
        artifact_scope, dict(output_row.get("authorizations") or {})
    )
    if contract_sha256 is None:
        contract_sha256 = get_round4_production_contract()[
            "round4_production_contract_sha256"
        ]

    failure_class, failure_errors = classify_stage_b_response(output_row)
    parent_text = str(parent["parent_source_text"])
    _require(
        str(batch["extraction_source_text_sha256"])
        == str(parent["extraction_source_text_sha256"]),
        "batch and parent source identities differ",
    )
    lines_by_id = {
        str(line["source_line_id"]): line for line in parent["lines"]
    }
    descriptors = list(parent["segment_descriptors"])
    target_by_slot = {
        str(target["slot"]): target for target in batch["targets"]
    }

    aligned_records: list[dict[str, Any]] = []
    withheld: list[dict[str, Any]] = []
    repairs: list[dict[str, Any]] = []
    unresolved_blocks: list[dict[str, Any]] = []

    if failure_class in {"infrastructure_failure", "unlocalizable_schema_failure"}:
        # A retryable failure never yields a partial projection.  The overlay
        # build refuses to proceed while one remains open.
        return {
            "batch_id": str(batch["batch_id"]),
            "parent_document_id": str(batch["parent_document_id"]),
            "failure_class": failure_class,
            "failure_errors": failure_errors,
            "aligned_records": [],
            "withheld_records": [],
            "optional_field_repairs": [],
            "unresolved_target_blocks": [
                {
                    "source_line_id": str(target["source_line_id"]),
                    "stage_b_slot": slot,
                    "reason": failure_class,
                }
                for slot, target in sorted(target_by_slot.items())
            ],
            "model_anchor_count": len(target_by_slot),
            "aligned_record_count": 0,
            "withheld_record_count": 0,
            "retry_required": True,
        }

    model_output = output_row.get("model_output")
    _require(isinstance(model_output, Mapping), "model output is not an object")
    projection = {}
    validation = output_row.get("stage_validation")
    if isinstance(validation, Mapping):
        projection = validation.get("source_projection") or {}

    for slot, target in sorted(target_by_slot.items()):
        source_line_id = str(target["source_line_id"])
        _require(
            str(target.get("stage_a_role")) in ANCHOR_ROLES,
            f"{slot}: Stage-B target is not a Stage-A anchor",
        )
        _require(
            source_line_id in lines_by_id,
            f"{slot}: target line is absent from the parent inventory",
        )
        raw_decision = model_output.get(slot)
        if raw_decision is None:
            unresolved_blocks.append({
                "source_line_id": source_line_id,
                "stage_b_slot": slot,
                "reason": "missing_slot_decision",
            })
            continue
        try:
            decision = _validated_decision(raw_decision, slot)
        except Round4ProjectionError as error:
            unresolved_blocks.append({
                "source_line_id": source_line_id,
                "stage_b_slot": slot,
                "reason": "invalid_slot_decision",
                "detail": str(error),
            })
            continue
        if decision["disposition"] != "records":
            # A resolved non-record decision.  It is not a failure and it is
            # not a record.  Coverage stays complete for this block.
            continue

        block_text, block_start, block_end = _trimmed_block_span(
            parent_text, target
        )
        slot_projection = projection.get(slot) or []
        for model_record_index, model_record in enumerate(decision["records"]):
            outcome = _project_one_record(
                model_record=model_record,
                model_record_index=model_record_index,
                slot=slot,
                target=target,
                source_line_id=source_line_id,
                block_text=block_text,
                block_start=block_start,
                block_end=block_end,
                descriptors=descriptors,
                owner_segment_id=str(lines_by_id[source_line_id]["owner_segment_id"]),
                parent=parent,
                batch=batch,
                contract_sha256=contract_sha256,
                slot_projection=slot_projection,
            )
            if outcome["kind"] == "record":
                aligned_records.append(outcome["record"])
                repairs.extend(outcome["repairs"])
            elif outcome["kind"] == "unresolved":
                unresolved_blocks.append(outcome["entry"])
                withheld.append(outcome["entry"])
            else:
                withheld.append(outcome["entry"])

    return {
        "batch_id": str(batch["batch_id"]),
        "parent_document_id": str(batch["parent_document_id"]),
        "failure_class": failure_class,
        "failure_errors": failure_errors,
        "aligned_records": aligned_records,
        "withheld_records": withheld,
        "optional_field_repairs": repairs,
        "unresolved_target_blocks": unresolved_blocks,
        "model_anchor_count": len(target_by_slot),
        "aligned_record_count": len(aligned_records),
        "withheld_record_count": len(withheld),
        "retry_required": False,
    }


def _project_one_record(
    *,
    model_record: Mapping[str, Any],
    model_record_index: int,
    slot: str,
    target: Mapping[str, Any],
    source_line_id: str,
    block_text: str,
    block_start: int,
    block_end: int,
    descriptors: Sequence[Mapping[str, Any]],
    owner_segment_id: str,
    parent: Mapping[str, Any],
    batch: Mapping[str, Any],
    contract_sha256: str,
    slot_projection: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Project one model record, or explain exactly why it cannot be projected."""
    base = {
        "source_line_id": source_line_id,
        "stage_b_slot": slot,
        "model_record_index": model_record_index,
    }
    try:
        item, repair_changes = _v2_shape_from_round4_record(
            model_record, block_text
        )
    except Round4ProjectionError as error:
        return {
            "kind": "unresolved",
            "entry": {**base, "reason": "invalid_model_record", "detail": str(error)},
        }

    # Every name part must project to an exact ordered span inside its own
    # target block.  This is the check that the 11 known unresolved blocks in
    # the independent sample fail.
    spans = resolve_ordered_name_parts(model_record["name_parts"], block_text)
    if not spans:
        return {
            "kind": "unresolved",
            "entry": {
                **base,
                "reason": "name_parts_have_no_ordered_source_projection",
                "name_parts": [str(part) for part in model_record["name_parts"]],
            },
        }

    carriers = [
        descriptor for descriptor in descriptors
        if block_start >= int(descriptor["segment_start_char"])
        and block_end <= int(descriptor["segment_end_char"])
    ]
    if not carriers:
        return {
            "kind": "withheld",
            "entry": {
                **base,
                "reason": "record_evidence_has_no_single_source_segment_carrier",
            },
        }
    carrier = min(
        carriers,
        key=lambda descriptor: (
            str(descriptor["segment_id"]) != owner_segment_id,
            int(descriptor["segment_index"]),
            str(descriptor["segment_id"]),
        ),
    )
    carried_context = carrier["endorsement_context_provenance"]
    support_mode = _support_mode(block_text, item)
    # The record block can state the endorsement by itself.  When it does and
    # the carrier segment carries no separate heading, the block is its own
    # context evidence.  The public schema requires context evidence for an
    # explicit record, so without this rule such a record could never publish.
    direct_support_context = bool(
        support_mode == "explicit_text"
        and not (
            carried_context.get("source_scope") == "segment"
            and carried_context.get("context_text") is not None
        )
    )
    projected_model_record = {
        **copy.deepcopy(item),
        "record_evidence": block_text,
        "endorsement_context_evidence": (
            block_text if direct_support_context
            else carried_context.get("context_text")
        ),
        "support_mode": support_mode,
    }
    carrier_start = int(carrier["segment_start_char"])
    record_span = (block_start - carrier_start, block_end - carrier_start)
    fixed_spans = {"record_evidence": record_span}
    if direct_support_context:
        fixed_spans["endorsement_context_evidence"] = record_span
    elif (
        carried_context.get("source_scope") == "segment"
        and _is_int(carried_context.get("context_local_start_char"))
        and _is_int(carried_context.get("context_local_end_char"))
    ):
        fixed_spans["endorsement_context_evidence"] = (
            int(carried_context["context_local_start_char"]),
            int(carried_context["context_local_end_char"]),
        )
    aligned, aligned_withheld, report = align_extraction_exact_spans(
        "endorsements",
        {"endorsements": [projected_model_record]},
        str(carrier["source_text"]),
        endorsement_context_provenance=carried_context,
        segment_start_char=carrier_start,
        fixed_anchor_spans={0: fixed_spans},
    )
    if aligned_withheld or aligned is None or len(aligned["endorsements"]) != 1:
        return {
            "kind": "withheld",
            "entry": {
                **base,
                "reason": "carrier_exact_alignment_failed",
                "carrier_segment_id": carrier["segment_id"],
                "carrier_alignment_report": report,
                "carrier_withheld": aligned_withheld,
            },
        }
    aligned_item = aligned["endorsements"][0]
    resolved_start = carrier_start + int(aligned_item["record_evidence_start_char"])
    resolved_end = carrier_start + int(aligned_item["record_evidence_end_char"])
    if (resolved_start, resolved_end) != (block_start, block_end):
        return {
            "kind": "withheld",
            "entry": {
                **base,
                "reason": "carrier_alignment_differs_from_fixed_target_block",
                "carrier_segment_id": carrier["segment_id"],
                "carrier_parent_start_char": resolved_start,
                "carrier_parent_end_char": resolved_end,
            },
        }

    if direct_support_context and (
        aligned_item.get("endorsement_context_evidence") != block_text
        or aligned_item.get("endorsement_context_evidence_parent_start_char")
        != block_start
        or aligned_item.get("endorsement_context_evidence_parent_end_char")
        != block_end
        or aligned_item.get("endorsement_context_evidence_source_scope")
        != "segment"
    ):
        return {
            "kind": "withheld",
            "entry": {
                **base,
                "reason": "direct_support_context_projection_mismatch",
                "carrier_segment_id": carrier["segment_id"],
            },
        }

    final = dict(aligned_item)
    carrier_source_model_item_id = final["source_model_item_id"]
    final["source_model_item_id"] = sha256_json({
        "round4_production_contract_sha256": contract_sha256,
        "stage_b_batch_id": str(batch["batch_id"]),
        "stage_b_slot": slot,
        "model_record_index": model_record_index,
        "model_record": model_record,
        "deterministic_projection": projected_model_record,
    })
    final.update({
        "line_inventory_id": parent["parent_line_inventory_sha256"],
        "source_line_id": source_line_id,
        "stage_a_batch_id": target["stage_a_batch_id"],
        "stage_a_slot": target["stage_a_slot"],
        "stage_b_batch_id": str(batch["batch_id"]),
        "stage_b_slot": slot,
        "model_record_index": model_record_index,
        "carrier_alignment_source_model_item_id": carrier_source_model_item_id,
        "inventory_owner_segment_id": owner_segment_id,
        "record_evidence_carrier_segment_id": carrier["segment_id"],
        "target_block_alignment_method": TARGET_BLOCK_ALIGNMENT_METHOD,
        "target_block_exact_match_count": 1,
        "record_evidence_parent_start_char": resolved_start,
        "record_evidence_parent_end_char": resolved_end,
    })
    repairs = [
        {
            "source_line_id": source_line_id,
            "stage_b_slot": slot,
            "model_record_index": model_record_index,
            **change,
        }
        for change in repair_changes
    ]
    _ = slot_projection
    return {"kind": "record", "record": final, "repairs": repairs}


def round4_parent_coverage(
    parent_document_id: str,
    stage_a_rows: Sequence[Mapping[str, Any]],
    batch_projections: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Summarize one parent under the round-four coverage rule.

    Round four has no adjudication stage.  A Stage-A ``unclear`` line and an
    unowned continuation line therefore stay open, and they make their parent
    incomplete.  A ``not_endorsement`` decision is resolved and keeps the
    parent complete.
    """
    unclear_lines: list[str] = []
    for row in stage_a_rows:
        output = row.get("model_output") or {}
        for slot, decision in output.items():
            if isinstance(decision, Mapping) and decision.get("role") == "unclear":
                unclear_lines.append(f"{row.get('batch_id')}:{slot}")

    aligned = sum(int(item["aligned_record_count"]) for item in batch_projections)
    withheld = sum(int(item["withheld_record_count"]) for item in batch_projections)
    unresolved = [
        entry
        for item in batch_projections
        for entry in item["unresolved_target_blocks"]
    ]
    retry_required = any(bool(item["retry_required"]) for item in batch_projections)
    repairs = [
        entry
        for item in batch_projections
        for entry in item["optional_field_repairs"]
    ]
    return {
        "parent_document_id": str(parent_document_id),
        "aligned_record_count": aligned,
        "withheld_record_count": withheld,
        "stage_a_unclear_lines": sorted(unclear_lines),
        "unresolved_target_blocks": unresolved,
        "optional_field_repairs": repairs,
        "retry_required": retry_required,
        "public_coverage_complete": (
            not unclear_lines and not unresolved and not retry_required
        ),
        "processing_status": _parent_processing_status(
            aligned_record_count=aligned,
            withheld_record_count=withheld,
            retry_required=retry_required,
        ),
    }


def _parent_processing_status(
    *, aligned_record_count: int, withheld_record_count: int,
    retry_required: bool,
) -> str | None:
    """Return the overlay processing status for one parent.

    A parent with an open retryable failure has no status yet, so this returns
    ``None`` and the ledger build refuses it.  A parent with no aligned record
    and at least one withheld record reports a withheld status, so the packager
    can never read it as a measured zero.
    """
    if retry_required:
        return None
    if withheld_record_count and not aligned_record_count:
        return "processed_all_withheld"
    if withheld_record_count:
        return "processed_partial_withheld"
    return "processed_success"


def build_round4_record_ledger(
    parent_summaries: Sequence[Mapping[str, Any]],
    *,
    stage_a_input_manifest_sha256: str,
    stage_a_generation_config_sha256: str,
    stage_b_input_manifest_sha256: str,
    stage_b_generation_config_sha256: str,
    stage_b_response_count: int,
    accounted_output_count: int,
) -> dict[str, Any]:
    """Assemble the ledger summary the packager and validator read."""
    summaries = [dict(item) for item in parent_summaries]
    unresolved_infrastructure = sum(
        1 for item in summaries if item.get("retry_required")
    )
    if unresolved_infrastructure:
        raise Round4ProjectionError(
            "cannot build the ledger while a retryable failure remains open"
        )
    unaccounted = int(stage_b_response_count) - int(accounted_output_count)
    contract = get_round4_production_contract()
    summary = {
        "version": RECORD_LEDGER_VERSION,
        "endorsement_architecture": ENDORSEMENT_ARCHITECTURE,
        "round4_production_contract": contract,
        "round4_production_contract_sha256": contract[
            "round4_production_contract_sha256"
        ],
        "stage_a_input_manifest_sha256": stage_a_input_manifest_sha256,
        "stage_a_generation_config_sha256": stage_a_generation_config_sha256,
        "stage_b_input_manifest_sha256": stage_b_input_manifest_sha256,
        "stage_b_generation_config_sha256": stage_b_generation_config_sha256,
        "parent_count": len(summaries),
        "aligned_record_count": sum(
            int(item["aligned_record_count"]) for item in summaries
        ),
        "withheld_record_count": sum(
            int(item["withheld_record_count"]) for item in summaries
        ),
        "stage_b_unresolved_target_blocks": sum(
            len(item["unresolved_target_blocks"]) for item in summaries
        ),
        "stage_b_optional_field_repairs": sum(
            len(item["optional_field_repairs"]) for item in summaries
        ),
        "stage_a_excluded_continuation_lines": sum(
            len(item.get("excluded_continuation_lines", []))
            for item in summaries
        ),
        "parents_complete": sum(
            1 for item in summaries if item["public_coverage_complete"]
        ),
        # The corpus gate is an accounting gate, not a coverage boolean.  Every
        # gap must carry a typed reason and an identifier.
        "all_model_records_accounted": unaccounted == 0,
        "unaccounted_outputs": unaccounted,
        "unresolved_infrastructure_failures": unresolved_infrastructure,
    }
    summary["parent_summaries_sha256"] = sha256_json(summaries)
    return {"summary": summary, "parents": summaries}


def apply_endorsement_round4_ledger(
    segment_row: Mapping[str, Any], parent_summary: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Overlay the round-four result onto one one-stage endorsement row.

    The overlay keeps the original one-stage row, saves its superseded status,
    empties the one-stage record variants, and replaces the resolved records.
    It mirrors the round-three overlay so the packager reads one shape.
    """
    _require(
        str(segment_row.get("route")) == "endorsements",
        "the round-four overlay applies only to an endorsement row",
    )
    row = copy.deepcopy(dict(segment_row))
    row["superseded_one_stage_processing_status"] = row.get("processing_status")
    row["processing_status"] = parent_summary["processing_status"]
    row["record_variants"] = []
    row["resolved_records"] = [copy.deepcopy(dict(item)) for item in records]
    row["extraction_architecture"] = ENDORSEMENT_ARCHITECTURE
    row["endorsement_round4_record_ledger"] = {
        "parent_document_id": parent_summary["parent_document_id"],
        "public_coverage_complete": parent_summary["public_coverage_complete"],
        "aligned_record_count": parent_summary["aligned_record_count"],
        "withheld_record_count": parent_summary["withheld_record_count"],
        "unresolved_target_blocks": copy.deepcopy(
            list(parent_summary["unresolved_target_blocks"])
        ),
        "stage_a_unclear_lines": copy.deepcopy(
            list(parent_summary["stage_a_unclear_lines"])
        ),
    }
    return row


def validate_endorsement_round4_record_ledger_private_rows(
    ledger: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed unless the private ledger is internally exact.

    The check never trusts a reported total.  It recomputes every count from
    the parent summaries and refuses a ledger that still carries an open
    retryable failure.
    """
    _require(isinstance(ledger, Mapping), "ledger is not an object")
    summary = ledger.get("summary")
    parents = ledger.get("parents")
    _require(isinstance(summary, Mapping), "ledger summary is not an object")
    _require(isinstance(parents, list), "ledger parents is not a list")
    _require(
        summary.get("version") == RECORD_LEDGER_VERSION,
        "ledger version is not the round-four record ledger",
    )
    _require(
        summary.get("endorsement_architecture") == ENDORSEMENT_ARCHITECTURE,
        "ledger architecture is not the round-four record ledger",
    )
    contract = get_round4_production_contract()
    _require(
        summary.get("round4_production_contract_sha256")
        == contract["round4_production_contract_sha256"],
        "ledger contract hash differs from the frozen production contract",
    )
    _require(
        summary.get("round4_production_contract") == contract,
        "ledger contract payload differs from the frozen production contract",
    )
    identifiers = {str(item["parent_document_id"]) for item in parents}
    _require(
        len(identifiers) == len(parents), "ledger parents repeat an identity"
    )
    for item in parents:
        _require(isinstance(item, Mapping), "ledger parent is not an object")
        _require(
            not item.get("retry_required"),
            "ledger retains an unresolved retryable failure",
        )
        for entry in item["unresolved_target_blocks"]:
            _require(
                str(entry.get("reason") or ""),
                "an unresolved target block carries no typed reason",
            )
            _require(
                str(entry.get("source_line_id") or entry.get("stage_b_slot") or ""),
                "an unresolved target block carries no identifier",
            )
    recomputed = {
        "parent_count": len(parents),
        "aligned_record_count": sum(
            int(item["aligned_record_count"]) for item in parents
        ),
        "withheld_record_count": sum(
            int(item["withheld_record_count"]) for item in parents
        ),
        "stage_b_unresolved_target_blocks": sum(
            len(item["unresolved_target_blocks"]) for item in parents
        ),
        "parents_complete": sum(
            1 for item in parents if item["public_coverage_complete"]
        ),
    }
    for key, value in recomputed.items():
        _require(
            summary.get(key) == value,
            f"ledger summary {key} differs from the recomputed value",
        )
    _require(
        summary.get("parent_summaries_sha256") == sha256_json(
            [dict(item) for item in parents]
        ),
        "ledger parent summaries differ from their digest",
    )
    _require(
        summary.get("unresolved_infrastructure_failures") == 0,
        "ledger retains an unresolved infrastructure failure",
    )
    _require(
        summary.get("all_model_records_accounted") is True
        and summary.get("unaccounted_outputs") == 0,
        "ledger does not account for every model output",
    )
    return dict(recomputed)


__all__ = [
    "ENDORSEMENT_ARCHITECTURE",
    "RECORD_LEDGER_VERSION",
    "ROUND4_RECORD_IDENTITY",
    "ROUND4_RECORD_RESOLUTION_METHOD",
    "ROUND4_RECORD_RESOLUTION_STATUS",
    "ROUND4_FAILURE_CLASSES",
    "Round4ProjectionError",
    "TARGET_BLOCK_ALIGNMENT_METHOD",
    "apply_endorsement_round4_ledger",
    "build_round4_record_ledger",
    "classify_stage_b_response",
    "parent_inventory_from_export_rows",
    "project_round4_stage_b_batch",
    "round4_parent_coverage",
    "validate_endorsement_round4_record_ledger_private_rows",
]
