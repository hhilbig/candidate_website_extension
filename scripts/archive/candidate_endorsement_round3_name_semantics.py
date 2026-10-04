#!/usr/bin/env python3
"""Isolated round-three human-name truth and aligned-record accounting.

Authoritative truth comes only from explicit human child rows. Source-name
heuristics are limited to model-output lint and a non-authoritative line-break
proposal. This module is not imported by the frozen production pipeline.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import hashlib
import json
import math
import re
import unicodedata
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit


HUMAN_CHILD_ROW_FIELDS = frozenset({
    "entity_type",
    "name_spans",
    "support_spans",
})

PRODUCTION_ALIGNED_RECORD_FIELDS = frozenset({
    "affiliation",
    "carrier_alignment_source_model_item_id",
    "endorsement_context_evidence",
    "endorsement_context_evidence_alignment_method",
    "endorsement_context_evidence_end_char",
    "endorsement_context_evidence_exact_match_count",
    "endorsement_context_evidence_parent_end_char",
    "endorsement_context_evidence_parent_start_char",
    "endorsement_context_evidence_source_scope",
    "endorsement_context_evidence_start_char",
    "endorsement_context_id",
    "endorsement_context_qualifying_original_path",
    "endorsement_context_qualifying_original_url",
    "endorsement_context_qualifying_wayback_url",
    "endorsement_context_record_distance_chars",
    "endorser_category",
    "endorser_name",
    "entity_type",
    "inventory_owner_segment_id",
    "jurisdiction",
    "line_inventory_id",
    "model_record_index",
    "title",
    "record_evidence",
    "record_evidence_alignment_method",
    "record_evidence_carrier_segment_id",
    "record_evidence_end_char",
    "record_evidence_exact_match_count",
    "record_evidence_parent_start_char",
    "record_evidence_parent_end_char",
    "record_evidence_start_char",
    "source_line_id",
    "source_model_item_id",
    "stage_a_batch_id",
    "stage_a_slot",
    "stage_b_batch_id",
    "stage_b_slot",
    "support_mode",
    "target_block_alignment_method",
    "target_block_exact_match_count",
})

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_STAGE_A_SLOT_PATTERN = re.compile(r"slot_(?:0[0-9]|1[0-5])\Z")
_STAGE_B_SLOT_PATTERN = re.compile(r"slot_0[0-7]\Z")
_VALIDATED_OBJECT_SEAL = object()
_EXPLICIT_SUPPORT_PATTERN = re.compile(
    r"\b(?:endors(?:e|es|ed|ing)|support(?:s|ed|ing)?|back(?:s|ed|ing)|"
    r"recommend(?:s|ed|ing)?|vote\s+for|stand(?:s|ing)?\s+with)\b",
    re.IGNORECASE,
)
_STRUCTURAL_SUPPORT_PATTERN = re.compile(
    r"\b(?:endors(?:ements?|ers?)|endorsed\s+by|supporters?|supported\s+by|"
    r"backed\s+by|who(?:'s|\s+is)\s+supporting|"
    r"proud(?:ly)?\s+(?:endorsed|supported|backed))\b",
    re.IGNORECASE,
)
_WAYBACK_ORIGINAL_PATTERN = re.compile(
    r"https?://web\.archive\.org/web/\d+[^/]*/(?P<original>.+)\Z"
)
_ENDORSEMENT_ROUTE_FIRST_SEGMENTS = {
    "campaign-supporters", "endorsement", "endorsements", "supporters",
}

ROUND3_NAME_SEMANTICS_SPEC = {
    "version": "candidate_endorsement_round3_name_semantics_v3",
    "authoritative_truth": (
        "one_distinct_explicit_human_child_row_per_identity;_multiplicity_of_"
        "rows_with_distinct_coordinates_is_truth_multiplicity;_no_source_"
        "heuristic_or_stage_b_field_may_create_split_merge_or_type_a_truth_child"
    ),
    "human_child_row_fields": sorted(HUMAN_CHILD_ROW_FIELDS),
    "human_child_row_rule": (
        "at_least_one_row_required;_entity_type_is_individual_organization_or_"
        "unresolved;_name_spans_and_support_spans_are_ordered_nonempty_exact_"
        "parent_coordinate_lists;_an_unresolved_child_cannot_be_fully_accounted"
    ),
    "production_aligned_record_fields": sorted(PRODUCTION_ALIGNED_RECORD_FIELDS),
    "aligned_record_rule": (
        "strict_exact_40_field_production_schema;_validate_every_field_type_and_"
        "every_locally_checkable_cross_field_relation;_exact_record_and_context_"
        "parent_coordinates_or_route_context_null_coordinates;_reject_extra_"
        "fields_wrong_types_booleans_nonfinite_numbers_and_duplicate_record_"
        "identities;_require_the_containing_stage_b_row_technical_pass_flag"
    ),
    "accounting_rule": (
        "require_exact_name_entity_type_row_multiset;_record_evidence_contains_"
        "every_child_name_span;_each_child_support_span_may_be_contained_by_"
        "either_the_record_or_context_carrier;_multiple_children_may_share_one_"
        "carrier"
    ),
    "name_affix_rule": (
        "known_title_rank_honorific_retired_marker_and_credential_detection_is_"
        "model_output_lint_only;_lint_does_not_rewrite_or_create_truth"
    ),
    "line_break_rule": (
        "non_authoritative_shared_surname_proposal_only;_exact_consecutive_"
        "source_coordinates_and_declared_stage_a_owner_required"
    ),
    "stage_a_output_ownership_validation": (
        "pending_authenticated_stage_a_output_binding_before_any_production_use"
    ),
    "adapter_input_authentication": (
        "caller_must_supply_records_from_an_authenticated_technically_passing_"
        "stage_b_artifact_row;_"
        "shape_and_coordinate_validation_does_not_authenticate_artifact_"
        "ownership_or_recompute_segment_scope_counts_without_the_carrier_segment"
    ),
    "human_input_authentication": (
        "pending_a_new_frozen_human_task_and_results_contract_that_supplies_one_"
        "child_row_per_identity_with_entity_type_name_spans_and_support_spans"
    ),
}


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


ROUND3_NAME_SEMANTICS_SPEC_SHA256 = hashlib.sha256(
    _canonical_json(ROUND3_NAME_SEMANTICS_SPEC).encode("utf-8")
).hexdigest()


def _normalized_space(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).split())


def _normalized_name(value: str) -> str:
    return _normalized_space(value).casefold()


def _require_nonblank_string(value: Any, field: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{field} must be a nonblank string")
    return value


def _require_nullable_nonblank_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _require_nonblank_string(value, field)


def _require_nonnegative_integer(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return value


def _require_integer(value: Any, field: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{field} must be an integer")
    return value


def _require_nullable_nonnegative_integer(value: Any, field: str) -> int | None:
    if value is None:
        return None
    return _require_nonnegative_integer(value, field)


def _require_nullable_integer(value: Any, field: str) -> int | None:
    if value is None:
        return None
    return _require_integer(value, field)


def _require_sha256(value: Any, field: str) -> str:
    value = _require_nonblank_string(value, field)
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 hex string")
    return value


def _reject_nonfinite(value: Any, *, path: str = "value") -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} contains a nonfinite number")
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_nonfinite(child, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for index, child in enumerate(value):
            _reject_nonfinite(child, path=f"{path}[{index}]")


@dataclass(frozen=True, order=True)
class SourceSpan:
    """A nonempty zero-based half-open span in a complete parent source."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if type(self.start) is not int or type(self.end) is not int:
            raise TypeError("source span coordinates must be integers, not booleans")
        if self.start < 0 or self.end <= self.start:
            raise ValueError("source span must be nonempty and nonnegative")

    def contains(self, other: "SourceSpan") -> bool:
        return self.start <= other.start and other.end <= self.end


@dataclass(frozen=True)
class HumanTruthChild:
    canonical_name: str
    entity_type: str
    name_spans: tuple[SourceSpan, ...]
    support_spans: tuple[SourceSpan, ...]


@dataclass(frozen=True)
class HumanTruthNameUnit:
    parent_document_id: str
    parent_source_start_char: int
    parent_source_n_chars: int
    parent_source_text_sha256: str
    children: tuple[HumanTruthChild, ...]
    _validation_seal: object | None = field(
        default=None, init=False, repr=False, compare=False
    )
    _integrity_sha256: str = field(
        default="", init=False, repr=False, compare=False
    )

    @property
    def canonical_entity_names(self) -> tuple[str, ...]:
        return tuple(child.canonical_name for child in self.children)


@dataclass(frozen=True)
class IndividualNameLint:
    canonical_candidate: str
    errors: tuple[str, ...]
    removed_exact_title: str | None
    removed_title_prefixes: tuple[str, ...]
    removed_title_suffixes: tuple[str, ...]
    removed_credentials: tuple[str, ...]
    removed_nonidentity_suffixes: tuple[str, ...]


@dataclass(frozen=True)
class AlignedRecordScoringView:
    parent_document_id: str
    parent_source_start_char: int
    parent_source_n_chars: int
    parent_source_text_sha256: str
    endorser_name: str
    entity_type: str
    title: str | None
    record_evidence: str
    record_span: SourceSpan
    endorsement_context_evidence: str | None
    context_span: SourceSpan | None
    support_mode: str
    name_lint_errors: tuple[str, ...]
    source_model_item_id: str
    stage_b_batch_id: str
    stage_b_slot: str
    model_record_index: int
    _validation_seal: object | None = field(
        default=None, init=False, repr=False, compare=False
    )
    _integrity_sha256: str = field(
        default="", init=False, repr=False, compare=False
    )

    @property
    def record_identity(self) -> tuple[str, str, int]:
        return (
            self.stage_b_batch_id,
            self.stage_b_slot,
            self.model_record_index,
        )


def _human_unit_integrity_sha256(unit: HumanTruthNameUnit) -> str:
    payload = {
        "parent_document_id": unit.parent_document_id,
        "parent_source_start_char": unit.parent_source_start_char,
        "parent_source_n_chars": unit.parent_source_n_chars,
        "parent_source_text_sha256": unit.parent_source_text_sha256,
        "children": [
            {
                "canonical_name": child.canonical_name,
                "entity_type": child.entity_type,
                "name_spans": [[span.start, span.end] for span in child.name_spans],
                "support_spans": [
                    [span.start, span.end] for span in child.support_spans
                ],
            }
            for child in unit.children
        ],
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _aligned_view_integrity_sha256(record: AlignedRecordScoringView) -> str:
    payload = {
        "parent_document_id": record.parent_document_id,
        "parent_source_start_char": record.parent_source_start_char,
        "parent_source_n_chars": record.parent_source_n_chars,
        "parent_source_text_sha256": record.parent_source_text_sha256,
        "endorser_name": record.endorser_name,
        "entity_type": record.entity_type,
        "title": record.title,
        "record_evidence": record.record_evidence,
        "record_span": [record.record_span.start, record.record_span.end],
        "endorsement_context_evidence": record.endorsement_context_evidence,
        "context_span": (
            [record.context_span.start, record.context_span.end]
            if record.context_span is not None else None
        ),
        "support_mode": record.support_mode,
        "name_lint_errors": list(record.name_lint_errors),
        "source_model_item_id": record.source_model_item_id,
        "stage_b_batch_id": record.stage_b_batch_id,
        "stage_b_slot": record.stage_b_slot,
        "model_record_index": record.model_record_index,
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _seal_validated_object(value: HumanTruthNameUnit | AlignedRecordScoringView) -> None:
    object.__setattr__(value, "_validation_seal", _VALIDATED_OBJECT_SEAL)
    digest = (
        _human_unit_integrity_sha256(value)
        if isinstance(value, HumanTruthNameUnit)
        else _aligned_view_integrity_sha256(value)
    )
    object.__setattr__(value, "_integrity_sha256", digest)


_TITLE_CORE = (
    r"the\s+honorable|honorable|hon\.?|doctor|dr\.?|reverend|rev\.?|"
    r"pastor|rabbi|imam|professor|prof\.?|"
    r"captain|capt\.?|rear\s+admiral|vice\s+admiral|admiral|adm\.?|"
    r"u\.s\.\s+senator|state\s+senator|senator|sen\.?|"
    r"u\.s\.\s+representative|state\s+representative|representative|rep\.?|"
    r"congressman|congresswoman|governor|gov\.?|mayor|sheriff|delegate|"
    r"judge|justice|president|vice\s+president|"
    r"chairman|chairwoman|chairperson|chair|"
    r"lieutenant\s+general(?:\s*\(ret\.?\))?|"
    r"lt\.?\s+general(?:\s*\(ret\.?\))?|"
    r"major\s+general(?:\s*\(ret\.?\))?|"
    r"brigadier\s+general(?:\s*\(ret\.?\))?|"
    r"general(?:\s*\(ret\.?\))?|"
    r"lieutenant\s+colonel(?:\s*\(ret\.?\))?|"
    r"lt\.?\s+colonel(?:\s*\(ret\.?\))?|"
    r"colonel(?:\s*\(ret\.?\))?|col\.?(?:\s*\(ret\.?\))?|"
    r"major(?:\s*\(ret\.?\))?|maj\.?(?:\s*\(ret\.?\))?"
)
_TITLE_PREFIX_PATTERN = re.compile(
    rf"^(?P<title>(?:former\s+)?(?:{_TITLE_CORE}))(?=\s|,)(?:\s*,?\s*)",
    re.IGNORECASE,
)
_KNOWN_TITLE_SUFFIX_PATTERN = re.compile(
    rf"(?:\s*,\s*|\s+[-\u2013\u2014]\s*)"
    rf"(?P<title>(?:{_TITLE_CORE})(?:\s+[^,]+)?)$",
    re.IGNORECASE,
)
_CREDENTIAL_KEYS = {
    "ba", "bba", "bs", "cfa", "cpa", "dds", "dmd", "do", "dvm", "edd",
    "esq", "fache", "jd", "llm", "ma", "mba", "md", "med", "mph", "ms",
    "msw", "pe", "pharmd", "phd", "pmp", "rn",
}
_GENERATIONAL_SUFFIX_KEYS = {"jr", "sr", "ii", "iii", "iv", "v"}
_NONIDENTITY_SUFFIX_KEYS = {"ret", "retired"}
_LINE_FINAL_LAYOUT_DASH = re.compile(r"[ \t]+[-\u2013\u2014][ \t]*$")
_SIMPLE_NAME_TOKEN = re.compile(r"[A-Z][A-Za-z'\u2019.-]*\Z")
_NON_PERSON_SHARED_GIVEN_TOKENS = {
    "action", "allied", "authority", "commercial", "community",
    "education", "family", "food", "health", "issues", "law", "leader",
    "life", "menu", "order", "political", "research", "safety", "workers",
}


def _affix_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", unicodedata.normalize("NFKC", value).casefold())


def _strip_exact_title(
    value: str,
    exact_title: str | None,
) -> tuple[str, str | None]:
    if type(exact_title) is not str or not exact_title.strip():
        return value, None
    title = _normalized_space(exact_title)
    prefix = re.compile(
        rf"^(?P<title>{re.escape(title)})(?=\s|,)(?:\s*,?\s*)",
        re.IGNORECASE,
    )
    match = prefix.match(value)
    if match is not None and value[match.end():].strip(" ,:;-\u2013\u2014"):
        return value[match.end():].strip(), match.group("title")
    suffix = re.compile(
        rf"(?:\s*,\s*|\s+[-\u2013\u2014]\s*|\s+)"
        rf"(?P<title>{re.escape(title)})$",
        re.IGNORECASE,
    )
    match = suffix.search(value)
    if match is not None and value[:match.start()].strip(" ,:;-\u2013\u2014"):
        return value[:match.start()].strip(), match.group("title")
    return value, None


def lint_individual_name(
    endorser_name: str,
    *,
    exact_title: str | None = None,
) -> IndividualNameLint:
    """Lint one model-produced individual name without changing truth."""
    _require_nonblank_string(endorser_name, "endorser_name")
    value = _normalized_space(endorser_name).strip(" \t,:;-\u2013\u2014")
    value, removed_exact_title = _strip_exact_title(value, exact_title)
    prefixes: list[str] = []
    suffixes: list[str] = []
    credentials: list[str] = []
    nonidentity: list[str] = []

    while True:
        changed = False
        match = _TITLE_PREFIX_PATTERN.match(value)
        if match is not None and value[match.end():].strip(" ,:;-\u2013\u2014"):
            prefixes.append(match.group("title"))
            value = value[match.end():].strip()
            changed = True
            continue

        tail = re.search(
            r"(?P<delimiter>\s*,\s*|\s+)(?P<value>\(?[^,\s]+\)?)[ \t]*$",
            value,
        )
        if tail is not None:
            token = tail.group("value")
            key = _affix_key(token)
            unwrapped = token.strip("()")
            has_affix_syntax = (
                "," in tail.group("delimiter")
                or "." in unwrapped
                or (len(unwrapped) >= 2 and unwrapped.isupper())
            )
            if key in _CREDENTIAL_KEYS and has_affix_syntax:
                credentials.append(token)
                value = value[:tail.start()].rstrip(" ,;")
                changed = True
                continue
            if key in _NONIDENTITY_SUFFIX_KEYS and has_affix_syntax:
                nonidentity.append(token)
                value = value[:tail.start()].rstrip(" ,;")
                changed = True
                continue

        match = _KNOWN_TITLE_SUFFIX_PATTERN.search(value)
        if match is not None and value[:match.start()].strip(" ,:;-\u2013\u2014"):
            suffixes.append(match.group("title"))
            value = value[:match.start()].rstrip(" ,:;-\u2013\u2014")
            changed = True
            continue
        if not changed:
            break

    value = value.strip(" \t,:;-\u2013\u2014")
    errors: list[str] = []
    if removed_exact_title is not None:
        errors.append("name_repeats_exact_title")
    if prefixes or suffixes:
        errors.append("name_includes_title_office_or_rank")
    if credentials:
        errors.append("name_includes_postnominal_credentials")
    if nonidentity:
        errors.append("name_includes_retired_marker")
    if not value:
        errors.append("invalid_individual_name")
    return IndividualNameLint(
        canonical_candidate=value,
        errors=tuple(errors),
        removed_exact_title=removed_exact_title,
        removed_title_prefixes=tuple(prefixes),
        removed_title_suffixes=tuple(reversed(suffixes)),
        removed_credentials=tuple(reversed(credentials)),
        removed_nonidentity_suffixes=tuple(reversed(nonidentity)),
    )


def individual_name_semantic_errors(
    endorser_name: str,
    *,
    entity_type: str,
    title: str | None = None,
) -> tuple[str, ...]:
    """Return model-output lint errors; organization names bypass person lint."""
    if entity_type != "individual":
        return ()
    try:
        return lint_individual_name(
            endorser_name,
            exact_title=title,
        ).errors
    except ValueError:
        return ("invalid_individual_name",)


def _source_sha256(source_text: str) -> str:
    return hashlib.sha256(source_text.encode("utf-8")).hexdigest()


def _projected_support_mode(
    evidence: str,
    *,
    endorser_name: str,
    title: str | None,
    affiliation: str | None,
    jurisdiction: str | None,
) -> str:
    masked = evidence
    values = sorted(
        {
            value
            for value in (endorser_name, title, affiliation, jurisdiction)
            if value is not None and value
        },
        key=len,
        reverse=True,
    )
    for value in values:
        masked = masked.replace(value, " " * len(value))
    return (
        "explicit_text"
        if _EXPLICIT_SUPPORT_PATTERN.search(masked) is not None
        else "page_or_section_context"
    )


def _source_slice(
    source_text: str,
    span: SourceSpan,
    *,
    source_start_char: int,
) -> str:
    local_start = span.start - source_start_char
    local_end = span.end - source_start_char
    if local_start < 0 or local_end > len(source_text):
        raise ValueError("source span falls outside the supplied parent source")
    return source_text[local_start:local_end]


def _strict_span_list(
    value: Any,
    *,
    field: str,
    source_text: str,
    source_start_char: int,
) -> tuple[SourceSpan, ...]:
    if type(value) is not list or not value:
        raise ValueError(f"{field} must be a nonempty list")
    spans: list[SourceSpan] = []
    previous: SourceSpan | None = None
    for index, pair in enumerate(value):
        if type(pair) is not list or len(pair) != 2:
            raise ValueError(f"{field}[{index}] must be a two-integer list")
        try:
            span = SourceSpan(pair[0], pair[1])
        except (TypeError, ValueError) as error:
            raise ValueError(f"{field}[{index}] has invalid coordinates") from error
        if previous is not None and span.start < previous.end:
            raise ValueError(f"{field} must be ordered and nonoverlapping")
        if not _source_slice(
            source_text,
            span,
            source_start_char=source_start_char,
        ).strip():
            raise ValueError(f"{field}[{index}] selects only whitespace")
        spans.append(span)
        previous = span
    return tuple(spans)


def build_human_truth_name_unit(
    parent_source_text: str,
    human_child_rows: list[Mapping[str, Any]],
    *,
    parent_document_id: str,
    parent_source_start_char: int = 0,
) -> HumanTruthNameUnit:
    """Build authoritative truth from strict explicit human child rows."""
    _require_nonblank_string(parent_document_id, "parent_document_id")
    if type(parent_source_text) is not str:
        raise ValueError("parent_source_text must be a string")
    if type(parent_source_start_char) is not int or parent_source_start_char < 0:
        raise ValueError("parent_source_start_char must be a nonnegative integer")
    if type(human_child_rows) is not list or not human_child_rows:
        raise ValueError("human_child_rows must be a nonempty list")
    _reject_nonfinite(human_child_rows, path="human_child_rows")

    children: list[HumanTruthChild] = []
    for row_index, row in enumerate(human_child_rows):
        if type(row) is not dict:
            raise ValueError(f"human child row {row_index} must be an object")
        if set(row) != HUMAN_CHILD_ROW_FIELDS:
            raise ValueError(
                f"human child row {row_index} must have exact fields "
                f"{sorted(HUMAN_CHILD_ROW_FIELDS)}"
            )
        entity_type = row["entity_type"]
        if type(entity_type) is not str or entity_type not in {
            "individual", "organization", "unresolved"
        }:
            raise ValueError(
                f"human child row {row_index} has invalid entity_type"
            )
        name_spans = _strict_span_list(
            row["name_spans"],
            field=f"human_child_rows[{row_index}].name_spans",
            source_text=parent_source_text,
            source_start_char=parent_source_start_char,
        )
        support_spans = _strict_span_list(
            row["support_spans"],
            field=f"human_child_rows[{row_index}].support_spans",
            source_text=parent_source_text,
            source_start_char=parent_source_start_char,
        )
        canonical_name = _normalized_space(" ".join(
            _source_slice(
                parent_source_text,
                span,
                source_start_char=parent_source_start_char,
            )
            for span in name_spans
        ))
        if not canonical_name:
            raise ValueError(f"human child row {row_index} has an empty name")
        children.append(HumanTruthChild(
            canonical_name=canonical_name,
            entity_type=entity_type,
            name_spans=name_spans,
            support_spans=support_spans,
        ))
    child_keys = [child.name_spans for child in children]
    if len(set(child_keys)) != len(child_keys):
        raise ValueError("human child rows must not repeat identical coordinates")
    unit = HumanTruthNameUnit(
        parent_document_id=parent_document_id,
        parent_source_start_char=parent_source_start_char,
        parent_source_n_chars=len(parent_source_text),
        parent_source_text_sha256=_source_sha256(parent_source_text),
        children=tuple(children),
    )
    _seal_validated_object(unit)
    return unit


def adapt_aligned_record(
    record: Mapping[str, Any],
    *,
    stage_b_technical_pass: bool,
    parent_document_id: str,
    parent_source_text: str,
    parent_source_start_char: int = 0,
) -> AlignedRecordScoringView:
    """Validate one complete 40-field production aligned record for scoring."""
    if type(stage_b_technical_pass) is not bool or stage_b_technical_pass is not True:
        raise ValueError("containing Stage-B row must have technical_pass true")
    _require_nonblank_string(parent_document_id, "parent_document_id")
    if type(parent_source_text) is not str:
        raise ValueError("parent_source_text must be a string")
    if type(parent_source_start_char) is not int or parent_source_start_char < 0:
        raise ValueError("parent_source_start_char must be a nonnegative integer")
    if type(record) is not dict:
        raise ValueError("production aligned record must be an object")
    _reject_nonfinite(record, path="aligned_record")
    if set(record) != PRODUCTION_ALIGNED_RECORD_FIELDS:
        raise ValueError(
            "production aligned record must have exact fields "
            f"{sorted(PRODUCTION_ALIGNED_RECORD_FIELDS)}"
        )

    sha_fields = (
        "carrier_alignment_source_model_item_id",
        "endorsement_context_id",
        "inventory_owner_segment_id",
        "line_inventory_id",
        "record_evidence_carrier_segment_id",
        "source_line_id",
        "source_model_item_id",
        "stage_a_batch_id",
        "stage_b_batch_id",
    )
    validated_sha = {
        name: _require_sha256(record[name], name) for name in sha_fields
    }
    stage_a_slot = _require_nonblank_string(record["stage_a_slot"], "stage_a_slot")
    stage_b_slot = _require_nonblank_string(record["stage_b_slot"], "stage_b_slot")
    if _STAGE_A_SLOT_PATTERN.fullmatch(stage_a_slot) is None:
        raise ValueError("stage_a_slot must be within slot_00 through slot_15")
    if _STAGE_B_SLOT_PATTERN.fullmatch(stage_b_slot) is None:
        raise ValueError("stage_b_slot must be within slot_00 through slot_07")
    model_record_index = _require_nonnegative_integer(
        record["model_record_index"], "model_record_index"
    )
    if model_record_index >= 32:
        raise ValueError("model_record_index must be less than 32")

    endorser_name = _require_nonblank_string(
        record["endorser_name"], "endorser_name"
    )
    entity_type = record["entity_type"]
    if type(entity_type) is not str or entity_type not in {
        "individual", "organization", "unclear"
    }:
        raise ValueError(
            "aligned entity_type must be individual, organization, or unclear"
        )
    category = record["endorser_category"]
    allowed_categories = {
        "private_individual", "government_official", "party_official",
        "labor_union", "media_outlet", "other_organization",
        "other_individual", "unclear",
    }
    if type(category) is not str or category not in allowed_categories:
        raise ValueError("endorser_category is invalid")
    category_entity_types = {
        "private_individual": {"individual"},
        "government_official": {"individual"},
        "party_official": {"individual"},
        "other_individual": {"individual"},
        "labor_union": {"organization"},
        "media_outlet": {"organization"},
        "other_organization": {"organization"},
        "unclear": {"individual", "organization", "unclear"},
    }
    if entity_type not in category_entity_types[category]:
        raise ValueError("entity_type and endorser_category are incompatible")
    title = _require_nullable_nonblank_string(record["title"], "title")
    affiliation = _require_nullable_nonblank_string(
        record["affiliation"], "affiliation"
    )
    jurisdiction = _require_nullable_nonblank_string(
        record["jurisdiction"], "jurisdiction"
    )
    record_evidence = _require_nonblank_string(
        record["record_evidence"], "record_evidence"
    )
    context_evidence = record["endorsement_context_evidence"]
    support_mode = record["support_mode"]
    if type(support_mode) is not str or support_mode not in {
        "explicit_text", "page_or_section_context"
    }:
        raise ValueError("support_mode is invalid")
    projected_support_mode = _projected_support_mode(
        record_evidence,
        endorser_name=endorser_name,
        title=title,
        affiliation=affiliation,
        jurisdiction=jurisdiction,
    )
    if support_mode != projected_support_mode:
        raise ValueError("support_mode differs from the masked record projection")

    record_method = _require_nonblank_string(
        record["record_evidence_alignment_method"],
        "record_evidence_alignment_method",
    )
    if record_method != "provided_authenticated_parent_projection_v1":
        raise ValueError("record_evidence_alignment_method is invalid")
    target_method = _require_nonblank_string(
        record["target_block_alignment_method"],
        "target_block_alignment_method",
    )
    if target_method != "deterministic_authenticated_record_block_projection_v1":
        raise ValueError("target_block_alignment_method is invalid")
    if _require_nonnegative_integer(
        record["record_evidence_exact_match_count"],
        "record_evidence_exact_match_count",
    ) != 1:
        raise ValueError("record_evidence_exact_match_count must equal one")
    if _require_nonnegative_integer(
        record["target_block_exact_match_count"],
        "target_block_exact_match_count",
    ) != 1:
        raise ValueError("target_block_exact_match_count must equal one")

    try:
        local_record_span = SourceSpan(
            record["record_evidence_start_char"],
            record["record_evidence_end_char"],
        )
    except (TypeError, ValueError) as error:
        raise ValueError("aligned record local coordinates are invalid") from error
    try:
        record_span = SourceSpan(
            record["record_evidence_parent_start_char"],
            record["record_evidence_parent_end_char"],
        )
    except (TypeError, ValueError) as error:
        raise ValueError("aligned record parent coordinates are invalid") from error
    if _source_slice(
        parent_source_text,
        record_span,
        source_start_char=parent_source_start_char,
    ) != record_evidence:
        raise ValueError("record evidence does not match its exact parent span")
    if (
        local_record_span.end - local_record_span.start != len(record_evidence)
        or record_span.start - local_record_span.start
        != record_span.end - local_record_span.end
        or record_span.start - local_record_span.start < 0
    ):
        raise ValueError("record local and parent coordinates are inconsistent")

    context_scope = record["endorsement_context_evidence_source_scope"]
    if type(context_scope) is not str or context_scope not in {
        "segment", "parent_document", "page_or_section_route_context"
    }:
        raise ValueError("endorsement_context_evidence_source_scope is invalid")
    context_method = _require_nonblank_string(
        record["endorsement_context_evidence_alignment_method"],
        "endorsement_context_evidence_alignment_method",
    )
    context_count = _require_nonnegative_integer(
        record["endorsement_context_evidence_exact_match_count"],
        "endorsement_context_evidence_exact_match_count",
    )
    context_local_start = _require_nullable_nonnegative_integer(
        record["endorsement_context_evidence_start_char"],
        "endorsement_context_evidence_start_char",
    )
    context_local_end = _require_nullable_nonnegative_integer(
        record["endorsement_context_evidence_end_char"],
        "endorsement_context_evidence_end_char",
    )
    context_start = record["endorsement_context_evidence_parent_start_char"]
    context_end = record["endorsement_context_evidence_parent_end_char"]
    context_distance = _require_nullable_integer(
        record["endorsement_context_record_distance_chars"],
        "endorsement_context_record_distance_chars",
    )
    route_fields = (
        "endorsement_context_qualifying_wayback_url",
        "endorsement_context_qualifying_original_url",
        "endorsement_context_qualifying_original_path",
    )
    route_values = tuple(record[name] for name in route_fields)

    if context_scope == "page_or_section_route_context":
        if (
            context_evidence is not None
            or context_local_start is not None
            or context_local_end is not None
            or context_start is not None
            or context_end is not None
            or context_distance is not None
        ):
            raise ValueError("route context requires null evidence and coordinates")
        if support_mode != "page_or_section_context":
            raise ValueError("route context requires page_or_section_context support")
        if context_method != "page_or_section_route_context" or context_count != 0:
            raise ValueError("route context alignment metadata is invalid")
        for name, value in zip(route_fields, route_values):
            _require_nonblank_string(value, name)
        wayback_match = _WAYBACK_ORIGINAL_PATTERN.fullmatch(route_values[0])
        if wayback_match is None or wayback_match.group("original") != route_values[1]:
            raise ValueError("route context Wayback and original URLs disagree")
        original_parts = urlsplit(route_values[1])
        if (
            original_parts.scheme not in {"http", "https"}
            or not original_parts.netloc
            or not route_values[2].startswith("/")
        ):
            raise ValueError("route context original URL is invalid")
        if (original_parts.path or "/") != route_values[2]:
            raise ValueError("route context original URL and path disagree")
        route_segments = [
            segment.casefold()
            for segment in original_parts.path.split("/")
            if segment
        ]
        if (
            not route_segments
            or route_segments[0] not in _ENDORSEMENT_ROUTE_FIRST_SEGMENTS
        ):
            raise ValueError("route context path is not an endorsement-page route")
        context_span = None
    else:
        _require_nonblank_string(
            context_evidence,
            "endorsement_context_evidence",
        )
        try:
            context_span = SourceSpan(context_start, context_end)
        except (TypeError, ValueError) as error:
            raise ValueError("aligned context parent coordinates are invalid") from error
        if _source_slice(
            parent_source_text,
            context_span,
            source_start_char=parent_source_start_char,
        ) != context_evidence:
            raise ValueError("context evidence does not match its exact parent span")
        if not (
            _EXPLICIT_SUPPORT_PATTERN.search(context_evidence)
            or _STRUCTURAL_SUPPORT_PATTERN.search(context_evidence)
        ):
            raise ValueError("literal context lacks a support marker")
        if any(value is not None for value in route_values):
            raise ValueError("literal context requires null route URL provenance")
        if context_scope == "segment":
            try:
                local_context_span = SourceSpan(
                    context_local_start, context_local_end
                )
            except (TypeError, ValueError) as error:
                raise ValueError("segment context local coordinates are invalid") from error
            if (
                local_context_span.end - local_context_span.start
                != len(context_evidence)
                or context_span.start - local_context_span.start
                != context_span.end - local_context_span.end
                or context_span.start - local_context_span.start
                != record_span.start - local_record_span.start
            ):
                raise ValueError("segment context coordinates are inconsistent")
        else:
            if context_local_start is not None or context_local_end is not None:
                raise ValueError("parent-document context requires null local coordinates")
            if context_method != "provided_exact_parent_span" or context_count < 1:
                raise ValueError("parent-document context alignment metadata is invalid")
            exact_parent_count = sum(
                1
                for position in range(len(parent_source_text))
                if parent_source_text.startswith(context_evidence, position)
            )
            if context_count != exact_parent_count:
                raise ValueError("parent-document context exact-match count is invalid")

    if support_mode == "explicit_text":
        if context_scope != "segment":
            raise ValueError("explicit_text support requires segment context")
        if (
            context_method not in {
                "unique_exact_unicode_substring_v1",
                "unique_exact_inference_display_substring_v1",
            }
            or context_count != 1
            or context_distance is not None
        ):
            raise ValueError("explicit_text context alignment metadata is invalid")
        if _EXPLICIT_SUPPORT_PATTERN.search(record_evidence) is None:
            raise ValueError("explicit_text record lacks a direct support marker")
    elif context_scope in {"segment", "parent_document"}:
        expected_methods = (
            {
                "provided_exact_segment_span",
                "provided_exact_segment_span_after_context_extension_projection_v1",
            }
            if context_scope == "segment" else {"provided_exact_parent_span"}
        )
        if context_method not in expected_methods or context_count < 1:
            raise ValueError("carried context alignment metadata is invalid")
        if context_distance != record_span.start - context_span.end:
            raise ValueError("context distance is inconsistent with parent coordinates")

    lint_errors = individual_name_semantic_errors(
        endorser_name,
        entity_type=entity_type,
        title=title,
    )
    view = AlignedRecordScoringView(
        parent_document_id=parent_document_id,
        parent_source_start_char=parent_source_start_char,
        parent_source_n_chars=len(parent_source_text),
        parent_source_text_sha256=_source_sha256(parent_source_text),
        endorser_name=endorser_name,
        entity_type=entity_type,
        title=title,
        record_evidence=record_evidence,
        record_span=record_span,
        endorsement_context_evidence=context_evidence,
        context_span=context_span,
        support_mode=support_mode,
        name_lint_errors=lint_errors,
        source_model_item_id=validated_sha["source_model_item_id"],
        stage_b_batch_id=validated_sha["stage_b_batch_id"],
        stage_b_slot=stage_b_slot,
        model_record_index=model_record_index,
    )
    _seal_validated_object(view)
    return view


def _record_matches_child(
    record: AlignedRecordScoringView,
    child: HumanTruthChild,
) -> bool:
    if _normalized_name(record.endorser_name) != _normalized_name(
        child.canonical_name
    ):
        return False
    if record.entity_type != child.entity_type:
        return False
    if not all(record.record_span.contains(span) for span in child.name_spans):
        return False
    carriers = tuple(
        span for span in (record.record_span, record.context_span)
        if span is not None
    )
    return all(
        any(carrier.contains(span) for carrier in carriers)
        for span in child.support_spans
    )


def _group_has_perfect_matching(
    records: Sequence[AlignedRecordScoringView],
    children: Sequence[HumanTruthChild],
) -> bool:
    edges = [
        [
            child_index
            for child_index, child in enumerate(children)
            if _record_matches_child(record, child)
        ]
        for record in records
    ]
    matched_record_by_child: dict[int, int] = {}

    def assign(record_index: int, seen: set[int]) -> bool:
        for child_index in edges[record_index]:
            if child_index in seen:
                continue
            seen.add(child_index)
            previous = matched_record_by_child.get(child_index)
            if previous is None or assign(previous, seen):
                matched_record_by_child[child_index] = record_index
                return True
        return False

    return all(assign(index, set()) for index in range(len(records)))


def human_truth_unit_fully_accounted(
    unit: HumanTruthNameUnit,
    aligned_records: Sequence[AlignedRecordScoringView],
) -> bool:
    """Require exact human-row multiplicity and containment-backed evidence."""
    if not isinstance(unit, HumanTruthNameUnit):
        raise TypeError("unit must be a validated human truth name unit")
    if unit._validation_seal is not _VALIDATED_OBJECT_SEAL:
        raise TypeError("unit was not produced by the human truth builder")
    if unit._integrity_sha256 != _human_unit_integrity_sha256(unit):
        raise TypeError("validated human truth unit was modified after construction")
    records = list(aligned_records)
    if any(not isinstance(record, AlignedRecordScoringView) for record in records):
        raise TypeError("aligned_records must contain only validated scoring views")
    if any(record._validation_seal is not _VALIDATED_OBJECT_SEAL for record in records):
        raise TypeError("aligned record was not produced by the production adapter")
    if any(
        record._integrity_sha256 != _aligned_view_integrity_sha256(record)
        for record in records
    ):
        raise TypeError("validated aligned record was modified after construction")
    if any(child.entity_type == "unresolved" for child in unit.children):
        return False
    if len(records) != len(unit.children):
        return False
    identities = [record.record_identity for record in records]
    source_ids = [record.source_model_item_id for record in records]
    if len(set(identities)) != len(identities) or len(set(source_ids)) != len(source_ids):
        return False
    if any(
        record.parent_document_id != unit.parent_document_id
        or record.parent_source_start_char != unit.parent_source_start_char
        or record.parent_source_n_chars != unit.parent_source_n_chars
        or record.parent_source_text_sha256 != unit.parent_source_text_sha256
        for record in records
    ):
        return False

    expected = Counter(
        (_normalized_name(child.canonical_name), child.entity_type)
        for child in unit.children
    )
    observed = Counter(
        (_normalized_name(record.endorser_name), record.entity_type)
        for record in records
    )
    if observed != expected:
        return False
    grouped_children: dict[tuple[str, str], list[HumanTruthChild]] = {}
    grouped_records: dict[tuple[str, str], list[AlignedRecordScoringView]] = {}
    for child in unit.children:
        key = (_normalized_name(child.canonical_name), child.entity_type)
        grouped_children.setdefault(key, []).append(child)
    for record in records:
        key = (_normalized_name(record.endorser_name), record.entity_type)
        grouped_records.setdefault(key, []).append(record)
    return all(
        _group_has_perfect_matching(grouped_records[key], grouped_children[key])
        for key in expected
    )


_RECORD_BLOCK_LINE_FIELDS = frozenset({
    "source_line_id",
    "line_text",
    "line_terminator",
    "parent_line_index",
    "parent_start_char",
    "parent_end_char",
    "parent_full_end_char",
    "stage_a_role",
    "continuation_owner_source_line_id",
})


def _validated_record_block_lines(
    record_block_lines: Sequence[Mapping[str, Any]],
    *,
    parent_source_text: str,
    parent_source_start_char: int,
) -> list[Mapping[str, Any]]:
    if type(record_block_lines) is not list or not record_block_lines:
        raise ValueError("record_block_lines must be a nonempty list")
    if type(parent_source_text) is not str:
        raise ValueError("parent_source_text must be a string")
    if type(parent_source_start_char) is not int or parent_source_start_char < 0:
        raise ValueError("parent_source_start_char must be a nonnegative integer")
    _reject_nonfinite(record_block_lines, path="record_block_lines")
    lines = list(record_block_lines)
    window_end = parent_source_start_char + len(parent_source_text)
    previous: Mapping[str, Any] | None = None
    seen_ids: set[str] = set()
    for position, line in enumerate(lines):
        if type(line) is not dict or set(line) != _RECORD_BLOCK_LINE_FIELDS:
            raise ValueError("record-block line must have the exact line fields")
        source_line_id = _require_nonblank_string(
            line["source_line_id"], "source_line_id"
        )
        if source_line_id in seen_ids:
            raise ValueError("source_line_id must be unique within a record block")
        seen_ids.add(source_line_id)
        line_text = line["line_text"]
        terminator = line["line_terminator"]
        if (
            type(line_text) is not str
            or type(terminator) is not str
            or terminator not in {"", "\n", "\r\n"}
        ):
            raise ValueError("line text or terminator is invalid")
        role = line["stage_a_role"]
        owner = line["continuation_owner_source_line_id"]
        if type(role) is not str or not role:
            raise ValueError("stage_a_role must be a nonblank string")
        if owner is not None:
            _require_nonblank_string(owner, "continuation_owner_source_line_id")
        if position < len(lines) - 1 and not terminator:
            raise ValueError("nonfinal record-block lines require a terminator")
        coordinates = (
            line["parent_line_index"],
            line["parent_start_char"],
            line["parent_end_char"],
            line["parent_full_end_char"],
        )
        if any(type(value) is not int for value in coordinates):
            raise ValueError("line coordinates must be integers")
        line_index, start, end, full_end = coordinates
        if line_index < 0 or not (
            parent_source_start_char <= start <= end <= full_end <= window_end
        ):
            raise ValueError("line coordinates are out of range")
        if end - start != len(line_text) or full_end - end != len(terminator):
            raise ValueError("line coordinate lengths do not match the line")
        span = SourceSpan(start, end) if end > start else None
        if span is None or _source_slice(
            parent_source_text,
            span,
            source_start_char=parent_source_start_char,
        ) != line_text:
            raise ValueError("line text does not match its source coordinates")
        terminator_start = end - parent_source_start_char
        terminator_end = full_end - parent_source_start_char
        if parent_source_text[terminator_start:terminator_end] != terminator:
            raise ValueError("line terminator does not match its source coordinates")
        if previous is not None:
            if line_index != previous["parent_line_index"] + 1:
                raise ValueError("record-block line indices must be consecutive")
            if start != previous["parent_full_end_char"]:
                raise ValueError("record-block line offsets must be consecutive")
        previous = line
    return lines


def derive_line_broken_person_name(
    record_block_lines: list[Mapping[str, Any]],
    anchor_source_line_id: str,
    *,
    parent_source_text: str,
    parent_source_start_char: int = 0,
) -> str | None:
    """Return a non-authoritative explicit shared-surname proposal only.

    Declared Stage-A ownership is checked locally. Binding those declarations
    to authenticated Stage-A output remains required before production use.
    """
    lines = _validated_record_block_lines(
        record_block_lines,
        parent_source_text=parent_source_text,
        parent_source_start_char=parent_source_start_char,
    )
    positions = [
        index for index, line in enumerate(lines)
        if line["source_line_id"] == anchor_source_line_id
    ]
    if len(positions) != 1:
        raise ValueError("anchor must occur exactly once in the record block")
    anchor_index = positions[0]
    if anchor_index + 1 >= len(lines):
        return None
    anchor = lines[anchor_index]
    continuation = lines[anchor_index + 1]
    if anchor["stage_a_role"] not in {"record_anchor", "compound_record_anchor"}:
        return None
    if anchor["continuation_owner_source_line_id"] is not None:
        return None
    if continuation["stage_a_role"] != "record_continuation_previous":
        return None
    if continuation["continuation_owner_source_line_id"] != anchor_source_line_id:
        return None

    anchor_text = _LINE_FINAL_LAYOUT_DASH.sub("", anchor["line_text"].strip()).strip()
    base = lint_individual_name(anchor_text).canonical_candidate
    shared = re.fullmatch(
        r"(?P<first>[A-Z][A-Za-z'\u2019.-]*)\s+"
        r"(?P<connector>and|&)\s+"
        r"(?P<second>[A-Z][A-Za-z'\u2019.-]*)",
        base,
        flags=re.IGNORECASE,
    )
    if shared is None:
        return None
    first = shared.group("first")
    second = shared.group("second")
    if not (_SIMPLE_NAME_TOKEN.fullmatch(first) and _SIMPLE_NAME_TOKEN.fullmatch(second)):
        return None
    if not any(char.islower() for char in first + second):
        return None
    if {_affix_key(first), _affix_key(second)} & _NON_PERSON_SHARED_GIVEN_TOKENS:
        return None
    surname = _normalized_space(continuation["line_text"])
    surname_tokens = surname.split()
    if not 1 <= len(surname_tokens) <= 2:
        return None
    if any(
        _SIMPLE_NAME_TOKEN.fullmatch(token) is None
        or not any(char.islower() for char in token)
        for token in surname_tokens
    ):
        return None
    connector = "&" if shared.group("connector") == "&" else "and"
    return f"{first} {connector} {second} {surname}"
