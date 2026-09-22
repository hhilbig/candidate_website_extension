"""Round-four development semantics for candidate-endorsement extraction.

This module is deliberately separate from the frozen round-three contracts.
It defines prompts, guided-output schemas, and deterministic source checks for
bounded development runs.  Nothing here authorizes production integration,
full expansion, or release, and provisional AI labels are not human truth.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping, Sequence
from typing import Any


ROUND4_VERSION = "candidate_endorsement_round4_development_semantics_v1"
ROUND4_DEVELOPMENT_BINDING = {
    "version": ROUND4_VERSION,
    "scope": "bounded_prompt_and_rule_development_only",
    "development_labels_authoritative": False,
    "human_truth_established": False,
    "production_integration_authorized": False,
    "full_expansion_authorized": False,
    "release_authorized": False,
}

STAGE_A_SLOT_COUNT = 16
STAGE_B_SLOT_COUNT = 4
MAX_RECORDS_PER_STAGE_B_SLOT = 32
MAX_NAME_PARTS = 4

STAGE_A_ROLES = (
    "record_anchor",
    "compound_record_anchor",
    "record_continuation_previous",
    "record_continuation_next",
    "support_or_category_heading",
    "candidate_self",
    "navigation_form_footer",
    "nonrecord_content",
    "unclear",
    "padding",
)
STAGE_B_DISPOSITIONS = (
    "records",
    "not_endorsement",
    "unclear",
    "padding",
)
ENTITY_TYPES = ("individual", "organization", "unclear")
ENDORSEMENT_CATEGORIES = (
    "private_individual",
    "government_official",
    "party_official",
    "labor_union",
    "media_outlet",
    "other_organization",
    "other_individual",
    "unclear",
)
CATEGORY_ALLOWED_ENTITY_TYPES = {
    "private_individual": ("individual",),
    "government_official": ("individual",),
    "party_official": ("individual",),
    "labor_union": ("organization",),
    "media_outlet": ("organization",),
    "other_organization": ("organization",),
    "other_individual": ("individual",),
    "unclear": ENTITY_TYPES,
}
OPTIONAL_FIELDS = ("title", "affiliation", "jurisdiction")


ROUND4_STAGE_A_PROMPT = """Classify every TARGET line from an archived U.S.
federal candidate endorsement page. Return exactly one decision for every
supplied slot. Only a slot explicitly marked PADDING may return padding.

First determine endorsement direction. A record anchor identifies a person or
organization endorsing the page candidate. A sentence that merely names the
page candidate as the object of someone else's endorsement is not, by itself,
an anchor for the page candidate. Candidates listed in a third-party slate are
also not endorsers of the page candidate. Classify those slate candidates,
offices, districts, and list headings as nonrecord_content. The organization
issuing the slate may be an anchor only when the source says that organization
endorsed the page candidate.

Use record_anchor for one endorser identity and compound_record_anchor for two
or more endorser identities. Read coordination markers case-insensitively:
"and", "And", and "&" have the same role. A coordinated mention may share a
surname printed after the final given name, including when that surname is on
the next owned line. A separately named local, affiliate, lodge, chapter, or
member organization is its own anchor. A line containing both an organization
and a named speaker is compound only if the source independently presents both
as endorsers; a title or affiliation does not create a second endorser.

Use record_continuation_previous or record_continuation_next for a title,
affiliation, jurisdiction, testimonial, surname, alias, suffix, or other
evidence that belongs to exactly one adjacent anchor. Distinguish headings,
candidate self-references, navigation/forms/footers, and unrelated content.
Interface labels, section labels, dates, and clipped fragments are not anchors.
Use unclear when the visible source does not support a unique classification.

Only TARGET lines receive decisions. CONTEXT lines are read-only evidence. Do
not infer missing identities or use outside knowledge. Output JSON only."""


ROUND4_STAGE_B_PROMPT = """For every TARGET block from an archived U.S.
federal candidate endorsement page, return exactly one extraction decision.
Return a decision for every supplied slot. Only a slot explicitly marked
PADDING may return padding.

Recheck the Stage-A anchor against the visible source. Stage A is a proposal,
not a binding truth label. Use disposition records only when the target block
identifies one or more people or organizations endorsing the page candidate.
Use the supplied endorsement heading and list structure as source evidence. A
standalone identity listed within an explicit endorsement section is sufficient
for records; it does not need its own testimonial sentence. Use
not_endorsement only when the visible evidence supports that decision.
Use not_endorsement when it is a false anchor, including candidate self-text,
interface text, headings, dates, fragments, and candidates in a third-party
slate. Use unclear when the evidence cannot support a unique decision. For
records and not_endorsement, source_mentions_fully_accounted must be true. For
unclear it must be false. Never force the Stage-A ordinary or compound count.

Return one record per endorser identity and account for every endorser mention
in the block. Each record uses name_parts instead of a rewritten name. Copy one
to four exact, ordered source substrings into name_parts. A deterministic
validator joins those parts and projects their source spans. A name_part may
contain spaces. Normally copy the complete printed name as one name_part; the
four-part limit is not a four-word limit. Use multiple parts only when one
identity is printed in non-contiguous substrings. For a coordinated individual
mention with a shared surname, reuse the exact surname substring in each
person's name_parts. Treat "and", "And", and "&" as equivalent
coordination markers. Copy the complete visible identity, not a shortened
brand, stem, or prefix. For organizations, include every visible word that is
part of the displayed name, including endings such as Association, Council,
Club, Committee, Union, Members, Fund, PAC, a parenthetical abbreviation, or a
local number. Do not split one printed organization identity into multiple
records. Preserve generational suffixes, parenthetical nicknames, digits,
punctuation, and source abbreviations that are part of identity. Exclude a
trailing comma, semicolon, colon, bullet, quotation mark, or dash when it only
separates list entries or surrounding prose; preserve a dash that connects
parts of the printed identity. Do not put honorifics, offices, military ranks,
professional roles, credentials, awards, or affiliations in name_parts. Copy
those only to an appropriate optional field when the exact source supports it.
Return at most one record for each source identity within a slot.

Each record also contains entity_type, endorser_category, and nullable title,
affiliation, and jurisdiction. Copy a non-null optional field as one exact,
contiguous substring of the visible target block; otherwise return null. Never
infer, silently repair OCR, modernize punctuation, or copy an identity from a
neighboring slot. The name matcher tolerates only NFKC-equivalent characters,
case differences, and Unicode-whitespace differences; punctuation remains
significant. This tolerance is not permission to rewrite source text.

Provisional development labels are not authoritative and are never model
instructions. This development contract does not authorize production
integration, full expansion, or release. Output JSON only."""


def canonical_json(value: Any) -> str:
    """Return the stable JSON representation used for semantic hashes."""
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def sha256_text(value: str) -> str:
    """Return a lowercase SHA-256 digest for UTF-8 text."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    """Return a lowercase SHA-256 digest for canonical JSON."""
    return sha256_text(canonical_json(value))


ROUND4_STAGE_A_PROMPT_SHA256 = sha256_text(ROUND4_STAGE_A_PROMPT)
ROUND4_STAGE_B_PROMPT_SHA256 = sha256_text(ROUND4_STAGE_B_PROMPT)
ROUND4_DEVELOPMENT_BINDING_SHA256 = sha256_json(ROUND4_DEVELOPMENT_BINDING)


def _validate_slot_counts(target_count: int, slot_count: int) -> None:
    if type(target_count) is not int or type(slot_count) is not int:
        raise TypeError("target_count and slot_count must be integers")
    if slot_count < 1:
        raise ValueError("slot_count must be positive")
    if not 1 <= target_count <= slot_count:
        raise ValueError("target_count must be between one and slot_count")


def _fixed_slot_schema(
    *,
    target_count: int,
    slot_count: int,
    target_schema: Mapping[str, Any],
    padding_schema: Mapping[str, Any],
) -> dict[str, Any]:
    _validate_slot_counts(target_count, slot_count)
    properties: dict[str, Any] = {}
    for index in range(slot_count):
        properties[f"slot_{index:02d}"] = copy.deepcopy(
            target_schema if index < target_count else padding_schema
        )
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def stage_a_schema(
    target_count: int, slot_count: int = STAGE_A_SLOT_COUNT,
) -> dict[str, Any]:
    """Build the fixed-slot round-four Stage-A guided-output schema."""
    target = {
        "type": "object",
        "properties": {
            "role": {
                "type": "string",
                "enum": [role for role in STAGE_A_ROLES if role != "padding"],
            },
        },
        "required": ["role"],
        "additionalProperties": False,
    }
    padding = {
        "type": "object",
        "properties": {"role": {"type": "string", "const": "padding"}},
        "required": ["role"],
        "additionalProperties": False,
    }
    return _fixed_slot_schema(
        target_count=target_count,
        slot_count=slot_count,
        target_schema=target,
        padding_schema=padding,
    )


def _record_schema() -> dict[str, Any]:
    nullable = {
        "oneOf": [
            {"type": "string", "minLength": 1},
            {"type": "null"},
        ]
    }
    category_variants: list[dict[str, Any]] = []
    for category in ENDORSEMENT_CATEGORIES:
        category_variants.append({
            "type": "object",
            "properties": {
                "name_parts": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                    "minItems": 1,
                    "maxItems": MAX_NAME_PARTS,
                },
                "entity_type": {
                    "type": "string",
                    "enum": list(CATEGORY_ALLOWED_ENTITY_TYPES[category]),
                },
                "endorser_category": {
                    "type": "string",
                    "const": category,
                },
                "title": copy.deepcopy(nullable),
                "affiliation": copy.deepcopy(nullable),
                "jurisdiction": copy.deepcopy(nullable),
            },
            "required": [
                "name_parts",
                "entity_type",
                "endorser_category",
                "title",
                "affiliation",
                "jurisdiction",
            ],
            "additionalProperties": False,
        })
    return {"oneOf": category_variants}


def _stage_b_target_schema() -> dict[str, Any]:
    common_required = [
        "disposition", "source_mentions_fully_accounted", "records",
    ]
    records_branch = {
        "type": "object",
        "properties": {
            "disposition": {"type": "string", "const": "records"},
            "source_mentions_fully_accounted": {
                "type": "boolean", "const": True,
            },
            "records": {
                "type": "array",
                "items": _record_schema(),
                "minItems": 1,
                "maxItems": MAX_RECORDS_PER_STAGE_B_SLOT,
            },
        },
        "required": common_required,
        "additionalProperties": False,
    }
    no_record_branch = {
        "type": "object",
        "properties": {
            "disposition": {"type": "string", "const": "not_endorsement"},
            "source_mentions_fully_accounted": {
                "type": "boolean", "const": True,
            },
            "records": {
                "type": "array", "items": _record_schema(), "maxItems": 0,
            },
        },
        "required": common_required,
        "additionalProperties": False,
    }
    unclear_branch = {
        "type": "object",
        "properties": {
            "disposition": {"type": "string", "const": "unclear"},
            "source_mentions_fully_accounted": {
                "type": "boolean", "const": False,
            },
            "records": {
                "type": "array", "items": _record_schema(), "maxItems": 0,
            },
        },
        "required": common_required,
        "additionalProperties": False,
    }
    return {"oneOf": [records_branch, no_record_branch, unclear_branch]}


def _stage_b_padding_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "disposition": {"type": "string", "const": "padding"},
            "source_mentions_fully_accounted": {
                "type": "boolean", "const": True,
            },
            "records": {
                "type": "array", "items": _record_schema(), "maxItems": 0,
            },
        },
        "required": [
            "disposition", "source_mentions_fully_accounted", "records",
        ],
        "additionalProperties": False,
    }


def stage_b_schema(
    target_count: int, slot_count: int = STAGE_B_SLOT_COUNT,
) -> dict[str, Any]:
    """Build the fixed-slot, disposition-based Stage-B schema."""
    return _fixed_slot_schema(
        target_count=target_count,
        slot_count=slot_count,
        target_schema=_stage_b_target_schema(),
        padding_schema=_stage_b_padding_schema(),
    )


def sanitize_optional_fields(
    record: Mapping[str, Any], visible_text: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Null optional fields not copied as exact contiguous source substrings.

    The input record is never mutated.  The returned change ledger makes each
    deterministic nulling operation auditable.
    """
    if not isinstance(record, Mapping):
        raise TypeError("record must be a mapping")
    if type(visible_text) is not str:
        raise TypeError("visible_text must be a string")
    sanitized = copy.deepcopy(dict(record))
    changes: list[dict[str, Any]] = []
    for field in OPTIONAL_FIELDS:
        if field not in sanitized or sanitized[field] is None:
            continue
        value = sanitized[field]
        if type(value) is str and value and value in visible_text:
            continue
        changes.append({
            "field": field,
            "original_value": copy.deepcopy(value),
            "replacement_value": None,
            "reason": "not_exact_contiguous_visible_substring",
        })
        sanitized[field] = None
    return sanitized, changes


def normalize_name_for_match(value: str) -> str:
    """Normalize NFKC, Unicode whitespace, and case while preserving punctuation."""
    if type(value) is not str:
        raise TypeError("name value must be a string")
    return " ".join(unicodedata.normalize("NFKC", value).split()).casefold()


def _normalization_clusters(value: str) -> list[tuple[int, int]]:
    clusters: list[tuple[int, int]] = []
    index = 0
    while index < len(value):
        start = index
        index += 1
        if value[start].isspace():
            while index < len(value) and value[index].isspace():
                index += 1
        else:
            while index < len(value) and unicodedata.combining(value[index]):
                index += 1
        clusters.append((start, index))
    return clusters


def _normalized_source_with_offsets(
    source: str,
) -> tuple[str, list[int], list[int]]:
    normalized_characters: list[str] = []
    starts: list[int] = []
    ends: list[int] = []
    for start, end in _normalization_clusters(source):
        normalized_cluster = unicodedata.normalize(
            "NFKC", source[start:end]
        ).casefold()
        for character in normalized_cluster:
            if character.isspace():
                if normalized_characters and normalized_characters[-1] == " ":
                    ends[-1] = end
                else:
                    normalized_characters.append(" ")
                    starts.append(start)
                    ends.append(end)
            else:
                normalized_characters.append(character)
                starts.append(start)
                ends.append(end)
    return "".join(normalized_characters), starts, ends


def _is_word_character(value: str) -> bool:
    return value.isalnum() or value == "_"


def normalized_source_matches(value: str, source: str) -> list[tuple[int, int]]:
    """Locate conservative normalized matches as source-codepoint spans.

    Matching applies NFKC, Unicode-whitespace collapse, and casefolding.  It
    preserves punctuation, rejects partial word matches, and returns half-open
    ``(start, end)`` offsets into the original source string.
    """
    if type(source) is not str:
        raise TypeError("source must be a string")
    needle = normalize_name_for_match(value)
    if not needle:
        return []
    haystack, starts, ends = _normalized_source_with_offsets(source)
    matches: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    search_from = 0
    while True:
        position = haystack.find(needle, search_from)
        if position < 0:
            break
        start = starts[position]
        end = ends[position + len(needle) - 1]
        starts_inside_word = (
            _is_word_character(needle[0])
            and start > 0
            and _is_word_character(source[start - 1])
        )
        ends_inside_word = (
            _is_word_character(needle[-1])
            and end < len(source)
            and _is_word_character(source[end])
        )
        span = (start, end)
        if (
            not starts_inside_word
            and not ends_inside_word
            and normalize_name_for_match(source[start:end]) == needle
            and span not in seen
        ):
            matches.append(span)
            seen.add(span)
        search_from = position + 1
    return matches


def _validated_name_parts(name_parts: Sequence[str]) -> tuple[str, ...]:
    if isinstance(name_parts, (str, bytes)) or not isinstance(
        name_parts, Sequence
    ):
        raise TypeError("name_parts must be a sequence of strings")
    parts = tuple(name_parts)
    if not 1 <= len(parts) <= MAX_NAME_PARTS:
        raise ValueError("name_parts must contain one to four parts")
    if any(type(part) is not str or not part.strip() for part in parts):
        raise ValueError("every name part must be a nonempty string")
    return parts


def resolve_ordered_name_parts(
    name_parts: Sequence[str], source: str,
) -> list[tuple[int, int]]:
    """Resolve the earliest nonoverlapping ordered source spans for name parts."""
    parts = _validated_name_parts(name_parts)
    candidates = [normalized_source_matches(part, source) for part in parts]
    if any(not values for values in candidates):
        return []

    def choose(index: int, previous_end: int) -> list[tuple[int, int]] | None:
        if index == len(candidates):
            return []
        for span in candidates[index]:
            if span[0] < previous_end:
                continue
            remainder = choose(index + 1, span[1])
            if remainder is not None:
                return [span, *remainder]
        return None

    return choose(0, 0) or []


def canonical_name_from_parts(name_parts: Sequence[str]) -> str:
    """Join validated ordered source parts without changing punctuation."""
    return " ".join(" ".join(_validated_name_parts(name_parts)).split())


_COORDINATION_PATTERN = re.compile(r"\s*(?:&|\band\b)\s*", re.IGNORECASE)


def split_compound_individual_name_parts(
    mention: str,
) -> tuple[tuple[str, ...], ...]:
    """Parse explicit coordinated individual names, including shared surnames.

    The parser is intentionally conservative and must not be applied to
    organization names.  It returns an empty tuple when it cannot identify a
    complete name for every coordinated person.
    """
    if type(mention) is not str:
        raise TypeError("mention must be a string")
    pieces = tuple(
        piece.strip() for piece in _COORDINATION_PATTERN.split(mention.strip())
    )
    if len(pieces) < 2 or any(not piece for piece in pieces):
        return ()
    words = tuple(tuple(piece.split()) for piece in pieces)
    if all(len(part) == 1 for part in words[:-1]) and len(words[-1]) == 2:
        surname = words[-1][1]
        return tuple((part[0], surname) for part in words)
    if all(len(part) >= 2 for part in words):
        return tuple((piece,) for piece in pieces)
    return ()


__all__ = [
    "ROUND4_DEVELOPMENT_BINDING",
    "ROUND4_DEVELOPMENT_BINDING_SHA256",
    "ROUND4_STAGE_A_PROMPT",
    "ROUND4_STAGE_A_PROMPT_SHA256",
    "ROUND4_STAGE_B_PROMPT",
    "ROUND4_STAGE_B_PROMPT_SHA256",
    "canonical_name_from_parts",
    "normalize_name_for_match",
    "normalized_source_matches",
    "resolve_ordered_name_parts",
    "sanitize_optional_fields",
    "split_compound_individual_name_parts",
    "stage_a_schema",
    "stage_b_schema",
]
