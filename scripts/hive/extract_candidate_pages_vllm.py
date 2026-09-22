#!/usr/bin/env python3
"""Run revision-pinned, evidence-linked candidate-page extraction on Hive."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import platform
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from src.candidate_extraction import (  # noqa: E402
    ARRAY_ITEM_CAP_REACHED_STATUS,
    ARRAY_ITEM_CAP_SPEC_ID,
    ARRAY_ITEM_CAP_SPEC_SHA256,
    DEFAULT_SCHEMA_VERSION,
    MODEL_ID,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    ENDORSEMENT_CONTEXT_USER_BLOCK_TEMPLATE_V2_1,
    INFERENCE_DISPLAY_ESCAPE_ID,
    INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256,
    JSON_STRING_CONTROL_CANONICALIZER_ID,
    JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256,
    MODEL_ITEM_REPETITION_ERROR_STATUS,
    MODEL_ITEM_REPETITION_SPEC_ID,
    MODEL_ITEM_REPETITION_SPEC_SHA256,
    USER_MESSAGE_TEMPLATE_V2_1,
    V2_1_MODEL_ITEM_CAPS,
    align_extraction_exact_spans,
    canonical_schema_version,
    get_extraction_contract,
    get_prompts,
    get_schemas,
    inference_display_escape,
    inference_display_provenance,
    model_item_array_cap_report,
    model_item_repetition_report,
    pre_alignment_model_item_guard_report,
    endorsement_context_provenance_errors,
    segment_overlap_status,
    validate_extraction,
)

COMPACT_REPAIR_RULE = """

REPAIR CONSTRAINTS FOR A PREVIOUSLY INVALID JSON RESPONSE:
- Return at most 25 issue blocks.
- Keep every evidence span at most 240 characters.
- Choose evidence that contains no line break and no quotation-mark character.
- Evidence must still be an exact contiguous substring of the supplied page text.
- Prefer shorter complete clauses; do not repeat or expand an item.
"""

LEGACY_GENERATION_CONFIG_VERSION = "candidate_extraction_generation_v1"
GENERATION_CONFIG_VERSION = "candidate_extraction_generation_v2"
STRUCTURED_DECODING_MODE = "json_schema"
STRUCTURED_DECODING_BACKEND = "xgrammar"
STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE = True
RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID = (
    "xgrammar_fixed_separator_json_whitespace_v1"
)
RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC = {
    "version": RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    "position": (
        "raw_model_output_before_json_string_control_canonicalization_and_"
        "strict_parse"
    ),
    "lexical_scope": "characters_outside_lexically_quoted_JSON_strings",
    "string_lexer": "double_quote_with_backslash_escape_state",
    "separator_rule": (
        "every_colon_or_comma_is_followed_by_exactly_one_U+0020"
    ),
    "space_rule": (
        "U+0020_is_forbidden_outside_strings_except_the_required_separator_space"
    ),
    "forbidden_outside_string_whitespace": ["U+0009", "U+000A", "U+000D"],
    "inside_string_rule": (
        "ignored_by_this_validator_and_left_to_json_string_control_canonicalizer"
    ),
    "syntax_boundary": (
        "all_nonwhitespace_JSON_syntax_remains_the_strict_JSON_parser_"
        "responsibility"
    ),
    "failure_status": "raw_structural_whitespace_error",
}
RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256 = hashlib.sha256(
    json.dumps(
        RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
FROZEN_RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256 = (
    "4a03ebdf798ceab3f841b7359d59ad17f34033c2f6e677edd74551935ffd8774"
)
if (
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
    != FROZEN_RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
):
    raise RuntimeError("raw structural-whitespace validator spec differs")
V2_1_MIN_OUTPUT_TOKENS = 24_576


class StrictJSONError(ValueError):
    """Raised when model output is not one unambiguous standards-valid JSON value."""


class RawStructuralWhitespaceError(ValueError):
    """Raised when raw v2.1 JSON violates xgrammar's fixed separators."""


class ArrayItemCapReachedError(ValueError):
    """Raised when a parsed route array exactly reaches its public-safe cap."""


class ModelItemRepetitionError(ValueError):
    """Raised when one canonical model item occurs at least three times."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def strict_json_loads(text: str) -> Any:
    """Reject duplicate keys and non-finite constants instead of repairing JSON."""
    def reject_constant(value: str) -> None:
        raise StrictJSONError(f"non-finite JSON constant is prohibited: {value}")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise StrictJSONError(f"duplicate JSON object key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(
            text,
            parse_constant=reject_constant,
            object_pairs_hook=unique_object,
        )
    except json.JSONDecodeError as error:
        raise StrictJSONError(str(error)) from error


def _json_control_escape(character: str) -> str:
    short = {
        "\b": "\\b",
        "\t": "\\t",
        "\n": "\\n",
        "\f": "\\f",
        "\r": "\\r",
    }.get(character)
    return short if short is not None else f"\\u{ord(character):04x}"


def _json_lexical_positions(text: str):
    """Yield characters with the shared JSON quote and escape state.

    ``inside_string`` and ``escape_pending`` describe the state immediately
    before the yielded character. This lexer deliberately does not decide
    whether the surrounding JSON syntax is valid; strict parsing remains a
    separate step.
    """
    inside_string = False
    escape_pending = False
    for index, character in enumerate(text):
        yield index, character, inside_string, escape_pending
        if not inside_string:
            if character == '"':
                inside_string = True
                escape_pending = False
            continue
        if escape_pending:
            escape_pending = False
        elif character == "\\":
            escape_pending = True
        elif character == '"':
            inside_string = False


def validate_raw_structural_whitespace(text: str) -> dict[str, Any]:
    """Attest xgrammar's fixed comma-space and colon-space separators.

    The pinned xgrammar grammar uses exactly one U+0020 after each comma and
    colon when arbitrary whitespace is disabled. No other structural JSON
    whitespace is permitted. Characters inside quoted strings are outside
    this check and remain subject to the narrow control canonicalizer.
    """
    if not isinstance(text, str):
        raise TypeError("raw model output must be a string")
    violations: dict[str, int] = {}
    separator_count = 0
    separator_space_count = 0
    previous_outside_separator = False

    def add_violation(kind: str) -> None:
        violations[kind] = violations.get(kind, 0) + 1

    for index, character, inside_string, _ in _json_lexical_positions(text):
        if inside_string:
            previous_outside_separator = False
            continue
        if character in (":", ","):
            separator_count += 1
            if index + 1 >= len(text) or text[index + 1] != " ":
                add_violation("delimiter_missing_fixed_U+0020")
            previous_outside_separator = True
            continue
        if character == " ":
            if previous_outside_separator:
                separator_space_count += 1
            else:
                add_violation("U+0020_outside_fixed_separator")
        elif character == "\t":
            add_violation("U+0009_outside_string")
        elif character == "\n":
            add_violation("U+000A_outside_string")
        elif character == "\r":
            add_violation("U+000D_outside_string")
        previous_outside_separator = False

    violation_count = sum(violations.values())
    return {
        "raw_structural_whitespace_validator_id": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        ),
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
        "raw_structural_whitespace_validation_passed": violation_count == 0,
        "raw_structural_separator_count": separator_count,
        "raw_structural_separator_space_count": separator_space_count,
        "raw_structural_whitespace_violation_count": violation_count,
        "raw_structural_whitespace_violation_counts": dict(
            sorted(violations.items())
        ),
    }


def canonicalize_json_string_controls(text: str) -> tuple[str, dict[str, Any]]:
    """Escape only raw C0 at unescaped JSON-string content positions.

    This is deliberately not a general JSON repair. Invalid escapes, duplicate
    keys, unbalanced quoting, non-finite constants, and controls following an
    unmatched escape introducer are left for :func:`strict_json_loads` to
    reject. The returned report attests both byte strings and every replacement.
    """
    if not isinstance(text, str):
        raise TypeError("raw model output must be a string")
    output: list[str] = []
    repaired: dict[str, int] = {}
    unrepaired_after_escape: dict[str, int] = {}
    permitted_outside_whitespace: dict[str, int] = {}
    invalid_outside_controls: dict[str, int] = {}
    for _, character, inside_string, escape_pending in _json_lexical_positions(text):
        codepoint = ord(character)
        label = f"U+{codepoint:04X}"
        if not inside_string:
            output.append(character)
            if character != '"' and codepoint < 0x20:
                target = (
                    permitted_outside_whitespace
                    if character in ("\t", "\n", "\r")
                    else invalid_outside_controls
                )
                target[label] = target.get(label, 0) + 1
            continue

        if escape_pending:
            output.append(character)
            if codepoint < 0x20:
                unrepaired_after_escape[label] = (
                    unrepaired_after_escape.get(label, 0) + 1
                )
            continue
        if character == "\\":
            output.append(character)
            continue
        if character == '"':
            output.append(character)
            continue
        if codepoint < 0x20:
            output.append(_json_control_escape(character))
            repaired[label] = repaired.get(label, 0) + 1
            continue
        output.append(character)

    canonical = "".join(output)
    raw_bytes = text.encode("utf-8")
    canonical_bytes = canonical.encode("utf-8")
    report = {
        "json_string_control_canonicalizer_id": (
            JSON_STRING_CONTROL_CANONICALIZER_ID
        ),
        "json_string_control_canonicalizer_spec_sha256": (
            JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256
        ),
        "raw_output_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "raw_output_n_chars": len(text),
        "raw_output_utf8_n_bytes": len(raw_bytes),
        "canonical_output_sha256": hashlib.sha256(canonical_bytes).hexdigest(),
        "canonical_output_n_chars": len(canonical),
        "canonical_output_utf8_n_bytes": len(canonical_bytes),
        "json_control_canonicalization_applied": canonical != text,
        "json_control_canonicalization_count": sum(repaired.values()),
        "json_control_canonicalization_codepoint_counts": dict(sorted(repaired.items())),
        "json_control_unrepaired_after_escape_count": sum(
            unrepaired_after_escape.values()
        ),
        "json_control_unrepaired_after_escape_codepoint_counts": dict(
            sorted(unrepaired_after_escape.items())
        ),
        "json_control_outside_string_permitted_whitespace_count": sum(
            permitted_outside_whitespace.values()
        ),
        "json_control_outside_string_permitted_whitespace_codepoint_counts": dict(
            sorted(permitted_outside_whitespace.items())
        ),
        "json_control_outside_string_invalid_count": sum(
            invalid_outside_controls.values()
        ),
        "json_control_outside_string_invalid_codepoint_counts": dict(
            sorted(invalid_outside_controls.items())
        ),
    }
    return canonical, report


def generation_config_payload(
    args: argparse.Namespace, contract: dict[str, Any]
) -> dict[str, Any]:
    """Return the complete, restart-bound deterministic inference identity."""
    schema_version = canonical_schema_version(args.schema_version)
    structured_decoding = {
        "mode": STRUCTURED_DECODING_MODE,
        "route_json_schema_sha256": contract["json_schema_sha256"],
    }
    generation_config_version = LEGACY_GENERATION_CONFIG_VERSION
    if schema_version == "2.1":
        generation_config_version = GENERATION_CONFIG_VERSION
        structured_decoding.update({
            "backend": STRUCTURED_DECODING_BACKEND,
            "disable_any_whitespace": STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        })
    payload = {
        "generation_config_version": generation_config_version,
        "schema_version": schema_version,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": contract["prompt_sha256"],
        "json_schema_sha256": contract["json_schema_sha256"],
        "extraction_contract_sha256": contract["extraction_contract_sha256"],
        "alignment_spec_sha256": contract.get("alignment_spec_sha256"),
        "semantic_validation_spec_sha256": contract.get(
            "semantic_validation_spec_sha256"
        ),
        "endorsement_context_provenance_spec_sha256": contract.get(
            "endorsement_context_provenance_spec_sha256"
        ),
        "user_message_template_id": contract.get("user_message_template_id"),
        "user_message_template_sha256": contract.get("user_message_template_sha256"),
        "seed": int(args.seed),
        "max_model_len": int(args.max_model_len),
        "max_output_tokens": int(args.max_output_tokens),
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": bool(args.compact_repair),
        "structured_decoding": structured_decoding,
    }
    if schema_version == "2.1":
        payload.update({
            "inference_display_escape_id": contract[
                "inference_display_escape_id"
            ],
            "inference_display_escape_spec_sha256": contract[
                "inference_display_escape_spec_sha256"
            ],
            "json_string_control_canonicalizer_id": contract[
                "json_string_control_canonicalizer_id"
            ],
            "json_string_control_canonicalizer_spec_sha256": contract[
                "json_string_control_canonicalizer_spec_sha256"
            ],
            "array_item_cap_spec_id": contract["array_item_cap_spec_id"],
            "array_item_cap_spec_sha256": contract[
                "array_item_cap_spec_sha256"
            ],
            "route_array_item_caps": contract["route_array_item_caps"],
            "model_item_repetition_spec_id": contract[
                "model_item_repetition_spec_id"
            ],
            "model_item_repetition_spec_sha256": contract[
                "model_item_repetition_spec_sha256"
            ],
        })
    return payload


def generation_config_sha256(
    args: argparse.Namespace, contract: dict[str, Any]
) -> str:
    return hashlib.sha256(
        canonical_json(generation_config_payload(args, contract)).encode("utf-8")
    ).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_gzip_jsonl(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def json_value_equal_typed(left: Any, right: Any) -> bool:
    """Compare JSON-shaped values without Python's bool/int/float coercions."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return set(left) == set(right) and all(
            json_value_equal_typed(left[key], right[key]) for key in left
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            json_value_equal_typed(left_item, right_item)
            for left_item, right_item in zip(left, right)
        )
    return bool(left == right)


_V2_1_PARSED_OUTPUT_CORE_FIELDS = frozenset({
    "document_id", "candidate_cycle_id", "route", "raw_text_sha256",
    "model_id", "model_revision", "runtime_lock_sha256", "schema_version",
    "prompt_sha256", "json_schema_sha256", "extraction_contract_sha256",
    "alignment_id", "alignment_spec_sha256",
    "semantic_validation_spec_sha256",
    "endorsement_context_provenance_spec_sha256",
    "user_message_template_id", "user_message_template_sha256",
    "user_message_sha256",
    "generation_config_sha256", "temperature", "max_output_tokens",
    "finish_reason", "stop_reason", "output_tokens", "thinking_enabled",
    "compact_repair", "input_truncated", "input_tokens_used",
    "input_chars_used", "structured_decoding_backend",
    "structured_decoding_disable_any_whitespace", "raw_output",
    "model_extraction", "model_extraction_sha256", "extraction",
    "aligned_extraction_sha256", "withheld_model_items",
    "pre_alignment_model_item_guard", "array_item_cap_report",
    "model_item_repetition_report", "exact_alignment", "parse_status",
    "validation", "route_array_item_cap", "array_item_cap_spec_id",
    "array_item_cap_spec_sha256", "model_item_repetition_spec_id",
    "model_item_repetition_spec_sha256",
})

_V2_1_COPIED_OPTIONAL_FIELDS = frozenset({
    "parent_document_id", "segment_index", "segment_count",
    "segment_start_char", "segment_end_char", "segment_text_sha256",
    "endorsement_context_provenance",
})


TRUNCATED_PARSE_STATUS = "output_truncated"


def truncated_row_is_terminal(row: dict[str, Any]) -> bool:
    """Report whether one cap-limited row is a terminal run outcome.

    The runner writes the whole row and the model's cap-limited output. It writes
    no parsed or aligned payload. Only a different seed can repair such a row, so
    the standard resume path must leave it in place instead of rewriting it.
    """
    if row.get("finish_reason") != "length":
        return False
    if row.get("parse_status") != TRUNCATED_PARSE_STATUS:
        return False
    if row.get("extraction") is not None:
        return False
    for field in (
        "model_extraction",
        "model_extraction_sha256",
        "aligned_extraction_sha256",
        "withheld_model_items",
        "exact_alignment",
        "validation",
    ):
        if field in row:
            return False
    raw_output = row.get("raw_output")
    if not isinstance(raw_output, str) or not raw_output:
        return False
    if row.get("raw_output_sha256") != hashlib.sha256(
        raw_output.encode("utf-8")
    ).hexdigest():
        return False
    max_output_tokens = row.get("max_output_tokens")
    output_tokens = row.get("output_tokens")
    if not isinstance(output_tokens, int) or isinstance(output_tokens, bool):
        return False
    if not isinstance(max_output_tokens, int) or isinstance(max_output_tokens, bool):
        return False
    return output_tokens == max_output_tokens


def chunk_is_complete(
    input_path: Path,
    output_path: Path,
    model_revision: str = MODEL_REVISION,
    schema_version: str | None = None,
    expected_generation_config_sha256: str | None = None,
    expected_max_output_tokens: int | None = None,
    *,
    accept_truncated_rows: bool = False,
) -> bool:
    if not output_path.exists():
        return False
    try:
        inputs = read_gzip_jsonl(input_path)
        outputs = read_gzip_jsonl(output_path)
    except (OSError, EOFError, json.JSONDecodeError):
        return False
    input_ids = [row.get("document_id") for row in inputs]
    output_ids = [row.get("document_id") for row in outputs]
    identity_matches = (
        input_ids == output_ids
        and len(input_ids) == len(set(input_ids))
        and all(row.get("model_revision") == model_revision for row in outputs)
    )
    if not identity_matches or schema_version is None:
        return identity_matches
    version = canonical_schema_version(schema_version)
    contract = get_extraction_contract(version)
    for input_row, row in zip(inputs, outputs):
        row_version = row.get("schema_version")
        if version == "1.0" and row_version is None:
            # Frozen v1 outputs predate explicit contract metadata.
            continue
        if row_version != version:
            return False
        route_value = row.get("route")
        if not isinstance(route_value, str):
            return False
        route = route_value
        if version == "2.1":
            source_identity_fields = (
                "document_id",
                "candidate_cycle_id",
                "route",
                "raw_text_sha256",
                "parent_document_id",
                "segment_index",
                "segment_count",
                "segment_start_char",
                "segment_end_char",
                "segment_text_sha256",
                "endorsement_context_provenance",
            )
            if any(not json_value_equal_typed(
                row.get(field), input_row.get(field)
            ) for field in source_identity_fields):
                return False
        if route not in contract["prompt_sha256"]:
            return False
        if row.get("prompt_sha256") != contract["prompt_sha256"][route]:
            return False
        if row.get("json_schema_sha256") != contract["json_schema_sha256"][route]:
            return False
        if row.get("extraction_contract_sha256") != contract["extraction_contract_sha256"]:
            return False
        if version == "2.1":
            contract_identity = {
                "model_id": MODEL_ID,
                "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
                "alignment_id": contract.get("alignment_id"),
                "alignment_spec_sha256": contract.get("alignment_spec_sha256"),
                "semantic_validation_spec_sha256": contract.get(
                    "semantic_validation_spec_sha256"
                ),
                "endorsement_context_provenance_spec_sha256": contract.get(
                    "endorsement_context_provenance_spec_sha256"
                ),
                "user_message_template_id": contract.get(
                    "user_message_template_id"
                ),
                "user_message_template_sha256": contract.get(
                    "user_message_template_sha256"
                ),
            }
            if (
                any(not json_value_equal_typed(row.get(key), value)
                    for key, value in contract_identity.items())
                or not json_value_equal_typed(row.get("temperature"), 0.0)
                or row.get("thinking_enabled") is not False
                or row.get("compact_repair") is not False
                or row.get("input_truncated") is not False
                or
                row.get("structured_decoding_backend")
                != STRUCTURED_DECODING_BACKEND
                or row.get("structured_decoding_disable_any_whitespace")
                is not STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
                or
                row.get("inference_display_escape_id")
                != contract["inference_display_escape_id"]
                or row.get("inference_display_escape_spec_sha256")
                != contract["inference_display_escape_spec_sha256"]
                or row.get("json_string_control_canonicalizer_id")
                != contract["json_string_control_canonicalizer_id"]
                or row.get("json_string_control_canonicalizer_spec_sha256")
                != contract["json_string_control_canonicalizer_spec_sha256"]
                or row.get("array_item_cap_spec_id")
                != contract["array_item_cap_spec_id"]
                or row.get("array_item_cap_spec_sha256")
                != contract["array_item_cap_spec_sha256"]
                or not json_value_equal_typed(
                    row.get("route_array_item_cap"),
                    contract["route_array_item_caps"][route],
                )
                or row.get("model_item_repetition_spec_id")
                != contract["model_item_repetition_spec_id"]
                or row.get("model_item_repetition_spec_sha256")
                != contract["model_item_repetition_spec_sha256"]
            ):
                return False
            raw_output = row.get("raw_output")
            if not isinstance(raw_output, str):
                return False
            canonical_text, observed_canonicalization = canonicalize_json_string_controls(
                raw_output
            )
            for key, value in observed_canonicalization.items():
                if not json_value_equal_typed(row.get(key), value):
                    return False
            observed_structural = validate_raw_structural_whitespace(raw_output)
            if not observed_structural["raw_structural_whitespace_validation_passed"]:
                return False
            for key, value in observed_structural.items():
                if not json_value_equal_typed(row.get(key), value):
                    return False
            source_text = input_row.get("source_text")
            if not isinstance(source_text, str):
                return False
            segment_hash = input_row.get("segment_text_sha256")
            if segment_hash is not None and not json_value_equal_typed(
                segment_hash,
                hashlib.sha256(source_text.encode("utf-8")).hexdigest(),
            ):
                return False
            if route == "endorsements":
                provenance_errors = endorsement_context_provenance_errors(
                    input_row.get("endorsement_context_provenance"),
                    segment_source_text=source_text,
                    segment_id=str(input_row.get("document_id") or ""),
                    segment_start_char=int(
                        input_row.get("segment_start_char", 0)
                    ),
                    parent_document_id=str(
                        input_row.get("parent_document_id") or ""
                    ),
                    source_urls=input_row.get("source_urls") or [],
                )
                if provenance_errors:
                    return False
            try:
                expected_user_message = build_user_message(
                    input_row, source_text, version
                )
            except (KeyError, TypeError, ValueError):
                return False
            if not json_value_equal_typed(
                row.get("user_message_sha256"),
                hashlib.sha256(expected_user_message.encode("utf-8")).hexdigest(),
            ):
                return False
            if not json_value_equal_typed(
                row.get("input_chars_used"), len(source_text)
            ):
                return False
            input_tokens_used = row.get("input_tokens_used")
            if (
                not isinstance(input_tokens_used, int)
                or isinstance(input_tokens_used, bool)
                or input_tokens_used < 0
            ):
                return False
            observed_display = inference_display_provenance(source_text)
            for key, value in observed_display.items():
                if not json_value_equal_typed(row.get(key), value):
                    return False
            expected_fields = (
                set(_V2_1_PARSED_OUTPUT_CORE_FIELDS)
                | set(observed_canonicalization)
                | set(observed_structural)
                | set(observed_display)
                | {
                    field for field in _V2_1_COPIED_OPTIONAL_FIELDS
                    if field in input_row
                }
            )
            if row.get("parse_status") == TRUNCATED_PARSE_STATUS:
                # A cap-limited row is terminal, not damaged: the run wrote every
                # row and an identical rerun returns identical bytes. The release
                # packager refuses the row, and the seed-vary repair run replaces
                # it. Accepting it here keeps the rest of the shard runnable.
                return accept_truncated_rows and truncated_row_is_terminal(row)
            if set(row) != expected_fields:
                return False
            if row.get("parse_status") != "parsed":
                return False
            try:
                parsed_extraction = strict_json_loads(canonical_text)
            except StrictJSONError:
                return False
            model_extraction = row.get("model_extraction")
            if not json_value_equal_typed(model_extraction, parsed_extraction):
                return False
            observed_model_hash = hashlib.sha256(
                canonical_json(parsed_extraction).encode("utf-8")
            ).hexdigest()
            if row.get("model_extraction_sha256") != observed_model_hash:
                return False
            observed_cap = model_item_array_cap_report(route, model_extraction)
            observed_repetition = model_item_repetition_report(
                route, model_extraction
            )
            observed_guard = pre_alignment_model_item_guard_report(
                route, model_extraction
            )
            if not json_value_equal_typed(
                row.get("pre_alignment_model_item_guard"), observed_guard
            ):
                return False
            if not json_value_equal_typed(
                row.get("array_item_cap_report"), observed_cap
            ):
                return False
            if not json_value_equal_typed(
                row.get("model_item_repetition_report"), observed_repetition
            ):
                return False
            if (
                observed_cap["array_item_cap_validation_passed"] is not True
                or observed_repetition[
                    "model_item_repetition_validation_passed"
                ] is not True
            ):
                return False
            try:
                observed_extraction, observed_withheld, observed_alignment = (
                    align_extraction_exact_spans(
                        route,
                        parsed_extraction,
                        source_text,
                        endorsement_context_provenance=input_row.get(
                            "endorsement_context_provenance"
                        ),
                        segment_start_char=int(
                            input_row.get("segment_start_char", 0)
                        ),
                    )
                )
                observed_validation = validate_extraction(
                    route,
                    observed_extraction,
                    source_text,
                    version,
                    endorsement_context_provenance=input_row.get(
                        "endorsement_context_provenance"
                    ),
                    segment_start_char=int(
                        input_row.get("segment_start_char", 0)
                    ),
                )
            except (KeyError, TypeError, ValueError):
                return False
            if (
                not json_value_equal_typed(
                    row.get("extraction"), observed_extraction
                )
                or not json_value_equal_typed(
                    row.get("withheld_model_items"), observed_withheld
                )
                or not json_value_equal_typed(
                    row.get("exact_alignment"), observed_alignment
                )
                or not json_value_equal_typed(
                    row.get("aligned_extraction_sha256"),
                    observed_alignment.get("aligned_extraction_sha256"),
                )
                or not json_value_equal_typed(
                    row.get("validation"), observed_validation
                )
            ):
                return False
            alignment = observed_alignment
            validation = observed_validation
            if (
                not isinstance(alignment, dict)
                or alignment.get("model_schema_valid") is not True
                or alignment.get("all_model_items_accounted_for") is not True
                or not isinstance(validation, dict)
                or validation.get("schema_valid") is not True
            ):
                return False
        if version == "2.1" and expected_generation_config_sha256 is None:
            return False
        if version == "2.1" and expected_max_output_tokens is None:
            return False
        if expected_generation_config_sha256 is not None:
            if not json_value_equal_typed(
                row.get("generation_config_sha256"),
                expected_generation_config_sha256,
            ):
                return False
            if row.get("finish_reason") != "stop":
                return False
            output_tokens = row.get("output_tokens")
            if (
                not isinstance(output_tokens, int)
                or isinstance(output_tokens, bool)
                or output_tokens < 1
                or not json_value_equal_typed(
                    row.get("max_output_tokens"), expected_max_output_tokens
                )
                or output_tokens > expected_max_output_tokens
            ):
                return False
    return True


def select_shard(
    chunks: list[Path], shard_index: int | None, shard_count: int | None
) -> list[Path]:
    if shard_index is None and shard_count is None:
        return chunks
    if shard_index is None or shard_count is None:
        raise ValueError("shard-index and shard-count must be supplied together")
    if shard_count < 1 or shard_index < 0 or shard_index >= shard_count:
        raise ValueError("invalid shard index/count")
    return [chunk for index, chunk in enumerate(chunks) if index % shard_count == shard_index]


REPAIR_MANIFEST_NAME = "repair_manifest.json"
REPAIR_MANIFEST_VERSION = "candidate_extraction_seed_vary_repair_v1"
REPAIR_ATTEMPT_INDEX = 2
REPAIR_REASON = (
    "output_truncated: greedy decoding repeated one item and reached the output cap, "
    "so an identical rerun returns identical bytes"
)


def _is_hex64(value: str) -> bool:
    """Report whether the value is a lowercase sha256 digest."""
    if len(value) != 64:
        return False
    return all(character in "0123456789abcdef" for character in value)


def _file_sha256(path: Path) -> str:
    """Return the sha256 digest of one file."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_repair_targets(
    path: Path,
    *,
    expected_repair_seed: int | None = None,
    expected_attempt_index: int | None = None,
) -> dict[str, list[str]]:
    """Read a repair target list and group it by chunk file name.

    The list holds one entry per affected window. A repeated window is an error.

    The target list declares the seed and the attempt index it was built for, and
    the run records its own values in the attestation. The caller passes both so
    that a target list built for one attempt cannot be inferred under another
    attempt's identity, which would give the attestation the wrong seed.
    """
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("repair target list must be a JSON object")
    if expected_repair_seed is not None:
        declared_seed = payload.get("repair_seed")
        if declared_seed is not None and int(declared_seed) != int(expected_repair_seed):
            raise ValueError(
                f"repair target list declares seed {declared_seed}, "
                f"but the run uses {expected_repair_seed}"
            )
    if expected_attempt_index is not None:
        declared_attempt = payload.get("attempt_index")
        if (
            declared_attempt is not None
            and int(declared_attempt) != int(expected_attempt_index)
        ):
            raise ValueError(
                f"repair target list declares attempt {declared_attempt}, "
                f"but the run uses attempt {expected_attempt_index}"
            )
    entries = payload.get("targets")
    if not isinstance(entries, list) or not entries:
        raise ValueError("repair target list holds no targets")
    targets: dict[str, list[str]] = {}
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("repair target entry must be a JSON object")
        chunk = str(entry.get("chunk") or "")
        document_id = str(entry.get("document_id") or "")
        if (
            not chunk.startswith("chunk_")
            or not chunk.endswith(".jsonl.gz")
            or "/" in chunk
            or ".out." in chunk
        ):
            raise ValueError(f"unsafe repair target chunk name: {chunk!r}")
        if not _is_hex64(document_id):
            raise ValueError(
                f"repair target document ID is not a sha256 digest: {document_id!r}"
            )
        key = (chunk, document_id)
        if key in seen:
            raise ValueError(f"duplicate repair target: {chunk} {document_id}")
        seen.add(key)
        targets.setdefault(chunk, []).append(document_id)
    return targets


def repair_chunk_is_complete(
    output_path: Path,
    document_ids: list[str],
    *,
    accept_truncated_rows: bool = False,
) -> bool:
    """Report whether a repair output holds exactly the target rows, all terminal.

    The comparison is order-free. The runner writes the rows in input-chunk order,
    while the target list is sorted by document id, so an ordered comparison fails
    on every chunk that holds more than one target even when the output is correct.
    Sorting both sides still refuses a repeated row, because a repeat lengthens the
    output list.

    The repair seed is the only permitted change, so a repaired window can reach
    the output cap again and a further identical rerun returns identical bytes.
    The repair scan therefore accepts such a row as terminal when the caller asks
    for it. The strict default keeps refusing it for every other caller.
    """
    if not output_path.exists():
        return False
    try:
        rows = read_gzip_jsonl(output_path)
    except (OSError, EOFError, json.JSONDecodeError):
        return False
    output_ids = [str(row.get("document_id")) for row in rows]
    if sorted(output_ids) != sorted(str(value) for value in document_ids):
        return False
    for row in rows:
        if row.get("finish_reason") == "stop":
            continue
        if accept_truncated_rows and truncated_row_is_terminal(row):
            continue
        return False
    return True


def repair_manifest_payload(
    args: argparse.Namespace,
    contract: dict[str, Any],
    targets: dict[str, list[str]],
    *,
    primary_seed: int,
    repair_seed: int,
    primary_generation_hash: str,
    repair_generation_hash: str,
    chunks_dir: Path,
    output_dir: Path,
    attempt_index: int = REPAIR_ATTEMPT_INDEX,
) -> dict[str, Any]:
    """Build the seed-vary repair attestation for every affected window.

    The payload records the attempt index, both seeds, and the accepted outcome.
    A later attempt carries a higher index and its own seed.
    """
    entries: list[dict[str, Any]] = []
    for chunk_name in sorted(targets):
        output_path = output_dir / chunk_name.replace(".jsonl.gz", ".out.jsonl.gz")
        input_path = chunks_dir / chunk_name
        input_sha256 = _file_sha256(input_path) if input_path.exists() else None
        output_sha256 = _file_sha256(output_path) if output_path.exists() else None
        rows: dict[str, dict[str, Any]] = {}
        if output_path.exists():
            for row in read_gzip_jsonl(output_path):
                rows[str(row.get("document_id"))] = row
        for document_id in targets[chunk_name]:
            row = rows.get(document_id)
            raw_output = row.get("raw_output") if row else None
            entries.append({
                "chunk": chunk_name,
                "document_id": document_id,
                "route": str(row.get("route")) if row else None,
                "input_chunk_sha256": input_sha256,
                "repair_output_file": output_path.name,
                "repair_output_sha256": output_sha256,
                "finish_reason": str(row.get("finish_reason")) if row else None,
                "parse_status": str(row.get("parse_status")) if row else None,
                "output_tokens": (
                    int(row["output_tokens"])
                    if row and isinstance(row.get("output_tokens"), int)
                    else None
                ),
                "raw_output_sha256": (
                    hashlib.sha256(raw_output.encode("utf-8")).hexdigest()
                    if isinstance(raw_output, str)
                    else None
                ),
                "generation_config_sha256": (
                    str(row.get("generation_config_sha256")) if row else None
                ),
                "repaired": bool(
                    row
                    and row.get("finish_reason") == "stop"
                    and row.get("parse_status") == "parsed"
                ),
            })
    repaired = sum(1 for entry in entries if entry["repaired"])
    return {
        "repair_manifest_version": REPAIR_MANIFEST_VERSION,
        "created_utc": utc_now(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "host": socket.gethostname(),
        "schema_version": canonical_schema_version(args.schema_version),
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": contract["prompt_sha256"],
        "json_schema_sha256": contract["json_schema_sha256"],
        "extraction_contract_sha256": contract["extraction_contract_sha256"],
        "max_model_len": int(args.max_model_len),
        "max_output_tokens": int(args.max_output_tokens),
        "primary_seed": int(primary_seed),
        "primary_generation_config_sha256": primary_generation_hash,
        "repair_seed": int(repair_seed),
        "repair_generation_config_sha256": repair_generation_hash,
        "attempt_index": int(attempt_index),
        "reason": REPAIR_REASON,
        "target_count": len(entries),
        "repaired_count": repaired,
        "unrepaired_count": len(entries) - repaired,
        "all_targets_repaired": repaired == len(entries),
        "targets": sorted(entries, key=lambda entry: (entry["chunk"], entry["document_id"])),
    }


def write_repair_manifest(
    args: argparse.Namespace,
    contract: dict[str, Any],
    targets: dict[str, list[str]],
    *,
    primary_seed: int,
    repair_seed: int,
    primary_generation_hash: str,
    repair_generation_hash: str,
    attempt_index: int = REPAIR_ATTEMPT_INDEX,
) -> dict[str, Any]:
    """Write the seed-vary repair attestation for the affected windows."""
    payload = repair_manifest_payload(
        args,
        contract,
        targets,
        primary_seed=primary_seed,
        repair_seed=repair_seed,
        primary_generation_hash=primary_generation_hash,
        repair_generation_hash=repair_generation_hash,
        chunks_dir=args.chunks_dir,
        output_dir=args.output_dir,
        attempt_index=attempt_index,
    )
    path = args.output_dir / REPAIR_MANIFEST_NAME
    temp = path.with_suffix(f".tmp-{os.getpid()}")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temp, path)
    print(json.dumps({
        "repair_manifest": str(path),
        "target_count": payload["target_count"],
        "repaired_count": payload["repaired_count"],
        "unrepaired_count": payload["unrepaired_count"],
        "repair_seed": payload["repair_seed"],
    }, sort_keys=True), flush=True)
    return payload


def structured_parameters(schema: dict[str, Any]) -> dict[str, Any]:
    try:
        from vllm.sampling_params import StructuredOutputsParams

        return {"structured_outputs": StructuredOutputsParams(json=schema)}
    except ImportError:
        from vllm.sampling_params import GuidedDecodingParams

        return {"guided_decoding": GuidedDecodingParams(json=schema)}


def llm_engine_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    """Return the engine identity, including the v2.1 whitespace hardening.

    The engine-level setting is deliberate: vLLM 0.26.0 does not reliably
    honor the equivalent per-request xgrammar flag. Disabling only JSON
    structural whitespace leaves whitespace inside decoded string values
    available and prevents unbounded TAB/LF generation between JSON tokens.
    """
    kwargs: dict[str, Any] = {
        "model": str(args.model_path),
        "tokenizer": str(args.model_path),
        "max_model_len": args.max_model_len,
        "gpu_memory_utilization": args.gpu_memory_utilization,
        "seed": args.seed,
        "trust_remote_code": False,
        "language_model_only": True,
    }
    if canonical_schema_version(args.schema_version) == "2.1":
        kwargs["structured_outputs_config"] = {
            "backend": STRUCTURED_DECODING_BACKEND,
            "disable_any_whitespace": STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        }
    return kwargs


def trim_to_budget(tokenizer: Any, text: str, token_budget: int) -> tuple[str, bool, int]:
    """Return the longest exact Unicode prefix within a tokenizer budget."""
    if token_budget < 0:
        raise ValueError("token budget must be nonnegative")
    token_count = len(tokenizer.encode(text))
    if token_count <= token_budget:
        return text, False, token_count
    low = 0
    high = len(text)
    while low < high:
        middle = (low + high + 1) // 2
        if len(tokenizer.encode(text[:middle])) <= token_budget:
            low = middle
        else:
            high = middle - 1
    trimmed = text[:low]
    return trimmed, True, len(tokenizer.encode(trimmed))


def _existing_output_schema_versions(output_dir: Path) -> set[str]:
    versions: set[str] = set()
    for path in sorted(output_dir.glob("run_metadata*.json")):
        row = json.loads(path.read_text())
        versions.add(str(row.get("schema_version") or "1.0"))
    for path in sorted(output_dir.glob("chunk_*.out.jsonl.gz")):
        for row in read_gzip_jsonl(path):
            versions.add(str(row.get("schema_version") or "1.0"))
            break
    return {canonical_schema_version(version) for version in versions}


def build_user_message(
    record: dict[str, Any], source_text: str, schema_version: str = "1.0"
) -> str:
    urls = record.get("source_urls") or []
    url = urls[0] if urls else ""
    version = canonical_schema_version(schema_version)
    if version == "2.1":
        candidate_id = record.get("candidate_icpsr")
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise ValueError("v2.1 input requires a nonempty candidate_icpsr field")
        message = USER_MESSAGE_TEMPLATE_V2_1.format(
            candidate_id=candidate_id,
            office=record["office"],
            state=record["state"],
            year=record["year"],
            party=record["party"],
            url=url,
            display_escape_id=INFERENCE_DISPLAY_ESCAPE_ID,
            display_escape_spec_sha256=INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256,
            source_text=inference_display_escape(source_text),
        )
        if str(record.get("route")) == "endorsements":
            provenance = record.get("endorsement_context_provenance")
            if not isinstance(provenance, dict):
                raise ValueError(
                    "v2.1 endorsement input requires context provenance"
                )
            # Production v2.1 exports always carry both values. Defaults retain
            # compatibility for the small synthetic message fixtures that
            # predate segmented inputs; the export validator remains the hard
            # gate on real inference inputs.
            segment_index = record.get("segment_index", 0)
            segment_count = record.get("segment_count", 1)
            context_text = provenance.get("context_text")
            block = ENDORSEMENT_CONTEXT_USER_BLOCK_TEMPLATE_V2_1.format(
                source_scope=provenance.get("source_scope"),
                context_id=provenance.get("context_id"),
                context_derivation_method=provenance.get(
                    "context_derivation_method"
                ),
                context_text=(
                    inference_display_escape(context_text)
                    if context_text is not None
                    else "null (no literal support anchor; do not invent text)"
                ),
                segment_starts_mid_line=(
                    "true"
                    if provenance.get("segment_starts_mid_line") is True
                    else "false"
                ),
                segment_has_complete_line=(
                    "true"
                    if provenance.get("segment_has_complete_line") is True
                    else "false"
                ),
                segment_first_complete_line_start_char=(
                    provenance.get("segment_first_complete_line_start_char")
                    if provenance.get("segment_has_complete_line") is True
                    else "null (segment has no complete line)"
                ),
                segment_first_complete_line_inference_display_start_char=(
                    provenance.get(
                        "segment_first_complete_line_inference_display_start_char"
                    )
                    if provenance.get("segment_has_complete_line") is True
                    else "null (segment has no complete line)"
                ),
                segment_ends_mid_line=(
                    "true"
                    if provenance.get("segment_ends_mid_line") is True
                    else "false"
                ),
                segment_last_complete_line_end_char=(
                    provenance.get("segment_last_complete_line_end_char")
                    if provenance.get("segment_has_complete_line") is True
                    else "null (segment has no complete line)"
                ),
                segment_last_complete_line_inference_display_end_char=(
                    provenance.get(
                        "segment_last_complete_line_inference_display_end_char"
                    )
                    if provenance.get("segment_has_complete_line") is True
                    else "null (segment has no complete line)"
                ),
                segment_index=segment_index,
                segment_count=segment_count,
                segment_overlap_status=segment_overlap_status(
                    segment_index, segment_count
                ),
                qualifying_wayback_url=provenance.get(
                    "qualifying_wayback_url"
                ),
                qualifying_original_url=provenance.get(
                    "qualifying_original_url"
                ),
                qualifying_original_path=provenance.get(
                    "qualifying_original_path"
                ),
                url_classifier_id=provenance.get("url_classifier_id"),
                url_classifier_spec_sha256=provenance.get(
                    "url_classifier_spec_sha256"
                ),
            )
            message = message.replace(
                "\n\nPage text (minimal reversible source-character display):\n",
                f"\n\n{block}\n\nPage text (minimal reversible source-character display):\n",
                1,
            )
            if block not in message:
                raise AssertionError("endorsement context block was not inserted")
        return message
    return (
        f"Candidate: {record['candidate_name']}\n"
        f"Contest: {record['office']} in {record['state']}, {record['year']}\n"
        f"Party: {record['party']}\n"
        f"Archived page: {url}\n\n"
        f"Page text:\n{source_text}"
    )


def run_metadata_name(shard_index: int | None) -> str:
    return (
        "run_metadata.json"
        if shard_index is None
        else f"run_metadata_{shard_index:05d}.json"
    )


def run_metadata_payload(
    args: argparse.Namespace,
    contract: dict[str, Any],
    *,
    started: str,
    completed: str,
    processed: int,
    parsed: int,
    schema_valid: int,
    exactly_supported: int,
    selected_documents: int,
    recovery_attestation: bool,
    finish_reason_counts: dict[str, int] | None = None,
    total_output_tokens: int = 0,
    max_output_tokens_observed: int = 0,
    model_items: int = 0,
    aligned_items: int = 0,
    withheld_items: int = 0,
    alignment_status_counts: dict[str, int] | None = None,
    json_control_canonicalized_outputs: int = 0,
    json_control_canonicalized_characters: int = 0,
    json_control_canonicalization_codepoint_counts: dict[str, int] | None = None,
    strict_json_parsed_outputs: int = 0,
    model_items_strictly_parsed: int = 0,
    array_item_cap_reached_outputs: int = 0,
    model_item_repetition_error_outputs: int = 0,
    model_item_repetition_groups: int = 0,
    max_exact_model_item_multiplicity: int = 0,
    route_model_item_count_max: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Build one shard attestation, including no-inference restart recovery."""
    generation_config = generation_config_payload(args, contract)
    payload = {
        "schema_version": canonical_schema_version(args.schema_version),
        "started_utc": started,
        "completed_utc": completed,
        "host": socket.gethostname(),
        "python": platform.python_version(),
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "model_path": str(args.model_path),
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": contract["prompt_sha256"],
        "json_schema_sha256": contract["json_schema_sha256"],
        "extraction_contract_sha256": contract["extraction_contract_sha256"],
        "alignment_id": contract.get("alignment_id"),
        "alignment_spec_sha256": contract.get("alignment_spec_sha256"),
        "semantic_validation_spec_sha256": contract.get(
            "semantic_validation_spec_sha256"
        ),
        "endorsement_context_provenance_spec_sha256": contract.get(
            "endorsement_context_provenance_spec_sha256"
        ),
        "user_message_template_id": contract.get("user_message_template_id"),
        "user_message_template_sha256": contract.get("user_message_template_sha256"),
        "generation_config": generation_config,
        "generation_config_sha256": hashlib.sha256(
            canonical_json(generation_config).encode("utf-8")
        ).hexdigest(),
        "generation": {
            "temperature": 0.0,
            "max_output_tokens": args.max_output_tokens,
            "max_model_len": args.max_model_len,
            "thinking_enabled": False,
            "seed": args.seed,
            "compact_repair": args.compact_repair,
        },
        "documents_selected_for_shard": selected_documents,
        "documents_processed_this_run": processed,
        "documents_parsed_this_run": parsed,
        "documents_schema_valid_this_run": schema_valid,
        "documents_exactly_supported_this_run": exactly_supported,
        "finish_reason_counts_this_run": dict(
            sorted((finish_reason_counts or {}).items())
        ),
        "output_tokens_total_this_run": int(total_output_tokens),
        "output_tokens_max_this_run": int(max_output_tokens_observed),
        "model_items_parsed_this_run": int(model_items),
        "aligned_items_this_run": int(aligned_items),
        "withheld_items_this_run": int(withheld_items),
        "alignment_status_counts_this_run": dict(
            sorted((alignment_status_counts or {}).items())
        ),
        "outputs_complete_for_selected_shard": True,
        "recovery_attestation_without_inference": recovery_attestation,
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
    }
    if canonical_schema_version(args.schema_version) == "2.1":
        payload.update({
            "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
            "structured_decoding_disable_any_whitespace": (
                STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
            ),
            "inference_display_escape_id": contract[
                "inference_display_escape_id"
            ],
            "inference_display_escape_spec_sha256": contract[
                "inference_display_escape_spec_sha256"
            ],
            "json_string_control_canonicalizer_id": contract[
                "json_string_control_canonicalizer_id"
            ],
            "json_string_control_canonicalizer_spec_sha256": contract[
                "json_string_control_canonicalizer_spec_sha256"
            ],
            "array_item_cap_spec_id": contract["array_item_cap_spec_id"],
            "array_item_cap_spec_sha256": contract[
                "array_item_cap_spec_sha256"
            ],
            "route_array_item_caps": contract["route_array_item_caps"],
            "model_item_repetition_spec_id": contract[
                "model_item_repetition_spec_id"
            ],
            "model_item_repetition_spec_sha256": contract[
                "model_item_repetition_spec_sha256"
            ],
            "strict_json_parsed_outputs_this_run": int(
                strict_json_parsed_outputs
            ),
            "model_items_strictly_parsed_this_run": int(
                model_items_strictly_parsed
            ),
            "array_item_cap_reached_outputs_this_run": int(
                array_item_cap_reached_outputs
            ),
            "model_item_repetition_error_outputs_this_run": int(
                model_item_repetition_error_outputs
            ),
            "model_item_repetition_groups_this_run": int(
                model_item_repetition_groups
            ),
            "max_exact_model_item_multiplicity_this_run": int(
                max_exact_model_item_multiplicity
            ),
            "route_model_item_count_max_this_run": {
                route: int((route_model_item_count_max or {}).get(route, 0))
                for route in sorted(V2_1_MODEL_ITEM_CAPS)
            },
            "json_control_canonicalized_outputs_this_run": int(
                json_control_canonicalized_outputs
            ),
            "json_control_canonicalized_characters_this_run": int(
                json_control_canonicalized_characters
            ),
            "json_control_canonicalization_codepoint_counts_this_run": dict(
                sorted(
                    (json_control_canonicalization_codepoint_counts or {}).items()
                )
            ),
        })
    return payload


def write_run_metadata(path: Path, payload: dict[str, Any]) -> None:
    temp = path.with_suffix(f".tmp-{os.getpid()}")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temp, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--max-model-len", type=int, default=32_768)
    parser.add_argument(
        "--max-output-tokens", type=int, default=V2_1_MIN_OUTPUT_TOKENS
    )
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.90)
    parser.add_argument("--seed", type=int, default=20260822)
    parser.add_argument(
        "--schema-version",
        default=DEFAULT_SCHEMA_VERSION,
        help="Extraction contract version (default: %(default)s; use 1.0 only for legacy artifacts)",
    )
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--shard-count", type=int)
    parser.add_argument("--compact-repair", action="store_true")
    parser.add_argument(
        "--repair-targets",
        type=Path,
        help=(
            "Seed-vary repair target list. The run selects chunks by name, "
            "infers only the listed windows, and writes a repair attestation."
        ),
    )
    parser.add_argument(
        "--repair-seed",
        type=int,
        help="Seed for the repair attempt. The primary seed stays the frozen value.",
    )
    parser.add_argument(
        "--repair-attempt-index",
        type=int,
        default=REPAIR_ATTEMPT_INDEX,
        help=(
            "Attempt number recorded in the repair attestation. The first repair "
            "is attempt 2, because the primary run is attempt 1."
        ),
    )
    args = parser.parse_args()

    schema_version = canonical_schema_version(args.schema_version)
    if args.compact_repair and schema_version != "1.0":
        raise ValueError("compact repair is a legacy v1 operation and is disabled for v2")
    prompts = get_prompts(schema_version)
    schemas = get_schemas(schema_version)
    contract = get_extraction_contract(schema_version)
    if schema_version == "2.1" and args.max_output_tokens < V2_1_MIN_OUTPUT_TOKENS:
        raise ValueError(
            "v2.1 requires max-output-tokens of at least "
            f"{V2_1_MIN_OUTPUT_TOKENS}"
        )
    repair_targets: dict[str, list[str]] | None = None
    repair_seed: int | None = None
    primary_seed = args.seed
    primary_generation_hash = generation_config_sha256(args, contract)
    if args.repair_targets is not None:
        if schema_version != "2.1":
            raise ValueError(
                "the seed-vary repair is defined for schema version 2.1 only"
            )
        if args.repair_seed is None:
            raise ValueError("--repair-seed is required with --repair-targets")
        if args.repair_seed == args.seed:
            raise ValueError("--repair-seed must differ from --seed")
        if args.repair_attempt_index < REPAIR_ATTEMPT_INDEX:
            raise ValueError(
                "--repair-attempt-index starts at "
                f"{REPAIR_ATTEMPT_INDEX}, because the primary run is attempt 1"
            )
        if args.shard_index is not None or args.shard_count is not None:
            raise ValueError(
                "the repair run selects chunks by name and does not shard"
            )
        repair_targets = load_repair_targets(
            args.repair_targets,
            expected_repair_seed=args.repair_seed,
            expected_attempt_index=args.repair_attempt_index,
        )
        repair_seed = int(args.repair_seed)
        args.seed = repair_seed
    generation_hash = generation_config_sha256(args, contract)

    if args.model_path.name.lower() != MODEL_REVISION.lower():
        raise ValueError(
            f"model snapshot must end in {MODEL_REVISION}; got {args.model_path}"
        )
    chunks = sorted(args.chunks_dir.glob("chunk_*.jsonl.gz"))
    if not chunks:
        raise FileNotFoundError(f"no chunks under {args.chunks_dir}")
    if repair_targets is None:
        chunks = select_shard(chunks, args.shard_index, args.shard_count)
    else:
        available = {chunk.name: chunk for chunk in chunks}
        absent = sorted(set(repair_targets) - set(available))
        if absent:
            raise FileNotFoundError(
                "repair targets name chunks that are absent from the chunks "
                "directory: " + ", ".join(absent)
            )
        chunks = [available[name] for name in sorted(repair_targets)]
    if not chunks:
        raise ValueError("selected shard has no chunks")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    existing_versions = _existing_output_schema_versions(args.output_dir)
    if existing_versions and existing_versions != {schema_version}:
        raise FileExistsError(
            "refusing to mix or overwrite extraction schema versions in "
            f"{args.output_dir}: existing={sorted(existing_versions)}, requested={schema_version}"
        )
    todo = []
    for chunk in chunks:
        output = args.output_dir / chunk.name.replace(".jsonl.gz", ".out.jsonl.gz")
        if repair_targets is not None:
            # A repaired window that reaches the cap again is terminal: the repair
            # seed is the only change the approved decision permits, so a rerun
            # returns identical bytes. The row is accounted for in the repair
            # manifest instead of blocking every chunk behind it.
            complete = repair_chunk_is_complete(
                output,
                repair_targets[chunk.name],
                accept_truncated_rows=True,
            )
        else:
            complete = chunk_is_complete(
                chunk,
                output,
                schema_version=schema_version,
                expected_generation_config_sha256=generation_hash,
                expected_max_output_tokens=args.max_output_tokens,
                accept_truncated_rows=True,
            )
        if not complete:
            if output.exists():
                raise ValueError(
                    f"existing output is incomplete or has the wrong contract; "
                    f"preserved without overwrite: {output}"
                )
            todo.append((chunk, output))
    print(f"chunks={len(chunks)} todo={len(todo)}", flush=True)
    if repair_targets is None:
        selected_documents = sum(len(read_gzip_jsonl(path)) for path in chunks)
    else:
        selected_documents = sum(len(ids) for ids in repair_targets.values())
    metadata_path = args.output_dir / run_metadata_name(args.shard_index)
    if not todo:
        if not metadata_path.exists():
            timestamp = utc_now()
            write_run_metadata(
                metadata_path,
                run_metadata_payload(
                    args,
                    contract,
                    started=timestamp,
                    completed=timestamp,
                    processed=0,
                    parsed=0,
                    schema_valid=0,
                    exactly_supported=0,
                    selected_documents=selected_documents,
                    recovery_attestation=True,
                ),
            )
            print(f"wrote recovery attestation {metadata_path.name}", flush=True)
        if repair_targets is not None and repair_seed is not None:
            write_repair_manifest(
                args,
                contract,
                repair_targets,
                primary_seed=primary_seed,
                repair_seed=repair_seed,
                primary_generation_hash=primary_generation_hash,
                repair_generation_hash=generation_hash,
                attempt_index=args.repair_attempt_index,
            )
        return 0

    from vllm import LLM, SamplingParams

    started = utc_now()
    llm_kwargs = llm_engine_kwargs(args)
    llm = LLM(**llm_kwargs)
    tokenizer = llm.get_tokenizer()
    sampling = {
        route: SamplingParams(
            temperature=0.0,
            max_tokens=args.max_output_tokens,
            seed=args.seed,
            **structured_parameters(schema),
        )
        for route, schema in schemas.items()
    }

    total = 0
    parsed = 0
    schema_valid = 0
    exact_supported = 0
    finish_reason_counts: dict[str, int] = {}
    total_output_tokens = 0
    max_output_tokens_observed = 0
    model_items = 0
    aligned_items = 0
    withheld_items = 0
    alignment_status_counts: dict[str, int] = {}
    json_control_canonicalized_outputs = 0
    json_control_canonicalized_characters = 0
    json_control_canonicalization_codepoint_counts: dict[str, int] = {}
    strict_json_parsed_outputs = 0
    model_items_strictly_parsed = 0
    array_item_cap_reached_outputs = 0
    model_item_repetition_error_outputs = 0
    model_item_repetition_groups = 0
    max_exact_model_item_multiplicity = 0
    route_model_item_count_max = {route: 0 for route in V2_1_MODEL_ITEM_CAPS}
    for input_path, output_path in todo:
        records = read_gzip_jsonl(input_path)
        document_ids = [str(record.get("document_id")) for record in records]
        if len(document_ids) != len(set(document_ids)):
            raise ValueError(f"{input_path}: duplicate document IDs")
        if repair_targets is not None:
            targets = set(repair_targets[input_path.name])
            absent_targets = sorted(targets - set(document_ids))
            if absent_targets:
                raise ValueError(
                    f"{input_path}: repair targets are absent from the input "
                    "chunk: " + ", ".join(absent_targets)
                )
            records = [
                record
                for record in records
                if str(record.get("document_id")) in targets
            ]
        conversations = []
        parameters = []
        prompt_meta = []
        for record in records:
            route = str(record["route"])
            if route not in prompts:
                raise ValueError(f"unknown route in input chunk: {route}")
            if not isinstance(record.get("source_text"), str):
                raise TypeError(f"{record['document_id']}: source_text must be a string")
            segment_hash = record.get("segment_text_sha256")
            if segment_hash is not None:
                observed_hash = hashlib.sha256(
                    record["source_text"].encode("utf-8")
                ).hexdigest()
                if segment_hash != observed_hash:
                    raise ValueError(f"{record['document_id']}: segment text hash mismatch")
            if schema_version == "2.1" and route == "endorsements":
                provenance_errors = endorsement_context_provenance_errors(
                    record.get("endorsement_context_provenance"),
                    segment_source_text=record["source_text"],
                    segment_id=str(record["document_id"]),
                    segment_start_char=int(record.get("segment_start_char", 0)),
                    parent_document_id=str(record.get("parent_document_id") or ""),
                    source_urls=record.get("source_urls") or [],
                )
                if provenance_errors:
                    raise ValueError(
                        f"{record['document_id']}: invalid endorsement context "
                        + ", ".join(provenance_errors)
                    )
            system_prompt = prompts[route]
            if args.compact_repair:
                system_prompt += COMPACT_REPAIR_RULE
            fixed_user_tokens = len(
                tokenizer.encode(build_user_message(record, "", schema_version))
            )
            fixed_tokens = len(tokenizer.encode(system_prompt)) + fixed_user_tokens + 500
            prompt_budget = args.max_model_len - args.max_output_tokens - fixed_tokens
            if prompt_budget < 1_000:
                raise ValueError(
                    f"{record['document_id']}: unsafe source token budget {prompt_budget}"
                )
            if schema_version == "2.1":
                trimmed = str(record["source_text"])
                display_text = inference_display_escape(trimmed)
                display_provenance = inference_display_provenance(trimmed)
                prompt_tokens = len(tokenizer.encode(display_text))
                full_user_tokens = len(tokenizer.encode(
                    build_user_message(record, trimmed, schema_version)
                ))
                if (
                    len(tokenizer.encode(system_prompt))
                    + full_user_tokens
                    + args.max_output_tokens
                    + 500
                    > args.max_model_len
                ):
                    raise ValueError(
                        f"{record['document_id']}: v2.1 escaped source exceeds the "
                        "model context; resegment instead of truncating"
                    )
                truncated = False
            else:
                trimmed, truncated, prompt_tokens = trim_to_budget(
                    tokenizer, str(record["source_text"]), prompt_budget
                )
                display_text = trimmed
                display_provenance = None
            user_message = build_user_message(record, trimmed, schema_version)
            conversations.append([
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": user_message,
                },
            ])
            parameters.append(sampling[route])
            prompt_meta.append({
                "input_truncated": truncated,
                "input_tokens_used": prompt_tokens,
                "input_chars_used": len(trimmed),
                "source_text": trimmed,
                "inference_display_chars_used": len(display_text),
                "inference_display_text_sha256": hashlib.sha256(
                    display_text.encode("utf-8")
                ).hexdigest(),
                "inference_display_provenance": display_provenance,
                "user_message_sha256": hashlib.sha256(
                    user_message.encode("utf-8")
                ).hexdigest(),
            })
        print(f"{input_path.name}: {len(records)} documents", flush=True)
        outputs = llm.chat(
            conversations,
            parameters,
            use_tqdm=True,
            chat_template_kwargs={"enable_thinking": False},
        )
        if len(outputs) != len(records):
            raise RuntimeError(
                f"{input_path}: vLLM returned {len(outputs)} outputs for {len(records)} inputs"
            )
        temp = output_path.with_suffix(output_path.suffix + f".tmp-{os.getpid()}")
        with gzip.open(temp, "wt", encoding="utf-8") as handle:
            for record, output, meta in zip(records, outputs, prompt_meta):
                completion = output.outputs[0]
                raw_completion_text = completion.text
                text = (
                    raw_completion_text
                    if schema_version == "2.1"
                    else raw_completion_text.strip()
                )
                finish_reason = str(completion.finish_reason or "")
                output_tokens = len(completion.token_ids or [])
                finish_reason_counts[finish_reason] = (
                    finish_reason_counts.get(finish_reason, 0) + 1
                )
                total_output_tokens += output_tokens
                max_output_tokens_observed = max(
                    max_output_tokens_observed, output_tokens
                )
                row = {
                    "document_id": record["document_id"],
                    "candidate_cycle_id": record["candidate_cycle_id"],
                    "route": record["route"],
                    "raw_text_sha256": record["raw_text_sha256"],
                    "model_id": MODEL_ID,
                    "model_revision": MODEL_REVISION,
                    "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
                    "schema_version": schema_version,
                    "prompt_sha256": contract["prompt_sha256"][str(record["route"])],
                    "json_schema_sha256": contract["json_schema_sha256"][str(record["route"])],
                    "extraction_contract_sha256": contract["extraction_contract_sha256"],
                    "alignment_id": contract.get("alignment_id"),
                    "alignment_spec_sha256": contract.get("alignment_spec_sha256"),
                    "semantic_validation_spec_sha256": contract.get(
                        "semantic_validation_spec_sha256"
                    ),
                    "endorsement_context_provenance_spec_sha256": contract.get(
                        "endorsement_context_provenance_spec_sha256"
                    ),
                    "user_message_template_id": contract.get(
                        "user_message_template_id"
                    ),
                    "user_message_template_sha256": contract.get(
                        "user_message_template_sha256"
                    ),
                    "user_message_sha256": meta["user_message_sha256"],
                    "generation_config_sha256": generation_hash,
                    "temperature": 0.0,
                    "max_output_tokens": args.max_output_tokens,
                    "finish_reason": finish_reason,
                    "stop_reason": completion.stop_reason,
                    "output_tokens": output_tokens,
                    "thinking_enabled": False,
                    "compact_repair": args.compact_repair,
                    "input_truncated": meta["input_truncated"],
                    "input_tokens_used": meta["input_tokens_used"],
                    "input_chars_used": meta["input_chars_used"],
                }
                if schema_version == "2.1":
                    row.update({
                        "structured_decoding_backend": (
                            STRUCTURED_DECODING_BACKEND
                        ),
                        "structured_decoding_disable_any_whitespace": (
                            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
                        ),
                        "inference_display_escape_id": contract[
                            "inference_display_escape_id"
                        ],
                        "inference_display_escape_spec_sha256": contract[
                            "inference_display_escape_spec_sha256"
                        ],
                        "json_string_control_canonicalizer_id": contract[
                            "json_string_control_canonicalizer_id"
                        ],
                        "json_string_control_canonicalizer_spec_sha256": contract[
                            "json_string_control_canonicalizer_spec_sha256"
                        ],
                        "array_item_cap_spec_id": contract[
                            "array_item_cap_spec_id"
                        ],
                        "array_item_cap_spec_sha256": contract[
                            "array_item_cap_spec_sha256"
                        ],
                        "route_array_item_cap": contract[
                            "route_array_item_caps"
                        ][str(record["route"])],
                        "model_item_repetition_spec_id": contract[
                            "model_item_repetition_spec_id"
                        ],
                        "model_item_repetition_spec_sha256": contract[
                            "model_item_repetition_spec_sha256"
                        ],
                        "raw_output": text,
                    })
                    assert isinstance(meta["inference_display_provenance"], dict)
                    row.update(meta["inference_display_provenance"])
                for field in (
                    "parent_document_id", "segment_index", "segment_count",
                    "segment_start_char", "segment_end_char", "segment_text_sha256",
                ):
                    if field in record:
                        row[field] = record[field]
                if "endorsement_context_provenance" in record:
                    row["endorsement_context_provenance"] = record[
                        "endorsement_context_provenance"
                    ]
                canonical_text = text
                structural_whitespace = None
                if schema_version == "2.1":
                    structural_whitespace = validate_raw_structural_whitespace(
                        text
                    )
                    row.update(structural_whitespace)
                    canonical_text, canonicalization = (
                        canonicalize_json_string_controls(text)
                    )
                    row.update(canonicalization)
                    repaired_count = int(
                        canonicalization["json_control_canonicalization_count"]
                    )
                    if repaired_count:
                        json_control_canonicalized_outputs += 1
                        json_control_canonicalized_characters += repaired_count
                        for codepoint, count in canonicalization[
                            "json_control_canonicalization_codepoint_counts"
                        ].items():
                            json_control_canonicalization_codepoint_counts[
                                codepoint
                            ] = (
                                json_control_canonicalization_codepoint_counts.get(
                                    codepoint, 0
                                )
                                + int(count)
                            )
                try:
                    if finish_reason != "stop":
                        raise RuntimeError(
                            f"non-stop generation finish reason: {finish_reason or 'missing'}"
                        )
                    if (
                        schema_version == "2.1"
                        and structural_whitespace is not None
                        and not structural_whitespace[
                            "raw_structural_whitespace_validation_passed"
                        ]
                    ):
                        raise RawStructuralWhitespaceError(
                            "raw JSON does not use xgrammar fixed separators"
                        )
                    parsed_extraction = strict_json_loads(canonical_text)
                    if schema_version == "2.1":
                        strict_json_parsed_outputs += 1
                        row["model_extraction"] = parsed_extraction
                        row["model_extraction_sha256"] = hashlib.sha256(
                            canonical_json(parsed_extraction).encode("utf-8")
                        ).hexdigest()
                        guard_report = pre_alignment_model_item_guard_report(
                            str(record["route"]), parsed_extraction
                        )
                        cap_report = guard_report["array_item_cap_report"]
                        repetition_report = guard_report[
                            "model_item_repetition_report"
                        ]
                        row["pre_alignment_model_item_guard"] = guard_report
                        row["array_item_cap_report"] = cap_report
                        row["model_item_repetition_report"] = repetition_report
                        item_count = cap_report["model_item_count"]
                        if isinstance(item_count, int):
                            model_items_strictly_parsed += item_count
                            route = str(record["route"])
                            route_model_item_count_max[route] = max(
                                route_model_item_count_max[route], item_count
                            )
                        multiplicity = repetition_report[
                            "max_exact_model_item_multiplicity"
                        ]
                        if isinstance(multiplicity, int):
                            max_exact_model_item_multiplicity = max(
                                max_exact_model_item_multiplicity, multiplicity
                            )
                        repetition_groups = repetition_report[
                            "repeated_model_item_group_count"
                        ]
                        if isinstance(repetition_groups, int):
                            model_item_repetition_groups += repetition_groups
                        if guard_report["failure_status"] == ARRAY_ITEM_CAP_REACHED_STATUS:
                            array_item_cap_reached_outputs += 1
                            raise ArrayItemCapReachedError(
                                f"{record['route']} array reached its declared cap"
                            )
                        if guard_report["failure_status"] == MODEL_ITEM_REPETITION_ERROR_STATUS:
                            model_item_repetition_error_outputs += 1
                            raise ModelItemRepetitionError(
                                "one canonical model item occurred at least "
                                f"{repetition_report['model_item_repetition_threshold']} times"
                            )
                        aligned, withheld, alignment = align_extraction_exact_spans(
                            str(record["route"]),
                            parsed_extraction,
                            meta["source_text"],
                            endorsement_context_provenance=record.get(
                                "endorsement_context_provenance"
                            ),
                            segment_start_char=int(
                                record.get("segment_start_char", 0)
                            ),
                        )
                        if row["model_extraction_sha256"] != alignment[
                            "original_model_extraction_sha256"
                        ]:
                            raise AssertionError("model extraction hash changed before alignment")
                        row["extraction"] = aligned
                        row["aligned_extraction_sha256"] = alignment[
                            "aligned_extraction_sha256"
                        ]
                        row["withheld_model_items"] = withheld
                        row["exact_alignment"] = alignment
                        model_items += int(alignment["model_item_count"])
                        aligned_items += int(alignment["aligned_item_count"])
                        withheld_items += int(alignment["withheld_item_count"])
                        alignment_status = str(alignment["status"])
                        alignment_status_counts[alignment_status] = (
                            alignment_status_counts.get(alignment_status, 0) + 1
                        )
                    else:
                        row["extraction"] = parsed_extraction
                    row["parse_status"] = "parsed"
                    row["validation"] = validate_extraction(
                        str(record["route"]),
                        row["extraction"],
                        meta["source_text"],
                        schema_version,
                        endorsement_context_provenance=record.get(
                            "endorsement_context_provenance"
                        ),
                        segment_start_char=int(
                            record.get("segment_start_char", 0)
                        ),
                    )
                    parsed += 1
                    schema_valid += int(row["validation"]["schema_valid"])
                    exact_supported += int(
                        row["validation"]["schema_valid"]
                        and row["validation"]["n_items"]
                        == row["validation"]["n_supported"]
                    )
                except RawStructuralWhitespaceError:
                    row["extraction"] = None
                    row["parse_status"] = "raw_structural_whitespace_error"
                    row["raw_output"] = text
                except StrictJSONError:
                    row["extraction"] = None
                    row["parse_status"] = "strict_json_parse_error"
                    row["raw_output"] = text
                except ArrayItemCapReachedError:
                    row["extraction"] = None
                    row["parse_status"] = ARRAY_ITEM_CAP_REACHED_STATUS
                    row["raw_output"] = text
                except ModelItemRepetitionError:
                    row["extraction"] = None
                    row["parse_status"] = MODEL_ITEM_REPETITION_ERROR_STATUS
                    row["raw_output"] = text
                except RuntimeError:
                    row["extraction"] = None
                    row["parse_status"] = (
                        TRUNCATED_PARSE_STATUS
                        if finish_reason == "length"
                        else "generation_incomplete"
                    )
                    row["raw_output"] = text
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
                total += 1
        os.replace(temp, output_path)
        print(f"wrote {output_path.name}", flush=True)

    metadata = run_metadata_payload(
        args,
        contract,
        started=started,
        completed=utc_now(),
        processed=total,
        parsed=parsed,
        schema_valid=schema_valid,
        exactly_supported=exact_supported,
        selected_documents=selected_documents,
        recovery_attestation=False,
        finish_reason_counts=finish_reason_counts,
        total_output_tokens=total_output_tokens,
        max_output_tokens_observed=max_output_tokens_observed,
        model_items=model_items,
        aligned_items=aligned_items,
        withheld_items=withheld_items,
        alignment_status_counts=alignment_status_counts,
        json_control_canonicalized_outputs=(
            json_control_canonicalized_outputs
        ),
        json_control_canonicalized_characters=(
            json_control_canonicalized_characters
        ),
        json_control_canonicalization_codepoint_counts=(
            json_control_canonicalization_codepoint_counts
        ),
        strict_json_parsed_outputs=strict_json_parsed_outputs,
        model_items_strictly_parsed=model_items_strictly_parsed,
        array_item_cap_reached_outputs=array_item_cap_reached_outputs,
        model_item_repetition_error_outputs=(
            model_item_repetition_error_outputs
        ),
        model_item_repetition_groups=model_item_repetition_groups,
        max_exact_model_item_multiplicity=(
            max_exact_model_item_multiplicity
        ),
        route_model_item_count_max=route_model_item_count_max,
    )
    write_run_metadata(metadata_path, metadata)
    if repair_targets is not None and repair_seed is not None:
        write_repair_manifest(
            args,
            contract,
            repair_targets,
            primary_seed=primary_seed,
            repair_seed=repair_seed,
            primary_generation_hash=primary_generation_hash,
            repair_generation_hash=generation_hash,
            attempt_index=args.repair_attempt_index,
        )
    print(json.dumps({
        "processed": total,
        "parsed": parsed,
        "schema_valid": schema_valid,
        "exactly_supported": exact_supported,
        "schema_version": schema_version,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
