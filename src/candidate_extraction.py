"""Versioned schemas and evidence checks for candidate-site extraction.

Versions 1.0 and 2.0 are retained for already-produced artifacts. New inference
uses version 2.1: the model copies exact spans, and deterministic code assigns
offsets only when an exact Unicode substring occurs once in the supplied
segment. Whitespace and punctuation are never normalized for v2 support.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlparse


MODEL_ID = "Qwen/Qwen3.6-27B-FP8"
MODEL_REVISION = "e89b16ebf1988b3d6befa7de50abc2d76f26eb09"
RUNTIME_LOCK_SHA256 = "4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3"
ROUTES = ("issues", "biography", "endorsements")
SCHEMA_VERSIONS = ("1.0", "2.0", "2.1")
DEFAULT_SCHEMA_VERSION = "2.1"

# Legacy v1 constants are public because the importer for the frozen failed
# audit still consumes them.
BIOGRAPHY_FIELDS = (
    "occupation",
    "education",
    "prior_elected_office",
    "prior_public_service",
    "military_service",
    "local_roots",
    "family",
    "other",
)
ENDORSER_TYPES = (
    "individual",
    "elected_official",
    "organization",
    "labor_union",
    "newspaper_or_media",
    "other",
)

BIOGRAPHY_FIELDS_V2 = (
    "occupation",
    "education",
    "elected_office_experience",
    "public_service_experience",
    "military_service",
    "local_roots",
)
BIOGRAPHY_TIMING_VALUES = ("current", "former", "unclear")
BIOGRAPHY_FIELDS_REQUIRING_TIMING = (
    "occupation",
    "elected_office_experience",
    "public_service_experience",
    "local_roots",
)
ISSUE_STATEMENT_KINDS = (
    "support",
    "oppose",
    "proposal",
    "past_action",
    "current_action",
    "unclear",
)
ENDORSEMENT_ENTITY_TYPES = ("individual", "organization", "unclear")
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
ENDORSEMENT_SUPPORT_MODES = ("explicit_text", "page_or_section_context")
V2_1_MODEL_ITEM_CAPS = {
    "issues": 128,
    "biography": 96,
    "endorsements": 256,
}
ARRAY_ITEM_CAP_REACHED_STATUS = "array_item_cap_reached"
MODEL_ITEM_REPETITION_ERROR_STATUS = "model_item_repetition_error"
MODEL_ITEM_REPETITION_THRESHOLD = 3
ARRAY_ITEM_CAP_SPEC_ID = "route_array_item_cap_fail_closed_v1"
ARRAY_ITEM_CAP_SPEC = {
    "version": ARRAY_ITEM_CAP_SPEC_ID,
    "route_caps": V2_1_MODEL_ITEM_CAPS,
    "guided_schema_rule": {
        "issues": "array_maxItems_equals_declared_cap",
        "biography": "array_maxItems_equals_declared_cap",
        "endorsements": (
            "maxItems_omitted_after_dense_bare_name_recall_regression;_"
            "post_generation_cap_remains_fail_closed"
        ),
    },
    "post_generation_rule": (
        "an_item_count_at_or_above_the_route_cap_is_failed_closed_before_"
        "alignment_and_never_public; equality_is_the_guided_decoder_boundary_"
        "only_for_routes_whose_schema_declares_maxItems;_for_endorsements_"
        "any_count_at_or_above_256_is_the_post_generation_hard_failure"
    ),
    "failure_status": ARRAY_ITEM_CAP_REACHED_STATUS,
    "status_precedence": (
        "array_item_cap_reached_precedes_model_item_repetition_error_when_both_apply"
    ),
}
ARRAY_ITEM_CAP_SPEC_SHA256 = hashlib.sha256(
    json.dumps(
        ARRAY_ITEM_CAP_SPEC,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
MODEL_ITEM_REPETITION_SPEC_ID = "exact_canonical_model_item_multiplicity_v1"
MODEL_ITEM_REPETITION_SPEC = {
    "version": MODEL_ITEM_REPETITION_SPEC_ID,
    "position": "after_strict_json_parse_before_exact_alignment",
    "scope": "all_items_in_one_route_array_including_nonconsecutive_items",
    "canonicalization": (
        "JSON_ensure_ascii_false_sort_keys_true_compact_separators"
    ),
    "threshold": MODEL_ITEM_REPETITION_THRESHOLD,
    "failure_rule": (
        "fail_the_entire_window_when_any_exact_canonical_item_occurs_at_least_"
        "threshold_times; two_exact_occurrences_remain_allowed"
    ),
    "failure_status": MODEL_ITEM_REPETITION_ERROR_STATUS,
    "raw_retention": (
        "retain_the_original_parsed_model_extraction_and_private_hash_count_"
        "provenance_but_emit_no_aligned_or_public_extraction"
    ),
}
MODEL_ITEM_REPETITION_SPEC_SHA256 = hashlib.sha256(
    json.dumps(
        MODEL_ITEM_REPETITION_SPEC,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
ENDORSEMENT_CONTEXT_SOURCE_SCOPES = (
    "segment",
    "parent_document",
    "page_or_section_route_context",
)
EXACT_ALIGNMENT_ID = "unique_exact_unicode_substring_v1"
ALIGNMENT_METHOD_UNIQUE = EXACT_ALIGNMENT_ID
ALIGNMENT_METHOD_UNIQUE_DISPLAY = (
    "unique_exact_inference_display_substring_v1"
)
ALIGNMENT_METHOD_AMBIGUOUS = "ambiguous_exact_unicode_substring"
ALIGNMENT_METHOD_MISSING = "missing_exact_unicode_substring"
ALIGNMENT_METHOD_NOT_APPLICABLE = "not_applicable"
ALIGNMENT_METHOD_PROVIDED_SEGMENT = "provided_exact_segment_span"
ALIGNMENT_METHOD_PROVIDED_PARENT = "provided_exact_parent_span"
ALIGNMENT_METHOD_PROVIDED_SEGMENT_EXTENSION = (
    "provided_exact_segment_span_after_context_extension_projection_v1"
)
ALIGNMENT_METHOD_PAGE_ROUTE = "page_or_section_route_context"
ALIGNMENT_METHOD_CARRIED_CONTEXT_MISMATCH = (
    "carried_endorsement_context_mismatch"
)
ENDORSEMENT_CONTEXT_PROVENANCE_VERSION = "endorsement_context_provenance_v2"
ENDORSEMENT_CONTEXT_CLASSIFIER_ID = "src.extract_text.classify_page_type_v1"
ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC = {
    "function": "src.extract_text.classify_page_type",
    "accepted_result": "endorsements",
    "accepted_first_path_segments": [
        "campaign-supporters", "endorsement", "endorsements", "supporters",
    ],
    "wayback_original_url_rule": (
        "strip_https_web.archive.org_web_timestamp_modifier_prefix"
    ),
    "path_rule": (
        "lowercase_first_nonempty_path_segment_after_known_cms_noise"
    ),
}
ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256 = hashlib.sha256(
    json.dumps(
        ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC = {
    "version": "controlled_parent_banner_v2",
    "parent_prefix_max_end_char": 512,
    "max_trimmed_line_chars": 160,
    "max_trimmed_line_words": 20,
    "selection": "last_qualifying_line_in_parent_prefix",
    "required_plural_marker_regex": (
        r"\b(?:endorsements|endorsers|supporters)\b"
    ),
    "allowed_standalone_marker_regex": (
        r"^(?:support|endorsement|endorsements|endorsers|supporters|"
        r"endorsement\s*[-–—:]\s*\d{4})[\s:.\u200b]*$"
    ),
    "excluded_call_to_action_regex": (
        r"\b(?:support|endorse)\s+(?:our|the|this|my)\s+(?:campaign|candidate)\b"
    ),
    "excluded_sentence_or_action_regex": (
        r"\b(?:add|announc\w*|are|back|becom\w*|believe|build|check|choose|"
        r"chip|click|come|coming|continu\w*|donat\w*|earn\w*|give|garner\w*|has|have|"
        r"hear|honor\w*|is|join|make|need|pledge|proud|read|receiv\w*|"
        r"show|sign|submit|thank|view|want|will)\b"
    ),
    "excluded_url_or_quote_regex": r"(?:https?://|www\.|^[\"'“”‘’])",
    "excluded_terminal_punctuation": ["!", "?"],
    "case_rule": "first_alphabetic_character_must_be_uppercase",
    "scope": "from_selected_line_end_through_parent_end",
}
ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256 = hashlib.sha256(
    json.dumps(
        ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
ENDORSEMENT_CONTEXT_PROVENANCE_FIELDS = {
    "version", "context_id", "parent_document_id", "route",
    "parent_source_text_sha256", "parent_source_n_chars", "source_urls_sha256",
    "source_scope", "context_derivation_method", "context_text",
    "context_text_sha256", "context_parent_start_char",
    "context_parent_end_char", "context_parent_exact_match_count",
    "context_parent_occurrence_ordinal", "context_line_index_zero_based",
    "context_scope_parent_start_char", "context_scope_parent_end_char",
    "context_source_segment_id", "context_source_segment_start_char",
    "context_source_segment_end_char", "context_local_start_char",
    "context_local_end_char", "record_to_context_distance_rule",
    "qualifying_wayback_url", "qualifying_original_url",
    "qualifying_original_path", "url_classifier_id",
    "url_classifier_spec_sha256", "url_classifier_result",
    "structural_heading_spec_sha256", "no_literal_anchor_reason",
    "segment_starts_mid_line", "segment_first_complete_line_start_char",
    "segment_first_complete_line_parent_start_char",
    "segment_first_complete_line_inference_display_start_char",
    "segment_ends_mid_line", "segment_last_complete_line_end_char",
    "segment_last_complete_line_parent_end_char",
    "segment_last_complete_line_inference_display_end_char",
    "segment_has_complete_line",
    "inference_display_escape_id",
    "inference_display_escape_spec_sha256",
}
ENDORSEMENT_CONTEXT_PROVENANCE_SPEC = {
    "version": ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
    "fields": sorted(ENDORSEMENT_CONTEXT_PROVENANCE_FIELDS),
    "source_scopes": list(ENDORSEMENT_CONTEXT_SOURCE_SCOPES),
    "text_scope_preference": "segment_then_exact_parent_document_span",
    "url_route_fallback": (
        "only_when_no_controlled_literal_parent_anchor; nullable_text_and_offsets"
    ),
    "segment_boundary": {
        "line_separator": "U+000A_LINE_FEED",
        "starts_mid_line_rule": (
            "segment_start_char_gt_zero_and_preceding_parent_character_is_not_LF"
        ),
        "first_complete_line_rule": (
            "zero_when_not_mid_line; otherwise_one_past_first_segment_local_LF;_"
            "nullable_when_the_resulting_complete_line_interval_is_empty"
        ),
        "ends_mid_line_rule": (
            "segment_end_char_lt_parent_length_and_neither_the_final_included_"
            "character_nor_the_first_excluded_parent_character_is_LF"
        ),
        "last_complete_line_rule": (
            "segment_length_when_not_mid_line; otherwise_one_past_last_segment_"
            "local_LF;_nullable_when_the_resulting_complete_line_interval_is_"
            "empty"
        ),
        "has_complete_line_rule": (
            "true_exactly_when_the_candidate_first_complete_source_offset_is_"
            "strictly_less_than_the_candidate_last_complete_source_offset;_"
            "otherwise_false_and_all_source,parent,and_inference_display_"
            "complete_line_offsets_are_null"
        ),
        "inference_display_offset_rule": (
            "length_of_the_reversible_inference_display_of_the_source_prefix_"
            "ending_at_the_corresponding_source_local_offset"
        ),
        "public_use": (
            "withhold_any_endorsement_record_whose_exact_local_evidence_"
            "intersects_the_leading_or_trailing_partial_line_outside_the_"
            "complete_line_half_open_range"
        ),
        "overlap_status_rule": {
            "single_segment": "segment_count_equals_1",
            "first_segment_with_trailing_overlap": (
                "segment_index_equals_0_and_segment_count_gt_1"
            ),
            "middle_segment_with_leading_and_trailing_overlap": (
                "zero_lt_segment_index_lt_segment_count_minus_1"
            ),
            "last_segment_with_leading_overlap": (
                "segment_index_equals_segment_count_minus_1_and_count_gt_1"
            ),
        },
    },
    "classifier_spec_sha256": ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
    "structural_heading_spec_sha256": (
        ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256
    ),
}
INFERENCE_DISPLAY_ESCAPE_ID = "minimal_non_lf_c0_backslash_u0022_v2"
INFERENCE_DISPLAY_ESCAPE_SPEC = {
    "version": INFERENCE_DISPLAY_ESCAPE_ID,
    "scope": ["page_text", "carried_exact_endorsement_context"],
    "encoding_order": "one source Unicode code point at a time",
    "escaped_source_characters": [
        "U+005C_BACKSLASH",
        "U+0022_DOUBLE_QUOTE",
        "U+0000-through-U+0009",
        "U+000B-through-U+001F",
    ],
    "escape_spellings": {
        "U+005C": "\\\\",
        "U+0022": "lowercase_\\u0022",
        "U+0008": "\\b",
        "U+0009": "\\t",
        "U+000C": "\\f",
        "U+000D": "\\r",
        "other_non_lf_c0": "lowercase_\\u00xx",
    },
    "preserved_source_characters": [
        "U+000A_LINE_FEED_to_preserve_page_layout",
        "U+007F_DELETE",
        "U+0080-through-U+009F_C1_controls",
    ],
    "scope_rationale": (
        "escape characters implicated in invalid JSON string copying plus "
        "literal double quotes implicated in guided-decoder early string "
        "closure; preserve line layout; DEL and C1 are JSON-valid"
    ),
    "reversibility": (
        "literal source backslashes are doubled and source double quotes use "
        "the visible six-character spelling \\u0022; every display boundary "
        "maps to one source boundary or an escape interior, and decoding the "
        "complete display exactly restores the source"
    ),
    "model_output_rule": (
        "return standards-valid JSON whose decoded copied strings contain the "
        "original source characters, not the visible display spellings; copying "
        "visible \\u0022 as a JSON Unicode escape decodes to one source quote"
    ),
    "alignment_rule": (
        "align decoded model strings against unchanged source_text first; when "
        "there is no exact source occurrence, allow only exact display matches "
        "whose two endpoints map to source boundaries and whose inverse exactly "
        "roundtrips to the unchanged source slice; retain the raw model excerpt "
        "and explicit adjustment provenance; source hashes and Unicode code-point "
        "offsets never use display coordinates"
    ),
}
INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256 = hashlib.sha256(
    json.dumps(
        INFERENCE_DISPLAY_ESCAPE_SPEC,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
ENDORSEMENT_CONTEXT_PROVENANCE_SPEC["inference_display_dependency"] = {
    "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
    "inference_display_escape_spec_sha256": (
        INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
    ),
}
ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256 = hashlib.sha256(
    json.dumps(
        ENDORSEMENT_CONTEXT_PROVENANCE_SPEC,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
JSON_STRING_CONTROL_CANONICALIZER_ID = "raw_json_string_c0_escape_v1"
JSON_STRING_CONTROL_CANONICALIZER_SPEC = {
    "version": JSON_STRING_CONTROL_CANONICALIZER_ID,
    "position": "after_generation_before_strict_json_parse",
    "eligible_input": (
        "literal U+0000-through-U+001F at an unescaped content position inside "
        "a lexically quoted JSON string"
    ),
    "replacement": {
        "U+0008": "\\b",
        "U+0009": "\\t",
        "U+000A": "\\n",
        "U+000C": "\\f",
        "U+000D": "\\r",
        "other_c0": "lowercase_\\u00xx",
    },
    "unchanged": (
        "all characters outside quoted strings and every non-C0 character; a "
        "control following an unmatched escape introducer is not repaired"
    ),
    "outside_string_controls": {
        "permitted_json_whitespace": ["U+0009", "U+000A", "U+000D"],
        "invalid_and_left_for_strict_parser": (
            "U+0000-through-U+0008,U+000B,U+000C,U+000E-through-U+001F"
        ),
        "gate_rule": (
            "permitted JSON whitespace is reported separately and never treated "
            "as a parse failure"
        ),
    },
    "parser_boundary": (
        "the canonical text must pass the existing duplicate-key, nonfinite-"
        "constant, standards-valid strict JSON parser; no other syntax repair"
    ),
    "provenance": (
        "retain the exact raw model payload and attest raw/canonical UTF-8 "
        "SHA-256 values plus exact repair counts by Unicode code point"
    ),
}
JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256 = hashlib.sha256(
    json.dumps(
        JSON_STRING_CONTROL_CANONICALIZER_SPEC,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
USER_MESSAGE_TEMPLATE_ID_V2_1 = (
    "candidate_id_context_boundary_and_quote_safe_display_user_message_v6"
)
USER_MESSAGE_TEMPLATE_V2_1 = (
    "Candidate ID: {candidate_id}\n"
    "Contest: {office} in {state}, {year}\n"
    "Party: {party}\n"
    "Archived page: {url}\n"
    "Inference display escape: {display_escape_id}\n"
    "Inference display spec SHA-256: {display_escape_spec_sha256}\n\n"
    "Page text (minimal reversible source-character display):\n{source_text}"
)
ENDORSEMENT_CONTEXT_USER_BLOCK_TEMPLATE_V2_1 = (
    "Endorsement context provenance (same archived parent page):\n"
    "Source scope: {source_scope}\n"
    "Context ID: {context_id}\n"
    "Context derivation: {context_derivation_method}\n"
    "Exact support context: {context_text}\n"
    "Page text starts mid-line: {segment_starts_mid_line}\n"
    "Segment has a complete line: {segment_has_complete_line}\n"
    "First complete source-text line starts at local character: "
    "{segment_first_complete_line_start_char}\n"
    "First complete visible Page-text line starts at inference-display "
    "character: {segment_first_complete_line_inference_display_start_char}\n"
    "Page text ends mid-line: {segment_ends_mid_line}\n"
    "Last complete source-text line ends at local character: "
    "{segment_last_complete_line_end_char}\n"
    "Last complete visible Page-text line ends at inference-display "
    "character: {segment_last_complete_line_inference_display_end_char}\n"
    "Source-text complete-line offsets are provenance only; scan the visible "
    "Page text using the inference-display offsets.\n"
    "Segment index (zero-based): {segment_index}\n"
    "Segment count: {segment_count}\n"
    "Segment overlap status: {segment_overlap_status}\n"
    "URL-route evidence: {qualifying_wayback_url}\n"
    "Decoded original URL: {qualifying_original_url}\n"
    "Decoded original path: {qualifying_original_path}\n"
    "URL classifier: {url_classifier_id}\n"
    "URL classifier spec SHA-256: {url_classifier_spec_sha256}"
)
USER_MESSAGE_TEMPLATE_V2_1_SHA256 = hashlib.sha256(
    json.dumps(
        {
            "base": USER_MESSAGE_TEMPLATE_V2_1,
            "endorsement_context_block": ENDORSEMENT_CONTEXT_USER_BLOCK_TEMPLATE_V2_1,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()

STREET_ADDRESS_PATTERN_TEXTS = (
    r"\bP\.?\s*O\.?\s+Box\s+\d+\b",
    r"\b\d{1,6}\s+(?:[\w.'’#-]+\s+){0,5}"
    r"(?:Street|St|Road|Rd|Avenue|Ave|Boulevard|Blvd|Lane|Ln|Drive|Dr|"
    r"Court|Ct|Highway|Hwy|Route|Way|Place|Pl)\.?\b",
)
EXPLICIT_ENDORSEMENT_PATTERN_TEXT = (
    r"\b(?:endors(?:e|es|ed|ing)|support(?:s|ed|ing)?|back(?:s|ed|ing)|"
    r"recommend(?:s|ed|ing)?|vote\s+for|stand(?:s|ing)?\s+with)\b"
)
ENDORSEMENT_CONTEXT_PATTERN_TEXT = (
    r"\b(?:endors(?:ements?|ers?)|endorsed\s+by|supporters?|supported\s+by|"
    r"backed\s+by|who(?:'s|\s+is)\s+supporting|"
    r"proud(?:ly)?\s+(?:endorsed|supported|backed))\b"
)
_STREET_ADDRESS_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE) for pattern in STREET_ADDRESS_PATTERN_TEXTS
)
_EXPLICIT_ENDORSEMENT_PATTERN = re.compile(
    EXPLICIT_ENDORSEMENT_PATTERN_TEXT, re.IGNORECASE
)
_ENDORSEMENT_CONTEXT_PATTERN = re.compile(
    ENDORSEMENT_CONTEXT_PATTERN_TEXT, re.IGNORECASE
)
_ENDORSEMENT_STRUCTURAL_HEADING_PATTERN = re.compile(
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC[
        "required_plural_marker_regex"
    ],
    re.IGNORECASE,
)
_ENDORSEMENT_STANDALONE_HEADING_PATTERN = re.compile(
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC[
        "allowed_standalone_marker_regex"
    ],
    re.IGNORECASE,
)
_ENDORSEMENT_CONTEXT_CALL_TO_ACTION_PATTERN = re.compile(
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC[
        "excluded_call_to_action_regex"
    ],
    re.IGNORECASE,
)
_ENDORSEMENT_CONTEXT_SENTENCE_OR_ACTION_PATTERN = re.compile(
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC[
        "excluded_sentence_or_action_regex"
    ],
    re.IGNORECASE,
)
_ENDORSEMENT_CONTEXT_URL_OR_QUOTE_PATTERN = re.compile(
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC[
        "excluded_url_or_quote_regex"
    ],
    re.IGNORECASE,
)

ENDORSEMENT_CATEGORY_ALLOWED_ENTITY_TYPES = {
    "private_individual": ("individual",),
    "government_official": ("individual",),
    "party_official": ("individual",),
    "other_individual": ("individual",),
    "labor_union": ("organization",),
    "media_outlet": ("organization",),
    "other_organization": ("organization",),
    "unclear": ENDORSEMENT_ENTITY_TYPES,
}


COMMON_RULES = """
Return only information explicitly stated in the supplied page text.
Every extracted record must include a short verbatim evidence excerpt copied
as one exact, contiguous span from that text. Never join separate passages,
insert an ellipsis, paraphrase the evidence, or change its punctuation. Keep
each evidence excerpt to the shortest span that directly supports the record.
Do not use outside knowledge, infer missing facts, or repair names and claims
from memory. Navigation, cookie notices, and fundraising boilerplate are not
substantive evidence. Return an empty list when the page contains none of the
requested information. Return valid, complete JSON even when the page contains
many records.
""".strip()


PROMPTS_V1 = {
    "issues": f"""You extract concrete policy statements from a U.S. federal
candidate campaign website. Identify each distinct issue statement that says
what the candidate supports, opposes, proposes, or has done. Keep the page's
own language. For each statement return: heading (the nearest issue heading,
or null), claim (a concise faithful statement), and evidence (a short verbatim
excerpt that directly supports the claim).

{COMMON_RULES}
""",
    "biography": f"""You extract concrete biographical facts that a U.S.
federal candidate states on the candidate's campaign website. Allowed fields
are: occupation, education, prior_elected_office, prior_public_service,
military_service, local_roots, family, and other. Return one record per fact
with field, value, and a short verbatim evidence excerpt. Use other only for a
specific biographical fact that does not fit a named field.

{COMMON_RULES}
""",
    "endorsements": f"""You extract endorsements of a U.S. federal candidate
from the candidate's campaign website. Return one record per endorser with:
name, organization (or null), endorser_type, and a short verbatim evidence
excerpt. Allowed endorser_type values are individual, elected_official,
organization, labor_union, newspaper_or_media, and other. Do not treat the
candidate's own statements, generic supporter counts, or calls to endorse as
endorsements.

{COMMON_RULES}
""",
}


COMMON_RULES_V2 = """
Use only the exact supplied Page text. Extract every explicitly supported
requested record, including records in long lists and records separated across
several sections. Do not use outside knowledge or infer an unstated fact.

Every evidence or source-heading string must be copied without any change from
one exact, contiguous substring of Page text. Do not normalize whitespace,
Unicode punctuation, quotation marks, capitalization, or spelling. Do not join
passages or insert ellipses. For every copied string, return zero-based,
half-open Unicode character offsets relative to Page text: Page text[start:end]
must equal the returned string exactly. Count Python Unicode characters, not
UTF-8 bytes and not positions in this instruction or the surrounding message.

Use the shortest complete span that supports the record. Navigation, cookie
notices, donation appeals, and generic campaign boilerplate are not substantive
evidence. Return an empty list only when Page text contains no requested
record. Return one complete JSON object matching the supplied schema.
""".strip()


PROMPTS_V2 = {
    "issues": f"""Extract distinct policy statements made by a U.S. federal
candidate on the candidate's campaign website. A statement must say what the
candidate supports, opposes, proposes, did, or is doing.

For each record:
- source_heading is the nearest heading copied exactly from Page text, with
  exact offsets; use null and null offsets if no source heading is present.
- claim is a concise faithful description of the page's statement.
- statement_kind is support, oppose, proposal, past_action, current_action, or
  unclear. Use past_action/current_action only for conduct, not a preference.
- evidence and its offsets identify the exact span supporting the claim.

Do not add a normalized policy topic or ideology label.

{COMMON_RULES_V2}
""",
    "biography": f"""Extract concrete biographical facts stated by a U.S.
federal candidate on the candidate's campaign website. The only allowed fields
are occupation, education, elected_office_experience,
public_service_experience, military_service, and local_roots.

For occupation, elected_office_experience, public_service_experience, and
local_roots, timing must be current, former, or unclear. Preserve past tense:
never make a former role look current. For local_roots, relationship must state
the page's geographic relationship (for example, born in, raised in, moved to,
lived in, works in, or represented); a bare place name is invalid. Do not infer
current residence from a historical connection. Do not extract a street
address. For education and military_service, timing may be null unless the page
states a decision-relevant current/former distinction. relationship must be
null for every field except local_roots.

Do not extract family facts or a residual other category. value must remain a
concise faithful description. evidence and its offsets identify the exact
supporting span.

{COMMON_RULES_V2}
""",
    "endorsements": f"""Extract endorsements of a U.S. federal candidate from
the candidate's campaign website. Do not treat the candidate's own statements,
calls to endorse, or generic supporter counts as endorsements.

For each endorser:
- endorser_name identifies the person or entity.
- entity_type is individual, organization, or unclear.
- endorser_category is private_individual, government_official,
  party_official, labor_union, media_outlet, other_organization,
  other_individual, or unclear.
- title, affiliation, and jurisdiction are separate nullable fields. Never
  combine them into a generic organization field and never infer them.
- record_evidence and its offsets identify the name-bearing record.
- endorsement_context_evidence and its offsets are required for every record
  and must establish support for the candidate. It may equal record_evidence
  when that exact text states the endorsement.
- support_mode is explicit_text only if record_evidence itself states support
  for the candidate. Use page_or_section_context for a bare name or title in an
  endorsement list. A bare name or title without exact page/section context is
  not a valid record.

{COMMON_RULES_V2}
""",
}


COMMON_RULES_V2_1 = """
Use only the exact supplied Page text. Extract every explicitly supported
requested record, including records in long lists. Do not use outside
knowledge or infer an unstated fact.

Page text and any carried exact endorsement context use a minimal reversible
source-character display. Literal line feeds preserve the page's line and
heading layout. A visible six-character \\u0022 represents one literal source
double quote. A visible \\t, \\b, \\f, \\r, or six-character \\u0003-style spelling
represents the corresponding single source control character. A visible \\\\
represents one literal source backslash. When returning a copied span, return
standards-valid JSON whose decoded string contains the original source
characters, not the visible escape spellings. Copy displayed \\u0022 as a JSON
Unicode escape so the decoded string contains the original quote; never end a
field at that quote. A displayed \\t must be a JSON tab escape, while a displayed
\\\\t represents a literal source backslash followed by "t". Likewise, a displayed
\\\\u0022 represents a literal source backslash followed by "u0022", not a source
quote. Never emit a literal
U+0000-through-U+001F control character inside the raw JSON text. Exact-span
code validates the decoded string against the unchanged source.

Every evidence or source-heading string must be copied without any change from
one exact, contiguous substring of Page text. Do not return character offsets;
deterministic code assigns them after generation. Do not normalize whitespace,
line breaks, Unicode punctuation, quotation marks, capitalization, or spelling.
Never join passages. Never insert "..." or an ellipsis unless those characters
literally occur in Page text. If the shortest supporting excerpt occurs more
than once, lengthen it with adjacent exact text until the excerpt is unique.

Use the shortest complete unique span that supports the record. Navigation,
cookie notices, donation appeals, and generic campaign boilerplate are not
substantive evidence. Return an empty list only when Page text contains no
requested record. Emit each supported record once. Never repeat an identical
record or cycle through identical JSON objects. Return one complete JSON object
matching the supplied schema.
""".strip()


PROMPTS_V2_1 = {
    "issues": f"""Extract distinct policy statements made by a U.S. federal
candidate on the candidate's campaign website. A statement must say what the
candidate supports, opposes, proposes, did, or is doing.

Read Page text from beginning to end. Extract policy statements in introductory
summaries and candidate-attributed sentence fragments as well as statements in
titled issue sections. Return one record for every independently meaningful
stance, proposal, approval or disapproval, past action, or current action. Split
coordinated sentences and lists into separate records when commas, semicolons,
bullets, or conjunctions connect predicates with distinct policy objects or
actions. This includes finite verbs, infinitive lists, and gerund or participial
action fragments attributed to the candidate. Candidate approval, praise,
welcome, disapproval, or rejection of a policy outcome is itself a statement.
Do not omit a short summary clause merely because a later section discusses the
same broad topic in more detail.

For each record:
- source_heading is the nearest heading copied exactly from Page text only when
  that exact heading occurs once in Page text; use null when there is no heading
  or the exact heading is not unique.
- claim is a concise faithful description of the page's statement.
- statement_kind is support, oppose, proposal, past_action, current_action, or
  unclear. Use past_action/current_action only for conduct, not a preference.
- evidence is the exact unique span supporting the claim.

Do not add a normalized policy topic or ideology label.

{COMMON_RULES_V2_1}
""",
    "biography": f"""Extract concrete biographical facts stated by a U.S.
federal candidate on the candidate's campaign website. The only allowed fields
are occupation, education, elected_office_experience,
public_service_experience, military_service, and local_roots.

Read Page text paragraph by paragraph from beginning to end. Extract each
distinct released fact separately. For public_service_experience, include each
specific public, civic, community, health, emergency-response, or nonprofit
project or action attributed to the candidate, including one-off initiatives
and actions mentioned alongside an occupation or another biography fact. Split
coordinated sentences and lists into separate records when they describe
distinct projects or actions. Do not collapse several initiatives into a
general role, and do not omit a specific action because a nearby broader role
was already extracted.

For occupation, elected_office_experience, public_service_experience, and
local_roots, timing must be current, former, or unclear. Preserve past tense:
never make a former role look current. For local_roots, relationship must state
the page's geographic relationship (for example, born in, raised in, moved to,
lived in, works in, or represented), and the relationship string itself must
appear verbatim within evidence; a bare place name is invalid. Do not infer
current residence from a historical connection. Do not extract a street
address or P.O. box. For education and military_service, timing may be null unless the page
states a decision-relevant current/former distinction. relationship must be
null for every field except local_roots.

Do not extract family facts or a residual other category. value must remain a
concise faithful description. evidence is the exact unique supporting span.

{COMMON_RULES_V2_1}
""",
    "endorsements": f"""Extract endorsements of a U.S. federal candidate from
the candidate's campaign website. Do not treat the candidate's own statements,
calls to endorse, or generic supporter counts as endorsements.

For each endorser:
- endorser_name identifies the person or entity.
- entity_type is individual, organization, or unclear.
- endorser_category is private_individual, government_official,
  party_official, labor_union, media_outlet, other_organization,
  other_individual, or unclear.
- title, affiliation, and jurisdiction are separate nullable fields. Never
  combine them into a generic organization field and never infer them.
- record_evidence is an exact unique name-bearing span. Include adjacent title
  or jurisdiction text when needed to make it unique.
- endorsement_context_evidence must establish why the name-bearing record is
  an endorsement. For explicit_text it is an exact unique Page text span and
  may equal record_evidence. For page_or_section_context, follow the separately
  supplied Endorsement context provenance: copy Exact support context without
  change when its scope is segment or parent_document; when its scope is
  page_or_section_route_context, return null rather than fabricating text.
- support_mode is explicit_text only if record_evidence itself states support.
  A bare name never qualifies as explicit_text. Use page_or_section_context for
  a bare name or title in an endorsement list. Never use a generic category
  heading, another endorser's testimonial, or an endorser row as its context.
  URL-route context is provenance for human review, not self-supporting text.

When the supplied Source scope is segment or parent_document, its exact support
context governs every complete person or entity entry in the attested Page text
below, including continuation segments that begin in the middle of a list and
do not repeat the heading locally. Extract every complete supported name or
name-and-title line in that scope. A line containing only a person's or
entity's name is a complete entry; do not wait for a title or other descriptive
text before beginning extraction. Scan continuation Page text line by line only
when Segment has a complete line is true, and only within the separately
supplied half-open visible inference-display character range, from First
complete visible Page-text line starts through Last complete visible Page-text
line ends. If Segment has a complete line is false, return no endorsements for
that segment. Stop sooner at the first navigation, form, or footer marker. Skip
text outside that range. Skip a clipped or incomplete first line or last line.
Always skip the candidate's own name, contact-form labels, and navigation,
footer, or other page-interface text.

{COMMON_RULES_V2_1}

For endorsement_context_evidence only, the carried context rule above overrides
the Page-text rule: copy exact context from the supplied provenance block for a
parent_document scope, and return null for page_or_section_route_context. All
record_evidence must still be copied exactly from Page text.
""",
}


def _nullable_string() -> dict[str, Any]:
    return {"type": ["string", "null"]}


PYTHON_STRIP_WHITESPACE_CODEPOINTS = (
    *range(0x0009, 0x000E),
    *range(0x001C, 0x0021),
    0x0085,
    0x00A0,
    0x1680,
    *range(0x2000, 0x200B),
    0x2028,
    0x2029,
    0x202F,
    0x205F,
    0x3000,
)


def _nonblank_string() -> dict[str, Any]:
    # xgrammar 0.2.3 cannot safely express Python's Unicode str.strip()
    # whitespace set. Guided decoding enforces only nonempty strings; the
    # independent Python model-item validator remains the fail-closed
    # whitespace-only gate.
    return {"type": "string", "minLength": 1}


def _exact_anchor_string() -> dict[str, Any]:
    # xgrammar 0.2.3 can suppress escaped quotes in exact-copy strings when
    # minLength is present. Exact anchors therefore retain only their string
    # type in guided decoding. The unchanged Python model-item validator is the
    # hard nonblank gate before exact alignment or public eligibility.
    return {"type": "string"}


def _nullable_nonblank_string() -> dict[str, Any]:
    # xgrammar 0.2.3 can suppress quote escapes when minLength is combined with
    # a nullable string. Guided decoding therefore permits any string or null;
    # the unchanged Python nullable-nonempty validator remains the hard gate.
    return {"type": ["string", "null"]}


def _nullable_offset() -> dict[str, Any]:
    return {"type": ["integer", "null"], "minimum": 0}


SCHEMAS_V1: dict[str, dict[str, Any]] = {
    "issues": {
        "type": "object",
        "properties": {
            "issue_blocks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "heading": {"type": ["string", "null"]},
                        "claim": {"type": "string"},
                        "evidence": {"type": "string"},
                    },
                    "required": ["heading", "claim", "evidence"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["issue_blocks"],
        "additionalProperties": False,
    },
    "biography": {
        "type": "object",
        "properties": {
            "facts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string", "enum": list(BIOGRAPHY_FIELDS)},
                        "value": {"type": "string"},
                        "evidence": {"type": "string"},
                    },
                    "required": ["field", "value", "evidence"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["facts"],
        "additionalProperties": False,
    },
    "endorsements": {
        "type": "object",
        "properties": {
            "endorsements": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "organization": {"type": ["string", "null"]},
                        "endorser_type": {"type": "string", "enum": list(ENDORSER_TYPES)},
                        "evidence": {"type": "string"},
                    },
                    "required": ["name", "organization", "endorser_type", "evidence"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["endorsements"],
        "additionalProperties": False,
    },
}


SCHEMAS_V2: dict[str, dict[str, Any]] = {
    "issues": {
        "type": "object",
        "properties": {
            "issue_blocks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "source_heading": _nullable_string(),
                        "source_heading_start_char": _nullable_offset(),
                        "source_heading_end_char": _nullable_offset(),
                        "claim": {"type": "string", "minLength": 1},
                        "statement_kind": {
                            "type": "string", "enum": list(ISSUE_STATEMENT_KINDS)
                        },
                        "evidence": {"type": "string", "minLength": 1},
                        "evidence_start_char": {"type": "integer", "minimum": 0},
                        "evidence_end_char": {"type": "integer", "minimum": 1},
                    },
                    "required": [
                        "source_heading", "source_heading_start_char",
                        "source_heading_end_char", "claim", "statement_kind",
                        "evidence", "evidence_start_char", "evidence_end_char",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["issue_blocks"],
        "additionalProperties": False,
    },
    "biography": {
        "type": "object",
        "properties": {
            "facts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string", "enum": list(BIOGRAPHY_FIELDS_V2)},
                        "value": {"type": "string", "minLength": 1},
                        "timing": {
                            "type": ["string", "null"],
                            "enum": [*BIOGRAPHY_TIMING_VALUES, None],
                        },
                        "relationship": _nullable_string(),
                        "evidence": {"type": "string", "minLength": 1},
                        "evidence_start_char": {"type": "integer", "minimum": 0},
                        "evidence_end_char": {"type": "integer", "minimum": 1},
                    },
                    "required": [
                        "field", "value", "timing", "relationship", "evidence",
                        "evidence_start_char", "evidence_end_char",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["facts"],
        "additionalProperties": False,
    },
    "endorsements": {
        "type": "object",
        "properties": {
            "endorsements": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "endorser_name": {"type": "string", "minLength": 1},
                        "entity_type": {
                            "type": "string", "enum": list(ENDORSEMENT_ENTITY_TYPES)
                        },
                        "endorser_category": {
                            "type": "string", "enum": list(ENDORSEMENT_CATEGORIES)
                        },
                        "title": _nullable_string(),
                        "affiliation": _nullable_string(),
                        "jurisdiction": _nullable_string(),
                        "record_evidence": {"type": "string", "minLength": 1},
                        "record_evidence_start_char": {"type": "integer", "minimum": 0},
                        "record_evidence_end_char": {"type": "integer", "minimum": 1},
                        "endorsement_context_evidence": {"type": "string", "minLength": 1},
                        "endorsement_context_evidence_start_char": {
                            "type": "integer", "minimum": 0
                        },
                        "endorsement_context_evidence_end_char": {
                            "type": "integer", "minimum": 1
                        },
                        "support_mode": {
                            "type": "string", "enum": list(ENDORSEMENT_SUPPORT_MODES)
                        },
                    },
                    "required": [
                        "endorser_name", "entity_type", "endorser_category", "title",
                        "affiliation", "jurisdiction", "record_evidence",
                        "record_evidence_start_char", "record_evidence_end_char",
                        "endorsement_context_evidence",
                        "endorsement_context_evidence_start_char",
                        "endorsement_context_evidence_end_char", "support_mode",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["endorsements"],
        "additionalProperties": False,
    },
}


SCHEMAS_V2_1: dict[str, dict[str, Any]] = {
    "issues": {
        "type": "object",
        "description": (
            "Exhaustively scan Page text in source order for candidate-attributed "
            "policy statements, including introductory fragments and coordinated "
            "or listed clauses."
        ),
        "properties": {
            "issue_blocks": {
                "type": "array",
                "maxItems": V2_1_MODEL_ITEM_CAPS["issues"],
                "description": (
                    "One item per independently meaningful stance, proposal, "
                    "approval or disapproval, past action, or current action."
                ),
                "items": {
                    "type": "object",
                    "description": (
                        "A single candidate-attributed policy predicate; do not "
                        "collapse distinct coordinated or listed predicates."
                    ),
                    "properties": {
                        "source_heading": _nullable_nonblank_string(),
                        "claim": _nonblank_string(),
                        "statement_kind": {
                            "type": "string", "enum": list(ISSUE_STATEMENT_KINDS)
                        },
                        "evidence": _exact_anchor_string(),
                    },
                    "required": [
                        "source_heading", "claim", "statement_kind", "evidence",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["issue_blocks"],
        "additionalProperties": False,
    },
    "biography": {
        "type": "object",
        "description": (
            "Exhaustively scan Page text in source order for the six released "
            "biography fields while preserving the privacy exclusions."
        ),
        "properties": {
            "facts": {
                "type": "array",
                "maxItems": V2_1_MODEL_ITEM_CAPS["biography"],
                "description": (
                    "One item per distinct released fact; each specific "
                    "public-service project or action is a separate item."
                ),
                "items": {
                    "description": (
                        "A single released biography fact. Split coordinated or "
                        "listed public-service projects and actions."
                    ),
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "field": {
                                    "type": "string", "enum": ["local_roots"]
                                },
                                "value": _nonblank_string(),
                                "timing": {
                                    "type": "string",
                                    "enum": list(BIOGRAPHY_TIMING_VALUES),
                                },
                                "relationship": _nonblank_string(),
                                "evidence": _exact_anchor_string(),
                            },
                            "required": [
                                "field", "value", "timing", "relationship",
                                "evidence",
                            ],
                            "additionalProperties": False,
                        },
                        {
                            "type": "object",
                            "properties": {
                                "field": {
                                    "type": "string",
                                    "enum": [
                                        "occupation",
                                        "elected_office_experience",
                                        "public_service_experience",
                                    ],
                                },
                                "value": _nonblank_string(),
                                "timing": {
                                    "type": "string",
                                    "enum": list(BIOGRAPHY_TIMING_VALUES),
                                },
                                "relationship": {"type": "null"},
                                "evidence": _exact_anchor_string(),
                            },
                            "required": [
                                "field", "value", "timing", "relationship",
                                "evidence",
                            ],
                            "additionalProperties": False,
                        },
                        {
                            "type": "object",
                            "properties": {
                                "field": {
                                    "type": "string",
                                    "enum": ["education", "military_service"],
                                },
                                "value": _nonblank_string(),
                                "timing": {
                                    "type": ["string", "null"],
                                    "enum": [*BIOGRAPHY_TIMING_VALUES, None],
                                },
                                "relationship": {"type": "null"},
                                "evidence": _exact_anchor_string(),
                            },
                            "required": [
                                "field", "value", "timing", "relationship",
                                "evidence",
                            ],
                            "additionalProperties": False,
                        },
                    ],
                },
            }
        },
        "required": ["facts"],
        "additionalProperties": False,
    },
    "endorsements": {
        "type": "object",
        "description": (
            "Endorsement records use explicit support text, an exact carried "
            "parent structural context, or a declared nullable URL-route context."
        ),
        "properties": {
            "endorsements": {
                "type": "array",
                "items": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "endorser_name": _nonblank_string(),
                                "entity_type": {
                                    "type": "string",
                                    "enum": list(ENDORSEMENT_ENTITY_TYPES),
                                },
                                "endorser_category": {
                                    "type": "string",
                                    "enum": list(ENDORSEMENT_CATEGORIES),
                                },
                                "title": _nullable_nonblank_string(),
                                "affiliation": _nullable_nonblank_string(),
                                "jurisdiction": _nullable_nonblank_string(),
                                "record_evidence": _exact_anchor_string(),
                                "endorsement_context_evidence": _exact_anchor_string(),
                                "support_mode": {
                                    "type": "string", "enum": ["explicit_text"]
                                },
                            },
                            "required": [
                                "endorser_name", "entity_type",
                                "endorser_category", "title", "affiliation",
                                "jurisdiction", "record_evidence",
                                "endorsement_context_evidence", "support_mode",
                            ],
                            "additionalProperties": False,
                        },
                        {
                            "type": "object",
                            "properties": {
                                "endorser_name": _nonblank_string(),
                                "entity_type": {
                                    "type": "string",
                                    "enum": list(ENDORSEMENT_ENTITY_TYPES),
                                },
                                "endorser_category": {
                                    "type": "string",
                                    "enum": list(ENDORSEMENT_CATEGORIES),
                                },
                                "title": _nullable_nonblank_string(),
                                "affiliation": _nullable_nonblank_string(),
                                "jurisdiction": _nullable_nonblank_string(),
                                "record_evidence": _exact_anchor_string(),
                                "endorsement_context_evidence": (
                                    _nullable_nonblank_string()
                                ),
                                "support_mode": {
                                    "type": "string",
                                    "enum": ["page_or_section_context"],
                                },
                            },
                            "required": [
                                "endorser_name", "entity_type",
                                "endorser_category", "title", "affiliation",
                                "jurisdiction", "record_evidence",
                                "endorsement_context_evidence", "support_mode",
                            ],
                            "additionalProperties": False,
                        },
                    ],
                },
            }
        },
        "required": ["endorsements"],
        "additionalProperties": False,
    },
}


PROMPTS_BY_VERSION = {
    "1.0": PROMPTS_V1,
    "2.0": PROMPTS_V2,
    "2.1": PROMPTS_V2_1,
}
SCHEMAS_BY_VERSION = {
    "1.0": SCHEMAS_V1,
    "2.0": SCHEMAS_V2,
    "2.1": SCHEMAS_V2_1,
}
LIST_KEY_BY_VERSION = {
    "1.0": {"issues": "issue_blocks", "biography": "facts", "endorsements": "endorsements"},
    "2.0": {"issues": "issue_blocks", "biography": "facts", "endorsements": "endorsements"},
    "2.1": {"issues": "issue_blocks", "biography": "facts", "endorsements": "endorsements"},
}

# Compatibility aliases: old import/repair scripts intentionally remain on v1.
PROMPTS = PROMPTS_V1
SCHEMAS = SCHEMAS_V1
LIST_KEY = LIST_KEY_BY_VERSION["1.0"]


def canonical_schema_version(schema_version: str) -> str:
    aliases = {
        "1": "1.0", "v1": "1.0", "1.0": "1.0",
        "2": "2.0", "v2": "2.0", "2.0": "2.0",
        "2.1": "2.1", "v2.1": "2.1",
    }
    try:
        return aliases[str(schema_version).lower()]
    except KeyError as error:
        raise ValueError(f"unknown extraction schema version: {schema_version}") from error


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _v2_1_model_item_array(
    route: str, model_extraction: Any
) -> tuple[str, list[Any] | None]:
    if route not in ROUTES:
        raise ValueError(f"unknown extraction route: {route}")
    key = LIST_KEY_BY_VERSION["2.1"][route]
    if (
        not isinstance(model_extraction, Mapping)
        or set(model_extraction) != {key}
        or not isinstance(model_extraction.get(key), list)
    ):
        return key, None
    return key, list(model_extraction[key])


def model_item_array_cap_report(
    route: str, model_extraction: Any
) -> dict[str, Any]:
    """Attest the route cap without treating an invalid root as a cap hit."""
    key, items = _v2_1_model_item_array(route, model_extraction)
    cap = V2_1_MODEL_ITEM_CAPS[route]
    count = len(items) if items is not None else None
    reached = count >= cap if count is not None else False
    return {
        "array_item_cap_spec_id": ARRAY_ITEM_CAP_SPEC_ID,
        "array_item_cap_spec_sha256": ARRAY_ITEM_CAP_SPEC_SHA256,
        "route": route,
        "list_key": key,
        "root_array_valid": items is not None,
        "model_item_count": count,
        "array_item_cap": cap,
        "array_item_cap_reached": reached,
        "array_item_cap_validation_passed": (
            count < cap if count is not None else None
        ),
        "failure_status": ARRAY_ITEM_CAP_REACHED_STATUS if reached else None,
    }


def model_item_repetition_report(
    route: str, model_extraction: Any
) -> dict[str, Any]:
    """Count exact canonical model items across one parsed route array."""
    key, items = _v2_1_model_item_array(route, model_extraction)
    if items is None:
        return {
            "model_item_repetition_spec_id": MODEL_ITEM_REPETITION_SPEC_ID,
            "model_item_repetition_spec_sha256": (
                MODEL_ITEM_REPETITION_SPEC_SHA256
            ),
            "model_item_repetition_threshold": MODEL_ITEM_REPETITION_THRESHOLD,
            "route": route,
            "list_key": key,
            "root_array_valid": False,
            "model_item_count": None,
            "max_exact_model_item_multiplicity": None,
            "repeated_model_item_group_count": None,
            "repeated_model_item_total_occurrences": None,
            "repeated_model_item_excess_occurrences": None,
            "repeated_model_item_groups": [],
            "model_item_repetition_validation_passed": None,
            "failure_status": None,
        }
    occurrences: dict[str, list[int]] = {}
    for item_index, item in enumerate(items):
        canonical_item = _canonical_json(item)
        occurrences.setdefault(canonical_item, []).append(item_index)
    groups = []
    for canonical_item, indices in occurrences.items():
        if len(indices) < MODEL_ITEM_REPETITION_THRESHOLD:
            continue
        groups.append({
            "canonical_model_item_sha256": _sha256_text(canonical_item),
            "multiplicity": len(indices),
            "item_indices": indices,
        })
    groups.sort(key=lambda group: group["canonical_model_item_sha256"])
    max_multiplicity = max((len(indices) for indices in occurrences.values()), default=0)
    total_occurrences = sum(group["multiplicity"] for group in groups)
    failed = bool(groups)
    return {
        "model_item_repetition_spec_id": MODEL_ITEM_REPETITION_SPEC_ID,
        "model_item_repetition_spec_sha256": MODEL_ITEM_REPETITION_SPEC_SHA256,
        "model_item_repetition_threshold": MODEL_ITEM_REPETITION_THRESHOLD,
        "route": route,
        "list_key": key,
        "root_array_valid": True,
        "model_item_count": len(items),
        "max_exact_model_item_multiplicity": max_multiplicity,
        "repeated_model_item_group_count": len(groups),
        "repeated_model_item_total_occurrences": total_occurrences,
        "repeated_model_item_excess_occurrences": sum(
            group["multiplicity"] - 1 for group in groups
        ),
        "repeated_model_item_groups": groups,
        "model_item_repetition_validation_passed": not failed,
        "failure_status": MODEL_ITEM_REPETITION_ERROR_STATUS if failed else None,
    }


def pre_alignment_model_item_guard_report(
    route: str, model_extraction: Any
) -> dict[str, Any]:
    """Return both private guards and their deterministic failure precedence."""
    cap_report = model_item_array_cap_report(route, model_extraction)
    repetition_report = model_item_repetition_report(route, model_extraction)
    if cap_report["array_item_cap_reached"]:
        failure_status = ARRAY_ITEM_CAP_REACHED_STATUS
    elif repetition_report["model_item_repetition_validation_passed"] is False:
        failure_status = MODEL_ITEM_REPETITION_ERROR_STATUS
    else:
        failure_status = None
    root_valid = bool(
        cap_report["root_array_valid"]
        and repetition_report["root_array_valid"]
    )
    return {
        "array_item_cap_report": cap_report,
        "model_item_repetition_report": repetition_report,
        "root_array_valid": root_valid,
        "pre_alignment_model_item_guard_validation_passed": (
            failure_status is None if root_valid else None
        ),
        "failure_status": failure_status,
    }


EXACT_ALIGNMENT_SPEC = {
    "alignment_id": EXACT_ALIGNMENT_ID,
    "source_unit": "Python Unicode code point",
    "offset_convention": "zero_based_half_open_segment_local",
    "matching": (
        "all_overlapping_exact_Python_str.find_source_matches_first;_when_none_"
        "exist_allow_only_canonical_inference_display_matches_whose_two_"
        "endpoints_map_to_source_boundaries_and_inverse_roundtrip_exactly"
    ),
    "display_inverse_scope": (
        "every_model_copied_segment_local_anchor_including_explicit_"
        "endorsement_context_and_the_private_model_context_super_span;_the_"
        "public_page_or_section_context_is_still_projected_to_the_frozen_"
        "carried_source_span"
    ),
    "success_rule": "exactly_one_occurrence",
    "required_anchor_ambiguous_rule": "withhold_offsets_and_fail_record",
    "required_anchor_missing_rule": "withhold_offsets_and_fail_record",
    "optional_anchor_rule": (
        "if_absent_or_not_uniquely_exact_set_public_anchor_and_offsets_to_null_"
        "with_not_applicable_method_and_retain_original_private_diagnostic"
    ),
    "endorsement_context": {
        "segment": "exact supplied segment span with explicit parent translation",
        "segment_extension": (
            "allow_one_unique_exact_local_context_that_starts_at_the_carried_"
            "segment_context_start,_has_the_carried_text_as_a_prefix,_ends_"
            "before_the_record_inside_the_attested_scope,_and_itself_contains_"
            "an_endorsement_support_marker;_retain_the_model_super-span_"
            "privately_and_project_the_public_context_back_to_the_frozen_"
            "carried_span"
        ),
        "parent_document": "exact supplied parent span; segment offsets are null",
        "page_or_section_route_context": (
            "nullable text and offsets with classifier-qualified URL provenance"
        ),
        "preference": "explicit_text_then_structural_parent_text_then_url_route",
    },
    "normalization": "none",
    "model_output_preservation": (
        "retain_original_and_sha256_before_alignment;_record_every_display_"
        "inverse_adjustment_with_model_and_final_excerpt_hashes"
    ),
}
EXACT_ALIGNMENT_SPEC_SHA256 = _sha256_text(_canonical_json(EXACT_ALIGNMENT_SPEC))

SEMANTIC_VALIDATION_SPEC = {
    "issues": {
        "source_heading_role": "exact_provenance_not_overlap_semantic_identity",
    },
    "local_roots": {
        "relationship": "nonempty_and_verbatim_exact_substring_of_evidence",
        "privacy": "reject_po_box_or_number_plus_street_suffix_in_value_or_evidence",
    },
    "endorsements": {
        "entity_category_allowed_entity_types": {
            key: list(value)
            for key, value in ENDORSEMENT_CATEGORY_ALLOWED_ENTITY_TYPES.items()
        },
        "context": (
            "text_scopes_require_exact_support_context; url_route_scope_requires_"
            "nullable_text_plus_qualifying_url_provenance"
        ),
        "explicit_text": "record_evidence_requires_direct_support_verb",
        "explicit_support_marker_regex": EXPLICIT_ENDORSEMENT_PATTERN_TEXT,
        "structural_context_marker_regex": ENDORSEMENT_CONTEXT_PATTERN_TEXT,
        "context_source_scopes": list(ENDORSEMENT_CONTEXT_SOURCE_SCOPES),
        "page_or_section_context": (
            "must_equal_carried_structural_context,_satisfy_the_constrained_"
            "exact_segment_extension_rule,_or_use_explicit_nullable_classifier_"
            "qualified_url_route_context"
        ),
        "url_route_classifier_id": ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
        "url_route_classifier_spec_sha256": (
            ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256
        ),
        "structural_heading_spec_sha256": (
            ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256
        ),
        "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
        "inference_display_escape_spec_sha256": (
            INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
        ),
        "segment_boundary_rule": (
            "when_segment_has_complete_line_is_true,_public_record_evidence_"
            "must_be_fully_inside_the_builder_attested_complete_line_half_open_"
            "source_range;_when_false_every_endorsement_item_is_withheld;_"
            "intersecting_partial_lines_are_withheld_with_private_provenance"
        ),
    },
    "address_pattern_regexes": list(STREET_ADDRESS_PATTERN_TEXTS),
}
SEMANTIC_VALIDATION_SPEC_SHA256 = _sha256_text(
    _canonical_json(SEMANTIC_VALIDATION_SPEC)
)


def computed_contract_hashes(schema_version: str) -> dict[str, Any]:
    """Compute route hashes plus one model/runtime-bound contract hash."""
    version = canonical_schema_version(schema_version)
    prompt_hashes = {
        route: _sha256_text(PROMPTS_BY_VERSION[version][route]) for route in ROUTES
    }
    schema_hashes = {
        route: _sha256_text(_canonical_json(SCHEMAS_BY_VERSION[version][route]))
        for route in ROUTES
    }
    contract = {
        "schema_version": version,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": prompt_hashes,
        "json_schema_sha256": schema_hashes,
    }
    if version == "2.1":
        contract.update({
            "alignment_id": EXACT_ALIGNMENT_ID,
            "alignment_spec_sha256": EXACT_ALIGNMENT_SPEC_SHA256,
            "semantic_validation_spec_sha256": SEMANTIC_VALIDATION_SPEC_SHA256,
            "endorsement_context_provenance_spec_sha256": (
                ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256
            ),
            "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
            "inference_display_escape_spec_sha256": (
                INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
            ),
            "json_string_control_canonicalizer_id": (
                JSON_STRING_CONTROL_CANONICALIZER_ID
            ),
            "json_string_control_canonicalizer_spec_sha256": (
                JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256
            ),
            "array_item_cap_spec_id": ARRAY_ITEM_CAP_SPEC_ID,
            "array_item_cap_spec_sha256": ARRAY_ITEM_CAP_SPEC_SHA256,
            "route_array_item_caps": dict(V2_1_MODEL_ITEM_CAPS),
            "model_item_repetition_spec_id": MODEL_ITEM_REPETITION_SPEC_ID,
            "model_item_repetition_spec_sha256": (
                MODEL_ITEM_REPETITION_SPEC_SHA256
            ),
            "user_message_template_id": USER_MESSAGE_TEMPLATE_ID_V2_1,
            "user_message_template_sha256": USER_MESSAGE_TEMPLATE_V2_1_SHA256,
        })
    return {
        **contract,
        "extraction_contract_sha256": _sha256_text(_canonical_json(contract)),
    }


# Literals are inserted after finalizing the text. Separate expected values make
# an accidental prompt/schema edit fail closed instead of silently changing v2.
FROZEN_V2_PROMPT_SHA256 = {
    "issues": "895fef883c7edd7a0e05c11b34db4cde0444ae89ecbc986d44dade928588ea77",
    "biography": "8774074c20453e6208f17aae12abe280e6b7ed5cd2a388f71590587678210459",
    "endorsements": "7783bce8d81d595d5590e223425c1bf2417c49a14b93893cf9e4aae41e63a283",
}
FROZEN_V2_SCHEMA_SHA256 = {
    "issues": "43bf34fd77c4a29ed71e91553e0eec46f0ef4116af62d51097ee8cccf60baceb",
    "biography": "b39de990aa9d07c87d36b58bac781b1f4b202fc2e0eedb94e8bc2b979a061f84",
    "endorsements": "25ba5dffc66755e1bc382a5e0666c9405764a003c3ab45c383a1da982a3b40a5",
}
FROZEN_V2_CONTRACT_SHA256 = "10fa9966b00a54671c6565477b74e12663eab4cc4709941e17f16e433ce07836"
FROZEN_V2_1_PROMPT_SHA256 = {
    "issues": "e7ad149cbbdfa5657b32235c3421adc4f5ca89fa564f7075e6a26e26f00fcb19",
    "biography": "d32cf8c87062d607cc1ee09e9485b90f2cc1a581559944583dc4839efa177960",
    "endorsements": "6a369a82639adf1ab6c068863b1a07f2ab5856eb48ffd0d639240c46be837f27",
}
FROZEN_V2_1_SCHEMA_SHA256 = {
    "issues": "c5b9922e9447cc5c573bd3eeefa432710a0657e37c484c46e30dc0e83a41c719",
    "biography": "ee197f260e3e2242d65ac05644d6cba72c2c2e3114fe127c579be6070ab3ffd7",
    "endorsements": "267901032a0bebbca582446d0df81ea508493bd970c51c061a4cd31ebf6ec3c6",
}
FROZEN_V2_1_ALIGNMENT_SPEC_SHA256 = (
    "c55aed337762f712c7a4edcf9dd109a94c881b936955cfa5e4d6ba8c114114e5"
)
FROZEN_V2_1_SEMANTIC_VALIDATION_SPEC_SHA256 = (
    "0ef0dc40eda7cd1ebfb83f4042360c432b8876315a8966756b5e79ccc795c9a5"
)
FROZEN_V2_1_USER_MESSAGE_TEMPLATE_SHA256 = (
    "ace05e7a19cc203b824506d41b5ebf0f5a3aacc39dd871cbbd11e9d97c8ee7d7"
)
FROZEN_V2_1_ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256 = (
    "b7689a181c3b9d323ee7dff10978b53d9f51e5e68d7ea709948a6529f483edb5"
)
FROZEN_V2_1_INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256 = (
    "92566223fd29db40a5b6031833a99992b58c4939d206ba8517d2309f9fb1724b"
)
FROZEN_V2_1_JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256 = (
    "64844ef4d08777e33c01795cb86a937200b8f5d5770a6ff0724b86f50c8ec5a9"
)
FROZEN_V2_1_ARRAY_ITEM_CAP_SPEC_SHA256 = (
    "d5c09cd2a12821248ad579aac6980866632a6fd96df17efb598c65080d5ad150"
)
FROZEN_V2_1_MODEL_ITEM_REPETITION_SPEC_SHA256 = (
    "c3942d9e703af8a83136c4c86eb20f4ed01ddb9627c30a457dac6b2fdcf10d3b"
)
FROZEN_V2_1_CONTRACT_SHA256 = (
    "82530ab0f4126e1640dcffbe48a66bffdfab1538fc40240b5f3e55ab0eed5574"
)


def get_extraction_contract(schema_version: str = DEFAULT_SCHEMA_VERSION) -> dict[str, Any]:
    version = canonical_schema_version(schema_version)
    contract = computed_contract_hashes(version)
    if version == "2.0" and FROZEN_V2_CONTRACT_SHA256:
        if contract["prompt_sha256"] != FROZEN_V2_PROMPT_SHA256:
            raise RuntimeError("v2 extraction prompt differs from its frozen SHA-256")
        if contract["json_schema_sha256"] != FROZEN_V2_SCHEMA_SHA256:
            raise RuntimeError("v2 extraction JSON schema differs from its frozen SHA-256")
        if contract["extraction_contract_sha256"] != FROZEN_V2_CONTRACT_SHA256:
            raise RuntimeError("v2 extraction contract differs from its frozen SHA-256")
    if version == "2.1" and FROZEN_V2_1_CONTRACT_SHA256:
        if contract["prompt_sha256"] != FROZEN_V2_1_PROMPT_SHA256:
            raise RuntimeError("v2.1 extraction prompt differs from its frozen SHA-256")
        if contract["json_schema_sha256"] != FROZEN_V2_1_SCHEMA_SHA256:
            raise RuntimeError("v2.1 extraction JSON schema differs from its frozen SHA-256")
        if contract["alignment_spec_sha256"] != FROZEN_V2_1_ALIGNMENT_SPEC_SHA256:
            raise RuntimeError("v2.1 exact-alignment spec differs from its frozen SHA-256")
        if (
            contract["semantic_validation_spec_sha256"]
            != FROZEN_V2_1_SEMANTIC_VALIDATION_SPEC_SHA256
        ):
            raise RuntimeError("v2.1 semantic-validation spec differs from its frozen SHA-256")
        if (
            contract["user_message_template_sha256"]
            != FROZEN_V2_1_USER_MESSAGE_TEMPLATE_SHA256
        ):
            raise RuntimeError("v2.1 user-message template differs from its frozen SHA-256")
        if (
            contract["endorsement_context_provenance_spec_sha256"]
            != FROZEN_V2_1_ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256
        ):
            raise RuntimeError(
                "v2.1 endorsement-context provenance spec differs from its frozen SHA-256"
            )
        if (
            contract["inference_display_escape_spec_sha256"]
            != FROZEN_V2_1_INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
        ):
            raise RuntimeError(
                "v2.1 inference-display escape spec differs from its frozen SHA-256"
            )
        if (
            contract["json_string_control_canonicalizer_spec_sha256"]
            != FROZEN_V2_1_JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256
        ):
            raise RuntimeError(
                "v2.1 JSON control canonicalizer spec differs from its frozen SHA-256"
            )
        if (
            contract["array_item_cap_spec_sha256"]
            != FROZEN_V2_1_ARRAY_ITEM_CAP_SPEC_SHA256
        ):
            raise RuntimeError("v2.1 array-item cap spec differs from its frozen SHA-256")
        if (
            contract["model_item_repetition_spec_sha256"]
            != FROZEN_V2_1_MODEL_ITEM_REPETITION_SPEC_SHA256
        ):
            raise RuntimeError(
                "v2.1 model-item repetition spec differs from its frozen SHA-256"
            )
        if contract["extraction_contract_sha256"] != FROZEN_V2_1_CONTRACT_SHA256:
            raise RuntimeError("v2.1 extraction contract differs from its frozen SHA-256")
    return contract


def get_prompts(schema_version: str = DEFAULT_SCHEMA_VERSION) -> dict[str, str]:
    version = canonical_schema_version(schema_version)
    get_extraction_contract(version)
    return PROMPTS_BY_VERSION[version]


def get_schemas(schema_version: str = DEFAULT_SCHEMA_VERSION) -> dict[str, dict[str, Any]]:
    version = canonical_schema_version(schema_version)
    get_extraction_contract(version)
    return SCHEMAS_BY_VERSION[version]


def _inference_display_piece(character: str) -> str:
    if character == '"':
        return "\\u0022"
    if character == "\\":
        return "\\\\"
    codepoint = ord(character)
    if codepoint < 0x20 and character != "\n":
        short = {
            "\b": "\\b",
            "\t": "\\t",
            "\f": "\\f",
            "\r": "\\r",
        }.get(character)
        return short if short is not None else f"\\u{codepoint:04x}"
    return character


def inference_display_escape_with_boundaries(
    source_text: str,
) -> tuple[str, tuple[int | None, ...]]:
    """Return the minimal display and its display-to-source boundary map.

    A boundary inside a multi-character escape maps to ``None``. This makes a
    display substring beginning inside a doubled source backslash impossible
    to mistake for a control-character escape. The map supports the narrowly
    constrained exact-display fallback used only after a parsed model string
    has no exact occurrence in the unchanged source.
    """
    if not isinstance(source_text, str):
        raise TypeError("inference display input must be a string")
    display_parts: list[str] = []
    boundaries: list[int | None] = [0]
    for source_index, character in enumerate(source_text):
        piece = _inference_display_piece(character)
        display_parts.append(piece)
        boundaries.extend(
            source_index + 1 if index == len(piece) - 1 else None
            for index in range(len(piece))
        )
    display = "".join(display_parts)
    if len(boundaries) != len(display) + 1:
        raise AssertionError("inference display boundary map has the wrong length")
    return display, tuple(boundaries)


def inference_display_unescape(display_text: str) -> str:
    """Invert one complete minimal display; reject noncanonical spellings."""
    if not isinstance(display_text, str):
        raise TypeError("inference display input must be a string")
    output: list[str] = []
    index = 0
    short = {"b": "\b", "t": "\t", "f": "\f", "r": "\r"}
    while index < len(display_text):
        character = display_text[index]
        if character != "\\":
            if ord(character) < 0x20 and character != "\n":
                raise ValueError("inference display contains an unescaped non-LF C0")
            output.append(character)
            index += 1
            continue
        if index + 1 >= len(display_text):
            raise ValueError("inference display ends with an incomplete escape")
        marker = display_text[index + 1]
        if marker == "\\":
            output.append("\\")
            index += 2
            continue
        if marker in short:
            output.append(short[marker])
            index += 2
            continue
        if marker == "u" and index + 6 <= len(display_text):
            spelling = display_text[index : index + 6]
            digits = spelling[2:]
            if re.fullmatch(r"[0-9a-f]{4}", digits):
                decoded = chr(int(digits, 16))
                if decoded == '"' or (ord(decoded) < 0x20 and decoded != "\n"):
                    if spelling == f"\\u{ord(decoded):04x}":
                        output.append(decoded)
                        index += 6
                        continue
        raise ValueError("inference display contains a noncanonical escape")
    source_text = "".join(output)
    if inference_display_escape_with_boundaries(source_text)[0] != display_text:
        raise AssertionError("inference display inverse failed its canonical roundtrip")
    return source_text


def inference_display_escape(source_text: str) -> str:
    """Return a reversible display while preserving page line structure."""
    display, _ = inference_display_escape_with_boundaries(source_text)
    if inference_display_unescape(display) != source_text:
        raise AssertionError("inference display is not exactly reversible")
    return display


def inference_display_provenance(source_text: str) -> dict[str, Any]:
    """Return compact, recomputable provenance for the reversible display.

    The complete boundary map is deterministic but too large to duplicate in
    every output row. Its SHA-256 attests that map, while the source and display
    hashes, escape counts, and roundtrip flag let downstream code recompute and
    verify every boundary without changing source coordinates.
    """
    display, boundaries = inference_display_escape_with_boundaries(source_text)
    restored = inference_display_unescape(display)
    if restored != source_text:
        raise AssertionError("inference display does not restore its exact source")
    escape_counts: dict[str, int] = {}
    for character in source_text:
        if _inference_display_piece(character) == character:
            continue
        label = f"U+{ord(character):04X}"
        escape_counts[label] = escape_counts.get(label, 0) + 1
    boundary_map_sha256 = _sha256_text(_canonical_json(list(boundaries)))
    return {
        "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
        "inference_display_escape_spec_sha256": (
            INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
        ),
        "inference_display_source_text_sha256": _sha256_text(source_text),
        "inference_display_source_n_chars": len(source_text),
        "inference_display_text_sha256": _sha256_text(display),
        "inference_display_chars_used": len(display),
        "inference_display_escape_count": sum(escape_counts.values()),
        "inference_display_escape_codepoint_counts": dict(sorted(escape_counts.items())),
        "inference_display_interior_boundary_count": sum(
            boundary is None for boundary in boundaries
        ),
        "inference_display_boundary_map_sha256": boundary_map_sha256,
        "inference_display_roundtrip_valid": True,
    }


def inference_display_span_to_source(
    source_text: str, display_start_char: int, display_end_char: int
) -> tuple[int, int]:
    """Map an exact display span only when both endpoints are source boundaries.

    It rejects spans that begin or end inside an escape (for example, at the
    second slash of a displayed literal source ``\\u0022`` spelling).
    """
    display, boundaries = inference_display_escape_with_boundaries(source_text)
    if (
        not isinstance(display_start_char, int)
        or isinstance(display_start_char, bool)
        or not isinstance(display_end_char, int)
        or isinstance(display_end_char, bool)
        or display_start_char < 0
        or display_end_char < display_start_char
        or display_end_char > len(display)
    ):
        raise ValueError("invalid inference-display span")
    source_start = boundaries[display_start_char]
    source_end = boundaries[display_end_char]
    if source_start is None or source_end is None:
        raise ValueError("inference-display span endpoint is inside an escape")
    display_excerpt = display[display_start_char:display_end_char]
    source_excerpt = source_text[source_start:source_end]
    if inference_display_escape(source_excerpt) != display_excerpt:
        raise ValueError("inference-display span is not a canonical source slice")
    return source_start, source_end


def exact_inference_display_occurrence_spans(
    source_text: str, model_excerpt: str
) -> list[tuple[int, int]]:
    """Map canonical exact-display matches back to exact source spans.

    This is not normalization. The model excerpt must occur verbatim in the
    deterministic inference display, both match endpoints must be original
    source-code-point boundaries, and re-encoding the resulting unchanged
    source slice must reproduce the model excerpt byte-for-byte. Duplicate
    source spans are removed while preserving source order.
    """
    if not isinstance(source_text, str) or not isinstance(model_excerpt, str):
        return []
    if not model_excerpt:
        return []
    display, _boundaries = inference_display_escape_with_boundaries(source_text)
    mapped: list[tuple[int, int]] = []
    for display_start, display_end in exact_occurrence_spans(display, model_excerpt):
        try:
            source_span = inference_display_span_to_source(
                source_text, display_start, display_end
            )
        except ValueError:
            continue
        source_start, source_end = source_span
        if inference_display_escape(source_text[source_start:source_end]) != model_excerpt:
            raise AssertionError("inference display alignment failed exact roundtrip")
        if source_span not in mapped:
            mapped.append(source_span)
    return mapped


def normalized_evidence(text: str) -> str:
    """Legacy v1 normalization; never use this to validate v2 records."""
    text = str(text).translate(str.maketrans({"’": "'", "‘": "'", "“": '"', "”": '"'}))
    return re.sub(r"\s+", " ", text).strip()


def evidence_is_supported(evidence: str, source_text: str) -> bool:
    """Legacy v1 normalized support check retained for frozen artifacts."""
    evidence = normalized_evidence(evidence)
    source = normalized_evidence(source_text)
    return bool(evidence) and evidence in source


def exact_span_is_valid(
    source_text: str, excerpt: Any, start_char: Any, end_char: Any
) -> bool:
    """Return whether offsets select exactly the nonempty Unicode excerpt."""
    if not isinstance(source_text, str) or not isinstance(excerpt, str) or not excerpt:
        return False
    if (
        not isinstance(start_char, int) or isinstance(start_char, bool)
        or not isinstance(end_char, int) or isinstance(end_char, bool)
    ):
        return False
    return (
        0 <= start_char < end_char <= len(source_text)
        and source_text[start_char:end_char] == excerpt
    )


V2_1_MODEL_ITEM_FIELDS = {
    "issues": {"source_heading", "claim", "statement_kind", "evidence"},
    "biography": {"field", "value", "timing", "relationship", "evidence"},
    "endorsements": {
        "endorser_name", "entity_type", "endorser_category", "title",
        "affiliation", "jurisdiction", "record_evidence",
        "endorsement_context_evidence", "support_mode",
    },
}
V2_1_ALIGNED_ENDORSEMENT_CONTEXT_FIELDS = {
    "endorsement_context_id",
    "endorsement_context_evidence_source_scope",
    "endorsement_context_evidence_parent_start_char",
    "endorsement_context_evidence_parent_end_char",
    "endorsement_context_record_distance_chars",
    "endorsement_context_qualifying_wayback_url",
    "endorsement_context_qualifying_original_url",
    "endorsement_context_qualifying_original_path",
}
V2_1_ANCHORS = {
    "issues": (
        ("source_heading", "source_heading_start_char", "source_heading_end_char", True),
        ("evidence", "evidence_start_char", "evidence_end_char", False),
    ),
    "biography": (
        ("evidence", "evidence_start_char", "evidence_end_char", False),
    ),
    "endorsements": (
        (
            "record_evidence", "record_evidence_start_char",
            "record_evidence_end_char", False,
        ),
        (
            "endorsement_context_evidence",
            "endorsement_context_evidence_start_char",
            "endorsement_context_evidence_end_char", False,
        ),
    ),
}


def exact_occurrence_spans(source_text: str, excerpt: str) -> list[tuple[int, int]]:
    """Enumerate overlapping, byte-preserving Unicode substring occurrences."""
    if not isinstance(source_text, str) or not isinstance(excerpt, str) or not excerpt:
        return []
    spans: list[tuple[int, int]] = []
    start = source_text.find(excerpt)
    while start >= 0:
        spans.append((start, start + len(excerpt)))
        start = source_text.find(excerpt, start + 1)
    return spans


def decoded_original_url_and_path(url: str) -> tuple[str, str]:
    """Return the Wayback target URL and its decoded path without guessing."""
    if not isinstance(url, str) or not url:
        raise ValueError("URL must be a nonempty string")
    match = re.match(r"https?://web\.archive\.org/web/\d+[^/]*/(.+)", url)
    original = match.group(1) if match else url
    parse_target = original if "://" in original else f"http://{original}"
    parsed = urlparse(parse_target)
    if not parsed.netloc:
        raise ValueError(f"cannot decode original URL: {url}")
    return original, parsed.path or "/"


def _qualifying_endorsement_url(source_urls: Iterable[Any]) -> tuple[str, str, str]:
    """Select the stable first URL accepted by the authoritative classifier."""
    from src.extract_text import classify_page_type

    urls = sorted({str(value) for value in source_urls if str(value).strip()})
    for url in urls:
        if classify_page_type(url) == "endorsements":
            original, path = decoded_original_url_and_path(url)
            return url, original, path
    raise ValueError("endorsement context requires a classifier-qualified source URL")


def _structural_endorsement_heading_candidates(
    parent_source_text: str,
) -> list[dict[str, Any]]:
    """Find conservative heading/banner candidates in the parent prefix."""
    candidates: list[dict[str, Any]] = []
    offset = 0
    prefix_limit = int(
        ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC[
            "parent_prefix_max_end_char"
        ]
    )
    max_chars = int(
        ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC["max_trimmed_line_chars"]
    )
    max_words = int(
        ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC["max_trimmed_line_words"]
    )
    for line_index, raw_line in enumerate(parent_source_text.splitlines(keepends=True)):
        text = raw_line.strip()
        leading = len(raw_line) - len(raw_line.lstrip())
        start = offset + leading
        end = start + len(text)
        offset += len(raw_line)
        if not text or end > prefix_limit:
            continue
        if len(text) > max_chars or len(text.split()) > max_words:
            continue
        standalone = bool(_ENDORSEMENT_STANDALONE_HEADING_PATTERN.fullmatch(text))
        if not standalone and not _ENDORSEMENT_STRUCTURAL_HEADING_PATTERN.search(text):
            continue
        first_alpha = next((char for char in text if char.isalpha()), None)
        if first_alpha is None or not first_alpha.isupper():
            continue
        if (
            not standalone
            and _ENDORSEMENT_CONTEXT_SENTENCE_OR_ACTION_PATTERN.search(text)
        ):
            continue
        if _ENDORSEMENT_CONTEXT_URL_OR_QUOTE_PATTERN.search(text):
            continue
        if text.endswith(tuple(
            ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC[
                "excluded_terminal_punctuation"
            ]
        )):
            continue
        if _ENDORSEMENT_CONTEXT_CALL_TO_ACTION_PATTERN.search(text):
            continue
        candidates.append({
            "text": text,
            "start": start,
            "end": end,
            "line_index_zero_based": line_index,
        })
    return candidates


def endorsement_segment_complete_line_provenance(
    segment_source_text: str,
    *,
    segment_start_char: int,
    segment_starts_mid_line: bool,
    segment_ends_mid_line: bool,
) -> dict[str, Any]:
    """Attest the exact complete-line interval in source and display coordinates."""
    if not isinstance(segment_source_text, str):
        raise TypeError("segment_source_text must be a string")
    if not isinstance(segment_start_char, int) or isinstance(
        segment_start_char, bool
    ) or segment_start_char < 0:
        raise ValueError("segment_start_char must be a nonnegative integer")
    if not isinstance(segment_starts_mid_line, bool) or not isinstance(
        segment_ends_mid_line, bool
    ):
        raise TypeError("segment line-boundary flags must be booleans")

    first_source = 0
    if segment_starts_mid_line:
        first_lf = segment_source_text.find("\n")
        first_source = (
            len(segment_source_text) if first_lf < 0 else first_lf + 1
        )
    last_source = len(segment_source_text)
    if segment_ends_mid_line:
        last_lf = segment_source_text.rfind("\n")
        last_source = 0 if last_lf < 0 else last_lf + 1
    has_complete_line = first_source < last_source
    if not has_complete_line:
        return {
            "segment_has_complete_line": False,
            "segment_first_complete_line_start_char": None,
            "segment_first_complete_line_parent_start_char": None,
            "segment_first_complete_line_inference_display_start_char": None,
            "segment_last_complete_line_end_char": None,
            "segment_last_complete_line_parent_end_char": None,
            "segment_last_complete_line_inference_display_end_char": None,
        }
    return {
        "segment_has_complete_line": True,
        "segment_first_complete_line_start_char": first_source,
        "segment_first_complete_line_parent_start_char": (
            segment_start_char + first_source
        ),
        "segment_first_complete_line_inference_display_start_char": len(
            inference_display_escape(segment_source_text[:first_source])
        ),
        "segment_last_complete_line_end_char": last_source,
        "segment_last_complete_line_parent_end_char": (
            segment_start_char + last_source
        ),
        "segment_last_complete_line_inference_display_end_char": len(
            inference_display_escape(segment_source_text[:last_source])
        ),
    }


def build_endorsement_context_provenance(
    *,
    parent_document_id: str,
    parent_source_text: str,
    source_urls: Iterable[Any],
    segment_id: str,
    segment_start_char: int,
    segment_end_char: int,
    parent_segments: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build one exact parent-text or URL-route context attestation.

    Text selection is parent-level, never the nearest preceding support-like
    sentence. If no conservative structural banner exists, the fallback is an
    explicit endorsement URL-route attestation with no fabricated evidence.
    """
    if not isinstance(parent_source_text, str):
        raise TypeError("parent_source_text must be a string")
    if not isinstance(parent_document_id, str) or not parent_document_id:
        raise ValueError("parent_document_id must be nonempty")
    if not isinstance(segment_id, str) or not segment_id:
        raise ValueError("segment_id must be nonempty")
    if not (
        isinstance(segment_start_char, int)
        and not isinstance(segment_start_char, bool)
        and isinstance(segment_end_char, int)
        and not isinstance(segment_end_char, bool)
        and 0 <= segment_start_char < segment_end_char <= len(parent_source_text)
    ):
        raise ValueError("invalid parent segment bounds")
    urls = sorted({str(value) for value in source_urls if str(value).strip()})
    wayback_url, original_url, original_path = _qualifying_endorsement_url(urls)
    segment_source_text = parent_source_text[segment_start_char:segment_end_char]
    segment_starts_mid_line = (
        segment_start_char > 0
        and parent_source_text[segment_start_char - 1] != "\n"
    )
    segment_ends_mid_line = (
        segment_end_char < len(parent_source_text)
        and bool(segment_source_text)
        and segment_source_text[-1] != "\n"
        and parent_source_text[segment_end_char] != "\n"
    )
    complete_line_provenance = endorsement_segment_complete_line_provenance(
        segment_source_text,
        segment_start_char=segment_start_char,
        segment_starts_mid_line=segment_starts_mid_line,
        segment_ends_mid_line=segment_ends_mid_line,
    )
    common = {
        "version": ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
        "parent_document_id": parent_document_id,
        "route": "endorsements",
        "parent_source_text_sha256": _sha256_text(parent_source_text),
        "parent_source_n_chars": len(parent_source_text),
        "source_urls_sha256": _sha256_text(_canonical_json(urls)),
        "qualifying_wayback_url": wayback_url,
        "qualifying_original_url": original_url,
        "qualifying_original_path": original_path,
        "url_classifier_id": ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
        "url_classifier_spec_sha256": ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
        "url_classifier_result": "endorsements",
        "structural_heading_spec_sha256": (
            ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256
        ),
        "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
        "inference_display_escape_spec_sha256": (
            INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
        ),
        "segment_starts_mid_line": segment_starts_mid_line,
        "segment_ends_mid_line": segment_ends_mid_line,
        **complete_line_provenance,
    }
    candidates = _structural_endorsement_heading_candidates(parent_source_text)
    if candidates:
        selected = candidates[-1]
        text = str(selected["text"])
        start = int(selected["start"])
        end = int(selected["end"])
        spans = exact_occurrence_spans(parent_source_text, text)
        ordinal = spans.index((start, end)) + 1
        in_segment = segment_start_char <= start and end <= segment_end_char
        source_segment: Mapping[str, Any] | None = None
        if in_segment:
            source_segment = {
                "document_id": segment_id,
                "segment_start_char": segment_start_char,
                "segment_end_char": segment_end_char,
            }
        else:
            for candidate_segment in sorted(
                list(parent_segments or []),
                key=lambda value: (
                    int(value["segment_start_char"]), str(value["document_id"])
                ),
            ):
                if (
                    int(candidate_segment["segment_start_char"]) <= start
                    and end <= int(candidate_segment["segment_end_char"])
                ):
                    source_segment = candidate_segment
                    break
        source_segment_start = (
            int(source_segment["segment_start_char"])
            if source_segment is not None else None
        )
        payload = {
            **common,
            "source_scope": "segment" if in_segment else "parent_document",
            "context_derivation_method": (
                "controlled_parent_structural_heading_in_segment_v1"
                if in_segment
                else "controlled_parent_structural_heading_carried_v1"
            ),
            "context_text": text,
            "context_text_sha256": _sha256_text(text),
            "context_parent_start_char": start,
            "context_parent_end_char": end,
            "context_parent_exact_match_count": len(spans),
            "context_parent_occurrence_ordinal": ordinal,
            "context_line_index_zero_based": int(
                selected["line_index_zero_based"]
            ),
            "context_scope_parent_start_char": end,
            "context_scope_parent_end_char": len(parent_source_text),
            "context_source_segment_id": (
                str(source_segment["document_id"])
                if source_segment is not None else None
            ),
            "context_source_segment_start_char": source_segment_start,
            "context_source_segment_end_char": (
                int(source_segment["segment_end_char"])
                if source_segment is not None else None
            ),
            "context_local_start_char": (
                start - source_segment_start
                if source_segment_start is not None else None
            ),
            "context_local_end_char": (
                end - source_segment_start
                if source_segment_start is not None else None
            ),
            "record_to_context_distance_rule": (
                "primary_record_parent_start_minus_context_parent_end"
            ),
            "no_literal_anchor_reason": None,
        }
    else:
        payload = {
            **common,
            "source_scope": "page_or_section_route_context",
            "context_derivation_method": (
                "validated_endorsement_url_no_literal_parent_anchor_v1"
            ),
            "context_text": None,
            "context_text_sha256": None,
            "context_parent_start_char": None,
            "context_parent_end_char": None,
            "context_parent_exact_match_count": 0,
            "context_parent_occurrence_ordinal": None,
            "context_line_index_zero_based": None,
            "context_scope_parent_start_char": None,
            "context_scope_parent_end_char": None,
            "context_source_segment_id": None,
            "context_source_segment_start_char": None,
            "context_source_segment_end_char": None,
            "context_local_start_char": None,
            "context_local_end_char": None,
            "record_to_context_distance_rule": None,
            "no_literal_anchor_reason": (
                "no_controlled_parent_structural_support_heading"
            ),
        }
    payload["context_id"] = _sha256_text(_canonical_json(payload))
    if set(payload) != ENDORSEMENT_CONTEXT_PROVENANCE_FIELDS:
        raise AssertionError("endorsement context provenance fields drifted")
    return payload


def endorsement_context_provenance_errors(
    provenance: Any,
    *,
    parent_source_text: str | None = None,
    segment_source_text: str | None = None,
    segment_id: str | None = None,
    segment_start_char: int | None = None,
    parent_document_id: str | None = None,
    source_urls: Iterable[Any] | None = None,
) -> list[str]:
    """Validate a carried context object, optionally against full source bytes."""
    errors: list[str] = []
    if not isinstance(provenance, Mapping):
        return ["endorsement_context_provenance_not_object"]
    if set(provenance) != ENDORSEMENT_CONTEXT_PROVENANCE_FIELDS:
        errors.append("endorsement_context_provenance_fields_differ")
        return errors
    payload = {key: value for key, value in provenance.items() if key != "context_id"}
    if provenance.get("context_id") != _sha256_text(_canonical_json(payload)):
        errors.append("endorsement_context_id_mismatch")
    fixed = {
        "version": ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
        "route": "endorsements",
        "url_classifier_id": ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
        "url_classifier_spec_sha256": ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
        "url_classifier_result": "endorsements",
        "structural_heading_spec_sha256": (
            ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256
        ),
        "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
        "inference_display_escape_spec_sha256": (
            INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
        ),
    }
    for field, expected in fixed.items():
        if provenance.get(field) != expected:
            errors.append(f"endorsement_context_{field}_mismatch")
    starts_mid_line = provenance.get("segment_starts_mid_line")
    ends_mid_line = provenance.get("segment_ends_mid_line")
    has_complete_line = provenance.get("segment_has_complete_line")
    boundary_fields = (
        "segment_first_complete_line_start_char",
        "segment_first_complete_line_parent_start_char",
        "segment_first_complete_line_inference_display_start_char",
        "segment_last_complete_line_end_char",
        "segment_last_complete_line_parent_end_char",
        "segment_last_complete_line_inference_display_end_char",
    )
    if not isinstance(starts_mid_line, bool):
        errors.append("endorsement_context_segment_mid_line_flag_invalid")
    if not isinstance(ends_mid_line, bool):
        errors.append("endorsement_context_segment_end_mid_line_flag_invalid")
    if not isinstance(has_complete_line, bool):
        errors.append("endorsement_context_segment_has_complete_line_invalid")
    elif has_complete_line:
        for field in boundary_fields:
            value = provenance.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                errors.append(f"endorsement_context_{field}_invalid")
        first_complete_local = provenance.get(
            "segment_first_complete_line_start_char"
        )
        last_complete_local = provenance.get(
            "segment_last_complete_line_end_char"
        )
        if (
            isinstance(first_complete_local, int)
            and not isinstance(first_complete_local, bool)
            and isinstance(last_complete_local, int)
            and not isinstance(last_complete_local, bool)
            and first_complete_local >= last_complete_local
        ):
            errors.append("endorsement_context_complete_line_range_invalid")
    elif any(provenance.get(field) is not None for field in boundary_fields):
        errors.append("endorsement_context_empty_complete_line_offsets_not_null")
    if (
        isinstance(segment_source_text, str)
        and isinstance(segment_start_char, int)
        and not isinstance(segment_start_char, bool)
        and isinstance(starts_mid_line, bool)
        and isinstance(ends_mid_line, bool)
    ):
        expected_boundaries = endorsement_segment_complete_line_provenance(
            segment_source_text,
            segment_start_char=segment_start_char,
            segment_starts_mid_line=starts_mid_line,
            segment_ends_mid_line=ends_mid_line,
        )
        for field, expected in expected_boundaries.items():
            observed = provenance.get(field)
            if type(observed) is not type(expected) or observed != expected:
                errors.append(f"endorsement_context_{field}_mismatch")
    if (
        parent_source_text is not None
        and isinstance(segment_start_char, int)
        and not isinstance(segment_start_char, bool)
        and 0 <= segment_start_char <= len(parent_source_text)
    ):
        expected_mid_line = (
            segment_start_char > 0
            and parent_source_text[segment_start_char - 1] != "\n"
        )
        if starts_mid_line is not expected_mid_line:
            errors.append("endorsement_context_segment_mid_line_flag_mismatch")
        segment_end = segment_start_char + (
            len(segment_source_text) if isinstance(segment_source_text, str) else 0
        )
        expected_end_mid_line = bool(
            isinstance(segment_source_text, str)
            and segment_source_text
            and segment_end < len(parent_source_text)
            and segment_source_text[-1] != "\n"
            and parent_source_text[segment_end] != "\n"
        )
        if ends_mid_line is not expected_end_mid_line:
            errors.append("endorsement_context_segment_end_mid_line_flag_mismatch")
    if parent_document_id is not None and provenance.get(
        "parent_document_id"
    ) != parent_document_id:
        errors.append("endorsement_context_parent_document_id_mismatch")
    urls = None if source_urls is None else sorted(
        {str(value) for value in source_urls if str(value).strip()}
    )
    if urls is not None:
        if provenance.get("source_urls_sha256") != _sha256_text(_canonical_json(urls)):
            errors.append("endorsement_context_source_urls_sha256_mismatch")
        if provenance.get("qualifying_wayback_url") not in urls:
            errors.append("endorsement_context_qualifying_url_not_in_parent")
    try:
        wayback = str(provenance.get("qualifying_wayback_url") or "")
        from src.extract_text import classify_page_type
        if classify_page_type(wayback) != "endorsements":
            errors.append("endorsement_context_url_classifier_rejected")
        original, path = decoded_original_url_and_path(wayback)
        if provenance.get("qualifying_original_url") != original:
            errors.append("endorsement_context_original_url_mismatch")
        if provenance.get("qualifying_original_path") != path:
            errors.append("endorsement_context_original_path_mismatch")
    except (TypeError, ValueError):
        errors.append("endorsement_context_qualifying_url_invalid")

    scope = provenance.get("source_scope")
    if scope not in ENDORSEMENT_CONTEXT_SOURCE_SCOPES:
        errors.append("endorsement_context_source_scope_invalid")
        return errors
    if parent_source_text is not None:
        if provenance.get("parent_source_text_sha256") != _sha256_text(
            parent_source_text
        ):
            errors.append("endorsement_context_parent_text_sha256_mismatch")
        if provenance.get("parent_source_n_chars") != len(parent_source_text):
            errors.append("endorsement_context_parent_text_length_mismatch")
    if scope == "page_or_section_route_context":
        nullable = (
            "context_text", "context_text_sha256", "context_parent_start_char",
            "context_parent_end_char", "context_parent_occurrence_ordinal",
            "context_line_index_zero_based", "context_scope_parent_start_char",
            "context_scope_parent_end_char", "context_source_segment_id",
            "context_source_segment_start_char",
            "context_source_segment_end_char",
            "context_local_start_char", "context_local_end_char",
            "record_to_context_distance_rule",
        )
        if any(provenance.get(field) is not None for field in nullable):
            errors.append("endorsement_route_context_has_text_provenance")
        if provenance.get("context_parent_exact_match_count") != 0:
            errors.append("endorsement_route_context_match_count_not_zero")
        if not _nonempty_string(provenance.get("no_literal_anchor_reason")):
            errors.append("endorsement_route_context_missing_reason")
        return errors

    text = provenance.get("context_text")
    start = provenance.get("context_parent_start_char")
    end = provenance.get("context_parent_end_char")
    if not _nonempty_string(text):
        errors.append("endorsement_text_context_missing_text")
        return errors
    if provenance.get("context_text_sha256") != _sha256_text(str(text)):
        errors.append("endorsement_text_context_text_sha256_mismatch")
    if not (
        isinstance(start, int) and not isinstance(start, bool)
        and isinstance(end, int) and not isinstance(end, bool)
        and 0 <= start < end
    ):
        errors.append("endorsement_text_context_parent_offsets_invalid")
        return errors
    if parent_source_text is not None:
        spans = exact_occurrence_spans(parent_source_text, str(text))
        if end > len(parent_source_text) or parent_source_text[start:end] != text:
            errors.append("endorsement_text_context_parent_span_not_exact")
        if provenance.get("context_parent_exact_match_count") != len(spans):
            errors.append("endorsement_text_context_parent_match_count_mismatch")
        ordinal = spans.index((start, end)) + 1 if (start, end) in spans else None
        if provenance.get("context_parent_occurrence_ordinal") != ordinal:
            errors.append("endorsement_text_context_occurrence_ordinal_mismatch")
    elif not (
        isinstance(provenance.get("context_parent_exact_match_count"), int)
        and provenance.get("context_parent_exact_match_count") >= 1
    ):
        errors.append("endorsement_text_context_parent_match_count_invalid")
    if provenance.get("context_scope_parent_start_char") != end:
        errors.append("endorsement_text_context_scope_start_mismatch")
    if provenance.get("context_scope_parent_end_char") != provenance.get(
        "parent_source_n_chars"
    ):
        errors.append("endorsement_text_context_scope_end_mismatch")
    if provenance.get("no_literal_anchor_reason") is not None:
        errors.append("endorsement_text_context_has_no_anchor_reason")
    if scope == "segment":
        local_start = provenance.get("context_local_start_char")
        local_end = provenance.get("context_local_end_char")
        if segment_id is not None and provenance.get(
            "context_source_segment_id"
        ) != segment_id:
            errors.append("endorsement_text_context_segment_id_mismatch")
        if segment_start_char is not None and (
            provenance.get("context_source_segment_start_char")
            != segment_start_char
            or local_start != start - segment_start_char
            or local_end != end - segment_start_char
        ):
            errors.append("endorsement_text_context_local_offsets_mismatch")
        if segment_source_text is not None and not (
            isinstance(local_start, int) and isinstance(local_end, int)
            and 0 <= local_start < local_end <= len(segment_source_text)
            and segment_source_text[local_start:local_end] == text
        ):
            errors.append("endorsement_text_context_segment_span_not_exact")
    else:
        source_values = tuple(provenance.get(field) for field in (
            "context_source_segment_id", "context_source_segment_start_char",
            "context_source_segment_end_char", "context_local_start_char",
            "context_local_end_char",
        ))
        if any(value is not None for value in source_values):
            source_id, source_start, source_end, local_start, local_end = source_values
            if not (
                _nonempty_string(source_id)
                and isinstance(source_start, int) and not isinstance(source_start, bool)
                and isinstance(source_end, int) and not isinstance(source_end, bool)
                and isinstance(local_start, int) and not isinstance(local_start, bool)
                and isinstance(local_end, int) and not isinstance(local_end, bool)
                and 0 <= source_start <= start < end <= source_end
                and start == source_start + local_start
                and end == source_start + local_end
            ):
                errors.append("endorsement_parent_context_source_segment_invalid")
    return errors


def contains_street_address(value: Any) -> bool:
    """Conservatively flag address-like text that must not be released."""
    return isinstance(value, str) and any(
        pattern.search(value) for pattern in _STREET_ADDRESS_PATTERNS
    )


def endorsement_entity_category_valid(entity_type: Any, category: Any) -> bool:
    """Apply the frozen endorsement entity/category crosswalk."""
    if not isinstance(category, str):
        return False
    allowed = ENDORSEMENT_CATEGORY_ALLOWED_ENTITY_TYPES.get(category, ())
    return entity_type in allowed


def local_roots_semantic_errors(item: Mapping[str, Any]) -> list[str]:
    """Return stable hard-gate codes for a local-roots record."""
    if item.get("field") != "local_roots":
        return []
    errors: list[str] = []
    relationship = item.get("relationship")
    evidence = item.get("evidence")
    if not (
        isinstance(relationship, str)
        and relationship
        and isinstance(evidence, str)
        and relationship in evidence
    ):
        errors.append("local_roots_relationship_not_verbatim_in_evidence")
    if contains_street_address(item.get("value")) or contains_street_address(evidence):
        errors.append("local_roots_address_like_text_prohibited")
    return errors


def endorsement_semantic_errors(
    item: Mapping[str, Any],
    endorsement_context_provenance: Mapping[str, Any] | None = None,
) -> list[str]:
    """Return stable hard-gate codes for an endorsement record."""
    errors: list[str] = []
    if not endorsement_entity_category_valid(
        item.get("entity_type"), item.get("endorser_category")
    ):
        errors.append("endorsement_entity_category_mismatch")
    record = item.get("record_evidence")
    context = item.get("endorsement_context_evidence")
    mode = item.get("support_mode")
    scope = item.get("endorsement_context_evidence_source_scope")
    context_has_direct_support = (
        isinstance(context, str) and bool(_EXPLICIT_ENDORSEMENT_PATTERN.search(context))
    )
    context_has_structural_support = (
        isinstance(context, str) and bool(_ENDORSEMENT_CONTEXT_PATTERN.search(context))
    )
    if mode == "explicit_text" and (
        not isinstance(record, str) or not _EXPLICIT_ENDORSEMENT_PATTERN.search(record)
    ):
        errors.append("explicit_text_record_lacks_direct_support_marker")
    if scope is None:
        if not (context_has_direct_support or context_has_structural_support):
            errors.append("endorsement_context_lacks_support_marker")
        return errors
    if scope not in ENDORSEMENT_CONTEXT_SOURCE_SCOPES:
        errors.append("endorsement_context_source_scope_invalid")
        return errors
    if mode == "explicit_text":
        if scope != "segment":
            errors.append("explicit_text_context_must_be_segment_scoped")
        if not (context_has_direct_support or context_has_structural_support):
            errors.append("endorsement_context_lacks_support_marker")
    elif mode == "page_or_section_context":
        if scope == "page_or_section_route_context":
            if context is not None:
                errors.append("endorsement_route_context_must_have_null_evidence")
            if item.get("endorsement_context_evidence_alignment_method") != (
                ALIGNMENT_METHOD_PAGE_ROUTE
            ):
                errors.append("endorsement_route_context_method_invalid")
            for field in (
                "endorsement_context_qualifying_wayback_url",
                "endorsement_context_qualifying_original_url",
                "endorsement_context_qualifying_original_path",
            ):
                if not _nonempty_string(item.get(field)):
                    errors.append("endorsement_route_context_url_provenance_missing")
                    break
        else:
            if not (context_has_direct_support or context_has_structural_support):
                errors.append("endorsement_context_lacks_support_marker")
            if endorsement_context_provenance is not None:
                carried_match = (
                    context == endorsement_context_provenance.get("context_text")
                    and scope == endorsement_context_provenance.get("source_scope")
                )
                projected_segment_extension = (
                    item.get("endorsement_context_evidence_alignment_method")
                    == ALIGNMENT_METHOD_PROVIDED_SEGMENT_EXTENSION
                    and scope == "segment"
                    and endorsement_context_provenance.get("source_scope")
                    == "segment"
                    and context == endorsement_context_provenance.get(
                        "context_text"
                    )
                    and item.get(
                        "endorsement_context_evidence_parent_start_char"
                    ) == endorsement_context_provenance.get(
                        "context_parent_start_char"
                    )
                    and item.get(
                        "endorsement_context_evidence_parent_end_char"
                    ) == endorsement_context_provenance.get(
                        "context_parent_end_char"
                    )
                )
                if not (carried_match or projected_segment_extension):
                    errors.append("endorsement_context_differs_from_carried_context")
    else:
        errors.append("endorsement_support_mode_invalid")
    if endorsement_context_provenance is not None and item.get(
        "endorsement_context_id"
    ) != endorsement_context_provenance.get("context_id"):
        errors.append("endorsement_context_id_differs_from_carried_context")
    return errors


def record_semantic_errors(
    route: str,
    item: Mapping[str, Any],
    *,
    endorsement_context_provenance: Mapping[str, Any] | None = None,
) -> list[str]:
    """Return deterministic public-release semantic gate failures."""
    if route not in ROUTES:
        raise ValueError(f"unknown extraction route: {route}")
    if route == "biography":
        return local_roots_semantic_errors(item)
    if route == "endorsements":
        return endorsement_semantic_errors(item, endorsement_context_provenance)
    return []


def _v2_1_model_item_schema_valid(route: str, item: Mapping[str, Any]) -> bool:
    if set(item) != V2_1_MODEL_ITEM_FIELDS[route]:
        return False
    if route == "issues":
        return (
            _nullable_nonempty_string(item.get("source_heading"))
            and _nonempty_string(item.get("claim"))
            and item.get("statement_kind") in ISSUE_STATEMENT_KINDS
            and _nonempty_string(item.get("evidence"))
        )
    if route == "biography":
        field = item.get("field")
        timing = item.get("timing")
        relationship = item.get("relationship")
        valid = (
            field in BIOGRAPHY_FIELDS_V2
            and _nonempty_string(item.get("value"))
            and (timing is None or timing in BIOGRAPHY_TIMING_VALUES)
            and _nullable_nonempty_string(relationship)
            and _nonempty_string(item.get("evidence"))
        )
        if field in BIOGRAPHY_FIELDS_REQUIRING_TIMING:
            valid = valid and timing in BIOGRAPHY_TIMING_VALUES
        if field == "local_roots":
            valid = valid and _nonempty_string(relationship)
        else:
            valid = valid and relationship is None
        return bool(valid)

    return (
        _nonempty_string(item.get("endorser_name"))
        and item.get("entity_type") in ENDORSEMENT_ENTITY_TYPES
        and item.get("endorser_category") in ENDORSEMENT_CATEGORIES
        and all(
            _nullable_nonempty_string(item.get(field))
            for field in ("title", "affiliation", "jurisdiction")
        )
        and _nonempty_string(item.get("record_evidence"))
        and _nullable_nonempty_string(item.get("endorsement_context_evidence"))
        and item.get("support_mode") in ENDORSEMENT_SUPPORT_MODES
        and (
            item.get("support_mode") != "explicit_text"
            or _nonempty_string(item.get("endorsement_context_evidence"))
        )
    )


def _source_model_item_id(
    route: str,
    item_index: int,
    item: Mapping[str, Any],
    model_extraction_sha256: str,
) -> str:
    return _sha256_text(_canonical_json({
        "route": route,
        "item_index": item_index,
        "model_item": item,
        "model_extraction_sha256": model_extraction_sha256,
    }))


def align_extraction_exact_spans(
    route: str,
    model_extraction: Any,
    source_text: str,
    *,
    endorsement_context_provenance: Mapping[str, Any] | None = None,
    segment_start_char: int = 0,
    fixed_anchor_spans: Mapping[int, Mapping[str, tuple[int, int]]] | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], dict[str, Any]]:
    """Create a separate exact-aligned extraction and retain every failure.

    The parsed model object is never mutated. A model item enters the aligned
    extraction only when every required copied span has exactly one literal
    occurrence. Missing and ambiguous required anchors are returned privately
    with stable IDs and complete diagnostics. A non-unique optional anchor is
    nulled in the separate aligned extraction, while its original value and
    occurrence diagnostics remain in the private alignment report.
    """
    if route not in ROUTES:
        raise ValueError(f"unknown extraction route: {route}")
    if not isinstance(source_text, str):
        raise TypeError("source_text must be a string")
    if not isinstance(segment_start_char, int) or isinstance(segment_start_char, bool):
        raise TypeError("segment_start_char must be an integer")
    context_provenance_errors: list[str] = []
    if route == "endorsements":
        context_provenance_errors = endorsement_context_provenance_errors(
            endorsement_context_provenance,
            segment_source_text=source_text,
            segment_id=(
                str(endorsement_context_provenance.get("context_source_segment_id"))
                if isinstance(endorsement_context_provenance, Mapping)
                and endorsement_context_provenance.get("context_source_segment_id")
                is not None
                else None
            ),
            segment_start_char=segment_start_char,
        )
        if context_provenance_errors:
            raise ValueError(
                "invalid endorsement context provenance: "
                + ", ".join(context_provenance_errors)
            )
    model_copy = copy.deepcopy(model_extraction)
    model_hash = _sha256_text(_canonical_json(model_copy))
    key = LIST_KEY_BY_VERSION["2.1"][route]
    root_valid = (
        isinstance(model_copy, dict)
        and set(model_copy) == {key}
        and isinstance(model_copy[key], list)
    )
    if not root_valid:
        report = {
            "alignment_id": EXACT_ALIGNMENT_ID,
            "alignment_spec_sha256": EXACT_ALIGNMENT_SPEC_SHA256,
            "source_text_sha256": _sha256_text(source_text),
            "model_extraction_sha256": model_hash,
            "original_model_extraction_sha256": model_hash,
            "aligned_extraction_sha256": None,
            "model_schema_valid": False,
            "model_semantic_valid": False,
            "model_item_count": 0,
            "aligned_item_count": 0,
            "withheld_item_count": 0,
            "all_model_items_accounted_for": False,
            "status": "invalid_schema",
            "aligned_source_model_item_ids": [],
            "withheld_source_model_item_ids": [],
            "withheld_item_counts_by_reason": {},
            "optional_anchor_adjustment_count": 0,
            "optional_anchor_adjustments": [],
            "display_anchor_adjustment_count": 0,
            "display_anchor_adjustments": [],
            "endorsement_context_extension_count": 0,
            "endorsement_context_extensions": [],
            "endorsement_context_id": (
                endorsement_context_provenance.get("context_id")
                if endorsement_context_provenance is not None else None
            ),
        }
        return None, [], report

    aligned_items: list[dict[str, Any]] = []
    withheld: list[dict[str, Any]] = []
    optional_anchor_adjustments: list[dict[str, Any]] = []
    display_anchor_adjustments: list[dict[str, Any]] = []
    endorsement_context_extensions: list[dict[str, Any]] = []
    model_schema_valid = True
    model_semantic_valid = True
    for item_index, raw_item in enumerate(model_copy[key]):
        item_is_mapping = isinstance(raw_item, Mapping)
        item = dict(raw_item) if item_is_mapping else raw_item
        raw_id = _source_model_item_id(
            route,
            item_index,
            item if item_is_mapping else {"non_object_item": item},
            model_hash,
        )
        if not item_is_mapping or not _v2_1_model_item_schema_valid(route, item):
            model_schema_valid = False
            withheld.append({
                "source_model_item_id": raw_id,
                "item_index": item_index,
                "model_item": copy.deepcopy(raw_item),
                "reason": "invalid_item_schema",
                "anchors": {},
                "semantic_errors": [],
            })
            continue

        aligned_item = copy.deepcopy(item)
        aligned_item["source_model_item_id"] = raw_id
        if route == "endorsements":
            assert endorsement_context_provenance is not None
            aligned_item.update({
                "endorsement_context_id": endorsement_context_provenance[
                    "context_id"
                ],
                "endorsement_context_evidence_source_scope": None,
                "endorsement_context_evidence_parent_start_char": None,
                "endorsement_context_evidence_parent_end_char": None,
                "endorsement_context_record_distance_chars": None,
                "endorsement_context_qualifying_wayback_url": None,
                "endorsement_context_qualifying_original_url": None,
                "endorsement_context_qualifying_original_path": None,
            })
        anchor_diagnostics: dict[str, dict[str, Any]] = {}
        failed_methods: list[str] = []
        alignment_semantic_errors: list[str] = []
        item_optional_anchor_adjustments: list[dict[str, Any]] = []
        item_display_anchor_adjustments: list[dict[str, Any]] = []
        item_context_extensions: list[dict[str, Any]] = []
        for text_field, start_field, end_field, nullable in V2_1_ANCHORS[route]:
            excerpt = item.get(text_field)
            count_field = f"{text_field}_exact_match_count"
            method_field = f"{text_field}_alignment_method"
            if route == "endorsements" and text_field == (
                "endorsement_context_evidence"
            ):
                assert endorsement_context_provenance is not None
                carried = endorsement_context_provenance
                mode = str(item["support_mode"])
                carried_scope = str(carried["source_scope"])
                start = end = None
                parent_start = parent_end = None
                count = 0
                method = ALIGNMENT_METHOD_MISSING
                final_scope: str | None = None
                if mode == "page_or_section_context":
                    if carried_scope == "page_or_section_route_context":
                        if excerpt is None:
                            method = ALIGNMENT_METHOD_PAGE_ROUTE
                            final_scope = carried_scope
                            aligned_item[
                                "endorsement_context_qualifying_wayback_url"
                            ] = carried["qualifying_wayback_url"]
                            aligned_item[
                                "endorsement_context_qualifying_original_url"
                            ] = carried["qualifying_original_url"]
                            aligned_item[
                                "endorsement_context_qualifying_original_path"
                            ] = carried["qualifying_original_path"]
                        else:
                            method = ALIGNMENT_METHOD_CARRIED_CONTEXT_MISMATCH
                            failed_methods.append(method)
                    elif excerpt != carried.get("context_text"):
                        extension_source_spans = exact_occurrence_spans(
                            source_text, str(excerpt or "")
                        )
                        extension_display_spans = (
                            exact_inference_display_occurrence_spans(
                                source_text, str(excerpt)
                            )
                            if not extension_source_spans
                            and isinstance(excerpt, str)
                            and excerpt
                            else []
                        )
                        extension_spans = (
                            extension_source_spans or extension_display_spans
                        )
                        carried_text = str(carried.get("context_text") or "")
                        carried_local_start = carried.get("context_local_start_char")
                        carried_local_end = carried.get("context_local_end_char")
                        record_start = aligned_item.get("record_evidence_start_char")
                        record_end = aligned_item.get("record_evidence_end_char")
                        extension_valid = False
                        if (
                            carried_scope == "segment"
                            and len(extension_spans) == 1
                            and _nonempty_string(excerpt)
                            and isinstance(carried_local_start, int)
                            and not isinstance(carried_local_start, bool)
                            and isinstance(carried_local_end, int)
                            and not isinstance(carried_local_end, bool)
                            and isinstance(record_start, int)
                            and not isinstance(record_start, bool)
                            and isinstance(record_end, int)
                            and not isinstance(record_end, bool)
                        ):
                            extension_start, extension_end = extension_spans[0]
                            extension_source_excerpt = source_text[
                                extension_start:extension_end
                            ]
                            record_parent_start = segment_start_char + record_start
                            record_parent_end = segment_start_char + record_end
                            extension_parent_start = (
                                segment_start_char + extension_start
                            )
                            extension_parent_end = segment_start_char + extension_end
                            extension_valid = bool(
                                extension_start == carried_local_start
                                and extension_end > carried_local_end
                                and extension_source_excerpt.startswith(
                                    carried_text
                                )
                                and source_text[
                                    extension_start:carried_local_end
                                ] == carried_text
                                and extension_parent_start
                                == carried.get("context_parent_start_char")
                                and int(carried["context_scope_parent_start_char"])
                                <= extension_parent_end
                                <= record_parent_start
                                < record_parent_end
                                <= int(carried["context_scope_parent_end_char"])
                                and (
                                    _EXPLICIT_ENDORSEMENT_PATTERN.search(
                                        extension_source_excerpt
                                    )
                                    or _ENDORSEMENT_CONTEXT_PATTERN.search(
                                        extension_source_excerpt
                                    )
                                )
                            )
                        if extension_valid:
                            extension_start, extension_end = extension_spans[0]
                            start = int(carried_local_start)
                            end = int(carried_local_end)
                            parent_start = int(carried["context_parent_start_char"])
                            parent_end = int(carried["context_parent_end_char"])
                            count = len(
                                exact_occurrence_spans(source_text, carried_text)
                            )
                            method = ALIGNMENT_METHOD_PROVIDED_SEGMENT_EXTENSION
                            final_scope = "segment"
                            aligned_item[text_field] = carried_text
                            aligned_item[
                                "endorsement_context_record_distance_chars"
                            ] = (
                                segment_start_char
                                + int(aligned_item["record_evidence_start_char"])
                                - parent_end
                            )
                            extension = {
                                "source_model_item_id": raw_id,
                                "item_index": item_index,
                                "carried_context_id": carried["context_id"],
                                "carried_context_text": carried_text,
                                "carried_context_text_sha256": _sha256_text(
                                    carried_text
                                ),
                                "model_context_excerpt": excerpt,
                                "model_context_excerpt_sha256": _sha256_text(
                                    str(excerpt)
                                ),
                                "model_context_source_excerpt": (
                                    extension_source_excerpt
                                ),
                                "model_context_source_excerpt_sha256": (
                                    _sha256_text(extension_source_excerpt)
                                ),
                                "model_context_source_exact_match_count": len(
                                    extension_source_spans
                                ),
                                "model_context_display_exact_boundary_match_count": (
                                    len(extension_display_spans)
                                ),
                                "model_context_source_alignment_method": (
                                    ALIGNMENT_METHOD_UNIQUE
                                    if extension_source_spans
                                    else ALIGNMENT_METHOD_UNIQUE_DISPLAY
                                ),
                                "model_context_start_char": extension_start,
                                "model_context_end_char": extension_end,
                                "final_source_scope": "segment",
                                "final_start_char": start,
                                "final_end_char": end,
                                "final_parent_start_char": parent_start,
                                "final_parent_end_char": parent_end,
                                "final_alignment_method": method,
                            }
                            extension[
                                "endorsement_context_extension_id"
                            ] = _sha256_text(_canonical_json(extension))
                            item_context_extensions.append(extension)
                        else:
                            method = ALIGNMENT_METHOD_CARRIED_CONTEXT_MISMATCH
                            failed_methods.append(method)
                    else:
                        final_scope = carried_scope
                        parent_start = int(carried["context_parent_start_char"])
                        parent_end = int(carried["context_parent_end_char"])
                        if carried_scope == "segment":
                            start = int(carried["context_local_start_char"])
                            end = int(carried["context_local_end_char"])
                            fixed_context_span = (
                                fixed_anchor_spans.get(item_index, {}).get(text_field)
                                if isinstance(fixed_anchor_spans, Mapping)
                                else None
                            )
                            if fixed_context_span == (start, end):
                                count = 1
                                method = ALIGNMENT_METHOD_PROVIDED_SEGMENT
                            else:
                                count = len(exact_occurrence_spans(
                                    source_text, str(excerpt)
                                ))
                                method = ALIGNMENT_METHOD_PROVIDED_SEGMENT
                        else:
                            count = int(carried["context_parent_exact_match_count"])
                            method = ALIGNMENT_METHOD_PROVIDED_PARENT
                        record_start = aligned_item.get(
                            "record_evidence_start_char"
                        )
                        record_end = aligned_item.get("record_evidence_end_char")
                        if isinstance(record_start, int) and isinstance(record_end, int):
                            record_parent_start = segment_start_char + record_start
                            record_parent_end = segment_start_char + record_end
                            aligned_item[
                                "endorsement_context_record_distance_chars"
                            ] = record_parent_start - parent_end
                            if not (
                                int(carried["context_scope_parent_start_char"])
                                <= record_parent_start < record_parent_end
                                <= int(carried["context_scope_parent_end_char"])
                            ):
                                alignment_semantic_errors.append(
                                    "endorsement_record_outside_context_scope"
                                )
                else:
                    fixed_context_span = (
                        fixed_anchor_spans.get(item_index, {}).get(text_field)
                        if isinstance(fixed_anchor_spans, Mapping)
                        else None
                    )
                    spans = exact_occurrence_spans(source_text, str(excerpt or ""))
                    display_spans = (
                        exact_inference_display_occurrence_spans(
                            source_text, str(excerpt)
                        )
                        if not spans and isinstance(excerpt, str) and excerpt
                        else []
                    )
                    if (
                        isinstance(fixed_context_span, tuple)
                        and len(fixed_context_span) == 2
                        and source_text[
                            fixed_context_span[0]:fixed_context_span[1]
                        ] == excerpt
                    ):
                        start, end = fixed_context_span
                        parent_start = segment_start_char + start
                        parent_end = segment_start_char + end
                        count = 1
                        method = ALIGNMENT_METHOD_PROVIDED_SEGMENT
                        final_scope = "segment"
                    elif len(spans) == 1:
                        start, end = spans[0]
                        parent_start = segment_start_char + start
                        parent_end = segment_start_char + end
                        count = 1
                        method = ALIGNMENT_METHOD_UNIQUE
                        final_scope = "segment"
                    elif len(display_spans) == 1:
                        start, end = display_spans[0]
                        final_excerpt = source_text[start:end]
                        parent_start = segment_start_char + start
                        parent_end = segment_start_char + end
                        count = 1
                        method = ALIGNMENT_METHOD_UNIQUE_DISPLAY
                        final_scope = "segment"
                        aligned_item[text_field] = final_excerpt
                        adjustment = {
                            "source_model_item_id": raw_id,
                            "item_index": item_index,
                            "text_field": text_field,
                            "model_excerpt": excerpt,
                            "model_excerpt_sha256": _sha256_text(str(excerpt)),
                            "model_source_exact_match_count": 0,
                            "model_display_exact_boundary_match_count": 1,
                            "alignment_disposition": (
                                "canonical_inference_display_inverse"
                            ),
                            "final_excerpt": final_excerpt,
                            "final_excerpt_sha256": _sha256_text(final_excerpt),
                            "final_start_char": start,
                            "final_end_char": end,
                            "final_exact_match_count": 1,
                            "final_alignment_method": method,
                            "canonical_display_roundtrip_valid": (
                                inference_display_escape(final_excerpt) == excerpt
                            ),
                        }
                        adjustment["display_anchor_adjustment_id"] = _sha256_text(
                            _canonical_json(adjustment)
                        )
                        item_display_anchor_adjustments.append(adjustment)
                    elif spans or display_spans:
                        count = len(spans) if spans else len(display_spans)
                        method = ALIGNMENT_METHOD_AMBIGUOUS
                        failed_methods.append(method)
                    else:
                        method = ALIGNMENT_METHOD_MISSING
                        failed_methods.append(method)
                aligned_item[start_field] = start
                aligned_item[end_field] = end
                aligned_item[count_field] = count
                aligned_item[method_field] = method
                aligned_item[
                    "endorsement_context_evidence_source_scope"
                ] = final_scope
                aligned_item[
                    "endorsement_context_evidence_parent_start_char"
                ] = parent_start
                aligned_item[
                    "endorsement_context_evidence_parent_end_char"
                ] = parent_end
                anchor_diagnostics[text_field] = {
                    "model_excerpt": excerpt,
                    "final_excerpt": aligned_item.get(text_field),
                    "carried_context_id": carried["context_id"],
                    "carried_source_scope": carried_scope,
                    "exact_match_count": count,
                    "alignment_method": method,
                    "final_source_scope": final_scope,
                    "final_start_char": start,
                    "final_end_char": end,
                    "final_parent_start_char": parent_start,
                    "final_parent_end_char": parent_end,
                }
                continue
            if excerpt is None and nullable:
                aligned_item[start_field] = None
                aligned_item[end_field] = None
                aligned_item[count_field] = 0
                aligned_item[method_field] = ALIGNMENT_METHOD_NOT_APPLICABLE
                anchor_diagnostics[text_field] = {
                    "exact_match_count": 0,
                    "alignment_method": ALIGNMENT_METHOD_NOT_APPLICABLE,
                    "final_start_char": None,
                    "final_end_char": None,
                }
                continue
            fixed_span = (
                fixed_anchor_spans.get(item_index, {}).get(text_field)
                if isinstance(fixed_anchor_spans, Mapping)
                else None
            )
            if fixed_span is not None:
                if (
                    not isinstance(fixed_span, tuple)
                    or len(fixed_span) != 2
                    or any(type(value) is not int for value in fixed_span)
                ):
                    raise ValueError("fixed anchor span must be an exact integer pair")
                start, end = fixed_span
                if not (0 <= start < end <= len(source_text)):
                    raise ValueError("fixed anchor span is outside segment source text")
                if source_text[start:end] != excerpt:
                    raise ValueError("fixed anchor span does not reproduce model excerpt")
                method = "provided_authenticated_parent_projection_v1"
                aligned_item[start_field] = start
                aligned_item[end_field] = end
                aligned_item[count_field] = 1
                aligned_item[method_field] = method
                anchor_diagnostics[text_field] = {
                    "model_excerpt": excerpt,
                    "source_exact_match_count": len(
                        exact_occurrence_spans(source_text, str(excerpt))
                    ),
                    "display_exact_boundary_match_count": 0,
                    "final_excerpt": excerpt,
                    "exact_match_count": 1,
                    "alignment_method": method,
                    "final_start_char": start,
                    "final_end_char": end,
                }
                continue
            spans = exact_occurrence_spans(source_text, str(excerpt or ""))
            display_spans = (
                exact_inference_display_occurrence_spans(source_text, excerpt)
                if not spans and isinstance(excerpt, str) and excerpt
                else []
            )
            if len(spans) == 1:
                start, end = spans[0]
                method = ALIGNMENT_METHOD_UNIQUE
            elif len(display_spans) == 1:
                start, end = display_spans[0]
                final_excerpt = source_text[start:end]
                method = ALIGNMENT_METHOD_UNIQUE_DISPLAY
                aligned_item[text_field] = final_excerpt
                adjustment = {
                    "source_model_item_id": raw_id,
                    "item_index": item_index,
                    "text_field": text_field,
                    "model_excerpt": excerpt,
                    "model_excerpt_sha256": _sha256_text(str(excerpt)),
                    "model_source_exact_match_count": 0,
                    "model_display_exact_boundary_match_count": 1,
                    "alignment_disposition": (
                        "canonical_inference_display_inverse"
                    ),
                    "final_excerpt": final_excerpt,
                    "final_excerpt_sha256": _sha256_text(final_excerpt),
                    "final_start_char": start,
                    "final_end_char": end,
                    "final_exact_match_count": 1,
                    "final_alignment_method": method,
                    "canonical_display_roundtrip_valid": (
                        inference_display_escape(final_excerpt) == excerpt
                    ),
                }
                adjustment["display_anchor_adjustment_id"] = _sha256_text(
                    _canonical_json(adjustment)
                )
                item_display_anchor_adjustments.append(adjustment)
            elif nullable:
                model_method = (
                    ALIGNMENT_METHOD_AMBIGUOUS
                    if spans or display_spans else ALIGNMENT_METHOD_MISSING
                )
                adjustment = {
                    "source_model_item_id": raw_id,
                    "item_index": item_index,
                    "text_field": text_field,
                    "model_excerpt": excerpt,
                    "model_excerpt_sha256": _sha256_text(str(excerpt)),
                    "model_exact_match_count": len(spans),
                    "model_display_exact_boundary_match_count": len(display_spans),
                    "model_alignment_method": model_method,
                    "reason": (
                        "ambiguous_optional_exact_span"
                        if spans or display_spans else "missing_optional_exact_span"
                    ),
                    "alignment_disposition": "optional_anchor_nulled",
                    "final_excerpt": None,
                    "final_start_char": None,
                    "final_end_char": None,
                    "final_exact_match_count": 0,
                    "final_alignment_method": ALIGNMENT_METHOD_NOT_APPLICABLE,
                }
                adjustment["optional_anchor_adjustment_id"] = _sha256_text(
                    _canonical_json(adjustment)
                )
                item_optional_anchor_adjustments.append(adjustment)
                aligned_item[text_field] = None
                aligned_item[start_field] = None
                aligned_item[end_field] = None
                aligned_item[count_field] = 0
                aligned_item[method_field] = ALIGNMENT_METHOD_NOT_APPLICABLE
                anchor_diagnostics[text_field] = {
                    "model_excerpt": excerpt,
                    "model_exact_match_count": len(spans),
                    "model_display_exact_boundary_match_count": len(display_spans),
                    "model_alignment_method": model_method,
                    "alignment_disposition": "optional_anchor_nulled",
                    "exact_match_count": 0,
                    "alignment_method": ALIGNMENT_METHOD_NOT_APPLICABLE,
                    "final_start_char": None,
                    "final_end_char": None,
                }
                continue
            elif spans or display_spans:
                start = end = None
                method = ALIGNMENT_METHOD_AMBIGUOUS
                failed_methods.append(method)
            else:
                start = end = None
                method = ALIGNMENT_METHOD_MISSING
                failed_methods.append(method)
            aligned_item[start_field] = start
            aligned_item[end_field] = end
            match_count = len(spans) if spans else len(display_spans)
            aligned_item[count_field] = match_count
            aligned_item[method_field] = method
            anchor_diagnostics[text_field] = {
                "model_excerpt": excerpt,
                "source_exact_match_count": len(spans),
                "display_exact_boundary_match_count": len(display_spans),
                "final_excerpt": aligned_item.get(text_field),
                "exact_match_count": match_count,
                "alignment_method": method,
                "final_start_char": start,
                "final_end_char": end,
            }

        if route == "endorsements" and endorsement_context_provenance is not None:
            has_complete_line = endorsement_context_provenance.get(
                "segment_has_complete_line"
            )
            first_complete_local = endorsement_context_provenance.get(
                "segment_first_complete_line_start_char"
            )
            last_complete_local = endorsement_context_provenance.get(
                "segment_last_complete_line_end_char"
            )
            record_start = aligned_item.get("record_evidence_start_char")
            record_end = aligned_item.get("record_evidence_end_char")
            if has_complete_line is False and (
                isinstance(record_start, int)
                and not isinstance(record_start, bool)
                and isinstance(record_end, int)
                and not isinstance(record_end, bool)
            ):
                alignment_semantic_errors.append(
                    "endorsement_record_intersects_clipped_segment_line"
                )
            elif (
                has_complete_line is True
                and
                isinstance(first_complete_local, int)
                and not isinstance(first_complete_local, bool)
                and isinstance(last_complete_local, int)
                and not isinstance(last_complete_local, bool)
                and isinstance(record_start, int)
                and not isinstance(record_start, bool)
                and isinstance(record_end, int)
                and not isinstance(record_end, bool)
                and (
                    record_start < first_complete_local
                    or record_end > last_complete_local
                )
            ):
                alignment_semantic_errors.append(
                    "endorsement_record_intersects_clipped_segment_line"
                )
        semantic_errors = alignment_semantic_errors + record_semantic_errors(
            route,
            aligned_item,
            endorsement_context_provenance=endorsement_context_provenance,
        )
        if semantic_errors:
            model_semantic_valid = False
        record_disposition = (
            "withheld" if failed_methods or semantic_errors else "aligned"
        )
        for adjustment in item_optional_anchor_adjustments:
            adjustment["record_disposition"] = record_disposition
            optional_anchor_adjustments.append(adjustment)
        for adjustment in item_display_anchor_adjustments:
            adjustment["record_disposition"] = record_disposition
            display_anchor_adjustments.append(adjustment)
        for extension in item_context_extensions:
            extension["record_disposition"] = record_disposition
            endorsement_context_extensions.append(extension)
        if failed_methods:
            if ALIGNMENT_METHOD_CARRIED_CONTEXT_MISMATCH in failed_methods:
                reason = "endorsement_context_mismatch"
            else:
                reason = (
                    "missing_exact_span"
                    if ALIGNMENT_METHOD_MISSING in failed_methods
                    else "ambiguous_exact_span"
                )
            withheld.append({
                "source_model_item_id": raw_id,
                "item_index": item_index,
                "model_item": copy.deepcopy(raw_item),
                "reason": reason,
                "anchors": anchor_diagnostics,
                "semantic_errors": semantic_errors,
            })
        elif semantic_errors:
            semantic_reason = (
                "clipped_segment_boundary"
                if "endorsement_record_intersects_clipped_segment_line"
                in semantic_errors
                else "invalid_item_semantics"
            )
            withheld.append({
                "source_model_item_id": raw_id,
                "item_index": item_index,
                "model_item": copy.deepcopy(raw_item),
                "reason": semantic_reason,
                "anchors": anchor_diagnostics,
                "semantic_errors": semantic_errors,
            })
        else:
            aligned_items.append(aligned_item)

    aligned = {key: aligned_items}
    aligned_ids = [item["source_model_item_id"] for item in aligned_items]
    withheld_ids = [item["source_model_item_id"] for item in withheld]
    reason_counts: dict[str, int] = {}
    for item in withheld:
        reason = str(item["reason"])
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    if not model_schema_valid:
        status = "invalid_schema"
    elif withheld and aligned_items:
        status = "partial_withheld"
    elif withheld:
        status = "all_withheld"
    else:
        status = "complete"
    report = {
        "alignment_id": EXACT_ALIGNMENT_ID,
        "alignment_spec_sha256": EXACT_ALIGNMENT_SPEC_SHA256,
        "source_text_sha256": _sha256_text(source_text),
        "model_extraction_sha256": model_hash,
        "original_model_extraction_sha256": model_hash,
        "aligned_extraction_sha256": _sha256_text(_canonical_json(aligned)),
        "model_schema_valid": model_schema_valid,
        "model_semantic_valid": model_semantic_valid,
        "model_item_count": len(model_copy[key]),
        "aligned_item_count": len(aligned_items),
        "withheld_item_count": len(withheld),
        "all_model_items_accounted_for": (
            len(model_copy[key]) == len(aligned_items) + len(withheld)
        ),
        "status": status,
        "aligned_source_model_item_ids": aligned_ids,
        "withheld_source_model_item_ids": withheld_ids,
        "withheld_item_counts_by_reason": dict(sorted(reason_counts.items())),
        "optional_anchor_adjustment_count": len(optional_anchor_adjustments),
        "optional_anchor_adjustments": optional_anchor_adjustments,
        "display_anchor_adjustment_count": len(display_anchor_adjustments),
        "display_anchor_adjustments": display_anchor_adjustments,
        "endorsement_context_extension_count": len(
            endorsement_context_extensions
        ),
        "endorsement_context_extensions": endorsement_context_extensions,
        "endorsement_context_id": (
            endorsement_context_provenance.get("context_id")
            if endorsement_context_provenance is not None else None
        ),
    }
    return aligned, withheld, report


def parent_char_span(
    segment_start_char: int, local_start_char: int, local_end_char: int
) -> tuple[int, int]:
    """Translate a validated segment-local half-open span to parent offsets."""
    values = (segment_start_char, local_start_char, local_end_char)
    if any(not isinstance(value, int) or isinstance(value, bool) for value in values):
        raise TypeError("character offsets must be integers, not booleans")
    if segment_start_char < 0 or local_start_char < 0 or local_end_char <= local_start_char:
        raise ValueError("invalid segment-local character span")
    return segment_start_char + local_start_char, segment_start_char + local_end_char


def segment_overlap_status(segment_index: Any, segment_count: Any) -> str:
    """Return the deterministic overlap-position label for one segment."""
    if (
        not isinstance(segment_index, int)
        or isinstance(segment_index, bool)
        or not isinstance(segment_count, int)
        or isinstance(segment_count, bool)
        or segment_count < 1
        or not 0 <= segment_index < segment_count
    ):
        raise ValueError("invalid segment index/count")
    if segment_count == 1:
        return "single_segment"
    if segment_index == 0:
        return "first_segment_with_trailing_overlap"
    if segment_index == segment_count - 1:
        return "last_segment_with_leading_overlap"
    return "middle_segment_with_leading_and_trailing_overlap"


def _nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _nullable_nonempty_string(value: Any) -> bool:
    return value is None or _nonempty_string(value)


def _validation_summary(
    schema_valid: bool,
    n_items: int,
    n_supported: int,
    *,
    n_required_anchors: int | None = None,
    n_exact_anchors: int | None = None,
) -> dict[str, Any]:
    rate = n_supported / n_items if n_items else None
    if not schema_valid:
        status = "invalid_schema"
    elif n_items == 0:
        status = "observed_zero"
    elif n_supported == n_items:
        status = "observed_supported"
    elif n_supported:
        status = "observed_partially_supported"
    else:
        status = "observed_unsupported"
    result: dict[str, Any] = {
        "schema_valid": schema_valid,
        "n_items": n_items,
        "n_supported": n_supported,
        "evidence_support_rate": rate,
        "status": status,
    }
    if n_required_anchors is not None and n_exact_anchors is not None:
        result.update({
            "n_required_anchors": n_required_anchors,
            "n_exact_anchors": n_exact_anchors,
            "exact_anchor_rate": (
                n_exact_anchors / n_required_anchors if n_required_anchors else None
            ),
        })
    return result


def _v1_validation(route: str, extraction: Any, source_text: str) -> dict[str, Any]:
    key = LIST_KEY[route]
    if not isinstance(extraction, dict) or set(extraction) != {key}:
        return {"schema_valid": False, "n_items": 0, "n_supported": 0,
                "evidence_support_rate": None, "status": "invalid_schema"}
    items = extraction[key]
    if not isinstance(items, list):
        return {"schema_valid": False, "n_items": 0, "n_supported": 0,
                "evidence_support_rate": None, "status": "invalid_schema"}

    required = {
        "issues": {"heading", "claim", "evidence"},
        "biography": {"field", "value", "evidence"},
        "endorsements": {"name", "organization", "endorser_type", "evidence"},
    }[route]
    schema_valid = True
    supported = 0
    for item in items:
        if not isinstance(item, dict) or set(item) != required:
            schema_valid = False
            continue
        if route == "issues" and not isinstance(item.get("claim"), str):
            schema_valid = False
        if route == "biography" and item.get("field") not in BIOGRAPHY_FIELDS:
            schema_valid = False
        if route == "endorsements" and item.get("endorser_type") not in ENDORSER_TYPES:
            schema_valid = False
        evidence = item.get("evidence")
        if not isinstance(evidence, str):
            schema_valid = False
        elif evidence_is_supported(evidence, source_text):
            supported += 1
    return _validation_summary(schema_valid, len(items), supported)


def _v2_issue_item(item: Mapping[str, Any], source: str) -> tuple[bool, bool, int, int]:
    required = {
        "source_heading", "source_heading_start_char", "source_heading_end_char",
        "claim", "statement_kind", "evidence", "evidence_start_char", "evidence_end_char",
    }
    if set(item) != required:
        return False, False, 0, 0
    schema_valid = (
        _nonempty_string(item["claim"])
        and item["statement_kind"] in ISSUE_STATEMENT_KINDS
        and _nonempty_string(item["evidence"])
    )
    evidence_ok = exact_span_is_valid(
        source, item["evidence"], item["evidence_start_char"], item["evidence_end_char"]
    )
    heading = item["source_heading"]
    if heading is None:
        heading_schema = (
            item["source_heading_start_char"] is None
            and item["source_heading_end_char"] is None
        )
        heading_ok = True
        anchors = 1
        exact = int(evidence_ok)
    else:
        heading_schema = _nonempty_string(heading)
        heading_ok = exact_span_is_valid(
            source, heading, item["source_heading_start_char"],
            item["source_heading_end_char"],
        )
        anchors = 2
        exact = int(evidence_ok) + int(heading_ok)
    return schema_valid and heading_schema, evidence_ok and heading_ok, anchors, exact


def _v2_biography_item(item: Mapping[str, Any], source: str) -> tuple[bool, bool, int, int]:
    required = {
        "field", "value", "timing", "relationship", "evidence",
        "evidence_start_char", "evidence_end_char",
    }
    if set(item) != required:
        return False, False, 0, 0
    field = item["field"]
    timing = item["timing"]
    relationship = item["relationship"]
    schema_valid = (
        field in BIOGRAPHY_FIELDS_V2
        and _nonempty_string(item["value"])
        and (timing is None or timing in BIOGRAPHY_TIMING_VALUES)
        and _nullable_nonempty_string(relationship)
        and _nonempty_string(item["evidence"])
    )
    if field in BIOGRAPHY_FIELDS_REQUIRING_TIMING:
        schema_valid = schema_valid and timing in BIOGRAPHY_TIMING_VALUES
    if field == "local_roots":
        schema_valid = schema_valid and _nonempty_string(relationship)
    else:
        schema_valid = schema_valid and relationship is None
    schema_valid = schema_valid and not record_semantic_errors("biography", item)
    evidence_ok = exact_span_is_valid(
        source, item["evidence"], item["evidence_start_char"], item["evidence_end_char"]
    )
    return schema_valid, evidence_ok, 1, int(evidence_ok)


def _v2_endorsement_item(item: Mapping[str, Any], source: str) -> tuple[bool, bool, int, int]:
    required = {
        "endorser_name", "entity_type", "endorser_category", "title", "affiliation",
        "jurisdiction", "record_evidence", "record_evidence_start_char",
        "record_evidence_end_char", "endorsement_context_evidence",
        "endorsement_context_evidence_start_char", "endorsement_context_evidence_end_char",
        "support_mode",
    }
    if set(item) != required:
        return False, False, 0, 0
    schema_valid = (
        _nonempty_string(item["endorser_name"])
        and item["entity_type"] in ENDORSEMENT_ENTITY_TYPES
        and item["endorser_category"] in ENDORSEMENT_CATEGORIES
        and all(
            _nullable_nonempty_string(item[key])
            for key in ("title", "affiliation", "jurisdiction")
        )
        and _nonempty_string(item["record_evidence"])
        and item["support_mode"] in ENDORSEMENT_SUPPORT_MODES
    )
    schema_valid = schema_valid and not record_semantic_errors("endorsements", item)
    record_ok = exact_span_is_valid(
        source, item["record_evidence"], item["record_evidence_start_char"],
        item["record_evidence_end_char"],
    )
    context = item["endorsement_context_evidence"]
    context_schema = _nonempty_string(context)
    context_ok = exact_span_is_valid(
        source, context, item["endorsement_context_evidence_start_char"],
        item["endorsement_context_evidence_end_char"],
    )
    anchors = 2
    exact = int(record_ok) + int(context_ok)
    return schema_valid and context_schema, record_ok and context_ok, anchors, exact


def _v2_validation(route: str, extraction: Any, source_text: str) -> dict[str, Any]:
    key = LIST_KEY_BY_VERSION["2.0"][route]
    if not isinstance(extraction, dict) or set(extraction) != {key}:
        return _validation_summary(False, 0, 0, n_required_anchors=0, n_exact_anchors=0)
    items = extraction[key]
    if not isinstance(items, list):
        return _validation_summary(False, 0, 0, n_required_anchors=0, n_exact_anchors=0)

    validator = {
        "issues": _v2_issue_item,
        "biography": _v2_biography_item,
        "endorsements": _v2_endorsement_item,
    }[route]
    schema_valid = True
    supported = 0
    required_anchors = 0
    exact_anchors = 0
    for item in items:
        if not isinstance(item, Mapping):
            schema_valid = False
            continue
        item_schema, item_supported, anchors, exact = validator(item, source_text)
        schema_valid = schema_valid and item_schema
        supported += int(item_supported)
        required_anchors += anchors
        exact_anchors += exact
    return _validation_summary(
        schema_valid, len(items), supported,
        n_required_anchors=required_anchors, n_exact_anchors=exact_anchors,
    )


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _v2_1_endorsement_item(
    item: Mapping[str, Any],
    source: str,
    *,
    endorsement_context_provenance: Mapping[str, Any] | None,
    segment_start_char: int,
    parent_source_text: str | None,
) -> tuple[bool, bool, int, int]:
    expected = (
        set(V2_1_MODEL_ITEM_FIELDS["endorsements"])
        | {"source_model_item_id"}
        | V2_1_ALIGNED_ENDORSEMENT_CONTEXT_FIELDS
    )
    for text_field, start_field, end_field, _nullable in V2_1_ANCHORS[
        "endorsements"
    ]:
        expected.update({
            start_field, end_field,
            f"{text_field}_exact_match_count",
            f"{text_field}_alignment_method",
        })
    if set(item) != expected or endorsement_context_provenance is None:
        return False, False, 0, 0
    provenance_errors = endorsement_context_provenance_errors(
        endorsement_context_provenance,
        parent_source_text=parent_source_text,
        segment_source_text=source,
        segment_id=(
            str(endorsement_context_provenance.get("context_source_segment_id"))
            if endorsement_context_provenance.get("context_source_segment_id")
            is not None else None
        ),
        segment_start_char=segment_start_char,
    )
    base = {
        field: item.get(field) for field in V2_1_MODEL_ITEM_FIELDS["endorsements"]
    }
    schema_valid = (
        not provenance_errors
        and _v2_1_model_item_schema_valid("endorsements", base)
        and _is_sha256(item.get("source_model_item_id"))
        and item.get("endorsement_context_id")
        == endorsement_context_provenance.get("context_id")
    )
    record_spans = exact_occurrence_spans(source, str(item.get("record_evidence") or ""))
    record_ok = (
        item.get("record_evidence_exact_match_count") == 1
        and item.get("record_evidence_alignment_method") in {
            ALIGNMENT_METHOD_UNIQUE,
            ALIGNMENT_METHOD_UNIQUE_DISPLAY,
        }
        and len(record_spans) == 1
        and record_spans[0] == (
            item.get("record_evidence_start_char"),
            item.get("record_evidence_end_char"),
        )
    )
    schema_valid = schema_valid and record_ok
    scope = item.get("endorsement_context_evidence_source_scope")
    context = item.get("endorsement_context_evidence")
    context_count = item.get("endorsement_context_evidence_exact_match_count")
    context_method = item.get("endorsement_context_evidence_alignment_method")
    local_start = item.get("endorsement_context_evidence_start_char")
    local_end = item.get("endorsement_context_evidence_end_char")
    parent_start = item.get("endorsement_context_evidence_parent_start_char")
    parent_end = item.get("endorsement_context_evidence_parent_end_char")
    context_ok = False
    if scope == "page_or_section_route_context":
        context_ok = (
            item.get("support_mode") == "page_or_section_context"
            and endorsement_context_provenance.get("source_scope") == scope
            and context is None
            and local_start is None and local_end is None
            and parent_start is None and parent_end is None
            and context_count == 0
            and context_method == ALIGNMENT_METHOD_PAGE_ROUTE
            and item.get("endorsement_context_record_distance_chars") is None
            and item.get("endorsement_context_qualifying_wayback_url")
            == endorsement_context_provenance.get("qualifying_wayback_url")
            and item.get("endorsement_context_qualifying_original_url")
            == endorsement_context_provenance.get("qualifying_original_url")
            and item.get("endorsement_context_qualifying_original_path")
            == endorsement_context_provenance.get("qualifying_original_path")
        )
        required_anchors = 1
        exact_anchors = int(record_ok)
    elif scope in {"segment", "parent_document"}:
        pointers_null = all(
            item.get(field) is None for field in (
                "endorsement_context_qualifying_wayback_url",
                "endorsement_context_qualifying_original_url",
                "endorsement_context_qualifying_original_path",
            )
        )
        if item.get("support_mode") == "page_or_section_context":
            carried_match = (
                scope == endorsement_context_provenance.get("source_scope")
                and context == endorsement_context_provenance.get("context_text")
                and parent_start
                == endorsement_context_provenance.get("context_parent_start_char")
                and parent_end
                == endorsement_context_provenance.get("context_parent_end_char")
            )
            if scope == "segment":
                local_match = (
                    local_start
                    == endorsement_context_provenance.get("context_local_start_char")
                    and local_end
                    == endorsement_context_provenance.get("context_local_end_char")
                    and context_method in {
                        ALIGNMENT_METHOD_PROVIDED_SEGMENT,
                        ALIGNMENT_METHOD_PROVIDED_SEGMENT_EXTENSION,
                    }
                    and context_count == len(exact_occurrence_spans(source, str(context)))
                    and exact_span_is_valid(source, context, local_start, local_end)
                )
            else:
                local_match = (
                    local_start is None and local_end is None
                    and context_method == ALIGNMENT_METHOD_PROVIDED_PARENT
                    and context_count
                    == endorsement_context_provenance.get(
                        "context_parent_exact_match_count"
                    )
                )
            context_ok = carried_match and local_match and pointers_null
            if all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in (item.get("record_evidence_start_char"), parent_end)
            ):
                expected_distance = (
                    segment_start_char + item["record_evidence_start_char"]
                    - parent_end
                )
                context_ok = context_ok and item.get(
                    "endorsement_context_record_distance_chars"
                ) == expected_distance
            else:
                context_ok = False
        else:
            spans = exact_occurrence_spans(source, str(context or ""))
            context_ok = (
                scope == "segment"
                and len(spans) == 1
                and spans[0] == (local_start, local_end)
                and context_count == 1
                and context_method in {
                    ALIGNMENT_METHOD_UNIQUE,
                    ALIGNMENT_METHOD_UNIQUE_DISPLAY,
                }
                and isinstance(local_start, int)
                and not isinstance(local_start, bool)
                and isinstance(local_end, int)
                and not isinstance(local_end, bool)
                and parent_start == segment_start_char + local_start
                and parent_end == segment_start_char + local_end
                and item.get("endorsement_context_record_distance_chars") is None
                and pointers_null
            )
        required_anchors = 2
        exact_anchors = int(record_ok) + int(context_ok)
    else:
        required_anchors = 2
        exact_anchors = int(record_ok)
    first_complete_local = endorsement_context_provenance.get(
        "segment_first_complete_line_start_char"
    )
    last_complete_local = endorsement_context_provenance.get(
        "segment_last_complete_line_end_char"
    )
    within_complete_lines = (
        endorsement_context_provenance.get("segment_has_complete_line") is True
        and
        isinstance(first_complete_local, int)
        and not isinstance(first_complete_local, bool)
        and isinstance(last_complete_local, int)
        and not isinstance(last_complete_local, bool)
        and isinstance(item.get("record_evidence_start_char"), int)
        and not isinstance(item.get("record_evidence_start_char"), bool)
        and isinstance(item.get("record_evidence_end_char"), int)
        and not isinstance(item.get("record_evidence_end_char"), bool)
        and first_complete_local <= item["record_evidence_start_char"]
        < item["record_evidence_end_char"] <= last_complete_local
    )
    schema_valid = schema_valid and context_ok and within_complete_lines and not record_semantic_errors(
        "endorsements",
        item,
        endorsement_context_provenance=endorsement_context_provenance,
    )
    return (
        schema_valid,
        schema_valid and exact_anchors == required_anchors,
        required_anchors,
        exact_anchors,
    )


def _v2_1_item(
    route: str,
    item: Mapping[str, Any],
    source: str,
    *,
    endorsement_context_provenance: Mapping[str, Any] | None = None,
    segment_start_char: int = 0,
    parent_source_text: str | None = None,
) -> tuple[bool, bool, int, int]:
    if route == "endorsements":
        return _v2_1_endorsement_item(
            item,
            source,
            endorsement_context_provenance=endorsement_context_provenance,
            segment_start_char=segment_start_char,
            parent_source_text=parent_source_text,
        )
    expected = set(V2_1_MODEL_ITEM_FIELDS[route]) | {"source_model_item_id"}
    for text_field, start_field, end_field, _nullable in V2_1_ANCHORS[route]:
        expected.update({
            start_field,
            end_field,
            f"{text_field}_exact_match_count",
            f"{text_field}_alignment_method",
        })
    if set(item) != expected:
        return False, False, 0, 0

    provenance_fields = {"source_model_item_id"}
    for text_field, _start_field, _end_field, _nullable in V2_1_ANCHORS[route]:
        provenance_fields.update({
            f"{text_field}_exact_match_count",
            f"{text_field}_alignment_method",
        })
    base = {key: value for key, value in item.items() if key not in provenance_fields}
    base_validator = {
        "issues": _v2_issue_item,
        "biography": _v2_biography_item,
        "endorsements": _v2_endorsement_item,
    }[route]
    base_schema, base_supported, required_anchors, _base_exact = base_validator(
        base, source
    )
    schema_valid = base_schema and _is_sha256(item.get("source_model_item_id"))
    exact_anchors = 0
    for text_field, start_field, end_field, nullable in V2_1_ANCHORS[route]:
        excerpt = item.get(text_field)
        match_count = item.get(f"{text_field}_exact_match_count")
        method = item.get(f"{text_field}_alignment_method")
        if excerpt is None and nullable:
            anchor_ok = (
                item.get(start_field) is None
                and item.get(end_field) is None
                and match_count == 0
                and not isinstance(match_count, bool)
                and method == ALIGNMENT_METHOD_NOT_APPLICABLE
            )
            schema_valid = schema_valid and anchor_ok
            continue
        spans = exact_occurrence_spans(source, excerpt) if isinstance(excerpt, str) else []
        anchor_ok = (
            match_count == 1
            and not isinstance(match_count, bool)
            and method in {
                ALIGNMENT_METHOD_UNIQUE,
                ALIGNMENT_METHOD_UNIQUE_DISPLAY,
            }
            and len(spans) == 1
            and spans[0] == (item.get(start_field), item.get(end_field))
            and exact_span_is_valid(
                source, excerpt, item.get(start_field), item.get(end_field)
            )
        )
        schema_valid = schema_valid and anchor_ok
        exact_anchors += int(anchor_ok)
    return (
        schema_valid,
        base_supported and exact_anchors == required_anchors,
        required_anchors,
        exact_anchors,
    )


def _v2_1_validation(
    route: str,
    extraction: Any,
    source_text: str,
    *,
    endorsement_context_provenance: Mapping[str, Any] | None = None,
    segment_start_char: int = 0,
    parent_source_text: str | None = None,
) -> dict[str, Any]:
    key = LIST_KEY_BY_VERSION["2.1"][route]
    if not isinstance(extraction, dict) or set(extraction) != {key}:
        return _validation_summary(False, 0, 0, n_required_anchors=0, n_exact_anchors=0)
    items = extraction[key]
    if not isinstance(items, list):
        return _validation_summary(False, 0, 0, n_required_anchors=0, n_exact_anchors=0)
    # The guided schema allows exactly the declared maximum so generation can
    # terminate cleanly. Equality is nevertheless a fail-closed truncation/
    # saturation signal and cannot yield a public v2.1 extraction.
    schema_valid = len(items) < V2_1_MODEL_ITEM_CAPS[route]
    supported = 0
    required_anchors = 0
    exact_anchors = 0
    for item in items:
        if not isinstance(item, Mapping):
            schema_valid = False
            continue
        item_schema, item_supported, anchors, exact = _v2_1_item(
            route,
            item,
            source_text,
            endorsement_context_provenance=endorsement_context_provenance,
            segment_start_char=segment_start_char,
            parent_source_text=parent_source_text,
        )
        schema_valid = schema_valid and item_schema
        supported += int(item_supported)
        required_anchors += anchors
        exact_anchors += exact
    return _validation_summary(
        schema_valid,
        len(items),
        supported,
        n_required_anchors=required_anchors,
        n_exact_anchors=exact_anchors,
    )


def validate_extraction(
    route: str,
    extraction: Any,
    source_text: str,
    schema_version: str = "1.0",
    *,
    endorsement_context_provenance: Mapping[str, Any] | None = None,
    segment_start_char: int = 0,
    parent_source_text: str | None = None,
) -> dict[str, Any]:
    """Validate final public exactness without retrying or dropping records.

    Adjustment-specific alignment methods cannot be authenticated from a final
    source-exact public item alone. The runner restart/import/release chain must
    also recompute ``align_extraction_exact_spans`` from the retained parsed
    model object and require typed equality with its private adjustment report.
    """
    if route not in ROUTES:
        raise ValueError(f"unknown extraction route: {route}")
    version = canonical_schema_version(schema_version)
    if version == "1.0":
        return _v1_validation(route, extraction, source_text)
    if version == "2.0":
        return _v2_validation(route, extraction, source_text)
    return _v2_1_validation(
        route,
        extraction,
        source_text,
        endorsement_context_provenance=endorsement_context_provenance,
        segment_start_char=segment_start_char,
        parent_source_text=parent_source_text,
    )


_OFFSET_FIELDS = {
    "source_heading_start_char", "source_heading_end_char",
    "evidence_start_char", "evidence_end_char",
    "record_evidence_start_char", "record_evidence_end_char",
    "endorsement_context_evidence_start_char", "endorsement_context_evidence_end_char",
    "source_model_item_id",
    "source_heading_exact_match_count", "source_heading_alignment_method",
    "evidence_exact_match_count", "evidence_alignment_method",
    "record_evidence_exact_match_count", "record_evidence_alignment_method",
    "endorsement_context_evidence_exact_match_count",
    "endorsement_context_evidence_alignment_method",
    "endorsement_context_id", "endorsement_context_evidence_source_scope",
    "endorsement_context_evidence_parent_start_char",
    "endorsement_context_evidence_parent_end_char",
    "endorsement_context_record_distance_chars",
    "endorsement_context_qualifying_wayback_url",
    "endorsement_context_qualifying_original_url",
    "endorsement_context_qualifying_original_path",
}


def semantic_record_key(
    route: str, item: Mapping[str, Any], schema_version: str = DEFAULT_SCHEMA_VERSION
) -> tuple[Any, ...]:
    """Return a conservative exact-text identity for overlap grouping."""
    if route not in ROUTES:
        raise ValueError(f"unknown extraction route: {route}")
    version = canonical_schema_version(schema_version)
    fields = {
        "1.0": {
            "issues": ("heading", "claim"),
            "biography": ("field", "value"),
            "endorsements": ("name", "organization", "endorser_type"),
        },
        "2.0": {
            "issues": ("source_heading", "claim", "statement_kind"),
            "biography": ("field", "value", "timing", "relationship"),
            "endorsements": (
                "endorser_name", "entity_type", "endorser_category", "title",
                "affiliation", "jurisdiction", "support_mode",
            ),
        },
        "2.1": {
            "issues": ("claim", "statement_kind"),
            "biography": ("field", "value", "timing", "relationship"),
            "endorsements": (
                "endorser_name", "entity_type", "endorser_category", "title",
                "affiliation", "jurisdiction", "support_mode",
            ),
        },
    }[version][route]
    return tuple(item.get(field) for field in fields)


def record_payload_sha256(item: Mapping[str, Any], *, include_offsets: bool = False) -> str:
    """Hash a record, normally excluding segment-local alignment provenance."""
    payload = {
        key: value for key, value in item.items()
        if include_offsets or key not in _OFFSET_FIELDS
    }
    return _sha256_text(_canonical_json(payload))


def resolve_overlap_variants(
    variants: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Resolve already-grouped variants without a silent first-row choice."""
    copied = [dict(variant) for variant in variants]
    if not copied:
        raise ValueError("cannot resolve an empty overlap-variant group")
    if any(not isinstance(variant.get("item"), Mapping) for variant in copied):
        raise TypeError("every overlap variant must contain a mapping named item")
    for variant in copied:
        variant["payload_sha256"] = record_payload_sha256(variant["item"])
        variant["payload_with_offsets_sha256"] = record_payload_sha256(
            variant["item"], include_offsets=True
        )
    copied.sort(key=lambda variant: (
        int(variant.get("segment_index", 0)),
        str(variant.get("segment_id", "")),
        variant["payload_with_offsets_sha256"],
    ))
    distinct = sorted({variant["payload_sha256"] for variant in copied})
    if len(distinct) != 1:
        return {
            "resolution_status": "conflict",
            "resolution_method": "withheld_distinct_nonoffset_payloads",
            "selected_variant": None,
            "variant_count": len(copied),
            "distinct_payload_count": len(distinct),
            "variants": copied,
        }
    return {
        "resolution_status": "unique" if len(copied) == 1 else "equivalent_overlap",
        "resolution_method": "stable_min_segment_key_among_equivalent_payloads",
        "selected_variant": copied[0],
        "variant_count": len(copied),
        "distinct_payload_count": 1,
        "variants": copied,
    }
