from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

from src.candidate_extraction import (
    ALIGNMENT_METHOD_PAGE_ROUTE,
    ALIGNMENT_METHOD_PROVIDED_PARENT,
    ALIGNMENT_METHOD_PROVIDED_SEGMENT,
    ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
    ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
    ENDORSEMENT_CONTEXT_SOURCE_SCOPES,
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256,
    MODEL_ITEM_REPETITION_THRESHOLD,
    get_extraction_contract,
)
from src.candidate_extraction_provenance import SOURCE_TEXT_TRANSFORM_VERSION
from src.candidate_endorsement_line_ledger import (
    FROZEN_LINE_LEDGER_CONTRACT_SHA256,
    FROZEN_STAGE_A_V2_CONTRACT_SHA256,
    LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256,
    LINE_ADJUDICATION_SPEC_SHA256,
    STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256,
    get_line_ledger_contract,
)
from scripts.import_candidate_extraction_v2 import (
    EXPECTED_GENERATION_CONFIG_SHA256,
    EXPECTED_GENERATION_SEED,
    EXPECTED_MAX_MODEL_LEN,
    EXPECTED_MAX_OUTPUT_TOKENS,
    FROZEN_STAGE_A2_GENERATION_CONFIG_SHA256,
    FROZEN_STAGE_A2_INPUT_MANIFEST_SHA256,
    FROZEN_STAGE_A3_HTML_SHA256,
    FROZEN_STAGE_A3_TASK_SHA256,
)
from scripts.build_candidate_endorsement_line_ledger_a3_handcode import (
    A3_SPEC_SHA256,
)
from scripts.hive.extract_candidate_pages_vllm import (
    GENERATION_CONFIG_VERSION,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    DEFAULT_MAX_OUTPUT_TOKENS as LINE_LEDGER_MAX_OUTPUT_TOKENS,
    FROZEN_GENERATION_SEED as LINE_LEDGER_GENERATION_SEED,
    PROMPT_TOKEN_COUNT_SPEC_SHA256,
)


REPO = Path(__file__).resolve().parent.parent
CODEBOOK = REPO / "docs" / "candidate_extraction_extension_codebook.md"
PACKAGER = REPO / "scripts" / "build_candidate_extraction_release.py"


def _literal_assignments(path: Path) -> dict[str, Any]:
    """Read simple constant expressions without importing the release builder."""
    values: dict[str, Any] = {}

    def evaluate(node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return values[node.id]
        if isinstance(node, ast.Tuple):
            return tuple(evaluate(value) for value in node.elts)
        if isinstance(node, ast.List):
            return [evaluate(value) for value in node.elts]
        if isinstance(node, ast.Set):
            return {evaluate(value) for value in node.elts}
        if isinstance(node, ast.Dict):
            return {
                evaluate(key): evaluate(value)
                for key, value in zip(node.keys, node.values, strict=True)
            }
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return evaluate(node.left) + evaluate(node.right)
        raise ValueError(f"unsupported constant expression: {ast.dump(node)}")

    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        try:
            values[target.id] = evaluate(node.value)
        except (KeyError, TypeError, ValueError):
            continue
    return values


def _schema_block(text: str, heading: str) -> tuple[str, ...]:
    marker = f"### {heading}\n"
    assert marker in text, f"missing codebook schema heading: {heading}"
    section = text.split(marker, maxsplit=1)[1].split("\n### ", maxsplit=1)[0]
    match = re.search(r"```text\n(.*?)\n```", section, flags=re.DOTALL)
    assert match is not None, f"missing schema block below {heading}"
    return tuple(
        field.strip()
        for field in re.sub(r"\s+", " ", match.group(1)).split(",")
    )


def test_extension_codebook_matches_frozen_public_contract() -> None:
    text = CODEBOOK.read_text(encoding="utf-8")
    constants = _literal_assignments(PACKAGER)

    for filename in constants["PUBLIC_FILES"]:
        assert f"`{filename}`" in text

    common = _schema_block(text, "Fields shared by all three long tables")
    documented = {
        "STATUS_COLUMNS": _schema_block(
            text, "`candidate_extraction_status.csv`"
        ),
        "DOCUMENT_COLUMNS": _schema_block(
            text, "`candidate_extraction_documents.parquet`"
        ),
        "SEGMENT_COLUMNS": _schema_block(
            text, "`candidate_extraction_segments.parquet`"
        ),
        "ISSUE_COLUMNS": common
        + _schema_block(text, "`candidate_issue_statements.parquet`"),
        "BIOGRAPHY_COLUMNS": common
        + _schema_block(text, "`candidate_biography_facts.parquet`"),
        "ENDORSEMENT_COLUMNS": common
        + _schema_block(text, "`candidate_endorsements.parquet`"),
    }
    for name, fields in documented.items():
        assert fields == constants[name], f"{name} differs from the public codebook"

    documented_tokens = set(re.findall(r"[a-z][a-z0-9_]*", text))
    for name in (
        "ROUTES",
        "SOURCE_OBSERVABILITY",
        "EXTRACTION_STATUSES",
        "STATEMENT_KINDS",
        "BIOGRAPHY_FIELDS",
        "TIMING_VALUES",
        "ENTITY_TYPES",
        "ENDORSER_CATEGORIES",
        "SUPPORT_MODES",
    ):
        assert set(constants[name]) <= documented_tokens

    contract = get_extraction_contract("2.1")
    frozen_values = {
        contract["schema_version"],
        contract["model_id"],
        contract["model_revision"],
        contract["runtime_lock_sha256"],
        contract["extraction_contract_sha256"],
        contract["alignment_id"],
        contract["alignment_spec_sha256"],
        contract["semantic_validation_spec_sha256"],
        contract["endorsement_context_provenance_spec_sha256"],
        contract["user_message_template_id"],
        contract["user_message_template_sha256"],
        contract["inference_display_escape_id"],
        contract["inference_display_escape_spec_sha256"],
        contract["json_string_control_canonicalizer_id"],
        contract["json_string_control_canonicalizer_spec_sha256"],
        contract["array_item_cap_spec_id"],
        contract["array_item_cap_spec_sha256"],
        *contract["route_array_item_caps"].values(),
        contract["model_item_repetition_spec_id"],
        contract["model_item_repetition_spec_sha256"],
        MODEL_ITEM_REPETITION_THRESHOLD,
        EXPECTED_GENERATION_CONFIG_SHA256,
        EXPECTED_GENERATION_SEED,
        EXPECTED_MAX_MODEL_LEN,
        EXPECTED_MAX_OUTPUT_TOKENS,
        GENERATION_CONFIG_VERSION,
        STRUCTURED_DECODING_BACKEND,
        str(STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE).lower(),
        ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
        ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
        ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
        ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256,
        ALIGNMENT_METHOD_PROVIDED_SEGMENT,
        ALIGNMENT_METHOD_PROVIDED_PARENT,
        ALIGNMENT_METHOD_PAGE_ROUTE,
        *ENDORSEMENT_CONTEXT_SOURCE_SCOPES,
        *contract["prompt_sha256"].values(),
        *contract["json_schema_sha256"].values(),
        constants["EXPECTED_CORE_RAW_SHA256"],
        SOURCE_TEXT_TRANSFORM_VERSION,
    }
    for value in frozen_values:
        assert str(value) in text

    if FROZEN_LINE_LEDGER_CONTRACT_SHA256:
        line_ledger = get_line_ledger_contract()
        line_ledger_values = {
            line_ledger["line_ledger_contract_sha256"],
            line_ledger["line_inventory_spec_sha256"],
            line_ledger["line_batch_spec_sha256"],
            FROZEN_STAGE_A_V2_CONTRACT_SHA256,
            LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256,
            LINE_ADJUDICATION_SPEC_SHA256,
            STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256,
            FROZEN_STAGE_A2_INPUT_MANIFEST_SHA256,
            FROZEN_STAGE_A2_GENERATION_CONFIG_SHA256,
            A3_SPEC_SHA256,
            FROZEN_STAGE_A3_TASK_SHA256,
            FROZEN_STAGE_A3_HTML_SHA256,
            *line_ledger["prompt_sha256"].values(),
            *line_ledger["json_schema_catalog_sha256"].values(),
            LINE_LEDGER_GENERATION_SEED,
            *LINE_LEDGER_MAX_OUTPUT_TOKENS.values(),
            PROMPT_TOKEN_COUNT_SPEC_SHA256,
        }
        for value in line_ledger_values:
            assert str(value) in text

    normalized_text = re.sub(r"\s+", " ", text)
    assert "It is not human validation" in normalized_text
    assert "Biography `timing` (wrong on 8 of 60)" in normalized_text
    assert (
        "fully accounted conflict is withholding rather than a technical failure"
        in normalized_text
    )
    assert "A conflict-only route is never `observed_zero`" in normalized_text
    assert "`endorsement_context_evidence` may be null only for" in normalized_text
    assert "Every cap failure rejects the entire window" in normalized_text
    assert (
        "issue and biography guided schemas set `maxItems` to 128 and 96"
        in normalized_text
    )
    assert "endorsement guided schema deliberately omits `maxItems`" in (
        normalized_text
    )
    assert (
        "rejects any endorsement output with 256 or more items before alignment"
        in normalized_text
    )
    assert "including when the copies are not consecutive" in normalized_text
    assert (
        "`array_item_cap_reached` takes precedence over "
        "`model_item_repetition_error`"
        in normalized_text
    )
    assert "A guard-failure row is valid JSON" in normalized_text
    assert "cannot contribute to `observed_zero`" in normalized_text
    assert "Guard IDs, specification hashes, route caps, row reports, and aggregate counts remain private" in normalized_text
    assert "No guard column is added to the six public files" in normalized_text
    assert "A zero-work metadata file must set" in normalized_text
    assert "without that recovery attestation must report at least one processed window" in normalized_text
    assert "selected-shard counts to match source and output coverage exactly" in normalized_text
    assert "must be no larger than the independently recomputed all-output ledger" in normalized_text
    assert "Exact-copy anchors omit `minLength`" in normalized_text
    assert "full Unicode `str.strip()` nonblank check" in normalized_text
    assert "entire inference window `failed_schema`" in normalized_text
    assert "All 22 free-text nodes use `minLength = 1`" not in normalized_text
    assert (
        "continuation segments that begin in the middle of a list and do not "
        "repeat the heading"
        in normalized_text
    )
    assert "skip a clipped or incomplete first line" in normalized_text
    assert "suffix-n contract is a frozen NO-GO audit" in normalized_text
    assert "pass a fresh all-route preflight" in normalized_text
    assert "`#+#`" in text and "U+000A" in text and "Python `str.replace`" in text
    assert "zero-based, half-open Python Unicode code-point indices" in text
    assert "Only Stage-B `aligned_records` can enter" in normalized_text
    assert "Every public endorsement carries these 14 lineage fields" in (
        normalized_text
    )
    assert "those fields are null on endorsement rows" in normalized_text
    assert (
        "Issue and biography rows retain their exact one-stage prompt, schema, "
        "generation, user-template, token, and alignment values"
        in normalized_text
    )
    # The release rule changed on 2026-09-28 from human validation to a
    # model-coded check, so the codebook must say so and name the gate.
    assert "and all 11 known omissions are confirmed" in normalized_text
    assert "record-level recall is at least 0.90" in normalized_text
    assert "Neither is a validation sample" in normalized_text


def test_public_codebook_names_every_status_and_released_variable() -> None:
    """The short codebook ships with the data, so it must cover the public schema."""
    from scripts.build_candidate_extraction_release import (
        BIOGRAPHY_COLUMNS,
        COMMON_RECORD_COLUMNS,
        ENDORSEMENT_COLUMNS,
        EXTRACTION_STATUSES,
        ISSUE_COLUMNS,
    )

    text = (REPO / "docs" / "candidate_extraction_codebook.md").read_text(encoding="utf-8")
    for status in EXTRACTION_STATUSES - {"failed"}:
        assert f"`{status}`" in text, status
    substantive = (
        set(ISSUE_COLUMNS) | set(BIOGRAPHY_COLUMNS) | set(ENDORSEMENT_COLUMNS)
    ) - set(COMMON_RECORD_COLUMNS)
    offsets_or_lineage = {
        name for name in substantive
        if name.endswith(("_char", "_exact_match_count", "_alignment_method", "_url", "_path"))
        or name in {
            "line_inventory_id", "source_line_id", "stage_a_batch_id", "stage_a_slot",
            "stage_b_batch_id", "stage_b_slot", "model_record_index",
            "carrier_alignment_source_model_item_id", "inventory_owner_segment_id",
            "record_evidence_carrier_segment_id", "endorsement_context_id",
            "endorsement_context_record_distance_chars",
        }
    }
    for column in sorted(substantive - offsets_or_lineage) + list(COMMON_RECORD_COLUMNS):
        assert f"`{column}`" in text, column
    for withheld in ("`timing`", "`title`", "`affiliation`", "`jurisdiction`"):
        assert f"| {withheld} |" not in text
