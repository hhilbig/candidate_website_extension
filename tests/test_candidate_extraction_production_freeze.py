"""The freeze record must agree with the code it freezes."""
from __future__ import annotations

import re
from pathlib import Path

from src.candidate_endorsement_round4 import (
    ROUND4_DEVELOPMENT_BINDING_SHA256,
    ROUND4_STAGE_A_PROMPT_SHA256,
    ROUND4_STAGE_B_PROMPT_SHA256,
)
from src.candidate_endorsement_round4_production import (
    ROUND4_COVERAGE_SPEC,
    ROUND4_COVERAGE_SPEC_SHA256,
    ROUND4_FAILURE_ACCOUNTING_SPEC_SHA256,
    ROUND4_FAILURE_CLASSES,
    ROUND4_PRODUCTION_BINDING_SHA256,
    ROUND4_RECORD_PROJECTION_SPEC_SHA256,
    ROUND4_STAGE_A_SCHEMA_CATALOG_SHA256,
    ROUND4_STAGE_B_SCHEMA_CATALOG_SHA256,
    get_round4_production_contract,
)
from src.candidate_extraction import (
    MODEL_ID,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    get_extraction_contract,
)

REPO = Path(__file__).resolve().parents[1]
FREEZE = REPO / "docs/candidate_extraction_production_freeze.md"
CONTRACT = get_extraction_contract("2.1")


def _text() -> str:
    return FREEZE.read_text(encoding="utf-8")


def test_the_freeze_record_exists():
    assert FREEZE.is_file()


def test_every_frozen_hash_in_the_record_comes_from_code():
    text = _text()
    expected = {
        "source corpus": (
            "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3"
        ),
        "export manifest": (
            "f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c"
        ),
        "runtime lock": RUNTIME_LOCK_SHA256,
        "extraction contract": CONTRACT["extraction_contract_sha256"],
        "issue prompt": CONTRACT["prompt_sha256"]["issues"],
        "issue schema": CONTRACT["json_schema_sha256"]["issues"],
        "biography prompt": CONTRACT["prompt_sha256"]["biography"],
        "biography schema": CONTRACT["json_schema_sha256"]["biography"],
        "round4 contract": get_round4_production_contract()[
            "round4_production_contract_sha256"
        ],
        "round4 stage A prompt": ROUND4_STAGE_A_PROMPT_SHA256,
        "round4 stage B prompt": ROUND4_STAGE_B_PROMPT_SHA256,
        "round4 stage A schema catalog": ROUND4_STAGE_A_SCHEMA_CATALOG_SHA256,
        "round4 stage B schema catalog": ROUND4_STAGE_B_SCHEMA_CATALOG_SHA256,
        "round4 production binding": ROUND4_PRODUCTION_BINDING_SHA256,
        "round4 projection spec": ROUND4_RECORD_PROJECTION_SPEC_SHA256,
        "round4 failure spec": ROUND4_FAILURE_ACCOUNTING_SPEC_SHA256,
        "round4 coverage spec": ROUND4_COVERAGE_SPEC_SHA256,
        "round4 development binding": ROUND4_DEVELOPMENT_BINDING_SHA256,
    }
    for label, digest in expected.items():
        assert digest in text, f"the freeze record omits the {label} hash"


def test_the_record_contains_no_unknown_hash():
    """A hash in the document must exist in code.  Nothing is invented."""
    known = {
        "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3",
        "f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c",
        RUNTIME_LOCK_SHA256,
        CONTRACT["extraction_contract_sha256"],
        CONTRACT["prompt_sha256"]["issues"],
        CONTRACT["json_schema_sha256"]["issues"],
        CONTRACT["prompt_sha256"]["biography"],
        CONTRACT["json_schema_sha256"]["biography"],
        get_round4_production_contract()["round4_production_contract_sha256"],
        ROUND4_STAGE_A_PROMPT_SHA256,
        ROUND4_STAGE_B_PROMPT_SHA256,
        ROUND4_STAGE_A_SCHEMA_CATALOG_SHA256,
        ROUND4_STAGE_B_SCHEMA_CATALOG_SHA256,
        ROUND4_PRODUCTION_BINDING_SHA256,
        ROUND4_RECORD_PROJECTION_SPEC_SHA256,
        ROUND4_FAILURE_ACCOUNTING_SPEC_SHA256,
        ROUND4_COVERAGE_SPEC_SHA256,
        ROUND4_DEVELOPMENT_BINDING_SHA256,
    }
    found = set(re.findall(r"\b[0-9a-f]{64}\b", _text()))
    assert found - known == set(), "the freeze record holds an unknown hash"


def test_the_record_names_the_model_configuration():
    text = _text()
    assert MODEL_ID in text
    assert MODEL_REVISION in text


def test_the_record_lists_the_five_failure_classes():
    text = _text()
    for name in ROUND4_FAILURE_CLASSES:
        assert f"`{name}`" in text, f"the record omits the {name} class"


def test_the_record_states_the_corpus_gate_fields():
    text = _text()
    for field in ROUND4_COVERAGE_SPEC["corpus_gate_fields"]:
        assert field in text, f"the record omits the {field} gate field"


def test_the_record_keeps_release_unauthorized():
    text = _text().lower()
    assert "release_authorized" in text
    assert "does not authorize a release" in text


def test_the_record_states_the_two_attempt_retry_limit():
    assert "at most two attempts" in _text()


def test_the_record_reports_the_exact_stage_a_census():
    """Step 2 sizes its request from this number."""
    assert "19,142" in _text()


def test_the_record_covers_all_three_routes():
    text = _text()
    for route in ("Issues", "Biography", "Endorsements"):
        assert f"| {route} |" in text, f"the per-route table omits {route}"


def test_the_record_preserves_the_stage_a3_disposition():
    text = _text()
    assert "candidate_endorsement_line_ledger_human_a3_21447139.json" in text
    assert "Do not repeat that adjudication." in text
