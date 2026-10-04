from __future__ import annotations

import copy
from pathlib import Path

import pytest

import scripts.evaluate_candidate_endorsement_line_ledger_pilot as evaluator
from scripts.build_candidate_endorsement_line_ledger import (
    CHAMPION_INTERFACE_LINE_TEXT,
    LONG_TESTIMONIAL_CONTINUATION_LINE_IDS,
    LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR,
    PILOT_CONTENT_GATES,
    PILOT_PARENT_IDS,
    TANAKA_INTERFACE_LINE_TEXT,
)
from scripts.evaluate_candidate_endorsement_line_ledger_pilot import (
    _field_observability_census,
    derive_issue_control,
    evaluate_pilot_observations,
)


def test_field_observability_census_is_explicitly_non_gating() -> None:
    census = _field_observability_census([
        {
            "entity_type": "unclear", "endorser_category": "unclear",
            "title": None, "affiliation": None, "jurisdiction": None,
        },
        {
            "entity_type": "individual", "endorser_category": "government_official",
            "title": "Mayor", "affiliation": None, "jurisdiction": "Example",
        },
    ])
    assert census["gate_status"] == "NON_GATING_DIAGNOSTIC_ONLY"
    assert census["entity_type_unclear_rate"] == 0.5
    assert census["endorser_category_unclear_rate"] == 0.5
    assert census["optional_field_nonnull_rates"] == {
        "title": 0.5, "affiliation": 0.0, "jurisdiction": 0.5,
    }
    assert census["full_expansion_or_release_authorized_by_this_census"] is False


REPO = Path(__file__).resolve().parents[1]
ISSUE_CONTROL_BUNDLE = (
    REPO / "build/hive_candidate_extraction/"
    "v2_1_quote_cap_preflight_bundle_20260824o"
)


def _parent(
    model_names: list[str] | None = None,
    aligned_names: list[str] | None = None,
) -> dict[str, object]:
    return {
        "model_unique_names": model_names or [],
        "aligned_unique_names": aligned_names or [],
        "model_records": [],
        "aligned_records": [],
        "model_unique_names_by_source_segment": {},
        "aligned_unique_names_by_source_segment": {},
    }


def _passing_observations() -> dict[str, object]:
    tanaka_id, champion_id, quote_id, compound_id, testimonial_id, json_id, magaziner_id, vets_id = PILOT_PARENT_IDS
    parents = {parent_id: _parent() for parent_id in PILOT_PARENT_IDS}
    tanaka_names = [f"Tanaka supporter {index}" for index in range(390)]
    parents[tanaka_id] = _parent(tanaka_names, tanaka_names[:360])
    parents[tanaka_id]["model_unique_names_by_source_segment"] = {
        "aa0b99eb1331464dce28db32d73a6f53bddbf7e4aa10f46f4c17c48b12720433": tanaka_names[:224],
        "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73": tanaka_names[224:310],
    }
    parents[tanaka_id]["aligned_unique_names_by_source_segment"] = {
        "aa0b99eb1331464dce28db32d73a6f53bddbf7e4aa10f46f4c17c48b12720433": tanaka_names[:224],
        "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73": tanaka_names[224:308],
    }
    champion_names = [f"Champion supporter {index}" for index in range(230)]
    parents[champion_id] = _parent(champion_names, champion_names[:222])

    quote_gate = PILOT_CONTENT_GATES[quote_id]
    parents[quote_id]["aligned_records"] = [{
        "source_line_id": quote_gate["required_source_line_id"],
        "endorser_name": 'Jolanda "Jo" Jones',
        "record_evidence": quote_gate["must_resolve_quote_bearing_record_evidence"],
    }]
    parents[quote_id]["aligned_unique_names"] = ['Jolanda "Jo" Jones']

    compound_gate = PILOT_CONTENT_GATES[compound_id]
    parents[compound_id]["aligned_records"] = [
        {
            "source_line_id": compound_gate["required_compound_source_line_id"],
            "endorser_name": name,
        }
        for name in compound_gate["exact_compound_names"]
    ]
    parents[compound_id]["aligned_unique_names"] = list(
        compound_gate["exact_compound_names"]
    )

    continuation_anchor_by_line = dict(
        LONG_TESTIMONIAL_CONTINUATION_TO_ANCHOR
    )
    testimonial_anchors = sorted(set(continuation_anchor_by_line.values()))
    parents[testimonial_id]["aligned_records"] = [
        {
            "source_line_id": anchor_id,
            "endorser_name": f"Speaker {index}",
        }
        for index, anchor_id in enumerate(testimonial_anchors)
    ]
    parents[testimonial_id]["aligned_unique_names"] = [
        f"Speaker {index}"
        for index in range(len(testimonial_anchors))
    ]

    parents[magaziner_id]["aligned_records"] = [{
        "source_line_id": "magaziner-anchor",
        "endorser_name": "Valid Supporter",
        "endorsement_context_evidence_source_scope": (
            "page_or_section_route_context"
        ),
    }]
    parents[magaziner_id]["aligned_unique_names"] = ["Valid Supporter"]
    parents[magaziner_id]["model_unique_names"] = ["Valid Supporter"]

    vets_gate = PILOT_CONTENT_GATES[vets_id]
    vets_names = [f"Vets supporter {index}" for index in range(11)]
    parents[vets_id]["aligned_unique_names"] = vets_names
    parents[vets_id]["aligned_records"] = [{
        "source_line_id": vets_gate["required_source_line_id"],
        "endorser_name": vets_names[0],
        "record_evidence": "Vets for Trump Organization",
    }]
    json_line_id = PILOT_CONTENT_GATES[json_id]["required_source_line_id"]
    return {
        "stage_a_unresolved_line_count": 0,
        "stage_a_roles": {
            **{
                line_id: "record_continuation_previous"
                for line_id in LONG_TESTIMONIAL_CONTINUATION_LINE_IDS
            },
            **{
                line_id: "navigation_form_footer"
                for line_id in [
                    *TANAKA_INTERFACE_LINE_TEXT,
                    *CHAMPION_INTERFACE_LINE_TEXT,
                ]
            },
            json_line_id: "nonrecord_content",
        },
        "source_lines": {},
        "stage_b_dispositions": {},
        "stage_b_continuation_use_counts": {
            line_id: 1 for line_id in LONG_TESTIMONIAL_CONTINUATION_LINE_IDS
        },
        "stage_b_continuation_anchor_by_line": continuation_anchor_by_line,
        "stage_b_withheld_record_count": 0,
        "stage_b_public_coverage_complete": True,
        "parents": parents,
    }


def _issue_control() -> dict[str, object]:
    return {
        "route": "issues",
        "parent_document_id": (
            "71c197e1b81d48216f8c2cb757f1da7e2b769819dd64e0e7865645d96b62bec8"
        ),
        "quote_bearing_resolved_issue_records": 2,
        "fully_accounted_private_conflict_groups": [{
            "fully_accounted": True,
            "disposition": "conflict_withheld_private",
            "statement_kinds": ["proposal", "oppose"],
        }],
    }


def test_mechanical_pilot_gate_accepts_only_complete_passing_observations() -> None:
    observations = _passing_observations()
    report = evaluate_pilot_observations(observations, _issue_control())
    assert report["gate_decision"] == "GO"
    assert report["failed_checks"] == 0

    failed = copy.deepcopy(observations)
    tanaka_id = PILOT_PARENT_IDS[0]
    failed["parents"][tanaka_id]["model_unique_names_by_source_segment"][
        "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73"
    ] = ["only one"]
    failed["stage_b_withheld_record_count"] = 1
    report = evaluate_pilot_observations(failed, _issue_control())
    assert report["gate_decision"] == "NO_GO"
    assert {check["gate"] for check in report["checks"] if not check["passed"]} >= {
        "tanaka_model_segment_2e9a0cd8e0bd",
        "all_stage_b_public_coverage_complete",
    }


def test_corrected_issue_gate_requires_private_conflict_accounting() -> None:
    issue = _issue_control()
    issue["fully_accounted_private_conflict_groups"] = []
    report = evaluate_pilot_observations(_passing_observations(), issue)
    assert report["gate_decision"] == "NO_GO"
    failed = [check for check in report["checks"] if not check["passed"]]
    assert [check["gate"] for check in failed] == [
        "issue_private_conflict_accounted"
    ]


def test_validated_run_forwards_explicit_contract_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    override = {"line_ledger_contract_sha256": "a" * 64}
    expected = ({}, [], [], {})
    observed: dict[str, object] = {}

    def fake_loader(
        input_export_dir: Path,
        output_dir: Path,
        *,
        stage: str,
        contract_override: object,
    ) -> tuple[dict[str, object], list[object], list[object], dict[str, object]]:
        observed.update({
            "input_export_dir": input_export_dir,
            "output_dir": output_dir,
            "stage": stage,
            "contract_override": contract_override,
        })
        return expected

    monkeypatch.setattr(evaluator, "load_validated_stage_run", fake_loader)
    assert evaluator.load_validated_run(
        tmp_path,
        tmp_path,
        stage=evaluator.RECORD_EXTRACTION_STAGE,
        contract_override=override,
    ) == expected
    assert observed == {
        "input_export_dir": tmp_path,
        "output_dir": tmp_path,
        "stage": evaluator.RECORD_EXTRACTION_STAGE,
        "contract_override": override,
    }


@pytest.mark.skipif(
    not (ISSUE_CONTROL_BUNDLE / "bundle_manifest.json").is_file(),
    reason="frozen suffix-o issue-control bundle is unavailable",
)
def test_issue_control_is_derived_from_frozen_bundle_not_self_reported() -> None:
    control = derive_issue_control(ISSUE_CONTROL_BUNDLE)
    assert control["quote_bearing_resolved_issue_records"] == 2
    qualifying = [
        group for group in control["fully_accounted_private_conflict_groups"]
        if group["fully_accounted"]
        and {"proposal", "oppose"} <= set(group["statement_kinds"])
    ]
    assert len(qualifying) == 1
    assert control["artifact_provenance"][
        "derived_without_self_reported_counts"
    ] is True
