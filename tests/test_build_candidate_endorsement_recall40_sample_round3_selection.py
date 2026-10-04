from __future__ import annotations

import copy
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pytest

import scripts.audit_candidate_endorsement_40_parent_feasibility as base
import scripts.build_candidate_endorsement_recall40_sample_round3_selection as selector


@pytest.fixture(scope="module")
def eligible_pool() -> tuple[list[dict], dict]:
    return selector.build_eligible_inventory()


def test_contract_is_frozen_before_materialization_and_reserve_stays_sealed() -> None:
    assert selector.SAMPLE_ROUND == 3
    assert selector.SEED == 20262827
    assert selector.PARENT_TARGETS == {"M": 32, "Z": 8}
    assert selector.SELECTION_SPEC_SHA256 == (
        "6224629ad98e91b5f2a09811db5473f0b7bd25d4607920ed3688fce3109eb640"
    )
    assert selector.EXPECTED_CANONICAL_ASSIGNMENT_SHA256 == (
        "59b03bcd70e3306218303fa7ae32576744229282c0eb68d9f51e8b3a7ab9da63"
    )
    assert selector.EXPECTED_ORDERED_COMPONENT_RANK_SHA256 == (
        "68bc457464557144595c6b4750a30966981b3e1e376613362cb2e20e99d80202"
    )
    assert selector.SELECTION_SPEC["status"] == (
        "frozen_in_selector_before_parent_materialization"
    )
    assert selector.SELECTION_SPEC["authorization"] == {
        "full_private_inference": False,
        "hive_submission": False,
        "human_truth": False,
        "private_sample_assignment": True,
        "release": False,
        "source_task": False,
    }
    assert "never reconstructed" in selector.SELECTION_SPEC["reserve_boundary"]
    assert "authentication necessarily reads" in selector.SELECTION_SPEC[
        "reserve_boundary"
    ]
    assert "density" not in selector.SELECTION_SPEC["canonical_selector"]


def test_pool_uses_full_inherited_source_component_closure(
    eligible_pool: tuple[list[dict], dict],
) -> None:
    inventory, metadata = eligible_pool
    assert len(inventory) == 1764
    assert metadata["eligible_component_count"] == 1720
    assert metadata["eligible_structure"] == {"M": 1530, "Z": 234}
    assert metadata["eligible_parent_ids_sha256"] == (
        "4d307104834f00c84893c5aa1426dc707bcdd946ace0e5d8989f7f03e8c75c8f"
    )
    assert metadata["eligible_source_hashes_sha256"] == (
        "25582d150111d9d4eed1d7f9b4611c91f124d642a5dcf8a37ddcc265a47ff74e"
    )
    assert metadata["year_quartile_boundaries"] == [2020, 2020, 2022]
    assert {row["pilot"] for row in inventory} == {"M", "Z"}

    full_rows, _ = base.authenticated_full_rows()
    rows_by_parent: dict[str, list[dict]] = defaultdict(list)
    all_parents_by_source: dict[str, set[str]] = defaultdict(set)
    for row in full_rows:
        if row["route"] != "endorsements":
            continue
        parent_id = row["parent_document_id"]
        source_hash = row["extraction_source_text_sha256"]
        rows_by_parent[parent_id].append(row)
        all_parents_by_source[source_hash].add(parent_id)
    forbidden, provenance, authenticated_sets = (
        selector.authenticated_forbidden_source_hashes(rows_by_parent)
    )
    eligible_ids = {row["id"] for row in inventory}
    eligible_hashes = {row["source_hash"] for row in inventory}
    assert not eligible_hashes & forbidden
    assert all(
        all_parents_by_source[source_hash] <= eligible_ids
        for source_hash in eligible_hashes
    )
    assert provenance[
        "round2_inherited_excluded_source_component_overlap"
    ] == 1
    assert provenance["sealed_reserve_source_text_reconstructed"] is False
    assert provenance["sealed_reserve_identifiers_emitted"] is False
    assert provenance["upstream_source_artifact_bytes_read_for_authentication"] is True
    assert forbidden == set().union(*authenticated_sets.values())


def test_forbidden_components_never_reach_parent_reconstruction() -> None:
    full_rows, _ = base.authenticated_full_rows()
    rows_by_parent: dict[str, list[dict]] = defaultdict(list)
    for row in full_rows:
        if row["route"] == "endorsements":
            rows_by_parent[row["parent_document_id"]].append(row)
    forbidden, _, _ = selector.authenticated_forbidden_source_hashes(rows_by_parent)
    reconstructed_hashes: list[str] = []

    def recording_reconstruction(rows: list[dict]) -> dict:
        source_hashes = {
            row["extraction_source_text_sha256"] for row in rows
        }
        assert len(source_hashes) == 1
        reconstructed_hashes.append(next(iter(source_hashes)))
        return selector.reconstruct_parent_inventory(rows)

    inventory, _ = selector.build_eligible_inventory(
        reconstruct=recording_reconstruction
    )
    assert len(reconstructed_hashes) == len(inventory)
    assert not set(reconstructed_hashes) & forbidden


def test_exact_solver_proves_requested_40_parent_mix(
) -> None:
    command = (
        "import json; "
        "import scripts.build_candidate_endorsement_recall40_sample_round3_selection as s; "
        "print(json.dumps(s.feasibility_check(),sort_keys=True))"
    )
    observed = json.loads(
        subprocess.check_output(
            ["python3", "-c", command],
            cwd=selector.REPO,
            text=True,
        )
    )
    assert observed["feasible"] is True
    assert observed["eligible_parent_count"] == 1764
    assert observed["eligible_component_count"] == 1720
    assert observed["selected_parent_count"] == 40
    assert observed["primary"] == {"M": 32, "Z": 8}
    assert observed["candidate_cap"] <= 2
    assert observed["solver_status"] == 0
    assert observed["mip_gap"] == 0.0
    assert all(
        observed["flags"][key] >= value
        for key, value in selector.FLAG_MINIMA.items()
    )


def test_selection_packaging_is_private_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inventory = []
    for index in range(40):
        structure = "Z" if index < 8 else "M"
        inventory.append(
            {
                "id": f"parent-{index:02d}",
                "source_hash": f"{index:064x}",
                "candidate": f"candidate-{index:02d}",
                "party": "D" if index % 2 == 0 else "R",
                "year": 2020,
                "year_quartile": "Y1",
                "legacy": 0 if structure == "Z" else 1,
                "windows": 0 if structure == "Z" else 2,
                "pilot": structure,
                "reserve": "RZ" if structure == "Z" else "RM",
                "testimonial": index < 10,
                "compound": index < 10,
                "context": index < 10,
                "url": index < 10,
                "list": index < 10,
                "density_ppm": 0,
            }
        )
    metadata = {
        "source_content_inventory_sha256": "b" * 64,
        "eligible_parent_count": 40,
        "eligible_component_count": 40,
        "eligible_parent_ids_sha256": "c" * 64,
        "eligible_source_hashes_sha256": "d" * 64,
        "eligible_structure": {"M": 32, "Z": 8},
        "year_quartile_boundaries": [2020, 2020, 2020],
        "exclusions": {
            "sealed_reserve_source_text_reconstructed": False,
            "sealed_reserve_identifiers_emitted": False,
        },
        "authenticated_exclusion_sets": {
            "inherited_excluded": set(),
            "initial_reserve": set(),
            "initial_pilot": set(),
            "round2": set(),
        },
    }
    pilot = [[row] for row in inventory]
    monkeypatch.setattr(
        selector, "build_eligible_inventory", lambda: (inventory, dict(metadata))
    )

    def fake_selector(clusters, *args, **kwargs):
        proof = selector._assignment_identity(clusters, [], pilot)
        proof.update({"mip_gap": 0.0, "success": True})
        return [], pilot, proof

    identity = selector._assignment_identity(pilot, [], pilot)
    monkeypatch.setattr(selector, "canonical_sample_witness", fake_selector)
    monkeypatch.setattr(
        selector,
        "EXPECTED_CANONICAL_ASSIGNMENT_SHA256",
        identity["canonical_assignment_sha256"],
    )
    monkeypatch.setattr(
        selector,
        "EXPECTED_ORDERED_COMPONENT_RANK_SHA256",
        identity["ordered_component_rank_sha256"],
    )
    selection, report = selector.build_selection()
    assert selection["pilot_parent_count"] == 40
    assert selection["sample_round"] == 3
    assert selection["seed"] == 20262827
    assert selection["selector_script_sha256"] == report[
        "selector_script_sha256"
    ]
    assert len(selection["selector_script_sha256"]) == 64
    assert selection["authorization_scope"] == (
        "private_sample_assignment_only_no_source_task_inference_or_release"
    )
    assert sum(
        len(component["parent_document_ids"])
        for component in selection["pilot_components"]
    ) == 40
    assert report["aggregate_only_no_parent_ids_source_hashes_or_text"] is True
    assert report["selected_parent_count"] == 40
    assert report["primary"] == {"M": 32, "Z": 8}
    assert report["inherited_excluded_component_overlap"] == 0
    assert report["initial_reserve_component_overlap"] == 0
    assert report["initial_pilot_component_overlap"] == 0
    assert report["round2_component_overlap"] == 0
    assert report["authorizations"]["human_truth"] is False
    assert report["authorizations"]["release"] is False
    serialized_report = json.dumps(report, sort_keys=True)
    selected_rows = [row for component in pilot for row in component]
    assert all(row["id"] not in serialized_report for row in selected_rows)
    assert all(
        row["source_hash"] not in serialized_report for row in selected_rows
    )


def test_solver_rows_are_rebound_to_complete_canonical_components() -> None:
    canonical = [
        [
            {"id": "parent-a", "source_hash": "a" * 64, "pilot": "M"},
            {"id": "parent-b", "source_hash": "a" * 64, "pilot": "M"},
        ]
    ]
    unknown = copy.deepcopy(canonical)
    unknown[0][0]["id"] = "unknown-parent"
    with pytest.raises(RuntimeError, match="unknown parent row"):
        selector._rebind_solver_components(unknown, canonical, "pilot")

    partial = [[copy.deepcopy(canonical[0][0])]]
    with pytest.raises(RuntimeError, match="partial component"):
        selector._rebind_solver_components(partial, canonical, "pilot")

    returned = copy.deepcopy(canonical)
    rebound = selector._rebind_solver_components(returned, canonical, "pilot")
    assert rebound[0] is canonical[0]


def test_assignment_proof_mismatch_fails_closed() -> None:
    component = [
        {
            "id": "parent-a",
            "source_hash": "a" * 64,
            "candidate": "candidate-a",
            "party": "D",
            "year_quartile": "Y1",
            "pilot": "M",
            "reserve": "RM",
            "testimonial": False,
            "compound": False,
            "context": False,
            "url": False,
            "list": False,
        }
    ]
    clusters = [component]
    proof = selector._assignment_identity(clusters, [], clusters)
    proof["canonical_assignment_sha256"] = "0" * 64
    with pytest.raises(RuntimeError, match="canonical_assignment_sha256 differs"):
        selector._verify_assignment_proof(clusters, [], clusters, proof)


def test_nonzero_authenticated_overlap_fails_closed() -> None:
    selected = {"a" * 64}
    authenticated_sets = {
        "inherited_excluded": set(),
        "initial_reserve": {"a" * 64},
        "initial_pilot": set(),
        "round2": set(),
    }
    with pytest.raises(RuntimeError, match="overlap"):
        selector._authenticated_overlap_counts(selected, authenticated_sets)


def test_authenticated_inputs_and_existing_outputs_fail_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    attacked = tmp_path / "selection.json"
    attacked.write_text(
        selector.INITIAL_SELECTION.read_text(encoding="utf-8") + " ",
        encoding="utf-8",
    )
    monkeypatch.setattr(selector, "INITIAL_SELECTION", attacked)
    with pytest.raises(ValueError, match="initial private selection bytes differ"):
        selector.authenticated_forbidden_source_hashes({})

    selection_output = tmp_path / "already-exists.json"
    selection_output.write_text("preserve", encoding="utf-8")
    report_output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(selector.__file__),
            "--selection",
            str(selection_output),
            "--report",
            str(report_output),
        ],
    )
    with pytest.raises(ValueError, match="output already exists"):
        selector.main()
    assert selection_output.read_text(encoding="utf-8") == "preserve"
    assert not report_output.exists()
