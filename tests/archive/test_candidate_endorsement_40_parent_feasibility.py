from __future__ import annotations

import json
import subprocess
from pathlib import Path

from scripts.audit_candidate_endorsement_40_parent_feasibility import (
    authenticated_full_rows,
    construct_recall_edges,
    maximum_coordinate_matching,
    score_recall_matching,
)


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/audit_candidate_endorsement_40_parent_feasibility.py"
REPORT = REPO / "quality_reports/candidate_endorsement_40_parent_recall_pilot_feasibility_2026-08-26.json"
SPEC = REPO / "quality_reports/candidate_endorsement_40_parent_recall_pilot_selection_spec_2026-08-26.json"


def test_aggregate_feasibility_replays_without_parent_ids_or_text() -> None:
    observed = json.loads(subprocess.check_output(
        ["python3", str(SCRIPT)], text=True, cwd=REPO,
    ))
    assert observed == json.loads(REPORT.read_text())
    assert observed["feasible"] is True
    assert observed["aggregate_only_no_ids_or_text"] is True
    assert observed["reserve_witness"] == {"RG": 10, "RM": 30, "RS": 10, "RZ": 30}
    assert observed["pilot_joint_witness"]["primary"] == {
        "D": 8, "L": 8, "M": 8, "S": 8, "Z": 8,
    }
    assert "selected_parent_ids" not in observed
    assert "reserve_parent_ids" not in observed
    assert "source_texts" not in observed
    assert observed["explicit_excluded_parent_id_count"] == 713
    assert observed["excluded_parent_count_after_component_closure"] == 660
    assert observed["joint_component_solver_proof"]["mip_gap"] == 0.0
    assert observed["joint_component_solver_proof"] == {
        "canonical_assignment_sha256": "68fe62747c357239e87e785b3a444ec946c9ee1d96bc521bcaeece73f06f6877",
        "d_density_ppm_sum": 7873295, "highs_version": "1.8.0",
        "lex_solver_status_counts": {
            "pilot_certified_infeasible": 1821, "pilot_feasible": 39,
            "pilot_quota_forced_zero": 17, "reserve_certified_infeasible": 477,
            "reserve_feasible": 79, "reserve_quota_forced_zero": 1321,
        },
        "mip_gap": 0.0,
        "objective_serialization_sha256": "63eb0e0c99edf2bb0426509d599f66efba0174fc0b1b84ab1ce55ddebadff1f7",
        "objective_stage_proofs": {
            "negative_D_density": {"dual_bound": -7873295.0, "mip_gap": 0.0, "objective": -7873295.0, "status": 0},
            "year_bin_max": {"dual_bound": 14.0, "mip_gap": 0.0, "objective": 14.0, "status": 0},
            "year_squared_deviation": {"dual_bound": 214.0, "mip_gap": 0.0, "objective": 214.0, "status": 0},
        },
        "ordered_component_rank_sha256": "912cb4187e0ebf819cc4d70e9f2f5e8c7564719383aa7fb25ec27a0aaabc85dd",
        "pilot_lex_decisions": 1877, "reserve_lex_decisions": 1877,
        "scipy_version": "1.17.0", "solver": "scipy.optimize.milp_highs",
        "success": True, "year_bin_max": 14, "year_squared_deviation": 214,
    }


def test_spec_keeps_inference_and_release_authorization_separate() -> None:
    spec = json.loads(SPEC.read_text())
    assert spec["authorization"] == {
        "bounded_private_40_parent_inference_pilot": True,
        "full_private_endorsement_inference_expansion": False,
        "release": False,
        "semantic_fields_validated": False,
        "boundary": (
            "a passing 40 may support consideration of full private endorsement inference; "
            "independent human semantic validation still blocks release"
        ),
    }
    assert spec["sealed_release_candidate_pool"]["parent_count"] == 80
    assert spec["sealed_release_candidate_pool"]["exact_parent_quotas"] == {
        "RZ_legacy_zero": 30, "RM_legacy_positive_windows_ge_2": 30,
        "RS_legacy_positive_one_window_at_or_below_median": 10,
        "RG_legacy_positive_remaining_one_window": 10,
    }
    assert spec["pilot"]["parent_count"] == 40


def test_coordinate_matcher_is_maximum_cardinality_not_greedy() -> None:
    # The lexically first H0 edge consumes M0, but H0->M1 plus H1->M0 is the
    # unique maximum-cardinality solution.
    edges = (
        (0, 0, (0, 1, "a")),
        (0, 1, (0, 2, "b")),
        (1, 0, (0, 3, "c")),
    )
    assert maximum_coordinate_matching(2, 2, edges) == ((0, 1), (1, 0))


def test_authenticated_jsonl_and_dense_coordinate_matching() -> None:
    rows, digest = authenticated_full_rows()
    assert len(rows) > 1000
    assert len(digest) == 64  # Exact frozen chunks include embedded U+2028.
    humans = [{"name_start": i * 10, "name_end": i * 10 + 5,
               "support_start": i * 10, "support_end": i * 10 + 5,
               "endorser_name": f"Name {i}"} for i in range(451)]
    models = [{"record_evidence_parent_span": (i * 10, i * 10 + 5),
               "endorsement_context_parent_span": (i * 10, i * 10 + 5),
               "endorser_name": f"Name {i}", "batch_id": f"b{i:03d}",
               "slot": "slot_00", "record_index": 0} for i in range(451)]
    constructed = construct_recall_edges(humans, models)
    assert all(edge[3] for edge in constructed)
    score = score_recall_matching(humans, models)
    assert score["matched_count"] == 451
    assert score["combined_support_covered_count"] == 451
    models[0]["record_evidence_parent_span"] = [0, 5]
    models[0]["endorsement_context_parent_span"] = [1000, 1005]
    humans[0]["support_start"], humans[0]["support_end"] = 6, 7
    score = score_recall_matching(humans, models)
    assert score["matched_count"] == 451
    assert score["combined_support_covered_count"] == 450
