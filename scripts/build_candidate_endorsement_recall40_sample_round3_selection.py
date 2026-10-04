#!/usr/bin/env python3
"""Select a third untouched recall40 sample without consuming reserve80.

The sample-round number is separate from the extraction-pipeline revision.
Pipeline round three reused sample round two; this selector draws the next
unused sample with seed 20262827.  It emits no source text and never
reconstructs a parent whose source component is forbidden.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path
from collections.abc import Callable
from typing import Any, Mapping

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import scripts.audit_candidate_endorsement_40_parent_feasibility as base
from src.candidate_endorsement_line_ledger import (
    LINE_DECISION_SLOT_COUNT,
    _compound_anchor_expected_names,
    reconstruct_parent_inventory,
    sha256_json,
)


SAMPLE_ROUND = 3
SEED = 20260827 + 1000 * (SAMPLE_ROUND - 1)
PARENT_TARGETS = {"M": 32, "Z": 8}
FLAG_MINIMA = {
    "compound": 6,
    "context": 8,
    "list": 10,
    "testimonial": 8,
    "url": 8,
}
PARTY_LIMITS = {"D": 12, "O": 4, "R": 12}
YEAR_BINS = ("Y0_NULL", "Y1", "Y2", "Y3", "Y4")

INITIAL_SELECTION = (
    REPO
    / "build/private_candidate_endorsement_40_parent_selection_20260827"
    / "selection.json"
)
INITIAL_SELECTION_SHA256 = (
    "ef378019b0216a3ef8d1cb7ab9777c6d4f383e383857b3da013802430932202d"
)
ROUND2_SELECTION = (
    REPO
    / "build/private_candidate_endorsement_recall40_round2_selection_20260901"
    / "selection.json"
)
ROUND2_SELECTION_SHA256 = (
    "cda7d58561c439aee65dbb4be391c6c9edcbc86a7209625512bf12c619cebdac"
)
ROUND3_TECHNICAL_GATE = (
    REPO
    / "quality_reports"
    / "candidate_endorsement_recall40_round3_stage_b_22685041_2026-09-06.json"
)
ROUND3_TECHNICAL_GATE_SHA256 = (
    "f775b6e3d7d27111f926516b449b85203d0bbdb263c1f1eeb8de9d7a58b082fa"
)
LEGACY_SHA256 = (
    "c20c2a3443edc13f985c5cb3b2711c653f57c8b79debb62612236ddf53c950b3"
)

# These identities freeze the inherited exclusion boundary without making
# later zero-contribution selection.csv files change the sample.
EXPECTED_INHERITED_EXCLUDED_PARENT_IDS_SHA256 = (
    "fcc286976353f281c7dfdc074518e096da164257e74ba0350599de4140b78d7c"
)
EXPECTED_INHERITED_EXCLUDED_SOURCE_HASHES_SHA256 = (
    "01394b11880d00c2145f72ab7a8b5872b19a402278e2005911b2ffc6aaa0a3f8"
)
EXPECTED_FORBIDDEN_SOURCE_HASHES_SHA256 = (
    "e25fe3d8807be8eec967738a739f843f78403edfad16fe60854aa5271c309631"
)
EXPECTED_ELIGIBLE_PARENT_IDS_SHA256 = (
    "4d307104834f00c84893c5aa1426dc707bcdd946ace0e5d8989f7f03e8c75c8f"
)
EXPECTED_ELIGIBLE_SOURCE_HASHES_SHA256 = (
    "25582d150111d9d4eed1d7f9b4611c91f124d642a5dcf8a37ddcc265a47ff74e"
)
EXPECTED_ELIGIBLE_PARENT_COUNT = 1764
EXPECTED_ELIGIBLE_COMPONENT_COUNT = 1720
EXPECTED_ELIGIBLE_STRUCTURE = {"M": 1530, "Z": 234}
EXPECTED_YEAR_QUARTILE_BOUNDARIES = [2020, 2020, 2022]

EXPECTED_CANONICAL_ASSIGNMENT_SHA256 = (
    "59b03bcd70e3306218303fa7ae32576744229282c0eb68d9f51e8b3a7ab9da63"
)
EXPECTED_ORDERED_COMPONENT_RANK_SHA256 = (
    "68bc457464557144595c6b4750a30966981b3e1e376613362cb2e20e99d80202"
)

SELECTION_SPEC: dict[str, Any] = {
    "version": "candidate_endorsement_recall40_sample_round3_selection_spec_v1",
    "status": "frozen_in_selector_before_parent_materialization",
    "sample_round": SAMPLE_ROUND,
    "pipeline_round_three_reused_sample_round": 2,
    "seed": SEED,
    "parent_count": 40,
    "exact_parent_quotas": PARENT_TARGETS,
    "cross_cutting_minima": FLAG_MINIMA,
    "party_minima_rule": {
        "caps": PARTY_LIMITS,
        "rule": "minimum equals min(cap, eligible-party parent count)",
    },
    "candidate_cycle_parent_cap": 2,
    "partition_unit": (
        "complete extraction_source_text_sha256 component, with parent-weighted "
        "constraints"
    ),
    "exclusions": (
        "close the complete source-hash component of every inherited exclusion, "
        "the initial reserve80 and pilot40, and sample-round-two"
    ),
    "eligible_structure": (
        "only legacy-zero Z and legacy-positive multi-window M parents remain; "
        "one-window parents are unavailable outside prior samples and reserve80"
    ),
    "canonical_selector": (
        "local exact staged MILP selector: lexicographically minimum reserve "
        "component-rank vector, minimum maximum absolute deviation of the five "
        "year-bin parent counts from eight, minimum total squared deviation of "
        "those counts from eight, then lexicographically minimum pilot "
        "component-rank vector; every optimization solve must prove zero gap"
    ),
    "reserve_boundary": (
        "upstream authentication necessarily reads and hashes the complete source "
        "artifact bytes; reserve80 source hashes are then exclusion keys only, "
        "and reserve80 components are never reconstructed, rendered, emitted, "
        "coded, or used for semantic feature construction"
    ),
    "authorization": {
        "private_sample_assignment": True,
        "source_task": False,
        "hive_submission": False,
        "human_truth": False,
        "full_private_inference": False,
        "release": False,
    },
}
SELECTION_SPEC_SHA256 = sha256_json(SELECTION_SPEC)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def strict_json(path: Path) -> Any:
    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=hook,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"nonfinite JSON value: {value}")
        ),
    )


def _authenticated_json(path: Path, expected_sha256: str, label: str) -> dict:
    if sha256_file(path) != expected_sha256:
        raise ValueError(f"{label} bytes differ")
    value = strict_json(path)
    if not isinstance(value, dict):
        raise ValueError(f"{label} is not an object")
    return value


def _component_source_hashes(
    value: Mapping[str, Any], key: str, expected_parent_count: int,
) -> set[str]:
    components = value.get(key)
    if not isinstance(components, list):
        raise ValueError(f"{key} is not a list")
    hashes: set[str] = set()
    parent_ids: list[str] = []
    for component in components:
        if not isinstance(component, Mapping):
            raise ValueError(f"{key} component is not an object")
        source_hash = component.get("source_text_sha256")
        ids = component.get("parent_document_ids")
        if (
            not isinstance(source_hash, str)
            or len(source_hash) != 64
            or not isinstance(ids, list)
            or not ids
            or any(not isinstance(parent_id, str) for parent_id in ids)
        ):
            raise ValueError(f"{key} component schema differs")
        if source_hash in hashes:
            raise ValueError(f"{key} source component is duplicated")
        hashes.add(source_hash)
        parent_ids.extend(ids)
    if len(parent_ids) != expected_parent_count or len(set(parent_ids)) != len(parent_ids):
        raise ValueError(f"{key} parent closure differs")
    return hashes


def authenticated_forbidden_source_hashes(
    rows_by_parent: Mapping[str, list[dict[str, Any]]],
) -> tuple[set[str], dict[str, Any], dict[str, set[str]]]:
    initial = _authenticated_json(
        INITIAL_SELECTION, INITIAL_SELECTION_SHA256, "initial private selection"
    )
    if initial.get("version") != "candidate_endorsement_private_reserve80_pilot40_v1":
        raise ValueError("initial private selection version differs")
    reserve_hashes = _component_source_hashes(initial, "reserve_components", 80)
    initial_pilot_hashes = _component_source_hashes(
        initial, "pilot_components", 40
    )
    if reserve_hashes & initial_pilot_hashes:
        raise ValueError("initial reserve and pilot source components overlap")

    round2 = _authenticated_json(
        ROUND2_SELECTION, ROUND2_SELECTION_SHA256, "sample-round-two selection"
    )
    if (
        round2.get("version")
        != "candidate_endorsement_private_recall40_round2_selection_v1"
        or round2.get("round") != 2
        or round2.get("seed") != 20261827
        or round2.get("pilot_parent_count") != 40
    ):
        raise ValueError("sample-round-two selection contract differs")
    round2_hashes = _component_source_hashes(round2, "pilot_components", 40)

    gate = _authenticated_json(
        ROUND3_TECHNICAL_GATE,
        ROUND3_TECHNICAL_GATE_SHA256,
        "pipeline-round-three technical gate",
    )
    if (
        gate.get("gate_decision") != "NO_GO"
        or gate.get("selected_parent_count") != 40
        or gate.get("full_expansion_authorized") is not False
        or gate.get("release_authorized") is not False
    ):
        raise ValueError("pipeline-round-three failure boundary differs")

    raw_hash_to_parent_ids: dict[str, set[str]] = defaultdict(set)
    parent_source_hashes: dict[str, str] = {}
    for parent_id, rows in rows_by_parent.items():
        raw_hashes = {str(row["raw_text_sha256"]) for row in rows}
        source_hashes = {
            str(row["extraction_source_text_sha256"]) for row in rows
        }
        if len(raw_hashes) != 1 or len(source_hashes) != 1:
            raise ValueError("parent source identity is inconsistent")
        raw_hash_to_parent_ids[next(iter(raw_hashes))].add(parent_id)
        parent_source_hashes[parent_id] = next(iter(source_hashes))

    inherited_ids, _ = base.exclusions(raw_hash_to_parent_ids)
    if sha256_json(sorted(inherited_ids)) != (
        EXPECTED_INHERITED_EXCLUDED_PARENT_IDS_SHA256
    ):
        raise ValueError("inherited exclusion parent identity differs")
    missing_inherited_ids = sorted(set(inherited_ids) - set(rows_by_parent))
    inherited_source_hashes = {
        parent_source_hashes[parent_id]
        for parent_id in inherited_ids
        if parent_id in parent_source_hashes
    }
    if sha256_json(sorted(inherited_source_hashes)) != (
        EXPECTED_INHERITED_EXCLUDED_SOURCE_HASHES_SHA256
    ):
        raise ValueError("inherited exclusion source-component identity differs")

    # This overlap documents the prior selector bug.  Closing by source hash
    # below ensures neither that component nor its unsampled sibling survives.
    round2_inherited_overlap = len(round2_hashes & inherited_source_hashes)
    if round2_inherited_overlap != 1:
        raise ValueError("historical round-two exclusion overlap differs")

    forbidden = (
        inherited_source_hashes
        | reserve_hashes
        | initial_pilot_hashes
        | round2_hashes
    )
    if sha256_json(sorted(forbidden)) != EXPECTED_FORBIDDEN_SOURCE_HASHES_SHA256:
        raise ValueError("forbidden source-component identity differs")
    provenance = {
        "initial_selection_sha256": INITIAL_SELECTION_SHA256,
        "round2_selection_sha256": ROUND2_SELECTION_SHA256,
        "round3_technical_gate_sha256": ROUND3_TECHNICAL_GATE_SHA256,
        "inherited_excluded_parent_ids_sha256": (
            EXPECTED_INHERITED_EXCLUDED_PARENT_IDS_SHA256
        ),
        "inherited_excluded_source_hash_count": len(inherited_source_hashes),
        "inherited_excluded_source_hashes_sha256": (
            EXPECTED_INHERITED_EXCLUDED_SOURCE_HASHES_SHA256
        ),
        "initial_reserve_source_component_count": len(reserve_hashes),
        "initial_pilot_source_component_count": len(initial_pilot_hashes),
        "round2_source_component_count": len(round2_hashes),
        "round2_inherited_excluded_source_component_overlap": (
            round2_inherited_overlap
        ),
        "forbidden_source_component_count": len(forbidden),
        "forbidden_source_hashes_sha256": EXPECTED_FORBIDDEN_SOURCE_HASHES_SHA256,
        "inherited_excluded_ids_absent_from_current_endorsement_universe": len(
            missing_inherited_ids
        ),
        "sealed_reserve_source_text_reconstructed": False,
        "sealed_reserve_identifiers_emitted": False,
        "upstream_source_artifact_bytes_read_for_authentication": True,
    }
    authenticated_sets = {
        "inherited_excluded": inherited_source_hashes,
        "initial_reserve": reserve_hashes,
        "initial_pilot": initial_pilot_hashes,
        "round2": round2_hashes,
    }
    return forbidden, provenance, authenticated_sets


def build_eligible_inventory(
    reconstruct: Callable[[list[dict[str, Any]]], dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if sha256_file(base.LEGACY) != LEGACY_SHA256:
        raise ValueError("legacy parent output bytes differ")
    full_rows, source_inventory_sha256 = base.authenticated_full_rows()
    rows_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in full_rows:
        if row.get("route") == "endorsements":
            rows_by_parent[str(row["parent_document_id"])].append(row)

    forbidden, exclusion_provenance, authenticated_sets = (
        authenticated_forbidden_source_hashes(rows_by_parent)
    )
    reconstruct_fn = reconstruct or reconstruct_parent_inventory
    legacy: dict[str, int] = {}
    with gzip.open(base.LEGACY, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("route") == "endorsements":
                legacy[str(row["parent_document_id"])] = len(row["records"])

    inventory: list[dict[str, Any]] = []
    for parent_id in sorted(rows_by_parent):
        rows = rows_by_parent[parent_id]
        source_hashes = {
            str(row["extraction_source_text_sha256"]) for row in rows
        }
        if len(source_hashes) != 1:
            raise ValueError("parent source hashes differ across segments")
        source_hash = next(iter(source_hashes))
        if source_hash in forbidden:
            continue
        # Forbidden components, including reserve80, are skipped before source
        # reconstruction and before any content-derived feature is computed.
        parent = reconstruct_fn(rows)
        if parent["extraction_source_text_sha256"] != source_hash:
            raise ValueError("reconstructed source hash differs")
        if parent_id not in legacy:
            raise ValueError("eligible parent is absent from legacy output")
        text = str(parent["parent_source_text"])
        lines = [str(line["line_text"]) for line in parent["lines"]]
        nonempty = [line for line in lines if line.strip()]
        short = [line for line in nonempty if len(line) <= 160]
        model_line_count = sum(
            bool(line["model_decision_required"]) for line in parent["lines"]
        )
        window_count = (
            math.ceil(model_line_count / LINE_DECISION_SLOT_COUNT)
            if model_line_count
            else 0
        )
        consecutive = 0
        longest_short_run = 0
        for line in lines:
            consecutive = (
                consecutive + 1 if line.strip() and len(line) <= 160 else 0
            )
            longest_short_run = max(longest_short_run, consecutive)
        quote = any(
            len(line) >= 80
            and (("“" in line and "”" in line) or line.count('"') >= 2)
            for line in lines
        )
        no_urls = base.URL.sub("", text)
        structure = (
            "Z"
            if legacy[parent_id] == 0
            else "M"
            if window_count >= 2
            else "ONE"
        )
        inventory.append(
            {
                "id": parent_id,
                "source_hash": source_hash,
                "candidate": str(rows[0]["candidate_cycle_id"]),
                "party": base.party(rows[0].get("party")),
                "year": rows[0].get("year"),
                "legacy": legacy[parent_id],
                "windows": window_count,
                "pilot": structure,
                "reserve": (
                    "RZ" if structure == "Z" else "RM" if structure == "M" else "RS"
                ),
                "testimonial": bool(base.SUPPORT.search(no_urls) or quote),
                "compound": any(
                    _compound_anchor_expected_names(line) is not None
                    for line in lines
                ),
                "context": any(
                    row.get("endorsement_context_provenance", {}).get(
                        "source_scope"
                    )
                    == "page_or_section_route_context"
                    for row in rows
                ),
                "url": bool(base.URL.search(text)),
                "list": (
                    longest_short_run >= 5
                    or sum(bool(base.BULLET.search(line)) for line in lines) >= 3
                ),
                "density_ppm": round(
                    1_000_000 * len(short) / max(1, len(nonempty))
                ),
            }
        )

    eligible_parent_ids = sorted(row["id"] for row in inventory)
    eligible_source_hashes = sorted({row["source_hash"] for row in inventory})
    structure = dict(sorted(Counter(row["pilot"] for row in inventory).items()))
    if (
        len(inventory) != EXPECTED_ELIGIBLE_PARENT_COUNT
        or len(eligible_source_hashes) != EXPECTED_ELIGIBLE_COMPONENT_COUNT
        or sha256_json(eligible_parent_ids) != EXPECTED_ELIGIBLE_PARENT_IDS_SHA256
        or sha256_json(eligible_source_hashes)
        != EXPECTED_ELIGIBLE_SOURCE_HASHES_SHA256
        or structure != EXPECTED_ELIGIBLE_STRUCTURE
    ):
        raise ValueError("eligible untouched pool identity differs")

    years = sorted(
        int(row["year"]) for row in inventory if row["year"] is not None
    )
    boundaries = [
        years[math.ceil(quantile * len(years)) - 1]
        for quantile in (0.25, 0.5, 0.75)
    ]
    if boundaries != EXPECTED_YEAR_QUARTILE_BOUNDARIES:
        raise ValueError("eligible year boundaries differ")
    for row in inventory:
        year = int(row["year"]) if row["year"] is not None else None
        row["year_quartile"] = (
            "Y0_NULL"
            if year is None
            else "Y1"
            if year <= boundaries[0]
            else "Y2"
            if year <= boundaries[1]
            else "Y3"
            if year <= boundaries[2]
            else "Y4"
        )
    metadata = {
        "source_content_inventory_sha256": source_inventory_sha256,
        "eligible_parent_count": len(inventory),
        "eligible_component_count": len(eligible_source_hashes),
        "eligible_parent_ids_sha256": EXPECTED_ELIGIBLE_PARENT_IDS_SHA256,
        "eligible_source_hashes_sha256": EXPECTED_ELIGIBLE_SOURCE_HASHES_SHA256,
        "eligible_structure": structure,
        "year_quartile_boundaries": boundaries,
        "exclusions": exclusion_provenance,
        # Kept private to the selector so overlap counts are derived from the
        # authenticated source sets rather than asserted as constants.
        "authenticated_exclusion_sets": authenticated_sets,
    }
    return inventory, metadata


def component_objective_vectors(
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return only the vectors that define the written sampling contract."""
    return {
        "parent_count": len(rows),
        "reserve": dict(sorted(Counter(row["reserve"] for row in rows).items())),
        "pilot": dict(sorted(Counter(row["pilot"] for row in rows).items())),
        "party": dict(sorted(Counter(row["party"] for row in rows).items())),
        "year_quartile": dict(
            sorted(Counter(row["year_quartile"] for row in rows).items())
        ),
        "candidate_cycle": dict(
            sorted(Counter(row["candidate"] for row in rows).items())
        ),
        "flags": {
            key: sum(bool(row[key]) for row in rows)
            for key in ("testimonial", "compound", "context", "url", "list")
        },
    }


def canonical_component_rank(rows: list[dict[str, Any]]) -> str:
    if not rows:
        raise ValueError("cannot rank an empty source component")
    source_hashes = {row.get("source_hash") for row in rows}
    if len(source_hashes) != 1:
        raise ValueError("component has multiple source hashes")
    payload = {
        "seed": SEED,
        "ordered_parent_identities": sorted(
            (
                {
                    "parent_document_id": row["id"],
                    "source_text_sha256": row["source_hash"],
                }
                for row in rows
            ),
            key=lambda value: (
                value["parent_document_id"],
                value["source_text_sha256"],
            ),
        ),
        "parent_count_vectors": component_objective_vectors(rows),
    }
    return sha256_json(payload)


def _canonical_order(
    clusters: list[list[dict[str, Any]]],
) -> tuple[list[int], list[str]]:
    ranks = [canonical_component_rank(component) for component in clusters]
    if len(ranks) != len(set(ranks)):
        raise RuntimeError("canonical component-rank collision")
    order = sorted(range(len(clusters)), key=lambda index: ranks[index])
    return order, [ranks[index] for index in order]


def _assignment_identity(
    clusters: list[list[dict[str, Any]]],
    reserve_components: list[list[dict[str, Any]]],
    pilot_components: list[list[dict[str, Any]]],
) -> dict[str, str]:
    order, ordered_ranks = _canonical_order(clusters)
    reserve_hashes = {component[0]["source_hash"] for component in reserve_components}
    pilot_hashes = {component[0]["source_hash"] for component in pilot_components}
    bits = {
        "reserve_bits": [
            int(clusters[index][0]["source_hash"] in reserve_hashes)
            for index in order
        ],
        "pilot_bits": [
            int(clusters[index][0]["source_hash"] in pilot_hashes)
            for index in order
        ],
    }
    return {
        "ordered_component_rank_sha256": sha256_json(ordered_ranks),
        "canonical_assignment_sha256": sha256_json(bits),
        "selected_component_source_hashes_sha256": sha256_json(
            sorted(pilot_hashes)
        ),
    }


def _rebind_solver_components(
    returned_components: list[list[dict[str, Any]]],
    canonical_clusters: list[list[dict[str, Any]]],
    assignment: str,
) -> list[list[dict[str, Any]]]:
    """Authenticate solver rows, then replace them with canonical pool rows."""
    canonical_by_hash: dict[str, list[dict[str, Any]]] = {}
    for component in canonical_clusters:
        source_hashes = {row.get("source_hash") for row in component}
        if len(source_hashes) != 1:
            raise RuntimeError("canonical eligible component is internally inconsistent")
        source_hash = next(iter(source_hashes))
        if not isinstance(source_hash, str) or source_hash in canonical_by_hash:
            raise RuntimeError("canonical eligible component identity is invalid")
        canonical_by_hash[source_hash] = component

    rebound: list[list[dict[str, Any]]] = []
    observed_hashes: set[str] = set()
    for component in returned_components:
        if not isinstance(component, list) or not component:
            raise RuntimeError(f"{assignment} solver component is empty or malformed")
        if any(not isinstance(row, Mapping) for row in component):
            raise RuntimeError(f"{assignment} solver row is malformed")
        source_hashes = {row.get("source_hash") for row in component}
        if len(source_hashes) != 1:
            raise RuntimeError(f"{assignment} solver component mixes source hashes")
        source_hash = next(iter(source_hashes))
        if not isinstance(source_hash, str) or source_hash not in canonical_by_hash:
            raise RuntimeError(f"{assignment} solver returned an unknown component")
        if source_hash in observed_hashes:
            raise RuntimeError(f"{assignment} solver duplicated a component")
        canonical = canonical_by_hash[source_hash]
        canonical_by_id = {row["id"]: row for row in canonical}
        returned_ids = [row.get("id") for row in component]
        if len(returned_ids) != len(set(returned_ids)):
            raise RuntimeError(f"{assignment} solver duplicated a parent row")
        unknown_ids = set(returned_ids) - set(canonical_by_id)
        if unknown_ids:
            raise RuntimeError(f"{assignment} solver returned an unknown parent row")
        missing_ids = set(canonical_by_id) - set(returned_ids)
        if missing_ids:
            raise RuntimeError(f"{assignment} solver returned a partial component")
        for row in component:
            if dict(row) != canonical_by_id[row["id"]]:
                raise RuntimeError(f"{assignment} solver row differs from canonical row")
        rebound.append(canonical)
        observed_hashes.add(source_hash)
    return rebound


def _verify_assignment_proof(
    canonical_clusters: list[list[dict[str, Any]]],
    reserve_components: list[list[dict[str, Any]]],
    pilot_components: list[list[dict[str, Any]]],
    proof: Mapping[str, Any],
) -> dict[str, str]:
    observed = _assignment_identity(
        canonical_clusters, reserve_components, pilot_components
    )
    for key, value in observed.items():
        if proof.get(key) != value:
            raise RuntimeError(f"solver proof {key} differs from canonical assignment")
    return observed


def _authenticated_overlap_counts(
    selected_source_hashes: set[str],
    authenticated_sets: Mapping[str, set[str]],
) -> dict[str, int]:
    expected = {"inherited_excluded", "initial_reserve", "initial_pilot", "round2"}
    if set(authenticated_sets) != expected:
        raise RuntimeError("authenticated exclusion-set names differ")
    counts = {
        f"{name}_component_overlap": len(selected_source_hashes & hashes)
        for name, hashes in authenticated_sets.items()
    }
    if any(counts.values()):
        raise RuntimeError("selected source hashes overlap an authenticated exclusion set")
    return dict(sorted(counts.items()))


def canonical_sample_witness(
    clusters: list[list[dict[str, Any]]],
    reserve_targets: dict[str, int],
    pilot_targets: dict[str, int],
    flag_targets: dict[str, int],
    party_targets: dict[str, int],
) -> tuple[list[list[dict[str, Any]]], list[list[dict[str, Any]]], dict[str, Any]]:
    """Solve the four written objectives exactly, retaining each prior optimum."""
    import numpy as np
    import scipy
    import scipy.optimize._highspy._core as highs_core
    from scipy.optimize import Bounds, LinearConstraint, milp

    if not clusters or any(not component for component in clusters):
        raise ValueError("eligible source components must be nonempty")
    n = len(clusters)
    z0 = 2 * n
    max_deviation_index = z0 + len(YEAR_BINS) * 41
    total_variables = max_deviation_index + 1
    constraint_rows: list[Any] = []
    lower: list[float] = []
    upper: list[float] = []

    def add(values: list[float], lo: float, hi: float) -> None:
        row = np.zeros(total_variables)
        row[: len(values)] = values
        constraint_rows.append(row)
        lower.append(lo)
        upper.append(hi)

    zeros = [0] * n
    for label, target in reserve_targets.items():
        values = [
            sum(row["reserve"] == label for row in component)
            for component in clusters
        ]
        add(values + zeros, target, target)
    for label, target in pilot_targets.items():
        values = [
            sum(row["pilot"] == label for row in component)
            for component in clusters
        ]
        add(zeros + values, target, target)
    for flag, target in flag_targets.items():
        values = [
            sum(bool(row[flag]) for row in component) for component in clusters
        ]
        add(zeros + values, target, np.inf)
    for label, target in party_targets.items():
        values = [
            sum(row["party"] == label for row in component)
            for component in clusters
        ]
        add(zeros + values, target, np.inf)
    candidates = sorted(
        {row["candidate"] for component in clusters for row in component}
    )
    for candidate in candidates:
        values = [
            sum(row["candidate"] == candidate for row in component)
            for component in clusters
        ]
        add(zeros + values, -np.inf, 2)
    for index in range(n):
        values = [0] * (2 * n)
        values[index] = 1
        values[n + index] = 1
        add(values, -np.inf, 1)

    for bin_index, label in enumerate(YEAR_BINS):
        counts = np.asarray(
            [
                sum(row["year_quartile"] == label for row in component)
                for component in clusters
            ],
            dtype=float,
        )
        count_start = z0 + bin_index * 41
        count_stop = count_start + 41

        row = np.zeros(total_variables)
        row[n : 2 * n] = counts
        row[count_start:count_stop] = -np.arange(41)
        constraint_rows.append(row)
        lower.append(0)
        upper.append(0)

        row = np.zeros(total_variables)
        row[count_start:count_stop] = 1
        constraint_rows.append(row)
        lower.append(1)
        upper.append(1)

        # count - 8 <= max_deviation
        row = np.zeros(total_variables)
        row[n : 2 * n] = counts
        row[max_deviation_index] = -1
        constraint_rows.append(row)
        lower.append(-np.inf)
        upper.append(8)

        # 8 - count <= max_deviation
        row = np.zeros(total_variables)
        row[n : 2 * n] = -counts
        row[max_deviation_index] = -1
        constraint_rows.append(row)
        lower.append(-np.inf)
        upper.append(-8)

    base_matrix = np.asarray(constraint_rows)
    base_lower = np.asarray(lower)
    base_upper = np.asarray(upper)
    variable_upper = np.ones(total_variables)
    variable_upper[max_deviation_index] = 40
    bounds = Bounds(np.zeros(total_variables), variable_upper)
    integrality = np.ones(total_variables)
    fixed: dict[int, int] = {}
    retained_rows: list[Any] = []
    retained_values: list[float] = []

    def solve(objective: Any):
        extra_rows = list(retained_rows)
        extra_values = list(retained_values)
        for index, value in fixed.items():
            row = np.zeros(total_variables)
            row[index] = 1
            extra_rows.append(row)
            extra_values.append(value)
        matrix = (
            np.vstack([base_matrix] + extra_rows) if extra_rows else base_matrix
        )
        solve_lower = (
            np.concatenate([base_lower, np.asarray(extra_values)])
            if extra_values
            else base_lower
        )
        solve_upper = (
            np.concatenate([base_upper, np.asarray(extra_values)])
            if extra_values
            else base_upper
        )
        return milp(
            c=np.asarray(objective),
            integrality=integrality,
            bounds=bounds,
            constraints=LinearConstraint(matrix, solve_lower, solve_upper),
            options={"presolve": True, "mip_rel_gap": 0.0},
        )

    def exact_integer_optimum(result: Any, stage: str) -> int:
        if (
            not result.success
            or result.status != 0
            or result.mip_gap != 0
            or result.fun is None
            or not float(result.fun).is_integer()
        ):
            raise RuntimeError(f"{stage} lacks an exact integer optimum: {result}")
        return int(result.fun)

    lex_statuses: Counter[str] = Counter()

    def inclusion_decision(result: Any, stage: str) -> int:
        if result.success and result.status == 0 and result.mip_gap == 0:
            lex_statuses[f"{stage}_feasible"] += 1
            return 1
        if not result.success and result.status == 2:
            lex_statuses[f"{stage}_certified_infeasible"] += 1
            return 0
        raise RuntimeError(f"{stage} lex decision was not proven: {result}")

    order, ordered_ranks = _canonical_order(clusters)
    reserve_parent_target = sum(reserve_targets.values())
    for index in order:
        selected_count = sum(
            len(clusters[fixed_index])
            for fixed_index, value in fixed.items()
            if fixed_index < n and value == 1
        )
        if selected_count == reserve_parent_target:
            fixed[index] = 0
            lex_statuses["reserve_quota_forced_zero"] += 1
            continue
        trial = dict(fixed)
        trial[index] = 1
        original = fixed
        fixed = trial
        result = solve(np.zeros(total_variables))
        fixed = original
        fixed[index] = inclusion_decision(result, "reserve")

    objective = np.zeros(total_variables)
    objective[max_deviation_index] = 1
    year_max_stage = solve(objective)
    max_deviation = exact_integer_optimum(
        year_max_stage, "pilot_year_bin_max_absolute_deviation"
    )
    retained = np.zeros(total_variables)
    retained[max_deviation_index] = 1
    retained_rows.append(retained)
    retained_values.append(max_deviation)

    squared_objective = np.zeros(total_variables)
    for bin_index in range(len(YEAR_BINS)):
        start = z0 + bin_index * 41
        squared_objective[start : start + 41] = [
            (count - 8) ** 2 for count in range(41)
        ]
    year_squared_stage = solve(squared_objective)
    squared_deviation = exact_integer_optimum(
        year_squared_stage, "pilot_year_squared_deviation"
    )
    retained_rows.append(squared_objective.copy())
    retained_values.append(squared_deviation)

    pilot_parent_target = sum(pilot_targets.values())
    for index in order:
        pilot_index = n + index
        selected_count = sum(
            len(clusters[fixed_index - n])
            for fixed_index, value in fixed.items()
            if fixed_index >= n and value == 1
        )
        if selected_count == pilot_parent_target:
            fixed[pilot_index] = 0
            lex_statuses["pilot_quota_forced_zero"] += 1
            continue
        trial = dict(fixed)
        trial[pilot_index] = 1
        original = fixed
        fixed = trial
        result = solve(np.zeros(total_variables))
        fixed = original
        fixed[pilot_index] = inclusion_decision(result, "pilot")

    final = solve(np.zeros(total_variables))
    if not final.success or final.status != 0 or final.mip_gap != 0:
        raise RuntimeError("canonical lexicographic refinement was not proven")
    reserve = [clusters[index] for index in range(n) if fixed[index] == 1]
    pilot = [clusters[index] for index in range(n) if fixed[n + index] == 1]
    pilot_rows = [row for component in pilot for row in component]
    observed_year = Counter(row["year_quartile"] for row in pilot_rows)
    observed_max_deviation = max(
        abs(observed_year.get(label, 0) - 8) for label in YEAR_BINS
    )
    observed_squared_deviation = sum(
        (observed_year.get(label, 0) - 8) ** 2 for label in YEAR_BINS
    )
    if observed_max_deviation != max_deviation:
        raise RuntimeError("final year-bin maximum deviation differs from optimum")
    if observed_squared_deviation != squared_deviation:
        raise RuntimeError("final year squared deviation differs from optimum")

    def stage_proof(result: Any) -> dict[str, Any]:
        return {
            "mip_gap": float(result.mip_gap),
            "dual_bound": float(result.mip_dual_bound),
            "objective": float(result.fun),
            "status": int(result.status),
        }

    identities = _assignment_identity(clusters, reserve, pilot)
    proof = {
        "solver": "scipy.optimize.milp_highs",
        "scipy_version": scipy.__version__,
        "highs_version": (
            f"{highs_core.HIGHS_VERSION_MAJOR}."
            f"{highs_core.HIGHS_VERSION_MINOR}."
            f"{highs_core.HIGHS_VERSION_PATCH}"
        ),
        "status": int(final.status),
        "success": True,
        "mip_gap": float(final.mip_gap),
        "year_bin_max_absolute_deviation_from_8": max_deviation,
        "year_squared_deviation_from_8": squared_deviation,
        "year_bin_parent_counts": {
            label: observed_year.get(label, 0) for label in YEAR_BINS
        },
        "reserve_lex_decisions": n,
        "pilot_lex_decisions": n,
        "lex_solver_status_counts": dict(sorted(lex_statuses.items())),
        "objective_stage_proofs": {
            "year_bin_max_absolute_deviation": stage_proof(year_max_stage),
            "year_squared_deviation": stage_proof(year_squared_stage),
        },
        **identities,
        "objective_serialization_sha256": sha256_json(
            {
                "precedence": [
                    "reserve_component_rank_list",
                    "pilot_year_bin_max_absolute_deviation_from_8",
                    "pilot_year_squared_deviation_from_8",
                    "pilot_component_rank_list",
                ],
                "year_bins": list(YEAR_BINS),
                "year_bin_target": 8,
                "ordered_component_ranks": ordered_ranks,
            }
        ),
    }
    return reserve, pilot, proof


def _validate_witness(
    pilot_components: list[list[dict[str, Any]]],
    eligible_metadata: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = [row for component in pilot_components for row in component]
    counts = Counter(row["pilot"] for row in rows)
    flags = {
        key: sum(bool(row[key]) for row in rows) for key in FLAG_MINIMA
    }
    parties = Counter(row["party"] for row in rows)
    candidate_counts = Counter(row["candidate"] for row in rows)
    component_hashes = [component[0]["source_hash"] for component in pilot_components]
    if (
        len(rows) != 40
        or len({row["id"] for row in rows}) != 40
        or dict(sorted(counts.items())) != PARENT_TARGETS
        or any(flags[key] < target for key, target in FLAG_MINIMA.items())
        or (candidate_counts and max(candidate_counts.values()) > 2)
        or len(component_hashes) != len(set(component_hashes))
        or any(
            len({row["source_hash"] for row in component}) != 1
            for component in pilot_components
        )
    ):
        raise RuntimeError("selected witness violates the frozen sample contract")
    # Party minima depend on pool availability, not on the selected witness.
    pool_party = eligible_metadata["party_available"]
    party_targets = {
        key: min(limit, int(pool_party.get(key, 0)))
        for key, limit in PARTY_LIMITS.items()
    }
    if any(parties[key] < target for key, target in party_targets.items()):
        raise RuntimeError("selected witness violates party minima")
    summary = {
        "selected_parent_count": len(rows),
        "selected_component_count": len(pilot_components),
        "primary": dict(sorted(counts.items())),
        "flags": dict(sorted(flags.items())),
        "party": dict(sorted(parties.items())),
        "party_targets": party_targets,
        "candidate_cap": max(candidate_counts.values()),
        "selected_source_hashes_sha256": sha256_json(sorted(component_hashes)),
    }
    return rows, summary


def feasibility_check() -> dict[str, Any]:
    """Prove the frozen aggregate quotas without emitting the selected IDs."""
    inventory, metadata = build_eligible_inventory()
    metadata["party_available"] = dict(
        sorted(Counter(row["party"] for row in inventory).items())
    )
    clusters_by_hash: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in inventory:
        clusters_by_hash[row["source_hash"]].append(row)
    party_targets = {
        key: min(limit, metadata["party_available"].get(key, 0))
        for key, limit in PARTY_LIMITS.items()
    }
    old_seed = base.SEED
    base.SEED = SEED
    try:
        reserve, pilot, proof = base.exact_joint_witness(
            list(clusters_by_hash.values()),
            {"RG": 0, "RM": 0, "RS": 0, "RZ": 0},
            PARENT_TARGETS,
            FLAG_MINIMA,
            party_targets,
        )
    finally:
        base.SEED = old_seed
    if reserve or proof.get("success") is not True or proof.get("mip_gap") != 0.0:
        raise RuntimeError("aggregate sample feasibility was not proven")
    _, summary = _validate_witness(pilot, metadata)
    return {
        "version": "candidate_endorsement_recall40_sample_round3_feasibility_v1",
        "aggregate_only_no_parent_ids_source_hashes_or_text": True,
        "feasible": True,
        "eligible_parent_count": len(inventory),
        "eligible_component_count": len(clusters_by_hash),
        "selected_parent_count": summary["selected_parent_count"],
        "selected_component_count": summary["selected_component_count"],
        "primary": summary["primary"],
        "flags": summary["flags"],
        "party": summary["party"],
        "party_targets": summary["party_targets"],
        "candidate_cap": summary["candidate_cap"],
        "solver_status": proof["status"],
        "mip_gap": proof["mip_gap"],
    }


def build_selection() -> tuple[dict[str, Any], dict[str, Any]]:
    inventory, metadata = build_eligible_inventory()
    selector_script_sha256 = sha256_file(Path(__file__))
    metadata["party_available"] = dict(
        sorted(Counter(row["party"] for row in inventory).items())
    )
    clusters_by_hash: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in inventory:
        clusters_by_hash[row["source_hash"]].append(row)
    clusters = [clusters_by_hash[key] for key in sorted(clusters_by_hash)]
    party_targets = {
        key: min(limit, metadata["party_available"].get(key, 0))
        for key, limit in PARTY_LIMITS.items()
    }
    reserve, pilot, proof = canonical_sample_witness(
        clusters,
        {"RG": 0, "RM": 0, "RS": 0, "RZ": 0},
        PARENT_TARGETS,
        FLAG_MINIMA,
        party_targets,
    )
    reserve = _rebind_solver_components(reserve, clusters, "reserve")
    pilot = _rebind_solver_components(pilot, clusters, "pilot")
    if reserve:
        raise RuntimeError("selector returned a reserve assignment")
    if proof.get("success") is not True or proof.get("mip_gap") != 0.0:
        raise RuntimeError("canonical selector did not prove an exact optimum")
    assignment_identity = _verify_assignment_proof(
        clusters, reserve, pilot, proof
    )
    if assignment_identity["canonical_assignment_sha256"] != (
        EXPECTED_CANONICAL_ASSIGNMENT_SHA256
    ):
        raise RuntimeError("canonical assignment identity differs")
    if assignment_identity["ordered_component_rank_sha256"] != (
        EXPECTED_ORDERED_COMPONENT_RANK_SHA256
    ):
        raise RuntimeError("ordered component-rank identity differs")
    selected_rows, selected_summary = _validate_witness(pilot, metadata)
    selected_source_hashes = {
        component[0]["source_hash"] for component in pilot
    }
    overlap_counts = _authenticated_overlap_counts(
        selected_source_hashes,
        metadata["authenticated_exclusion_sets"],
    )

    def private_component(component: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "component_rank": canonical_component_rank(component),
            "source_text_sha256": component[0]["source_hash"],
            "parent_document_ids": sorted(row["id"] for row in component),
        }

    private_components = [private_component(component) for component in pilot]
    private_components.sort(key=lambda component: component["component_rank"])
    selection = {
        "version": "candidate_endorsement_private_recall40_sample_round3_selection_v1",
        "authorization_scope": (
            "private_sample_assignment_only_no_source_task_inference_or_release"
        ),
        "sample_round": SAMPLE_ROUND,
        "pipeline_round_three_reused_sample_round": 2,
        "seed": SEED,
        "selection_spec": SELECTION_SPEC,
        "selection_spec_sha256": SELECTION_SPEC_SHA256,
        "selector_script_sha256": selector_script_sha256,
        "source_content_inventory_sha256": metadata[
            "source_content_inventory_sha256"
        ],
        "initial_selection_sha256": INITIAL_SELECTION_SHA256,
        "round2_selection_sha256": ROUND2_SELECTION_SHA256,
        "round3_technical_gate_sha256": ROUND3_TECHNICAL_GATE_SHA256,
        "forbidden_source_hashes_sha256": EXPECTED_FORBIDDEN_SOURCE_HASHES_SHA256,
        "eligible_parent_ids_sha256": EXPECTED_ELIGIBLE_PARENT_IDS_SHA256,
        "eligible_source_hashes_sha256": EXPECTED_ELIGIBLE_SOURCE_HASHES_SHA256,
        "exact_parent_quotas": PARENT_TARGETS,
        "pilot_components": private_components,
        "pilot_parent_count": len(selected_rows),
        "canonical_assignment_sha256": assignment_identity[
            "canonical_assignment_sha256"
        ],
    }
    report = {
        "version": "candidate_endorsement_recall40_sample_round3_selection_report_v1",
        "aggregate_only_no_parent_ids_source_hashes_or_text": True,
        "selection_spec_sha256": SELECTION_SPEC_SHA256,
        "selector_script_sha256": selector_script_sha256,
        "source_content_inventory_sha256": metadata[
            "source_content_inventory_sha256"
        ],
        "eligible_parent_count": metadata["eligible_parent_count"],
        "eligible_component_count": metadata["eligible_component_count"],
        "eligible_structure": metadata["eligible_structure"],
        "year_quartile_boundaries": metadata["year_quartile_boundaries"],
        "exclusion_census": metadata["exclusions"],
        **selected_summary,
        **overlap_counts,
        "sealed_reserve_source_text_reconstructed": False,
        "sealed_reserve_identifiers_emitted": False,
        "solver_proof": proof,
        "authorizations": SELECTION_SPEC["authorization"],
    }
    return selection, report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.selection.exists() or args.report.exists():
        raise ValueError("sample-round-three output already exists")
    selection, report = build_selection()
    args.selection.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.selection.write_text(
        json.dumps(selection, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    args.report.write_text(
        json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
