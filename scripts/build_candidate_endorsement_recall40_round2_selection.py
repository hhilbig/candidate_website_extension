#!/usr/bin/env python3
"""Draw the untouched recall40 round-two sample after the frozen round-one failure."""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import scripts.audit_candidate_endorsement_40_parent_feasibility as base
from src.candidate_endorsement_line_ledger import (
    LINE_DECISION_SLOT_COUNT,
    RECALL40_ROUND2_ROUTE_REVISION_SPEC_SHA256,
    _compound_anchor_expected_names,
    reconstruct_parent_inventory,
)

ROUND = 2
SEED = 20260827 + 1000 * (ROUND - 1)
PILOT_TARGETS = {"Z": 8, "S": 3, "L": 4, "D": 5, "M": 20}
PRIOR = REPO / "build/private_candidate_endorsement_40_parent_selection_20260827/selection.json"
PRIOR_SHA256 = "ef378019b0216a3ef8d1cb7ab9777c6d4f383e383857b3da013802430932202d"
FAILED_RESULTS = REPO / "build/private_candidate_endorsement_recall40_human_truth_20260828/results.json"
FAILED_RESULTS_SHA256 = "0192339258ad40cd8b5b341c16c236e2222c5d014def2a7e4744e09041fb76db"
FAILED_AUDIT = REPO / "quality_reports/candidate_endorsement_recall40_human_truth_failed_2026-09-01.json"
FAILED_AUDIT_SHA256 = "865610fd84ecda77f5c033298ca651c0c2ec016c633971c42d5933c186c74d4d"
SUPPORT = base.SUPPORT
URL = base.URL
BULLET = base.BULLET


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_selection() -> tuple[dict, dict]:
    if sha(PRIOR) != PRIOR_SHA256 or sha(FAILED_RESULTS) != FAILED_RESULTS_SHA256:
        raise ValueError("round-one private artifacts differ")
    if sha(FAILED_AUDIT) != FAILED_AUDIT_SHA256:
        raise ValueError("round-one failed audit differs")
    prior = json.loads(PRIOR.read_text())
    forbidden_hashes = {
        row["source_text_sha256"]
        for key in ("reserve_components", "pilot_components")
        for row in prior[key]
    }
    all_rows, source_inventory_sha = base.authenticated_full_rows()
    rows_by_parent: dict[str, list[dict]] = defaultdict(list)
    for row in all_rows:
        if row["route"] == "endorsements":
            rows_by_parent[row["parent_document_id"]].append(row)
    legacy = {}
    with gzip.open(base.LEGACY, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row["route"] == "endorsements":
                legacy[row["parent_document_id"]] = len(row["records"])
    raw_to_ids: dict[str, set[str]] = defaultdict(set)
    for pid, rows in rows_by_parent.items():
        raw_to_ids[str(rows[0]["raw_text_sha256"])].add(pid)
    excluded, exclusion_provenance = base.exclusions(raw_to_ids)

    inventory = []
    for pid, rows in rows_by_parent.items():
        # Sampling strata stay on the pre-frozen mechanical inventory.  The
        # round-two route revision is applied only after prediction scope is
        # frozen, never used to redefine who enters the sample.
        parent = reconstruct_parent_inventory(rows)
        source_hash = parent["extraction_source_text_sha256"]
        if pid in excluded or source_hash in forbidden_hashes:
            continue
        text = parent["parent_source_text"]
        lines = [str(line["line_text"]) for line in parent["lines"]]
        model_count = sum(bool(line["model_decision_required"]) for line in parent["lines"])
        windows = math.ceil(model_count / LINE_DECISION_SLOT_COUNT) if model_count else 0
        nonempty = [line for line in lines if line.strip()]
        short = [line for line in nonempty if len(line) <= 160]
        no_urls = URL.sub("", text)
        quote = any(len(line) >= 80 and (("“" in line and "”" in line) or line.count('"') >= 2) for line in lines)
        context = any(row.get("endorsement_context_provenance", {}).get("source_scope") == "page_or_section_route_context" for row in rows)
        consecutive = best = 0
        for line in lines:
            consecutive = consecutive + 1 if line.strip() and len(line) <= 160 else 0
            best = max(best, consecutive)
        inventory.append({
            "id": pid, "source_hash": source_hash,
            "candidate": rows[0]["candidate_cycle_id"], "party": base.party(rows[0].get("party")),
            "year": rows[0].get("year"), "chars": len(text), "windows": windows,
            "legacy": legacy[pid], "testimonial": bool(SUPPORT.search(no_urls) or quote),
            "compound": any(_compound_anchor_expected_names(line) is not None for line in lines),
            "context": context, "url": bool(URL.search(text)),
            "list": best >= 5 or sum(bool(BULLET.search(line)) for line in lines) >= 3,
            "density": len(short) / max(1, len(nonempty)),
        })
    # Close over every remaining duplicate-content component.
    clusters: dict[str, list[dict]] = defaultdict(list)
    for row in inventory:
        clusters[row["source_hash"]].append(row)
    chars = sorted(row["chars"] for row in inventory if row["legacy"] > 0 and row["windows"] == 1)
    q25, q75 = chars[math.ceil(.25 * len(chars)) - 1], chars[math.ceil(.75 * len(chars)) - 1]
    years = sorted(int(row["year"]) for row in inventory if row["year"] is not None)
    year_bounds = [years[math.ceil(q * len(years)) - 1] for q in (.25, .5, .75)]
    for row in inventory:
        row["reserve"] = "RZ" if row["legacy"] == 0 else "RM" if row["windows"] >= 2 else "RS"
        row["pilot"] = "Z" if row["legacy"] == 0 else "M" if row["windows"] >= 2 else "S" if row["chars"] <= q25 else "L" if row["chars"] >= q75 else "D"
        year = int(row["year"]) if row["year"] is not None else None
        row["year_quartile"] = "Y0_NULL" if year is None else "Y1" if year <= year_bounds[0] else "Y2" if year <= year_bounds[1] else "Y3" if year <= year_bounds[2] else "Y4"
        row["density_ppm"] = round(1_000_000 * row["density"])
    flags = {"testimonial": 8, "compound": 6, "context": 8, "url": 8, "list": 10}
    parties = {key: min(limit, sum(row["party"] == key for row in inventory)) for key, limit in {"D": 12, "R": 12, "O": 4}.items()}
    availability = {
        "primary": dict(Counter(row["pilot"] for row in inventory)),
        "flags": {key: sum(bool(row[key]) for row in inventory) for key in flags},
        "party": dict(Counter(row["party"] for row in inventory)),
    }
    old_seed = base.SEED
    base.SEED = SEED
    try:
        try:
            reserve, pilot, proof = base.canonical_joint_witness(
                list(clusters.values()), {"RZ": 0, "RM": 0, "RS": 0, "RG": 0},
                PILOT_TARGETS, flags, parties,
            )
        except RuntimeError as error:
            raise RuntimeError(f"{error}; availability={availability}") from error
    finally:
        base.SEED = old_seed
    if reserve or sum(len(rows) for rows in pilot) != 40:
        raise RuntimeError("round-two exact sample closure failed")
    def private_row(rows: list[dict]) -> dict:
        old = base.SEED; base.SEED = SEED
        try:
            rank = base.canonical_component_rank(rows, base.component_objective_vectors(rows))
        finally:
            base.SEED = old
        return {"component_rank": rank, "source_text_sha256": rows[0]["source_hash"], "parent_document_ids": sorted(row["id"] for row in rows)}
    components = [private_row(rows) for rows in pilot]
    components.sort(key=lambda row: row["component_rank"])
    selection = {
        "version": "candidate_endorsement_private_recall40_round2_selection_v1",
        "round": ROUND, "seed": SEED,
        "route_revision_spec_sha256": RECALL40_ROUND2_ROUTE_REVISION_SPEC_SHA256,
        "exact_primary_parent_quotas": PILOT_TARGETS,
        "round1_selection_sha256": PRIOR_SHA256,
        "round1_results_sha256": FAILED_RESULTS_SHA256,
        "round1_failed_audit_sha256": FAILED_AUDIT_SHA256,
        "source_content_inventory_sha256": source_inventory_sha,
        "pilot_components": components,
        "pilot_parent_count": 40,
        "canonical_assignment_sha256": proof["canonical_assignment_sha256"],
        "authorization_scope": "private_prediction_blind_round2_human_task_only",
    }
    witness_rows = [row for rows in pilot for row in rows]
    report = {
        "version": "candidate_endorsement_recall40_round2_selection_report_v1",
        "contains_parent_ids_or_text": False,
        "eligible_parent_count": len(inventory), "eligible_component_count": len(clusters),
        "selected_parent_count": len(witness_rows), "selected_component_count": len(pilot),
        "primary": dict(Counter(row["pilot"] for row in witness_rows)),
        "flags": {key: sum(bool(row[key]) for row in witness_rows) for key in flags},
        "party": dict(Counter(row["party"] for row in witness_rows)),
        "candidate_cap": max(Counter(row["candidate"] for row in witness_rows).values()),
        "round1_and_reserve_component_overlap": len({row["source_hash"] for row in witness_rows} & forbidden_hashes),
        "exclusion_inventory_sha256": hashlib.sha256(json.dumps(exclusion_provenance, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "solver_proof": proof,
    }
    return selection, report


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--selection", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()
    if args.selection.exists() or args.report.exists():
        raise ValueError("round-two output already exists")
    selection, report = build_selection()
    args.selection.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.selection.write_text(json.dumps(selection, sort_keys=True, separators=(",", ":")) + "\n")
    args.report.write_text(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
