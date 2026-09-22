"""Aggregate feasibility census for the sealed endorsement reserve and 40-page pilot.

This script never writes or prints parent IDs, source text, or a selection.  Its
constructive witness is held only in memory and is discarded on exit.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import re
import unicodedata
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.build_candidate_endorsement_line_ledger import PILOT_PARENT_IDS
from src.candidate_endorsement_line_ledger import (
    LINE_DECISION_SLOT_COUNT, LINE_INVENTORY_SPEC_SHA256,
    _compound_anchor_expected_names,
    reconstruct_parent_inventory,
)

FULL = REPO / "build/hive_candidate_extraction/v2_1_full_export_20260824p"
LEGACY = REPO / "build/hive_candidate_extraction/full_segmented_imported/full_parent_extractions.jsonl.gz"
VALIDATION = REPO / "build/hive_candidate_extraction/human_validation_completed"
SEED = 20260827
SUPPORT = re.compile(r"\b(?:endorse|endorsed|endorses|support|supported|supports|recommend|recommended|recommends)\b", re.I)
URL = re.compile(r"https?://\S+", re.I)
BULLET = re.compile(r"^\s*(?:[-*•▪◦]|\d+[.)])\s+")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def authenticated_full_rows() -> tuple[list[dict], str]:
    manifest_path = FULL / "manifest.json"
    if sha(manifest_path) != "f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c":
        raise ValueError("full-export manifest bytes differ")
    manifest = json.loads(manifest_path.read_text())
    expected = manifest["chunks"]
    actual_paths = sorted((FULL / "chunks").glob("*.jsonl.gz"))
    if [path.name for path in actual_paths] != [entry["file"] for entry in expected]:
        raise ValueError("full-export chunk inventory differs")
    rows: list[dict] = []
    content_inventory = []
    for path, entry in zip(actual_paths, expected, strict=True):
        if sha(path) != entry["sha256"]:
            raise ValueError(f"full-export chunk bytes differ: {path.name}")
        raw = gzip.decompress(path.read_bytes())
        encoded_lines = raw.split(b"\n")
        if encoded_lines and encoded_lines[-1] == b"":
            encoded_lines.pop()
        chunk_rows = [json.loads(line.decode("utf-8")) for line in encoded_lines]
        if len(chunk_rows) != entry["records"]:
            raise ValueError(f"full-export chunk row count differs: {path.name}")
        if (chunk_rows[0]["document_id"] != entry["first_document_id"] or
                chunk_rows[-1]["document_id"] != entry["last_document_id"]):
            raise ValueError(f"full-export chunk order differs: {path.name}")
        content_inventory.append({"file": path.name, "content_sha256": hashlib.sha256(raw).hexdigest()})
        rows.extend(chunk_rows)
    return rows, hashlib.sha256(json.dumps(
        content_inventory, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()


def rank(parent_id: str, source_hash: str, stratum: str) -> str:
    payload = json.dumps({"parent_document_id": parent_id, "seed": SEED,
                          "source_text_sha256": source_hash, "stratum": stratum},
                         sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def maximum_coordinate_matching(
    human_count: int, model_count: int,
    edges: tuple[tuple, ...],
) -> tuple[tuple[int, int], ...]:
    """Polynomial maximum-cardinality matching with an exact lexicographic optimum."""
    ordered = sorted(edges, key=lambda edge: (edge[2], edge[0], edge[1]))
    for human_index, model_index, *_ in ordered:
        if not (0 <= human_index < human_count and 0 <= model_index < model_count):
            raise ValueError("matching edge index out of range")

    def cardinality(available, blocked_h=frozenset(), blocked_m=frozenset()):
        adjacency: dict[int, list[int]] = defaultdict(list)
        for h, m, *_ in available:
            if h not in blocked_h and m not in blocked_m:
                adjacency[h].append(m)
        matched_model: dict[int, int] = {}
        def augment(h: int, seen: set[int]) -> bool:
            for m in adjacency.get(h, ()):
                if m in seen:
                    continue
                seen.add(m)
                if m not in matched_model or augment(matched_model[m], seen):
                    matched_model[m] = h
                    return True
            return False
        return sum(augment(h, set()) for h in sorted(adjacency))

    optimum = cardinality(ordered)
    selected: list[tuple[int, int]] = []
    blocked_h: set[int] = set()
    blocked_m: set[int] = set()
    for index, edge in enumerate(ordered):
        h, m = edge[:2]
        if len(selected) == optimum:
            break
        if h in blocked_h or m in blocked_m:
            continue
        remainder = ordered[index + 1:]
        if 1 + cardinality(remainder, blocked_h | {h}, blocked_m | {m}) >= optimum - len(selected):
            selected.append((h, m))
            blocked_h.add(h)
            blocked_m.add(m)
    if len(selected) != optimum:
        raise RuntimeError("lexicographic maximum matching construction failed")
    return tuple(sorted(selected))


def construct_recall_edges(humans: list[dict], models: list[dict]):
    """Construct authenticated coordinate/name/support edges used by recall scoring."""
    def norm(value: object) -> str:
        text = unicodedata.normalize("NFKC", str(value))
        text = text.replace("\r\n", " ").replace("\n", " ")
        return re.sub(" +", " ", text).strip().casefold()
    edges = []
    for hi, human in enumerate(humans):
        for mi, model in enumerate(models):
            evidence = tuple(model["record_evidence_parent_span"])
            contains_name = evidence[0] <= human["name_start"] and evidence[1] >= human["name_end"]
            if not contains_name or norm(model["endorser_name"]) != norm(human["endorser_name"]):
                continue
            support = tuple(model["endorsement_context_parent_span"])
            support_covered = (
                (evidence[0] <= human["support_start"] and evidence[1] >= human["support_end"])
                or (support[0] <= human["support_start"] and support[1] >= human["support_end"])
            )
            key = (0 if evidence == (human["name_start"], human["name_end"]) else 1,
                   evidence[1] - evidence[0], model["batch_id"], model["slot"],
                   model["record_index"], human["name_start"], human["name_end"])
            edges.append((hi, mi, key, support_covered))
    return tuple(edges)


def score_recall_matching(humans: list[dict], models: list[dict]) -> dict[str, object]:
    """Construct edges, choose the canonical maximum, and retain support status."""
    edges = construct_recall_edges(humans, models)
    pairs = maximum_coordinate_matching(len(humans), len(models), edges)
    support_by_pair = {(h, m): support for h, m, _, support in edges}
    return {
        "matched_pairs": pairs,
        "matched_count": len(pairs),
        "combined_support_covered_count": sum(support_by_pair[pair] for pair in pairs),
        "all_matched_support_covered": all(support_by_pair[pair] for pair in pairs),
    }


def canonical_component_rank(rows: list[dict], vectors: dict[str, object]) -> str:
    payload = {
        "seed": SEED,
        "ordered_parent_identities": sorted((
            {"parent_document_id": row["id"], "source_text_sha256": row["source_hash"]}
            for row in rows
        ), key=lambda value: (value["parent_document_id"], value["source_text_sha256"])),
        "parent_count_vectors": vectors,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def component_objective_vectors(rows: list[dict]) -> dict[str, object]:
    return {
        "parent_count": len(rows),
        "reserve": dict(sorted(Counter(r["reserve"] for r in rows).items())),
        "pilot": dict(sorted(Counter(r["pilot"] for r in rows).items())),
        "party": dict(sorted(Counter(r["party"] for r in rows).items())),
        "year_quartile": dict(sorted(Counter(r["year_quartile"] for r in rows).items())),
        "candidate_cycle": dict(sorted(Counter(r["candidate"] for r in rows).items())),
        "flags": {key: sum(bool(r[key]) for r in rows)
                  for key in ("testimonial", "compound", "context", "url", "list")},
        "D_density_ppm_sum": sum(r["density_ppm"] for r in rows if r["pilot"] == "D"),
    }


def exact_joint_witness(
    clusters: list[list[dict]], reserve_targets: dict[str, int],
    pilot_targets: dict[str, int], flag_targets: dict[str, int],
    party_targets: dict[str, int],
) -> tuple[list[list[dict]], list[list[dict]], dict[str, object]]:
    """Solve the parent-weighted, whole-component reserve/pilot problem exactly."""
    import numpy as np
    from scipy.optimize import Bounds, LinearConstraint, milp
    import scipy.optimize._highspy._core as highs_core
    n = len(clusters)
    constraints: list[list[float]] = []
    lower: list[float] = []
    upper: list[float] = []

    def add(reserve_values, pilot_values, lo, hi) -> None:
        constraints.append(list(reserve_values) + list(pilot_values))
        lower.append(lo)
        upper.append(hi)

    zeros = [0] * n
    for label, target in reserve_targets.items():
        values = [sum(row["reserve"] == label for row in rows) for rows in clusters]
        add(values, zeros, target, target)
    for label, target in pilot_targets.items():
        values = [sum(row["pilot"] == label for row in rows) for rows in clusters]
        add(zeros, values, target, target)
    for flag, target in flag_targets.items():
        values = [sum(bool(row[flag]) for row in rows) for rows in clusters]
        add(zeros, values, target, np.inf)
    for label, target in party_targets.items():
        values = [sum(row["party"] == label for row in rows) for rows in clusters]
        add(zeros, values, target, np.inf)
    for candidate in sorted({row["candidate"] for rows in clusters for row in rows}):
        values = [sum(row["candidate"] == candidate for row in rows) for rows in clusters]
        add(zeros, values, -np.inf, 2)
    for index in range(n):
        reserve = [0] * n
        pilot = [0] * n
        reserve[index] = 1
        pilot[index] = 1
        add(reserve, pilot, -np.inf, 1)
    matrix = np.asarray(constraints, dtype=float)
    result = milp(
        c=np.zeros(2 * n), integrality=np.ones(2 * n),
        bounds=Bounds(np.zeros(2 * n), np.ones(2 * n)),
        constraints=LinearConstraint(matrix, np.asarray(lower), np.asarray(upper)),
        options={"presolve": True},
    )
    if not result.success or result.x is None:
        raise RuntimeError(f"joint component-vector feasibility failed: {result.message}")
    reserve = [clusters[i] for i in range(n) if result.x[i] > .5]
    pilot = [clusters[i] for i in range(n) if result.x[n + i] > .5]
    proof = {
        "solver": "scipy.optimize.milp_highs",
        "scipy_version": __import__("scipy").__version__,
        "status": int(result.status), "success": bool(result.success),
        "message": str(result.message), "mip_gap": float(result.mip_gap),
    }
    return reserve, pilot, proof


def canonical_joint_witness(
    clusters: list[list[dict]], reserve_targets: dict[str, int],
    pilot_targets: dict[str, int], flag_targets: dict[str, int],
    party_targets: dict[str, int],
) -> tuple[list[list[dict]], list[list[dict]], dict[str, object]]:
    """Exact staged canonical selector; every stage retains prior optima."""
    import numpy as np
    from scipy.optimize import Bounds, LinearConstraint, milp
    import scipy.optimize._highspy._core as highs_core
    n = len(clusters)
    bins = ("Y0_NULL", "Y1", "Y2", "Y3", "Y4")
    z0, m_index, total = 2 * n, 2 * n + len(bins) * 41, 2 * n + len(bins) * 41 + 1
    rows, lows, highs = [], [], []
    def add(values, lo, hi):
        row = np.zeros(total); row[:len(values)] = values
        rows.append(row); lows.append(lo); highs.append(hi)
    zeros = [0] * n
    for label, target in reserve_targets.items():
        add([sum(r["reserve"] == label for r in c) for c in clusters] + zeros, target, target)
    for label, target in pilot_targets.items():
        add(zeros + [sum(r["pilot"] == label for r in c) for c in clusters], target, target)
    for flag, target in flag_targets.items():
        add(zeros + [sum(bool(r[flag]) for r in c) for c in clusters], target, np.inf)
    for label, target in party_targets.items():
        add(zeros + [sum(r["party"] == label for r in c) for c in clusters], target, np.inf)
    for candidate in sorted({r["candidate"] for c in clusters for r in c}):
        add(zeros + [sum(r["candidate"] == candidate for r in c) for c in clusters], -np.inf, 2)
    for i in range(n):
        values = [0] * (2 * n); values[i] = values[n + i] = 1
        add(values, -np.inf, 1)
    for bi, label in enumerate(bins):
        row = np.zeros(total)
        row[n:2*n] = [sum(r["year_quartile"] == label for r in c) for c in clusters]
        row[z0 + bi*41:z0 + (bi+1)*41] = -np.arange(41)
        rows.append(row); lows.append(0); highs.append(0)
        row = np.zeros(total); row[z0 + bi*41:z0 + (bi+1)*41] = 1
        rows.append(row); lows.append(1); highs.append(1)
        row = np.zeros(total); row[z0 + bi*41:z0 + (bi+1)*41] = np.arange(41); row[m_index] = -1
        rows.append(row); lows.append(-np.inf); highs.append(0)
    base = np.asarray(rows); base_low, base_high = np.asarray(lows), np.asarray(highs)
    bounds = Bounds(np.zeros(total), np.concatenate([np.ones(total-1), [40]]))
    integrality = np.ones(total)
    fixed: dict[int, int] = {}
    extra_rows: list[np.ndarray] = []
    extra_vals: list[float] = []
    def solve(c, additions=()):
        er, ev = list(extra_rows), list(extra_vals)
        for index, value in fixed.items():
            row = np.zeros(total); row[index] = 1; er.append(row); ev.append(value)
        for row, value in additions: er.append(row); ev.append(value)
        matrix = np.vstack([base] + er) if er else base
        lo = np.concatenate([base_low, np.asarray(ev)]) if ev else base_low
        hi = np.concatenate([base_high, np.asarray(ev)]) if ev else base_high
        return milp(c=np.asarray(c), integrality=integrality, bounds=bounds,
                    constraints=LinearConstraint(matrix, lo, hi),
                    options={"presolve": True, "mip_rel_gap": 0.0})
    def exact_integer_optimum(result, stage: str) -> int:
        if (not result.success or result.status != 0 or result.mip_gap != 0
                or not float(result.fun).is_integer()):
            raise RuntimeError(f"{stage} lacks an exact integer optimum: {result}")
        return int(result.fun)
    order = sorted(range(n), key=lambda i: canonical_component_rank(
        clusters[i], component_objective_vectors(clusters[i])
    ))
    lex_statuses = Counter()
    def inclusion_decision(result, stage: str) -> int:
        if result.success and result.status == 0:
            lex_statuses[f"{stage}_feasible"] += 1
            return 1
        if not result.success and result.status == 2:
            lex_statuses[f"{stage}_certified_infeasible"] += 1
            return 0
        raise RuntimeError(f"{stage} lex decision was not proven: {result}")
    # Reserve vector has first precedence. Include each earliest component iff a
    # complete feasible reserve+pilot assignment still exists.
    for i in order:
        if sum(len(clusters[j]) for j, value in fixed.items() if j < n and value == 1) == 80:
            fixed[i] = 0; lex_statuses["reserve_quota_forced_zero"] += 1; continue
        trial = dict(fixed); trial[i] = 1
        old = fixed; fixed = trial
        result = solve(np.zeros(total))
        fixed = old
        fixed[i] = inclusion_decision(result, "reserve")
    # Pilot objective 1: minimum maximum year-bin count, including Y0_NULL.
    c = np.zeros(total); c[m_index] = 1
    result = solve(c)
    if not result.success: raise RuntimeError("canonical reserve refinement became infeasible")
    max_year = exact_integer_optimum(result, "pilot_year_bin_max"); year_stage = result
    row = np.zeros(total); row[m_index] = 1
    extra_rows.append(row); extra_vals.append(max_year)
    # Objective 2: exact integer squared deviations from 8 (40 / five bins).
    c = np.zeros(total)
    for bi in range(len(bins)): c[z0 + bi*41:z0 + (bi+1)*41] = [(k-8)**2 for k in range(41)]
    result = solve(c); squared = exact_integer_optimum(result, "pilot_year_squared_deviation"); squared_stage = result
    extra_rows.append(c.copy()); extra_vals.append(squared)
    # Objective 3: maximize the integer-scaled D-stratum density sum.
    c = np.zeros(total); c[n:2*n] = [-sum(r["density_ppm"] for r in comp if r["pilot"] == "D") for comp in clusters]
    result = solve(c); density = -exact_integer_optimum(result, "negative_pilot_D_density"); density_stage = result
    extra_rows.append(c.copy()); extra_vals.append(-density)
    # Objective 4: lexicographically minimum pilot component-rank vector.
    for i in order:
        pi = n + i
        if sum(len(clusters[j-n]) for j, value in fixed.items() if j >= n and value == 1) == 40:
            fixed[pi] = 0; lex_statuses["pilot_quota_forced_zero"] += 1; continue
        trial = dict(fixed); trial[pi] = 1
        old = fixed; fixed = trial; candidate = solve(np.zeros(total)); fixed = old
        fixed[pi] = inclusion_decision(candidate, "pilot")
    result = solve(np.zeros(total))
    if not result.success: raise RuntimeError("canonical lexicographic refinement failed")
    reserve = [clusters[i] for i in range(n) if fixed[i] == 1]
    pilot = [clusters[i] for i in range(n) if fixed[n+i] == 1]
    pilot_rows = [r for component in pilot for r in component]
    observed_year = Counter(r["year_quartile"] for r in pilot_rows)
    if max(observed_year.get(label, 0) for label in bins) != max_year:
        raise RuntimeError("final year-bin maximum differs from frozen optimum")
    if sum((observed_year.get(label, 0) - 8) ** 2 for label in bins) != squared:
        raise RuntimeError("final year squared deviation differs from frozen optimum")
    if sum(r["density_ppm"] for r in pilot_rows if r["pilot"] == "D") != density:
        raise RuntimeError("final D-density differs from frozen optimum")
    stage_proofs = {
        name: {"mip_gap": float(stage.mip_gap), "dual_bound": float(stage.mip_dual_bound),
               "objective": float(stage.fun), "status": int(stage.status)}
        for name, stage in (("year_bin_max", year_stage),
                            ("year_squared_deviation", squared_stage),
                            ("negative_D_density", density_stage))
    }
    proof = {"solver":"scipy.optimize.milp_highs","scipy_version":__import__("scipy").__version__,
             "highs_version": f"{highs_core.HIGHS_VERSION_MAJOR}.{highs_core.HIGHS_VERSION_MINOR}.{highs_core.HIGHS_VERSION_PATCH}",
             "success":True,"mip_gap":float(result.mip_gap),"year_bin_max":max_year,
             "year_squared_deviation":squared,"d_density_ppm_sum":density,
             "reserve_lex_decisions":n,"pilot_lex_decisions":n,
             "lex_solver_status_counts": dict(sorted(lex_statuses.items())),
             "objective_stage_proofs": stage_proofs,
             "ordered_component_rank_sha256":hashlib.sha256(json.dumps([
                 canonical_component_rank(clusters[i], component_objective_vectors(clusters[i])) for i in order
             ],separators=(",",":")).encode()).hexdigest(),
             "canonical_assignment_sha256": hashlib.sha256(json.dumps({
                 "reserve_bits": [fixed[i] for i in order],
                 "pilot_bits": [fixed[n+i] for i in order],
             }, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
             "objective_serialization_sha256": hashlib.sha256(json.dumps({
                 "precedence": ["reserve_rank_vector", "pilot_year_bin_max",
                                "pilot_year_squared_deviation_from_8",
                                "negative_pilot_D_density_ppm_sum", "pilot_rank_vector"],
                 "year_bins": list(bins), "density_scale": 1000000,
             }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    return reserve, pilot, proof


def party(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip().casefold()
    if text in {"democrat", "democratic", "d"}:
        return "D"
    if text in {"republican", "gop", "r"}:
        return "R"
    return "O"


def exclusions(
    raw_hash_to_parent_ids: dict[str, set[str]],
) -> tuple[set[str], list[dict[str, object]]]:
    result = set(PILOT_PARENT_IDS)
    provenance = [{
        "source": "PILOT_PARENT_IDS",
        "value_sha256": hashlib.sha256(json.dumps(
            list(PILOT_PARENT_IDS), separators=(",", ":")
        ).encode()).hexdigest(),
    }]
    for name in ("page_recall_coding.csv", "record_precision_coding.csv"):
        validation_path = VALIDATION / name
        with validation_path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                value = row.get("parent_document_id")
                if value:
                    result.add(value)
        provenance.append({
            "file": str(validation_path.relative_to(REPO)),
            "sha256": sha(validation_path),
        })
    for path in sorted((REPO / "build/hive_candidate_extraction").glob("*/selection.csv")):
        label = path.parent.name.casefold()
        if not any(token in label for token in ("pilot", "preflight", "stress")):
            continue
        count_before = len(result)
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if row.get("route") == "endorsements" and row.get("parent_document_id"):
                    result.add(row["parent_document_id"])
        provenance.append({
            "file": str(path.relative_to(REPO)), "sha256": sha(path),
            "new_parent_ids": len(result) - count_before,
        })
    for directory_name in (
        "pilot_segmented_export", "pilot_segmented_6k_export",
        "pilot_segmented_route_export",
    ):
        directory = REPO / "build/hive_candidate_extraction" / directory_name
        chunk_entries = []
        count_before = len(result)
        for path in sorted((directory / "chunks").glob("*.jsonl.gz")):
            chunk_entries.append({"file": path.name, "sha256": sha(path)})
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    row = json.loads(line)
                    if row.get("route") == "endorsements":
                        result.add(row["parent_document_id"])
        provenance.append({
            "directory": directory_name,
            "manifest_sha256": sha(directory / "manifest.json"),
            "chunk_inventory_sha256": hashlib.sha256(json.dumps(
                chunk_entries, sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest(),
            "new_parent_ids": len(result) - count_before,
        })
    earliest = REPO / "build/hive_candidate_extraction/pilot_export"
    earliest_hashes = set()
    earliest_chunks = []
    for path in sorted((earliest / "chunks").glob("*.jsonl.gz")):
        earliest_chunks.append({"file": path.name, "sha256": sha(path)})
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                if row.get("route") == "endorsements" and row.get("raw_text_sha256"):
                    earliest_hashes.add(row["raw_text_sha256"])
    mapped = set().union(*(raw_hash_to_parent_ids.get(value, set()) for value in earliest_hashes))
    result.update(mapped)
    provenance.append({
        "directory": "pilot_export",
        "manifest_sha256": sha(earliest / "manifest.json"),
        "chunk_inventory_sha256": hashlib.sha256(json.dumps(
            earliest_chunks, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest(),
        "endorsement_raw_text_sha256_count": len(earliest_hashes),
        "mapped_current_parent_count": len(mapped),
    })
    return result, provenance


def main() -> None:
    private_output = None
    if len(sys.argv) == 3 and sys.argv[1] == "--private-selection-output":
        private_output = Path(sys.argv[2])
    elif len(sys.argv) != 1:
        raise SystemExit("usage: audit_candidate_endorsement_40_parent_feasibility.py [--private-selection-output PATH]")
    rows_by_parent: dict[str, list[dict]] = defaultdict(list)
    full_rows, full_content_inventory_sha256 = authenticated_full_rows()
    for row in full_rows:
        if row["route"] == "endorsements":
            rows_by_parent[row["parent_document_id"]].append(row)
    legacy = {}
    with gzip.open(LEGACY, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row["route"] == "endorsements":
                legacy[row["parent_document_id"]] = len(row["records"])
    raw_hash_to_parent_ids: dict[str, set[str]] = defaultdict(set)
    for parent_id, rows in rows_by_parent.items():
        raw_hash_to_parent_ids[str(rows[0]["raw_text_sha256"])].add(parent_id)
    excluded, exclusion_files = exclusions(raw_hash_to_parent_ids)
    inventory = []
    for parent_id, rows in rows_by_parent.items():
        parent = reconstruct_parent_inventory(rows)
        text = parent["parent_source_text"]
        lines = [str(line["line_text"]) for line in parent["lines"]]
        model_count = sum(bool(line["model_decision_required"]) for line in parent["lines"])
        windows = math.ceil(model_count / LINE_DECISION_SLOT_COUNT) if model_count else 0
        short = [line for line in lines if line.strip() and len(line) <= 160]
        quote = any(len(line) >= 80 and (("“" in line and "”" in line) or line.count('"') >= 2) for line in lines)
        no_urls = URL.sub("", text)
        context = any(
            row.get("endorsement_context_provenance", {}).get("source_scope")
            == "page_or_section_route_context" for row in rows
        )
        consecutive = 0
        best = 0
        for line in lines:
            consecutive = consecutive + 1 if line.strip() and len(line) <= 160 else 0
            best = max(best, consecutive)
        inventory.append({
            "id": parent_id, "source_hash": parent["extraction_source_text_sha256"],
            "candidate": rows[0]["candidate_cycle_id"], "party": party(rows[0].get("party")),
            "year": rows[0].get("year"), "chars": len(text), "windows": windows,
            "legacy": legacy[parent_id], "testimonial": bool(SUPPORT.search(no_urls) or quote),
            "compound": any(
                _compound_anchor_expected_names(line) is not None for line in lines
            ), "context": context,
            "url": bool(URL.search(text)), "list": best >= 5 or sum(bool(BULLET.search(line)) for line in lines) >= 3,
            "density": len(short) / max(1, sum(bool(line.strip()) for line in lines)),
        })
    # Close exclusions over source-text duplicate components before computing
    # any eligible-pool boundary.
    all_clusters: dict[str, list[dict]] = defaultdict(list)
    for row in inventory:
        all_clusters[row["source_hash"]].append(row)
    excluded_components = {
        source_hash for source_hash, rows in all_clusters.items()
        if any(row["id"] in excluded for row in rows)
    }
    excluded_parent_count = sum(
        len(all_clusters[key]) for key in excluded_components
    )
    inventory = [
        row for row in inventory if row["source_hash"] not in excluded_components
    ]
    chars = sorted(row["chars"] for row in inventory)
    q25 = chars[math.ceil(.25 * len(chars)) - 1]
    q50 = chars[math.ceil(.50 * len(chars)) - 1]
    q75 = chars[math.ceil(.75 * len(chars)) - 1]
    years = sorted(int(row["year"]) for row in inventory if row["year"] is not None)
    year_boundaries = [years[math.ceil(q * len(years)) - 1] for q in (.25, .50, .75)]
    for row in inventory:
        year = int(row["year"]) if row["year"] is not None else None
        row["year_quartile"] = (
            "Y0_NULL" if year is None else
            "Y1" if year <= year_boundaries[0] else
            "Y2" if year <= year_boundaries[1] else
            "Y3" if year <= year_boundaries[2] else "Y4"
        )
        row["density_ppm"] = round(1_000_000 * row["density"])
    reserve_single_chars = sorted(
        row["chars"] for row in inventory
        if row["legacy"] > 0 and row["windows"] == 1
    )
    reserve_single_median = reserve_single_chars[
        math.ceil(.50 * len(reserve_single_chars)) - 1
    ]
    for row in inventory:
        if row["legacy"] == 0:
            row["reserve"] = "RZ"
        elif row["windows"] >= 2:
            row["reserve"] = "RM"
        elif row["windows"] == 1 and row["chars"] <= reserve_single_median:
            row["reserve"] = "RS"
        else:
            row["reserve"] = "RG"
    # Freeze pilot boundaries before either disjoint component partition is drawn.
    single_chars = sorted(
        row["chars"] for row in inventory
        if row["legacy"] > 0 and row["windows"] == 1
    )
    pilot_q25 = single_chars[math.ceil(.25 * len(single_chars)) - 1]
    pilot_q75 = single_chars[math.ceil(.75 * len(single_chars)) - 1]
    for row in inventory:
        if row["legacy"] == 0:
            row["pilot"] = "Z"
        elif row["windows"] >= 2:
            row["pilot"] = "M"
        elif row["chars"] <= pilot_q25:
            row["pilot"] = "S"
        elif row["chars"] >= pilot_q75:
            row["pilot"] = "L"
        else:
            row["pilot"] = "D"
    # A source-hash cluster is indivisible. Parent IDs are already unique keys.
    clusters: dict[str, list[dict]] = defaultdict(list)
    for row in inventory:
        clusters[row["source_hash"]].append(row)
    reserve_targets = {"RZ": 30, "RM": 30, "RS": 10, "RG": 10}
    targets = {"testimonial": 8, "compound": 6, "context": 8, "url": 8, "list": 10}
    party_targets = {key: min(limit, sum(row["party"] == key for row in inventory))
                     for key, limit in {"D": 12, "R": 12, "O": 4}.items()}
    reserve_components, pilot_components, solver_proof = canonical_joint_witness(
        list(clusters.values()), reserve_targets, {key: 8 for key in "ZSLMD"},
        targets, party_targets,
    )
    reserve_rows = [row for rows in reserve_components for row in rows]
    chosen = [row for rows in pilot_components for row in rows]
    reserve_component_hashes = {rows[0]["source_hash"] for rows in reserve_components}
    pilot_component_hashes = {rows[0]["source_hash"] for rows in pilot_components}
    selected_ids = [row["id"] for row in reserve_rows + chosen]
    selected_component_closure = (
        len(reserve_rows) == 80 and len(chosen) == 40
        and reserve_component_hashes.isdisjoint(pilot_component_hashes)
        and len(selected_ids) == len(set(selected_ids)) == 120
        and all(set(row["id"] for row in clusters[source_hash]) <= set(selected_ids)
                for source_hash in reserve_component_hashes | pilot_component_hashes)
    )
    pilot_candidate_cap_observed = max(Counter(row["candidate"] for row in chosen).values())
    reserve_counts = Counter(row["reserve"] for row in reserve_rows)
    witness = {
        "primary": Counter(row["pilot"] for row in chosen),
        "flags": {flag: sum(row[flag] for row in chosen) for flag in targets},
        "party": Counter(row["party"] for row in chosen),
    }
    passed = (
        reserve_counts == Counter(reserve_targets)
        and witness["primary"] == Counter({key: 8 for key in "ZSLMD"})
        and all(witness["flags"][key] >= value for key, value in targets.items())
        and all(witness["party"][key] >= value for key, value in party_targets.items())
        and selected_component_closure and pilot_candidate_cap_observed <= 2
    )
    report = {
        "version": "candidate_endorsement_40_parent_feasibility_v1",
        "aggregate_only_no_ids_or_text": True, "joint_constructive_witness_discarded": True,
        "feasible": passed, "eligible_parent_count": len(inventory),
        "full_endorsement_parent_count_before_exclusions": len(rows_by_parent),
        "duplicate_component_count": len(clusters),
        "explicit_excluded_parent_id_count": len(excluded),
        "excluded_parent_count_after_component_closure": excluded_parent_count,
        "excluded_parent_share_after_component_closure": round(
            excluded_parent_count / len(rows_by_parent), 9
        ),
        "exclusion_file_count": len(exclusion_files),
        "exclusion_inventory_sha256": hashlib.sha256(json.dumps({
            "provenance": exclusion_files,
            "sorted_explicit_parent_ids_sha256": hashlib.sha256(json.dumps(
                sorted(excluded), separators=(",", ":")
            ).encode()).hexdigest(),
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "quartiles_parent_chars": {"q25": q25, "q50": q50, "q75": q75},
        "year_quartile_boundaries": year_boundaries,
        "density_integer_scale": 1000000,
        "reserve_single_window_char_median": reserve_single_median,
        "pilot_single_window_char_boundaries": {"q25": pilot_q25, "q75": pilot_q75},
        "reserve_available": dict(Counter(row["reserve"] for row in inventory)),
        "reserve_witness": dict(reserve_counts),
        "pilot_available_before_joint_partition": dict(Counter(row["pilot"] for row in inventory)),
        "pilot_joint_witness": {"primary": dict(witness["primary"]), "flags": witness["flags"], "party": dict(witness["party"])},
        "selected_parent_totals": {"reserve": len(reserve_rows), "pilot": len(chosen)},
        "selected_component_closure_and_disjointness": selected_component_closure,
        "pilot_candidate_cycle_parent_cap_observed": pilot_candidate_cap_observed,
        "joint_component_solver_proof": solver_proof,
        "party_targets": party_targets, "source_manifest_sha256": sha(FULL / "manifest.json"),
        "source_chunk_content_inventory_sha256": full_content_inventory_sha256,
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_ledger_implementation_sha256": sha(REPO / "src/candidate_endorsement_line_ledger.py"),
        "selection_spec_sha256": sha(REPO / "quality_reports/candidate_endorsement_40_parent_recall_pilot_selection_spec_2026-08-26.json"),
        "selector_script_sha256": sha(Path(__file__)),
        "python_version": sys.version.split()[0],
        "canonical_json_serializer": "json.dumps(sort_keys=True,separators=(',',':'),ensure_ascii=True)",
        "legacy_parent_output_sha256": sha(LEGACY),
    }
    if not passed:
        raise RuntimeError(json.dumps(report, sort_keys=True))
    if private_output is not None:
        def private_rows(components):
            return [{
                "component_rank": canonical_component_rank(
                    rows, component_objective_vectors(rows)
                ),
                "source_text_sha256": rows[0]["source_hash"],
                "parent_document_ids": sorted(row["id"] for row in rows),
            } for rows in sorted(components, key=lambda values: canonical_component_rank(
                values, component_objective_vectors(values)
            ))]
        private_payload = {
            "version": "candidate_endorsement_private_reserve80_pilot40_v1",
            "authorization_scope": "private_id_assignment_only_no_source_tasks_or_inference",
            "selector_script_sha256": sha(Path(__file__)),
            "selection_spec_sha256": report["selection_spec_sha256"],
            "aggregate_feasibility_report_sha256": sha(
                REPO / "quality_reports/candidate_endorsement_40_parent_recall_pilot_feasibility_2026-08-26.json"
            ),
            "canonical_assignment_sha256": solver_proof["canonical_assignment_sha256"],
            "reserve_components": private_rows(reserve_components),
            "pilot_components": private_rows(pilot_components),
        }
        private_output.parent.mkdir(parents=True, exist_ok=False)
        private_output.write_text(json.dumps(
            private_payload, sort_keys=True, separators=(",", ":")
        ) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
