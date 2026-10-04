"""Compare existing extraction occurrences with the provisional Codex review."""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import review_candidate_endorsement_codex as review
from scripts.evaluate_candidate_endorsement_round4_regression import strict_json
from src.candidate_endorsement_round4 import normalize_name_for_match

REFERENCE = review.OUTPUT / "ai_reviewed_results_v2.json"
REFERENCE_SHA256 = "56abd8a8f34f2ab3f13498b838792f714784430747a7405d08810e6ca4887704"
OUTPUT = ROOT / "build/private_candidate_endorsement_codex_diagnostic_20260914a"
CONTEXT_INPUT = ROOT / "build/hive_candidate_extraction/v2_1_endorsement_recall40_codex_context_stage_b_20260914a"
CONTEXT_MANIFEST_SHA256 = "e5fd86f9a5353202d71318439497ee001e00962525ced55be1dda67df4ddbe92"
CONTEXT_OUTPUT = ROOT / "build/private_candidate_endorsement_codex_context_evaluation_20260914a"


def overlap(left, right):
    return any(a < d and c < b for a, b in left for c, d in right)


def name(source, row):
    return " ".join(source[a:b] for a, b in row["name_spans"])


def normalized_identity(source, row):
    return row["entity_type"], normalize_name_for_match(name(source, row))


def validate_reference(task, reference, task_hash):
    if (reference.get("source_task_sha256") != task_hash or
            reference.get("review_status") != "ai_reviewed" or
            reference.get("human_reviewed") is not False or
            reference.get("parent_count") != len(task["parents"]) or
            any(reference.get("authorizations", {}).values())):
        raise ValueError("Reference source or authority differs")
    responses = reference.get("responses")
    if not isinstance(responses, list) or len(responses) != len(task["parents"]):
        raise ValueError("Reference parent closure differs")
    seen = set()
    for original, response in zip(task["parents"], responses, strict=True):
        source = original["parent_source_text"]
        if (response.get("parent_document_id") != original["parent_document_id"] or
                response["parent_document_id"] in seen or
                response.get("metadata") != original["metadata"] or
                response.get("source_text_sha256") != review.digest(source.encode()) or
                response.get("human_reviewed") is not False or
                response.get("complete_source_ai_reviewed") is not True):
            raise ValueError("Reference parent identity or coverage differs")
        seen.add(response["parent_document_id"])
        identities = set()
        for c in response["children"]:
            if (c.get("entity_type") not in {"individual", "organization"} or
                    c.get("human_reviewed") is not False or c.get("ai_reviewed") is not True or
                    c.get("endorsement_status") not in {"accepted_by_ai_review", "uncertain"}):
                raise ValueError("Reference child authority differs")
            review.validate_spans(c["name_spans"], source, "name")
            review.validate_spans(c["support_spans"], source, "support")
            if review.identity(c) in identities:
                raise ValueError("Duplicate reference occurrence")
            identities.add(review.identity(c))
        for issue in response["uncertainties"]:
            review.validate_spans(issue["source_spans"], source, "uncertainty")
    expected = reference["counts"]
    children = [c for p in responses for c in p["children"]]
    if (expected["reviewed_children"] != len(children) or
            expected["accepted_children"] != sum(c["endorsement_status"] == "accepted_by_ai_review" for c in children) or
            expected["uncertain_children"] != sum(c["endorsement_status"] == "uncertain" for c in children)):
        raise ValueError("Reference counts differ")


def multiset_comparison(source, predictions, reference, *, coordinates):
    key = review.identity if coordinates else lambda row: normalized_identity(source, row)
    pred = Counter(key(c) for c in predictions)
    accepted = Counter(key(c) for c in reference if c["endorsement_status"] == "accepted_by_ai_review")
    uncertain = Counter(key(c) for c in reference if c["endorsement_status"] == "uncertain")
    accepted_matched = pred & accepted
    remainder = pred - accepted_matched
    uncertain_matched = remainder & uncertain
    unmatched = remainder - uncertain_matched
    a, u, extra = map(lambda c: sum(c.values()), (accepted_matched, uncertain_matched, unmatched))
    accepted_n, uncertain_n = sum(accepted.values()), sum(uncertain.values())
    prediction_n = len(predictions)
    return {
        "prediction_count": prediction_n,
        "ai_accepted_reference_count": accepted_n,
        "ai_uncertain_reference_count": uncertain_n,
        "prediction_matches_ai_accepted": a,
        "prediction_matches_ai_uncertain": u,
        "prediction_without_ai_reference": extra,
        "ai_accepted_reference_not_matched": accepted_n - a,
        "ai_uncertain_reference_not_matched": uncertain_n - u,
        "accepted_reference_match_rate": a / accepted_n if accepted_n else None,
        "all_provisional_reference_match_rate": (a + u) / (accepted_n + uncertain_n) if accepted_n + uncertain_n else None,
        "prediction_match_rate_all_provisional": (a + u) / prediction_n if prediction_n else None,
    }


def enrich(source, row):
    return {**row, "printed_name": name(source, row),
            "support_text": [source[a:b] for a, b in row["support_spans"]]}


def disagreement(source, row, other_rows, blocks):
    same_coordinates = [c for c in other_rows if c["name_spans"] == row["name_spans"]]
    overlaps = [c for c in other_rows if overlap(c["name_spans"], row["name_spans"])]
    repeated = [c for c in other_rows if normalized_identity(source, c) == normalized_identity(source, row)]
    if same_coordinates:
        relation = "same_coordinates_different_type"
    elif overlaps:
        relation = "overlapping_name_boundaries_or_compound_identity"
    elif repeated:
        relation = "same_normalized_name_at_different_coordinates"
    else:
        relation = "no_name_candidate_in_other_output"
    return {**enrich(source, row), "comparison_relation": relation,
            "other_output_candidates": [enrich(source, c) for c in same_coordinates or overlaps or repeated],
            "overlapping_unresolved_extraction_block_ids": [b["unresolved_block_id"] for b in blocks if overlap(row["name_spans"], [b["block_span"]])]}


def build_diagnostic(task, reference, task_hash):
    validate_reference(task, reference, task_hash)
    parents = []
    aggregate = {"exact_name_coordinates_and_type": Counter(), "normalized_name_and_type_multiset": Counter()}
    relation_counts = Counter()
    for index, (original, response) in enumerate(zip(task["parents"], reference["responses"], strict=True), 1):
        source = original["parent_source_text"]
        predictions, expected = original["ai_children"], response["children"]
        for c in predictions:
            review.validate_spans(c["name_spans"], source, "prediction name")
            review.validate_spans(c["support_spans"], source, "prediction support")
        predicted_keys = {review.identity(c) for c in predictions}
        expected_keys = {review.identity(c) for c in expected}
        if len(predicted_keys) != len(predictions):
            raise ValueError("Duplicate prediction occurrence")
        metrics = {}
        for label, coordinates in (("exact_name_coordinates_and_type", True), ("normalized_name_and_type_multiset", False)):
            m = multiset_comparison(source, predictions, expected, coordinates=coordinates)
            metrics[label] = m
            aggregate[label].update({k: v for k, v in m.items() if type(v) is int})
        missing = [disagreement(source, c, predictions, original["ai_unresolved_blocks"]) for c in expected if review.identity(c) not in predicted_keys]
        extra = [disagreement(source, c, expected, original["ai_unresolved_blocks"]) for c in predictions if review.identity(c) not in expected_keys]
        relation_counts.update("reference_only:" + c["comparison_relation"] for c in missing)
        relation_counts.update("prediction_only:" + c["comparison_relation"] for c in extra)
        parents.append({"parent_index": index, "parent_document_id": original["parent_document_id"],
                        "metadata": original["metadata"], "source_text_sha256": original["source_text_sha256"],
                        "metrics": metrics, "reference_only": missing, "prediction_only": extra,
                        "ai_uncertainties": response["uncertainties"],
                        "unresolved_extraction_blocks": original["ai_unresolved_blocks"]})
    totals = {}
    for label, counter in aggregate.items():
        m = dict(counter)
        a, u = m["prediction_matches_ai_accepted"], m["prediction_matches_ai_uncertain"]
        accepted = m["ai_accepted_reference_count"]
        all_reference = accepted + m["ai_uncertain_reference_count"]
        m.update(accepted_reference_match_rate=a / accepted if accepted else None,
                 all_provisional_reference_match_rate=(a + u) / all_reference if all_reference else None,
                 prediction_match_rate_all_provisional=(a + u) / m["prediction_count"] if m["prediction_count"] else None)
        assert a + u + m["prediction_without_ai_reference"] == m["prediction_count"]
        totals[label] = m
    report = {"version": "candidate_endorsement_codex_diagnostic_v1", "parent_count": len(parents),
              "source_task_sha256": task_hash, "reference_is_ai_not_human_truth": True,
              "human_reviewed": False, "authorizations": reference["authorizations"],
              "comparison": totals, "relation_counts": dict(sorted(relation_counts.items())),
              "unresolved_extraction_block_count": sum(len(p["unresolved_extraction_blocks"]) for p in parents),
              "interpretation": "Match rates measure agreement with provisional AI labels. Uncertain references are separate; unmatched predictions are not established false endorsements.",
              "definitions": {"exact": "One occurrence matches only when every name coordinate and entity type agree.",
                              "normalized": "Names use the existing pipeline normalization. Counts remain occurrence multisets within each parent.",
                              "accepted_priority": "Predictions first match accepted references, then uncertain references. Each prediction counts once."}}
    return report, {"version": "candidate_endorsement_codex_private_disagreements_v1", "parents": parents}


def trace_frozen_run(task, ledger):
    from scripts import build_candidate_endorsement_round4_development_inputs as builder
    from scripts import build_candidate_endorsement_round4_human_review_app as app

    raw = ROOT / "build/hive_candidate_extraction"
    provenance = task["input_provenance"]
    a = raw / "v2_1_endorsement_recall40_sample_round3_round4_stage_a_20260907a"
    ao = raw / "v2_1_endorsement_recall40_sample_round3_round4_stage_a_outputs_20260907a"
    b = raw / "v2_1_endorsement_recall40_sample_round3_round4_stage_b_20260907d"
    bo = raw / "v2_1_endorsement_recall40_sample_round3_round4_stage_b_outputs_20260907d"
    ab, ar, authenticated, ap = builder.authenticate_fresh_stage_a_run(
        a, ao, expected_input_manifest_sha256=provenance["stage_a_input_manifest_sha256"],
        expected_output_inventory_sha256=provenance["stage_a_output_inventory_sha256"],
    )
    bb, br, _manifest, bp = app.authenticate_fresh_stage_b_run(
        b, bo, expected_input_manifest_sha256=provenance["stage_b_input_manifest_sha256"],
        expected_output_inventory_sha256=provenance["stage_b_output_inventory_sha256"],
        stage_a_input_manifest_sha256=provenance["stage_a_input_manifest_sha256"],
        stage_a_output_provenance=ap, selected_parent_ids=[p["parent_document_id"] for p in task["parents"]],
    )
    roles = {t["source_line_id"]: out["model_output"][t["slot"]]["role"]
             for batch, out in zip(ab, ar, strict=True) for t in batch["targets"]}
    lines = {p["parent_document_id"]: p["lines"] for p in authenticated["parents"]}
    source_by_id = {p["parent_document_id"]: p["parent_source_text"] for p in authenticated["parents"]}
    for parent in task["parents"]:
        if source_by_id[parent["parent_document_id"]] != parent["parent_source_text"]:
            raise ValueError("Trace source differs from the authoritative task")
    targets = {}
    for batch, out in zip(bb, br, strict=True):
        for t in batch["targets"]:
            targets.setdefault(batch["parent_document_id"], []).append((t, out.get("model_output", {}).get(t["slot"]), batch["batch_id"]))
    traces, counts = [], Counter()
    for parent in ledger["parents"]:
        parent_id = parent["parent_document_id"]
        for row in parent["reference_only"]:
            touched = [line for line in lines[parent_id] if overlap(row["name_spans"], [[line["parent_start_char"], line["parent_end_char"]]])]
            related = []
            for target, decision, batch_id in targets.get(parent_id, []):
                block = target["record_block_lines"]
                span = [block[0]["parent_start_char"], block[-1]["parent_full_end_char"]]
                if overlap(row["name_spans"], [span]):
                    related.append({"batch_id": batch_id, "slot": target["slot"], "block_span": span,
                                    "raw_disposition": decision.get("disposition") if isinstance(decision, dict) else None})
            line_roles = [{"parent_line_index": line["parent_line_index"], "line_text": line["line_text"],
                           "stage_a_role": roles.get(line["source_line_id"], "deterministic_outside_scope")}
                          for line in touched]
            if row["comparison_relation"] == "overlapping_name_boundaries_or_compound_identity":
                category = "name_boundary_or_compound_difference"
            elif row["overlapping_unresolved_extraction_block_ids"]:
                category = "nonrepairable_stage_b_target"
            elif any(t["raw_disposition"] == "not_endorsement" for t in related):
                category = "stage_b_rejected_target"
            elif any(t["raw_disposition"] == "unclear" for t in related):
                category = "stage_b_unclear_target"
            elif not related:
                category = "no_stage_b_target_at_name_coordinates"
            else:
                category = "target_did_not_return_this_occurrence"
            counts[row["endorsement_status"] + ":" + category] += 1
            traces.append({"parent_index": parent["parent_index"], "parent_document_id": parent_id,
                           "metadata": parent["metadata"], "printed_name": row["printed_name"],
                           "name_spans": row["name_spans"], "endorsement_status": row["endorsement_status"],
                           "diagnostic_category": category, "source_line_roles": line_roles, "related_stage_b_targets": related})
    return {"version": "candidate_endorsement_codex_source_trace_v1", "parent_count": 40,
            "stage_a_batch_count": len(ab), "stage_b_batch_count": len(bb),
            "source_task_sha256": review.TASK_SHA256, "stage_a_output_inventory_sha256": ap["output_inventory_sha256"],
            "stage_b_output_inventory_sha256": bp["output_inventory_sha256"], "human_reviewed": False,
            "counts": dict(sorted(counts.items())), "reference_only_traces": traces,
            "interpretation": "Categories locate AI disagreements in the saved pipeline. They do not establish human errors or authorize semantic repairs."}


def require_same_sources(original, candidate):
    """Require identical pages and no human confirmations before comparison."""
    if len(original["parents"]) != len(candidate["parents"]):
        raise ValueError("Rerun parent closure differs")
    for old, new in zip(original["parents"], candidate["parents"], strict=True):
        for field in ("parent_document_id", "parent_source_text", "source_text_sha256",
                      "source_n_chars", "metadata"):
            if old[field] != new[field]:
                raise ValueError(f"Rerun source differs: {field}")
        if new["human_parent_confirmed"] is not False:
            raise ValueError("Rerun parent has human confirmation")
        if any(c["human_confirmed"] is not False for c in new["ai_children"]):
            raise ValueError("Rerun child has human confirmation")
        if any(b["human_reviewed"] is not False for b in new["ai_unresolved_blocks"]):
            raise ValueError("Rerun block has human confirmation")


def load_run_task(original, stage_b_input_dir, stage_b_output_dir,
                  expected_manifest_sha256, expected_inventory_sha256):
    from scripts import build_candidate_endorsement_round4_human_review_app as app

    if (not isinstance(expected_inventory_sha256, str)
            or len(expected_inventory_sha256) != 64
            or any(c not in "0123456789abcdef" for c in expected_inventory_sha256)):
        raise ValueError("An independently recorded output inventory hash is required")
    raw = ROOT / "build/hive_candidate_extraction"
    provenance = original["input_provenance"]
    draft, conversion, parents, manifest = app.prepare_provisional_draft_from_authenticated_runs(
        stage_a_input_dir=raw / "v2_1_endorsement_recall40_sample_round3_round4_stage_a_20260907a",
        stage_a_output_dir=raw / "v2_1_endorsement_recall40_sample_round3_round4_stage_a_outputs_20260907a",
        expected_stage_a_input_manifest_sha256=provenance["stage_a_input_manifest_sha256"],
        expected_stage_a_output_inventory_sha256=provenance["stage_a_output_inventory_sha256"],
        stage_b_input_dir=stage_b_input_dir,
        stage_b_output_dir=stage_b_output_dir,
        expected_stage_b_input_manifest_sha256=expected_manifest_sha256,
        expected_stage_b_output_inventory_sha256=expected_inventory_sha256,
    )
    task = app.build_task(parents=parents, stage_a_manifest=manifest,
        stage_a_manifest_sha256=provenance["stage_a_input_manifest_sha256"],
        draft=draft, draft_sha256=app.sha256_bytes(app.canonical_bytes(draft)),
        task_id="candidate_endorsement_authenticated_comparison_20260914a",
        task_revision="candidate_endorsement_authenticated_comparison_v1_20260914a")
    require_same_sources(original, task)
    return task, draft, conversion


def paired_change_summary(baseline, candidate):
    """Compare both runs against the same provisional reference denominators."""
    changes = {}
    for label, old in baseline["comparison"].items():
        new = candidate["comparison"][label]
        for key in ("ai_accepted_reference_count", "ai_uncertain_reference_count"):
            if old[key] != new[key]:
                raise ValueError("Paired reference denominator differs")
        changes[label] = {key: new[key] - value for key, value in old.items()
                          if type(value) is int and key not in {
                              "ai_accepted_reference_count", "ai_uncertain_reference_count"}}
    return {"version": "candidate_endorsement_paired_ai_comparison_v1",
            "baseline": baseline["comparison"], "candidate": candidate["comparison"],
            "candidate_minus_baseline_counts": changes,
            "baseline_unresolved_blocks": baseline["unresolved_extraction_block_count"],
            "candidate_unresolved_blocks": candidate["unresolved_extraction_block_count"],
            "reference_is_ai_not_human_truth": True, "human_reviewed": False,
            "interpretation": "Count differences describe the same pages against unchanged AI labels. They do not establish human accuracy or authorize expansion."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--trace-existing-run", action="store_true")
    parser.add_argument("--context-run-output-dir", type=Path)
    parser.add_argument("--expected-output-inventory-sha256")
    args = parser.parse_args()
    if args.context_run_output_dir and (args.trace_existing_run or not args.expected_output_inventory_sha256):
        parser.error("Context evaluation requires an inventory hash and cannot trace the old run")
    if args.expected_output_inventory_sha256 and not args.context_run_output_dir:
        parser.error("An output inventory hash requires a context run")
    output_dir = args.output_dir or (CONTEXT_OUTPUT if args.context_run_output_dir else OUTPUT)
    if review.digest(REFERENCE.read_bytes()) != REFERENCE_SHA256:
        raise ValueError("Pinned Codex reference changed")
    task = review.load_task()
    reference = strict_json(REFERENCE)
    for entry in reference["review_file_inventory"]:
        if review.digest((ROOT / entry["path"]).read_bytes()) != entry["sha256"]:
            raise ValueError("Review inventory hash differs")
    original = task
    if args.context_run_output_dir:
        task, draft, conversion = load_run_task(original, CONTEXT_INPUT,
            args.context_run_output_dir, CONTEXT_MANIFEST_SHA256,
            args.expected_output_inventory_sha256)
    report, ledger = build_diagnostic(task, reference, review.TASK_SHA256)
    report["reference_sha256"] = REFERENCE_SHA256
    report["extraction_provenance"] = task["input_provenance"]
    review.write_json(output_dir / "disagreements.json", ledger)
    report["disagreement_file_sha256"] = review.digest((output_dir / "disagreements.json").read_bytes())
    review.write_json(output_dir / "report.json", report)
    if args.context_run_output_dir:
        baseline, _ = build_diagnostic(original, reference, review.TASK_SHA256)
        paired = paired_change_summary(baseline, report)
        paired["source_task_sha256"] = review.TASK_SHA256
        paired["reference_sha256"] = REFERENCE_SHA256
        paired["candidate_provenance"] = draft["provenance"]
        paired["candidate_conversion"] = conversion
        review.write_json(output_dir / "paired_comparison.json", paired)
        review.write_json(output_dir / "provisional_draft.json", draft)
    if args.trace_existing_run:
        trace = trace_frozen_run(task, ledger)
        review.write_json(output_dir / "source_trace.json", trace)
        print(json.dumps(trace["counts"], indent=2))
    print(json.dumps(report["comparison"], indent=2))


if __name__ == "__main__":
    main()
