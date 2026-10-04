"""Prepare and validate a separate AI review of the current 40-page sample."""

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "build/private_candidate_endorsement_recall40_round4_human_review_20260907b/task.json"
TASK_SHA256 = "c92cb24a4c692f839f009b40f4be898e729db669da4c2e11e25fdec53fd195ea"
OUTPUT = ROOT / "build/private_candidate_endorsement_codex_ai_review_20260914a"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    if path.exists() and path.read_bytes() != data:
        raise ValueError(f"Refuse to overwrite different artifact: {path}")
    path.write_bytes(data)


def load_task():
    data = TASK.read_bytes()
    if digest(data) != TASK_SHA256:
        raise ValueError("Authoritative task hash changed")
    task = json.loads(data)
    if len(task["parents"]) != 40:
        raise ValueError("Expected 40 parents")
    return task


def source_lines(text):
    lines = []
    start = 0
    for number, raw in enumerate(text.splitlines(keepends=True), 1):
        line = raw.rstrip("\r\n")
        lines.append({"line": number, "start": start, "end": start + len(line), "text": line})
        start += len(raw)
    return lines


def prepare():
    task = load_task()
    entries = []
    for index, parent in enumerate(task["parents"], 1):
        source = parent["parent_source_text"]
        if digest(source.encode()) != parent["source_text_sha256"]:
            raise ValueError("Parent source hash differs")
        item = {"parent_index": index, "parent_document_id": parent["parent_document_id"],
                "metadata": parent["metadata"], "source_text": source,
                "source_text_sha256": parent["source_text_sha256"], "lines": source_lines(source)}
        write_json(OUTPUT / "source" / f"parent_{index:02d}.json", item)
        seeded = dict(item)
        seeded["ai_children"] = parent["ai_children"]
        seeded["ai_unresolved_blocks"] = parent["ai_unresolved_blocks"]
        write_json(OUTPUT / "seeded" / f"parent_{index:02d}.json", seeded)
        entries.append({"parent_index": index, "parent_document_id": parent["parent_document_id"],
                        "source_text_sha256": parent["source_text_sha256"], "metadata": parent["metadata"]})
    manifest = {"version": "codex_endorsement_ai_review_inputs_v1", "task_sha256": TASK_SHA256,
                "parent_count": 40, "parents": entries, "review_status": "ai_review_pending",
                "human_reviewed": False, "commercial_model_review_authorized": True}
    write_json(OUTPUT / "manifest.json", manifest)
    print(json.dumps({"parent_count": 40, "source_chars": sum(len(p["parent_source_text"]) for p in task["parents"]),
                      "output_dir": str(OUTPUT)}, indent=2))


def read_parent(index, seeded=False):
    parent = json.loads((OUTPUT / ("seeded" if seeded else "source") / f"parent_{index:02d}.json").read_text())
    original = load_task()["parents"][index - 1]
    if (parent["parent_index"] != index or parent["parent_document_id"] != original["parent_document_id"] or
            parent["metadata"] != original["metadata"] or
            parent["source_text_sha256"] != original["source_text_sha256"] or
            digest(parent["source_text"].encode()) != original["source_text_sha256"] or
            parent["lines"] != source_lines(parent["source_text"])):
        raise ValueError("Prepared source identity or bytes changed")
    return parent


def name_span(parent, line, text=None, occurrence=0):
    """Project an exact name on a one-based source line to parent offsets."""
    item = parent["lines"][line - 1]
    if item["line"] != line:
        raise ValueError("Line identity differs")
    if text is None:
        text = item["text"].strip()
    if not text:
        raise ValueError("Empty name")
    offset = -1
    for _ in range(occurrence + 1):
        offset = item["text"].find(text, offset + 1)
        if offset < 0:
            raise ValueError(f"Name is not exact on line {line}: {text!r}")
    return [item["start"] + offset, item["start"] + offset + len(text)]


def child(parent, kind, line=None, text=None, *, parts=None, support=None):
    """Create a reviewed row from exact line names and support ranges."""
    if parts is None:
        parts = [(line, text)]
    spans = [name_span(parent, *part) for part in parts]
    if support is None:
        support = [(parts[0][0], parts[-1][0])]
    support_spans = [[parent["lines"][a - 1]["start"], parent["lines"][b - 1]["end"]] for a, b in support]
    return {"entity_type": kind, "name_spans": spans, "support_spans": support_spans}


def validate_spans(spans, text, label):
    if not isinstance(spans, list) or not spans:
        raise ValueError(f"Missing {label} spans")
    previous = -1
    for span in spans:
        if (not isinstance(span, list) or len(span) != 2 or
                any(type(n) is not int for n in span) or not 0 <= span[0] < span[1] <= len(text) or
                span[0] < previous or not text[span[0]:span[1]].strip()):
            raise ValueError(f"Invalid {label} span: {span}")
        previous = span[1]


def validate_review(review, parent):
    if digest(parent["source_text"].encode()) != parent["source_text_sha256"]:
        raise ValueError("Source bytes differ from the hash")
    for key in ("parent_index", "parent_document_id", "source_text_sha256"):
        if review.get(key) != parent[key]:
            raise ValueError(f"Review {key} differs")
    if review.get("review_status") != "ai_reviewed" or review.get("human_reviewed") is not False:
        raise ValueError("AI authority flags differ")
    if review.get("complete_source_reviewed") is not True or not review.get("reviewer_id"):
        raise ValueError("Review coverage is incomplete")
    children = review.get("children")
    if not isinstance(children, list) or not isinstance(review.get("uncertainties"), list):
        raise ValueError("Review arrays differ")
    seen = set()
    for row in children:
        if row.get("entity_type") not in {"individual", "organization"}:
            raise ValueError("Invalid entity type")
        validate_spans(row.get("name_spans"), parent["source_text"], "name")
        validate_spans(row.get("support_spans"), parent["source_text"], "support")
        key = identity(row)
        if key in seen:
            raise ValueError("Duplicate name coordinates and type")
        seen.add(key)
    for item in review["uncertainties"]:
        validate_spans(item.get("source_spans"), parent["source_text"], "uncertainty")
        if not item.get("reason"):
            raise ValueError("Uncertainty reason is missing")
    return review


def identity(row):
    return row["entity_type"], tuple(tuple(x) for x in row["name_spans"])


def write_review(index, children, reviewer_id, *, pass_name, uncertainties=None, notes=""):
    if pass_name not in {"blind", "seed_audit", "adjudicated", "adjudicated_v2", "context_audit"}:
        raise ValueError("Unknown review pass")
    parent = read_parent(index)
    value = {key: parent[key] for key in ("parent_index", "parent_document_id", "source_text_sha256")}
    value.update(review_status="ai_reviewed", human_reviewed=False, complete_source_reviewed=True,
                 reviewer_id=reviewer_id, reviewer_model="inherited_primary_codex_model",
                 children=children, uncertainties=uncertainties or [], notes=notes)
    validate_review(value, parent)
    write_json(OUTPUT / pass_name / f"parent_{index:02d}.json", value)
    return value


def compare():
    differences = []
    counts = {"blind_children": 0, "seed_audit_children": 0, "exact_identity_agreements": 0}
    for index in range(1, 41):
        parent = read_parent(index)
        a = validate_review(json.loads((OUTPUT / "blind" / f"parent_{index:02d}.json").read_text()), parent)
        b = validate_review(json.loads((OUTPUT / "seed_audit" / f"parent_{index:02d}.json").read_text()), parent)
        aa = {identity(row): row for row in a["children"]}
        bb = {identity(row): row for row in b["children"]}
        counts["blind_children"] += len(aa)
        counts["seed_audit_children"] += len(bb)
        counts["exact_identity_agreements"] += len(aa.keys() & bb.keys())
        if aa.keys() != bb.keys() or a["uncertainties"] or b["uncertainties"]:
            differences.append({"parent_index": index, "metadata": parent["metadata"],
                                "blind_only": [aa[k] for k in sorted(aa.keys() - bb.keys())],
                                "seed_audit_only": [bb[k] for k in sorted(bb.keys() - aa.keys())],
                                "blind_uncertainties": a["uncertainties"],
                                "seed_audit_uncertainties": b["uncertainties"]})
    report = {"parent_count": 40, "review_status": "two_ai_passes_complete_adjudication_pending",
              "human_reviewed": False, "counts": counts, "differences": differences,
              "interpretation": "Agreement between AI reviewers is not a human-validated accuracy estimate."}
    write_json(OUTPUT / "comparison.json", report)
    print(json.dumps({"counts": counts, "parents_with_disagreements": len(differences)}, indent=2))


def assemble():
    task = load_task()
    revision = 2 if (OUTPUT / "adjudicated_v2").exists() else 1
    responses = []
    inventory = []
    counts = {"original_ai_children": 0, "reviewed_children": 0, "retained_original_identities": 0,
              "added_identities": 0, "removed_identities": 0, "uncertainties": 0,
              "adjudicated_parents": 0, "accepted_children": 0, "uncertain_children": 0}
    for index in range(1, 41):
        parent = read_parent(index)
        passes = {}
        for name in ("blind", "seed_audit", "adjudicated", "adjudicated_v2", "context_audit"):
            path = OUTPUT / name / f"parent_{index:02d}.json"
            if path.exists():
                passes[name] = validate_review(json.loads(path.read_text()), parent)
                inventory.append({"path": str(path.relative_to(ROOT)), "sha256": digest(path.read_bytes())})
        if not {"blind", "seed_audit"} <= passes.keys():
            raise ValueError(f"Parent {index} lacks two full-source reviews")
        blind, seeded = passes["blind"], passes["seed_audit"]
        need_adjudication = ({identity(c) for c in blind["children"]} != {identity(c) for c in seeded["children"]}
                             or blind["uncertainties"] or seeded["uncertainties"])
        if need_adjudication and not {"adjudicated", "adjudicated_v2"} & passes.keys():
            raise ValueError(f"Parent {index} lacks a disagreement decision")
        chosen = passes.get("adjudicated_v2", passes.get("adjudicated", blind))
        if {"adjudicated", "adjudicated_v2"} & passes.keys():
            if not chosen["notes"]:
                raise ValueError("Adjudication reason is missing")
            counts["adjudicated_parents"] += 1
        before = {identity(c) for c in task["parents"][index - 1]["ai_children"]}
        after = {identity(c) for c in chosen["children"]}
        counts["original_ai_children"] += len(before)
        counts["reviewed_children"] += len(after)
        counts["retained_original_identities"] += len(before & after)
        counts["added_identities"] += len(after - before)
        counts["removed_identities"] += len(before - after)
        counts["uncertainties"] += len(chosen["uncertainties"])
        children = []
        for c in chosen["children"]:
            uncertain = any(a < y and x < b for a, b in c["name_spans"]
                            for item in chosen["uncertainties"] for x, y in item["source_spans"])
            children.append({"entity_type": c["entity_type"], "name_spans": c["name_spans"],
                             "support_spans": c["support_spans"], "ai_reviewed": True, "human_reviewed": False,
                             "endorsement_status": "uncertain" if uncertain else "accepted_by_ai_review"})
            counts["uncertain_children" if uncertain else "accepted_children"] += 1
        response = {"parent_index": index, "parent_document_id": parent["parent_document_id"],
                    "source_text_sha256": parent["source_text_sha256"], "metadata": parent["metadata"],
                    "complete_source_ai_reviewed": True, "human_reviewed": False,
                    "reviewer_ids": [r["reviewer_id"] for r in passes.values()],
                    "parent_disposition": "contains_children" if any(c["endorsement_status"] == "accepted_by_ai_review" for c in children) else ("unclear" if children or chosen["uncertainties"] else "true_zero"),
                    "children": children, "uncertainties": chosen["uncertainties"], "adjudication_notes": chosen["notes"]}
        responses.append(response)
    results = {"format": f"candidate-endorsement-codex-ai-review-v{revision}", "review_status": "ai_reviewed",
               "human_reviewed": False, "parent_count": 40, "source_task_sha256": TASK_SHA256,
               "model_provenance": "Commercial Codex review; inherited primary model; exact deployment identifier unavailable to this tool.",
               "authorizations": {"human_truth": False, "production_integration": False, "full_expansion": False, "release": False},
               "counts": counts, "review_file_inventory": inventory, "responses": responses,
               "count_definitions": "Added and removed identities compare exact name coordinates and type. Name-boundary corrections can appear in both counts.",
               "interpretation": "This reference contains AI labels. Comparisons measure AI agreement, not human-validated accuracy."}
    suffix = "_v2" if revision == 2 else ""
    write_json(OUTPUT / f"ai_reviewed_results{suffix}.json", results)
    write_json(OUTPUT / f"ai_review_report{suffix}.json", {k: v for k, v in results.items() if k != "responses"})
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["prepare", "compare", "assemble"])
    args = parser.parse_args()
    {"prepare": prepare, "compare": compare, "assemble": assemble}[args.mode]()
