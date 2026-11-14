#!/usr/bin/env python3
"""Audit one-stage extraction output for the two known contract defects.

The full one-stage run was attempted twice and cancelled both times. Neither
attempt crashed. Both were stopped because a rolling audit found a contract
defect in output that otherwise looked healthy:

* Job 21250200 wrote literal TAB and U+0003 into JSON string values, so strict
  parsing failed. A display codec and a JSON-string control canonicalizer fixed
  it, and the runner now reports what it canonicalized.
* Job 21296223 hit structured-decoder whitespace degeneracy: one row emitted
  23,268 grammar-permitted TABs outside the JSON string until it reached the
  output cap. `disable_any_whitespace` fixed it.

Both show up as a `finish_reason` of `length`, or as a non-`parsed`
`parse_status`, or in the canonicalizer counters. This audit reads those
signals directly so the same defects cannot pass unnoticed again.

It reads published output only. It makes no request and loads no model.
"""
from __future__ import annotations

import argparse
import collections
import glob
import gzip
import json
from pathlib import Path


def read_rows(directory: Path):
    for path in sorted(glob.glob(str(directory / "*.out.jsonl.gz"))):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield path, json.loads(line)


def repeated_record_counts(extraction) -> int:
    """Count items repeated exactly within one model extraction.

    The suffix-p contract added a repetition guard after a run emitted 299
    identical copies of one record. An exact repeat is the signal.
    """
    if not isinstance(extraction, dict):
        return 0
    worst = 0
    for value in extraction.values():
        if not isinstance(value, list):
            continue
        seen = collections.Counter(
            json.dumps(item, sort_keys=True, ensure_ascii=False)
            for item in value
            if isinstance(item, (dict, list, str, int, float))
        )
        if seen:
            worst = max(worst, max(seen.values()))
    return worst


def audit(directory: Path, repetition_threshold: int = 3) -> dict:
    parse_status: collections.Counter = collections.Counter()
    finish_reason: collections.Counter = collections.Counter()
    routes: collections.Counter = collections.Counter()
    rows = 0
    truncated = []
    unparsed = []
    unrepaired_controls = []
    invalid_outside_string = []
    repeats = []
    canonicalized_rows = 0
    min_margin = None
    max_output_ratio = 0.0

    for path, row in read_rows(directory):
        rows += 1
        status = str(row.get("parse_status"))
        reason = str(row.get("finish_reason"))
        parse_status[status] += 1
        finish_reason[reason] += 1
        routes[str(row.get("route"))] += 1
        ident = {
            "document_id": row.get("document_id"),
            "route": row.get("route"),
            "chunk": Path(path).name,
        }
        if reason == "length":
            truncated.append(ident)
        if status != "parsed":
            unparsed.append(dict(ident, parse_status=status))
        if int(row.get("json_control_canonicalization_count") or 0):
            canonicalized_rows += 1
        if int(row.get("json_control_unrepaired_after_escape_count") or 0):
            unrepaired_controls.append(ident)
        if int(row.get("json_control_outside_string_invalid_count") or 0):
            invalid_outside_string.append(ident)
        worst = repeated_record_counts(row.get("model_extraction"))
        if worst >= repetition_threshold:
            repeats.append(dict(ident, max_identical_items=worst))
        used = int(row.get("output_tokens") or 0)
        cap = int(row.get("max_output_tokens") or 0)
        if cap:
            max_output_ratio = max(max_output_ratio, used / cap)

    clean = not (truncated or unparsed or unrepaired_controls
                 or invalid_outside_string or repeats)
    return {
        "version": "candidate_extraction_one_stage_output_audit_v1",
        "directory": str(directory),
        "network_requests": 0,
        "rows": rows,
        "routes": dict(sorted(routes.items())),
        "parse_status_counts": dict(sorted(parse_status.items())),
        "finish_reason_counts": dict(sorted(finish_reason.items())),
        "highest_output_token_use_fraction_of_cap": round(max_output_ratio, 4),
        "rows_with_canonicalized_controls": canonicalized_rows,
        "defects": {
            "output_truncated_finish_reason_length": truncated[:50],
            "parse_status_not_parsed": unparsed[:50],
            "control_characters_unrepaired_in_strings": unrepaired_controls[:50],
            "invalid_control_characters_outside_strings": invalid_outside_string[:50],
            "repeated_identical_items": repeats[:50],
        },
        "defect_counts": {
            "output_truncated_finish_reason_length": len(truncated),
            "parse_status_not_parsed": len(unparsed),
            "control_characters_unrepaired_in_strings": len(unrepaired_controls),
            "invalid_control_characters_outside_strings": len(invalid_outside_string),
            "repeated_identical_items": len(repeats),
        },
        "clean": clean,
        "interpretation": (
            "Canonicalized control characters are expected and healthy: the runner "
            "repairs them by design and records the count. Unrepaired controls, a "
            "finish reason of length, a parse status other than parsed, or exact "
            "item repetition are the signals that stopped the two earlier runs."),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--repetition-threshold", type=int, default=3)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = audit(args.output_dir, args.repetition_threshold)
    text = json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result["clean"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
