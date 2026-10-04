#!/usr/bin/env python3
"""Derive the private pilot40-only assignment from the reviewed sealed ledger."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

EXPECTED_SOURCE_SHA256 = "ef378019b0216a3ef8d1cb7ab9777c6d4f383e383857b3da013802430932202d"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: build_candidate_endorsement_40_parent_pilot_selection.py SOURCE OUTPUT")
    source, output = map(Path, sys.argv[1:])
    if sha(source) != EXPECTED_SOURCE_SHA256 or output.exists():
        raise ValueError("sealed selection differs or output already exists")
    data = json.loads(source.read_text(encoding="utf-8"))
    pilot = data["pilot_components"]
    payload = {
        "version": "candidate_endorsement_private_pilot40_selection_v1",
        "source_private_selection_sha256": EXPECTED_SOURCE_SHA256,
        "canonical_assignment_sha256": data["canonical_assignment_sha256"],
        "pilot_components": pilot,
        "pilot_parent_count": sum(len(row["parent_document_ids"]) for row in pilot),
    }
    if payload["pilot_parent_count"] != 40:
        raise ValueError("pilot parent count differs")
    output.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
