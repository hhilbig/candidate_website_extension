#!/usr/bin/env python3
"""Reproduce a seed-vary repair resume scan before submitting the job.

Reads a target list and the published repair outputs for one attempt, and reports
which chunks the run would skip and which it would process. Exits 1 when an
existing output is present but not accepted, which is the condition that killed
job 23649113 fourteen seconds after it started.

Run it on the Hive login node from the scratch root, for example:

  python3 preflight_repair_scan.py \
    --targets build/hive_candidate_extraction/candidate_extraction_repair_targets_attempt3.json \
    --output-dir build/hive_candidate_extraction/v2_1_seed_vary_repair_attempt3_20260918a
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASE = Path("/nfs/hive/scratch/hhilbig/candidate_extraction")
sys.path.insert(0, str(BASE))

from scripts.hive.extract_candidate_pages_vllm import (  # noqa: E402
    load_repair_targets,
    repair_chunk_is_complete,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    targets = load_repair_targets(args.targets)
    todo: list[str] = []
    skipped: list[str] = []
    for chunk in sorted(targets):
        output = args.output_dir / chunk.replace(".jsonl.gz", ".out.jsonl.gz")
        complete = repair_chunk_is_complete(
            output, targets[chunk], accept_truncated_rows=True
        )
        state = "complete" if complete else ("absent" if not output.exists() else "BLOCKED")
        print(
            f"{chunk} targets={len(targets[chunk])} output={output.exists()} {state}",
            flush=True,
        )
        if complete:
            skipped.append(chunk)
        elif output.exists():
            print(f"BLOCKED by {output}", flush=True)
            return 1
        else:
            todo.append(chunk)
    print(
        json.dumps(
            {
                "target_list": str(args.targets),
                "output_dir": str(args.output_dir),
                "target_chunks": len(targets),
                "target_windows": sum(len(value) for value in targets.values()),
                "skipped_chunks": len(skipped),
                "todo_chunks": len(todo),
                "todo": todo,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
