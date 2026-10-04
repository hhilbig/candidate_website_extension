"""Fail-closed technical evaluator for the job-21576064 residual confirmations."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.import_candidate_endorsement_recall40_stage_b_residual import import_results


def evaluate(task: Path, html: Path, results: Path) -> dict[str, Any]:
    imported = import_results(task, html, results)
    checks = [
        {"id": "all_stage_b_rows_technical", "observed": imported["technical_pass_count"],
         "expected": 62, "passed": imported["technical_pass_count"] == 62},
        {"id": "all_records_aligned", "observed": imported["aligned_record_count"],
         "expected": 362, "passed": imported["aligned_record_count"] == 362},
        {"id": "zero_withheld", "observed": imported["withheld_record_count"],
         "expected": 0, "passed": imported["withheld_record_count"] == 0},
    ]
    passed = all(type(check["passed"]) is bool and check["passed"] for check in checks)
    return {
        "version": "candidate_endorsement_recall40_stage_b_residual_evaluation_v1",
        "gate_decision": "GO_SCORE_UNTOUCHED_RECALL40" if passed else "NO_GO",
        "checks": checks,
        "import_artifact": imported,
        "content_gates_evaluated": False,
        "larger_run_or_release_authorized": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = evaluate(args.task, args.html, args.results)
    args.output.write_text(
        json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return 0 if report["gate_decision"] == "GO_SCORE_UNTOUCHED_RECALL40" else 2


if __name__ == "__main__":
    raise SystemExit(main())
