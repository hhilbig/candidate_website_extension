#!/usr/bin/env python3
"""Authenticate round-three Stage A/A2 and gate only bounded Stage-B inputs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.build_candidate_endorsement_line_ledger import (  # noqa: E402
    DEFAULT_FULL_EXPORT_DIR,
    RECALL40_ROUND3_POST_A2_REPORT_FIELDS,
    load_json_no_duplicates,
    recall40_round3_post_a2_evaluation_report,
    replay_recall40_round3_stage_a_and_a2,
    sha256_file,
    sha256_json,
    validate_recall40_round3_post_a2_evaluation_report,
)


DEFAULT_STAGE_A_EXPORT_DIR = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a_preflight_20260906a"
)
DEFAULT_STAGE_A_OUTPUT_DIR = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a_outputs_20260906a"
)
DEFAULT_STAGE_A2_EXPORT_DIR = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a2_preflight_20260906a"
)


def evaluate(
    stage_a2_output_dir: Path,
    *,
    full_export_dir: Path = DEFAULT_FULL_EXPORT_DIR,
    stage_a_export_dir: Path = DEFAULT_STAGE_A_EXPORT_DIR,
    stage_a_output_dir: Path = DEFAULT_STAGE_A_OUTPUT_DIR,
    stage_a2_export_dir: Path = DEFAULT_STAGE_A2_EXPORT_DIR,
) -> dict[str, Any]:
    """Replay both exact stages and return their canonical bounded-build gate."""
    replay = replay_recall40_round3_stage_a_and_a2(
        full_export_dir,
        stage_a_export_dir,
        stage_a_output_dir,
        stage_a2_export_dir,
        stage_a2_output_dir,
    )
    report = recall40_round3_post_a2_evaluation_report(replay)
    return validate_recall40_round3_post_a2_evaluation_report(
        report, replay
    )


def _validate_report_for_write(report: Mapping[str, Any]) -> None:
    if set(report) != RECALL40_ROUND3_POST_A2_REPORT_FIELDS:
        raise ValueError("round-three post-A2 report field set differs")
    payload = dict(report)
    recorded = payload.pop("report_payload_sha256", None)
    if not isinstance(recorded, str) or sha256_json(payload) != recorded:
        raise ValueError("round-three post-A2 report payload hash differs")
    decision = report.get("gate_decision")
    authorized = report.get("bounded_stage_b_input_build_authorized")
    expected_authorized = decision == "GO_BUILD_BOUNDED_STAGE_B_INPUTS"
    if (
        decision not in {"GO_BUILD_BOUNDED_STAGE_B_INPUTS", "NO_GO"}
        or authorized is not expected_authorized
        or report.get("stage_b_hive_submission_authorized") is not False
        or report.get("full_expansion_or_release_authorized") is not False
    ):
        raise ValueError("round-three post-A2 report authorization differs")


def write_report(report: Mapping[str, Any], report_path: Path) -> None:
    """Write one new canonical report and verify its bytes by strict reload."""
    if report_path.exists():
        raise FileExistsError(
            f"refusing to overwrite post-A2 report: {report_path}"
        )
    _validate_report_for_write(report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n",
        encoding="utf-8",
    )
    reloaded = load_json_no_duplicates(report_path)
    if not isinstance(reloaded, Mapping) or sha256_json(reloaded) != sha256_json(
        report
    ):
        raise ValueError("written round-three post-A2 report differs")
    if report_path.stat().st_size <= 1 or not sha256_file(report_path):
        raise ValueError("written round-three post-A2 report is empty")


def cli_exit_code(report: Mapping[str, Any]) -> int:
    """Map the two named gate decisions to stable command-line statuses."""
    _validate_report_for_write(report)
    if report["gate_decision"] == "GO_BUILD_BOUNDED_STAGE_B_INPUTS":
        return 0
    return 2


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage-a2-output-dir", type=Path, required=True)
    parser.add_argument(
        "--full-export-dir", type=Path, default=DEFAULT_FULL_EXPORT_DIR
    )
    parser.add_argument(
        "--stage-a-export-dir", type=Path,
        default=DEFAULT_STAGE_A_EXPORT_DIR,
    )
    parser.add_argument(
        "--stage-a-output-dir", type=Path,
        default=DEFAULT_STAGE_A_OUTPUT_DIR,
    )
    parser.add_argument(
        "--stage-a2-export-dir", type=Path,
        default=DEFAULT_STAGE_A2_EXPORT_DIR,
    )
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    report = evaluate(
        args.stage_a2_output_dir,
        full_export_dir=args.full_export_dir,
        stage_a_export_dir=args.stage_a_export_dir,
        stage_a_output_dir=args.stage_a_output_dir,
        stage_a2_export_dir=args.stage_a2_export_dir,
    )
    write_report(report, args.report)
    print(json.dumps({
        "gate_decision": report["gate_decision"],
        "passed_checks": report["passed_checks"],
        "failed_checks": report["failed_checks"],
        "report": str(args.report),
        "report_sha256": sha256_file(args.report),
    }, indent=2, sort_keys=True))
    return cli_exit_code(report)


if __name__ == "__main__":
    raise SystemExit(main())
