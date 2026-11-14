#!/usr/bin/env python3
"""Score completed human coding for candidate-page extraction."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


YES_NO = {"yes", "no"}
PRECISION_FIELDS = (
    "record_present_in_page",
    "normalized_value_faithful",
    "record_type_correct",
    "evidence_alone_supports_record",
)


def normalized_answers(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.casefold()


def score_validation(
    precision: pd.DataFrame, recall: pd.DataFrame, require_complete: bool = True
) -> dict:
    missing_precision = set(PRECISION_FIELDS) - set(precision.columns)
    missing_recall = {"all_requested_records_captured"} - set(recall.columns)
    if missing_precision or missing_recall:
        raise ValueError(
            f"coding columns missing: precision={sorted(missing_precision)}, "
            f"recall={sorted(missing_recall)}"
        )
    precision_answers = {
        field: normalized_answers(precision[field]) for field in PRECISION_FIELDS
    }
    recall_answer = normalized_answers(recall["all_requested_records_captured"])
    invalid = {
        field: int((~values.isin(YES_NO)).sum())
        for field, values in precision_answers.items()
    }
    invalid["all_requested_records_captured"] = int(
        (~recall_answer.isin(YES_NO)).sum()
    )
    if require_complete and any(invalid.values()):
        raise ValueError(f"coding is incomplete or invalid: {invalid}")

    precision_complete = pd.Series(True, index=precision.index)
    for values in precision_answers.values():
        precision_complete &= values.isin(YES_NO)
    recall_complete = recall_answer.isin(YES_NO)
    precision_scored = precision.loc[precision_complete].copy()
    recall_scored = recall.loc[recall_complete].copy()
    all_correct = pd.Series(True, index=precision_scored.index)
    dimensions = {}
    for field in PRECISION_FIELDS:
        answers = normalized_answers(precision_scored[field])
        dimensions[field] = {
            "yes": int(answers.eq("yes").sum()),
            "n": int(len(answers)),
            "rate": float(answers.eq("yes").mean()) if len(answers) else None,
        }
        all_correct &= answers.eq("yes")

    by_stratum = {}
    if len(precision_scored):
        scored = precision_scored.assign(_all_correct=all_correct)
        for (route, subtype), group in scored.groupby(["route", "subtype"], sort=True):
            by_stratum[f"{route}|{subtype}"] = {
                "all_correct": int(group["_all_correct"].sum()),
                "n": int(len(group)),
                "rate": float(group["_all_correct"].mean()),
            }
    recall_yes = normalized_answers(recall_scored["all_requested_records_captured"]).eq("yes")
    return {
        "schema_version": "1.0",
        "coding_complete": not any(invalid.values()),
        "invalid_or_pending_by_field": invalid,
        "precision_records_scored": int(len(precision_scored)),
        "record_all_dimensions_correct": {
            "yes": int(all_correct.sum()),
            "n": int(len(all_correct)),
            "rate": float(all_correct.mean()) if len(all_correct) else None,
        },
        "precision_dimensions": dimensions,
        "record_accuracy_by_stratum": by_stratum,
        "recall_pages_scored": int(len(recall_scored)),
        "pages_with_no_missed_records": {
            "yes": int(recall_yes.sum()),
            "n": int(len(recall_yes)),
            "rate": float(recall_yes.mean()) if len(recall_yes) else None,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--precision-csv", type=Path, required=True)
    parser.add_argument("--recall-csv", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    report = score_validation(
        pd.read_csv(args.precision_csv),
        pd.read_csv(args.recall_csv),
        require_complete=not args.allow_incomplete,
    )
    args.output_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
