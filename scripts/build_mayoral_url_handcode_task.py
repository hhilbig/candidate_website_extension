#!/usr/bin/env python3
"""Build the handcoding task for the current mayoral search pilot."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]


def stable_rank(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _text(value: object) -> str:
    return "" if pd.isna(value) else str(value)


def build_review_rows(
    leads: pd.DataFrame,
    expansion: pd.DataFrame,
    archive_checks: pd.DataFrame,
    negative_share: float = 0.10,
) -> pd.DataFrame:
    direct = leads.loc[leads["proposed_campaign_url"].fillna("").ne("")].copy()
    direct["candidate_url"] = direct["proposed_campaign_url"]
    direct["lead_origin"] = "search_proposal"
    direct["fetch_status"] = ""
    direct["extraction_method"] = direct["evidence_type"]

    expanded = expansion.loc[
        expansion["discovered_campaign_url"].fillna("").ne("")
    ].copy()
    expanded["candidate_url"] = expanded["discovered_campaign_url"]
    expanded["lead_origin"] = "source_page_expansion"
    context = leads[[
        "candidate_year_id", "evidence_source_url", "result_title", "result_snippet",
        "query_variant", "evidence_type", "temporal_conflict", "office_mismatch_signal",
    ]].drop_duplicates(["candidate_year_id", "evidence_source_url"])
    expanded = expanded.merge(
        context,
        on=["candidate_year_id", "evidence_source_url"],
        how="left",
        validate="many_to_one",
    )

    negatives = expansion.loc[
        expansion["discovered_campaign_url"].fillna("").eq("")
        & expansion["fetch_status"].isin([
            "checked_no_candidate_link", "archive_checked_no_candidate_link",
            "no_source_archive_in_year_window",
        ])
    ].copy()
    negative_count = math.ceil(len(negatives) * negative_share) if len(negatives) else 0
    negatives["_rank"] = negatives.apply(
        lambda row: stable_rank(f"{row.candidate_year_id}|{row.evidence_source_url}"), axis=1
    )
    negatives = negatives.sort_values("_rank").head(negative_count).drop(columns="_rank")
    negatives["candidate_url"] = ""
    negatives["lead_origin"] = "negative_audit"
    negatives["extraction_method"] = "no_candidate_link_detected"
    negatives = negatives.merge(
        context,
        on=["candidate_year_id", "evidence_source_url"],
        how="left",
        validate="many_to_one",
    )

    columns = [
        "candidate_year_id", "full_name", "geo_name", "state_abb", "year",
        "candidate_url", "evidence_source_url", "lead_origin", "extraction_method",
        "fetch_status", "result_title", "result_snippet", "query_variant",
        "evidence_type", "temporal_conflict", "office_mismatch_signal",
    ]
    frames = [frame.reindex(columns=columns) for frame in (direct, expanded, negatives)]
    result = pd.concat(frames, ignore_index=True)
    result = result.drop_duplicates([
        "candidate_year_id", "candidate_url", "evidence_source_url", "lead_origin"
    ])
    archive = archive_checks.rename(columns={"proposed_campaign_url": "candidate_url"})
    archive_columns = [
        "candidate_year_id", "candidate_url", "archive_query_status",
        "archive_capture_count", "nearest_capture_timestamp", "nearest_capture_days",
        "temporal_validation_status",
    ]
    result = result.merge(
        archive.reindex(columns=archive_columns),
        on=["candidate_year_id", "candidate_url"],
        how="left",
        validate="many_to_one",
    )
    result["item_id"] = result.apply(
        lambda row: hashlib.sha256(
            f"{row.candidate_year_id}|{row.candidate_url}|{row.evidence_source_url}|{row.lead_origin}".encode()
        ).hexdigest()[:20],
        axis=1,
    )
    return result.sort_values(
        ["lead_origin", "year", "state_abb", "geo_name", "full_name"]
    ).reset_index(drop=True)


def task_payload(rows: pd.DataFrame) -> dict:
    items = []
    for row in rows.to_dict("records"):
        candidate_url = _text(row.get("candidate_url"))
        evidence = {
            "Candidate URL": candidate_url or "No URL detected",
            "Evidence source": _text(row.get("evidence_source_url")),
            "Search title": _text(row.get("result_title")),
            "Search snippet": _text(row.get("result_snippet")),
            "Lead origin": _text(row.get("lead_origin")),
            "Extraction method": _text(row.get("extraction_method")),
            "Fetch status": _text(row.get("fetch_status")),
            "Archive status": _text(row.get("temporal_validation_status")),
            "Archive captures": _text(row.get("archive_capture_count")),
            "Nearest capture": _text(row.get("nearest_capture_timestamp")),
        }
        items.append({
            "id": row["item_id"],
            "label": f"{row['full_name']} ({int(row['year'])})",
            "display": [
                {
                    "label": "Candidate",
                    "format": "badges",
                    "content": {
                        "Name": _text(row.get("full_name")),
                        "City": _text(row.get("geo_name")),
                        "State": _text(row.get("state_abb")),
                        "Year": int(row["year"]),
                        "Pilot ID": _text(row.get("candidate_year_id")),
                    },
                },
                {"label": "Claim evidence", "format": "json", "content": evidence},
            ],
        })
    return {
        "task_id": "mayoral_url_pilot_adjudication_20260824",
        "title": "Mayoral campaign URL pilot adjudication",
        "instructions": (
            "### Decision rule\n"
            "- Review the candidate, city, election year, URL, and source evidence.\n"
            "- Accept only a campaign URL for this named mayoral candidacy.\n"
            "- Grade A requires an election-period exact-URL capture.\n"
            "- Grade B requires a contemporaneous source plus archived campaign content.\n"
            "- Grade C has source support but no qualifying capture.\n"
            "- Grade D is a lead only. Grades C and D are not strict coverage.\n"
            "- For a negative audit, enter a corrected URL only when the automation missed one."
        ),
        "items": items,
        "questions": [
            {
                "id": "identity_match", "label": "Does the evidence match the named candidate, mayoral office, and city?",
                "type": "yesno", "help": "Answer yes only when all three identity elements match."
            },
            {
                "id": "temporal_match", "label": "Does the evidence belong to this election cycle?",
                "type": "yesno", "help": "Current or later campaigns do not qualify."
            },
            {
                "id": "accept_url", "label": "Accept this as a candidate campaign URL claim?",
                "type": "yesno", "help": "Grades A through C can be accepted as claims. Only A and B are strict."
            },
            {
                "id": "url_type", "label": "URL type", "type": "select",
                "options": [
                    "custom_domain", "candidate_subdomain", "shared_host_path",
                    "fundraising_profile", "social_profile", "officeholder_site",
                    "not_campaign_url"
                ]
            },
            {
                "id": "evidence_grade", "label": "Evidence grade", "type": "select",
                "options": ["A", "B", "C", "D", "reject"]
            },
            {
                "id": "corrected_url", "label": "Corrected or recovered URL", "type": "text",
                "show_if": {"question": "accept_url", "equals": "yes"}
            },
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot-dir", default="data/municipal_discovery/search_pilot")
    parser.add_argument(
        "--output",
        default="quality_reports/mayoral_url_discovery_2026-08-24/handcode/task.json",
    )
    args = parser.parse_args()
    pilot = REPO_ROOT / args.pilot_dir
    rows = build_review_rows(
        pd.read_csv(pilot / "search_lead_review.csv"),
        pd.read_csv(pilot / "source_page_expansion.csv"),
        pd.read_csv(pilot / "pilot_domain_archive_checks.csv"),
    )
    output = REPO_ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(task_payload(rows), indent=2) + "\n", encoding="utf-8")
    rows.to_csv(output.with_name("review_items.csv"), index=False)
    print(json.dumps({
        "items": len(rows),
        "positive_leads": int(rows["candidate_url"].fillna("").ne("").sum()),
        "negative_audits": int(rows["lead_origin"].eq("negative_audit").sum()),
        "output": str(output),
    }))


if __name__ == "__main__":
    main()
