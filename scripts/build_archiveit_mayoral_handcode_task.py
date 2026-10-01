#!/usr/bin/env python3
"""Build the handcoding task for Archive-It mayoral URL claims."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = (
    "data/municipal_discovery/archiveit/"
    "mayoral_url_evidence_archiveit_review.csv"
)
DEFAULT_OUTPUT = (
    "quality_reports/mayoral_url_discovery_2026-08-24/"
    "handcode_archiveit/task.json"
)

REQUIRED_COLUMNS = {
    "claim_id",
    "source_id",
    "candidate_year_id",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "raw_url",
    "exact_normalized_url",
    "url_type",
    "evidence_grade",
    "review_status",
    "temporal_validation_status",
}
URL_PATTERN = re.compile(r"https?://[^\s\"'<>]+", flags=re.IGNORECASE)


def stable_rank(value: str) -> str:
    """Return a deterministic sort key for audit sampling."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _text(value: object) -> str:
    return "" if pd.isna(value) else str(value)


def _bool_text(value: object) -> str:
    if pd.isna(value):
        return "unknown"
    return "yes" if bool(value) else "no"


def metadata_values(raw_metadata: object, field: str) -> list[str]:
    """Extract the string values for one Archive-It metadata field."""

    text = _text(raw_metadata)
    if not text:
        return []
    try:
        metadata = json.loads(text)
    except json.JSONDecodeError:
        return []
    values = metadata.get(field, []) if isinstance(metadata, dict) else []
    result: list[str] = []
    for value in values if isinstance(values, list) else []:
        if isinstance(value, dict) and value.get("value") is not None:
            result.append(str(value["value"]))
        elif isinstance(value, str):
            result.append(value)
    return result


def metadata_urls(raw_metadata: object) -> list[str]:
    """Recover URLs mentioned in collection metadata without changing the claim URL."""

    urls: list[str] = []
    for field in ("Description", "Title", "Creator"):
        for value in metadata_values(raw_metadata, field):
            for match in URL_PATTERN.findall(value):
                url = match.rstrip(".,;:)]}")
                if url not in urls:
                    urls.append(url)
    return urls


def prepare_review_rows(
    claims: pd.DataFrame,
    negative_share: float = 0.10,
) -> pd.DataFrame:
    """Include every claim and flag a deterministic audit subset among non-A/B rows."""

    missing = REQUIRED_COLUMNS - set(claims.columns)
    if missing:
        raise ValueError(f"Archive-It review input is missing columns: {sorted(missing)}")
    if not 0 <= negative_share <= 1:
        raise ValueError("negative_share must lie between zero and one")
    if claims["claim_id"].isna().any() or claims["claim_id"].duplicated().any():
        raise ValueError("Archive-It review input must have unique, nonmissing claim_id values")

    rows = claims.copy()
    rows["source_review_status"] = rows["review_status"]
    rows["source_acceptance"] = rows.get("acceptance", pd.Series(index=rows.index, dtype=object))
    if ~rows["source_review_status"].fillna("").eq("pending").all():
        raise ValueError("Archive-It handcoding input must contain only pending claims")

    rows["item_id"] = rows["claim_id"].astype(str)
    rows["candidate_url"] = rows["exact_normalized_url"].fillna("")
    rows.loc[rows["candidate_url"].eq(""), "candidate_url"] = rows["raw_url"].fillna("")
    metadata = rows.get("source_metadata_json", pd.Series("", index=rows.index))
    rows["metadata_original_urls"] = metadata.map(metadata_urls).map(" | ".join)
    rows["metadata_descriptions"] = metadata.map(
        lambda value: " | ".join(metadata_values(value, "Description"))
    )

    rows["proposed_strict_positive"] = rows["evidence_grade"].isin(["A", "B"])
    rows["deterministic_non_strict_audit"] = False
    non_strict = rows.loc[~rows["proposed_strict_positive"]].copy()
    audit_count = math.ceil(len(non_strict) * negative_share) if len(non_strict) else 0
    if audit_count:
        audit_ids = (
            non_strict.assign(_rank=non_strict["claim_id"].astype(str).map(stable_rank))
            .sort_values(["_rank", "claim_id"])
            .head(audit_count)["claim_id"]
        )
        rows.loc[rows["claim_id"].isin(audit_ids), "deterministic_non_strict_audit"] = True

    rows["review_inclusion_reason"] = "candidate_domain_lead_full_review"
    rows.loc[
        rows["deterministic_non_strict_audit"], "review_inclusion_reason"
    ] = "deterministic_non_strict_audit"
    rows.loc[
        rows["proposed_strict_positive"], "review_inclusion_reason"
    ] = "proposed_strict_positive"
    if "audit_inclusion_reason" in rows:
        explicit_reason = rows["audit_inclusion_reason"].fillna("").astype(str).str.strip()
        rows.loc[explicit_reason.ne(""), "review_inclusion_reason"] = explicit_reason

    rows["_sort_group"] = 2
    rows.loc[rows["deterministic_non_strict_audit"], "_sort_group"] = 1
    rows.loc[rows["proposed_strict_positive"], "_sort_group"] = 0
    rows = rows.sort_values(
        ["_sort_group", "collection_id", "year", "geo_name", "full_name", "claim_id"],
        kind="stable",
    ).drop(columns="_sort_group")
    return rows.reset_index(drop=True)


def select_collections(claims: pd.DataFrame, value: str | None) -> pd.DataFrame:
    """Restrict a review batch to explicit Archive-It collection IDs."""
    if not value:
        return claims.copy()
    selected = {int(item.strip()) for item in value.split(",") if item.strip()}
    result = claims.loc[
        pd.to_numeric(claims["collection_id"], errors="coerce").isin(selected)
    ].copy()
    if result.empty:
        raise ValueError(f"No claims found for selected collections: {sorted(selected)}")
    return result


def task_payload(
    rows: pd.DataFrame,
    task_id: str = "mayoral_url_archiveit_adjudication_20260824",
    title: str = "Archive-It mayoral URL adjudication",
) -> dict:
    """Translate the review table into the generic handcoding-app schema."""

    items = []
    for row in rows.to_dict("records"):
        metadata = {
            "Claimed exact URL": _text(row.get("candidate_url")),
            "Raw URL": _text(row.get("raw_url")),
            "Site root": _text(row.get("site_root_url")),
            "Original URL(s) mentioned in collection metadata": _text(
                row.get("metadata_original_urls")
            ),
            "Collection metadata description": _text(row.get("metadata_descriptions")),
            "Archive capture replay": _text(row.get("archive_capture_replay_url")),
            "Archive calendar": _text(row.get("archive_calendar_url")),
            "Collection record": _text(row.get("collection_access_url")),
        }
        validation = {
            "Machine-proposed grade (not an acceptance decision)": _text(
                row.get("evidence_grade")
            ),
            "Machine URL type": _text(row.get("url_type")),
            "Temporal validation status": _text(row.get("temporal_validation_status")),
            "Archive query status": _text(row.get("archive_query_status")),
            "Validation reason": _text(row.get("archive_validation_reason")),
            "Nearest capture timestamp": _text(
                row.get("archive_nearest_capture_timestamp")
            ),
            "Nearest capture distance (days)": _text(
                row.get("archive_nearest_capture_days_absolute")
            ),
            "Within 365-day headline window": _bool_text(
                row.get("archive_within_headline_window")
            ),
            "Within sensitivity window": _bool_text(
                row.get("archive_within_sensitivity_window")
            ),
            "Candidate identity in capture": _bool_text(
                row.get("archive_candidate_identity_match")
            ),
            "Mayoral campaign in capture": _bool_text(
                row.get("archive_mayoral_campaign_match")
            ),
            "City in capture": _bool_text(row.get("archive_city_match")),
            "Domain reuse status": _text(row.get("archive_domain_reuse_status")),
            "Inclusion reason": _text(row.get("review_inclusion_reason")),
        }
        items.append({
            "id": row["item_id"],
            "label": f"{row['full_name']} ({int(row['year'])})",
            "display": [
                {
                    "label": "Candidate and contest",
                    "format": "badges",
                    "content": {
                        "Name": _text(row.get("full_name")),
                        "City": _text(row.get("geo_name")),
                        "State": _text(row.get("state_abb")),
                        "Election": f"{int(row['year'])}-{int(row['month']):02d}",
                        "Round": _text(row.get("source_election_stage")),
                        "Collection": _text(row.get("collection_id")),
                        "Candidate-year ID": _text(row.get("candidate_year_id")),
                    },
                },
                {"label": "URL and source evidence", "format": "json", "content": metadata},
                {"label": "Archive validation", "format": "json", "content": validation},
                {
                    "label": "Collection evidence text",
                    "format": "text",
                    "content": _text(row.get("evidence_text")),
                },
            ],
        })

    return {
        "task_id": task_id,
        "title": title,
        "instructions": (
            "### What to decide\n"
            "- Review the named candidate, mayoral contest, claimed URL, collection metadata, and archive evidence.\n"
            "- The machine-proposed grade is context only. No claim is accepted until you code it here.\n"
            "- If collection metadata identifies the original campaign domain but the claimed URL is an archive-container path, enter the original domain as the corrected URL.\n"
            "- Code URL type for the URL you accept (the corrected URL when one is entered).\n\n"
            "### Evidence grades\n"
            "- A: an election-period capture of the exact URL identifies the candidate, mayoral campaign, and city.\n"
            "- B: a contemporaneous official source links the URL and archived content confirms the campaign.\n"
            "- C: a source supports the URL, but no qualifying campaign capture exists.\n"
            "- D: domain shape, registration data, or current content provides only a lead.\n"
            "- Only grades A and B count as strict coverage.\n\n"
            "### Scope\n"
            "- The task contains every Archive-It candidate-domain claim.\n"
            "- The inclusion-reason field identifies proposed strict positives and the deterministic 10% non-strict audit subset."
        ),
        "items": items,
        "questions": [
            {
                "id": "identity_match",
                "label": "Does the evidence match the named candidate, mayoral office, and city?",
                "type": "yesno",
                "help": "Answer yes only when all three identity elements match.",
            },
            {
                "id": "temporal_match",
                "label": "Does the evidence belong to this election cycle?",
                "type": "yesno",
                "help": "Apply the displayed election date and capture window; later candidacies do not qualify.",
            },
            {
                "id": "accept_url",
                "label": "Accept this as a candidate campaign URL claim?",
                "type": "yesno",
                "help": "Grades A through C may be accepted as claims; only A and B are strict coverage.",
            },
            {
                "id": "url_type",
                "label": "URL type for the accepted or reviewed URL",
                "type": "select",
                "options": [
                    "custom_domain",
                    "candidate_subdomain",
                    "shared_host_path",
                    "fundraising_profile",
                    "social_profile",
                    "officeholder_site",
                    "archive_container_or_wrapper",
                    "not_campaign_url",
                ],
            },
            {
                "id": "evidence_grade",
                "label": "Final evidence grade",
                "type": "select",
                "options": ["A", "B", "C", "D", "reject"],
            },
            {
                "id": "corrected_url",
                "label": "Corrected or recovered original campaign URL",
                "type": "text",
                "show_if": {"question": "accept_url", "equals": "yes"},
            },
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--negative-share", type=float, default=0.10)
    parser.add_argument(
        "--collection-ids",
        help="Optional comma-separated collection IDs to include in this review batch.",
    )
    parser.add_argument(
        "--task-id", default="mayoral_url_archiveit_adjudication_20260824",
    )
    parser.add_argument("--title", default="Archive-It mayoral URL adjudication")
    args = parser.parse_args()

    claims = select_collections(
        pd.read_csv(REPO_ROOT / args.input, low_memory=False), args.collection_ids,
    )
    rows = prepare_review_rows(
        claims,
        negative_share=args.negative_share,
    )
    output = REPO_ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(task_payload(rows, task_id=args.task_id, title=args.title), indent=2) + "\n",
        encoding="utf-8",
    )
    rows.to_csv(output.with_name("review_items.csv"), index=False)
    print(json.dumps({
        "items": len(rows),
        "proposed_strict_positives": int(rows["proposed_strict_positive"].sum()),
        "non_strict_claims": int((~rows["proposed_strict_positive"]).sum()),
        "deterministic_non_strict_audits": int(
            rows["deterministic_non_strict_audit"].sum()
        ),
        "pending_claims": int(rows["source_review_status"].eq("pending").sum()),
        "output": str(output),
    }))


if __name__ == "__main__":
    main()
