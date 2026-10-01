#!/usr/bin/env python3
"""Freeze one source-preserving candidate URL list after full discovery."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE = REPO_ROOT / "data/municipal_discovery/screening_2010_2021/full_discovery_2010_2021"
DEFAULT_EXISTING = REPO_ROOT / "data/municipal_discovery/final/mayoral_url_claims.csv"
DEFAULT_SERPER = BASE / "serper/direct_url_leads.csv"
DEFAULT_OPENAI = BASE / "openai/extracted/candidate_site_targets.csv"
DEFAULT_OPENAI_RETRY = BASE / "openai/retry_parse_errors/extracted/candidate_site_targets.csv"
DEFAULT_ARCHIVEIT = REPO_ROOT / "data/municipal_discovery/archiveit/cross_collection_2010_2021/url_leads.csv"
DEFAULT_OUTPUT_DIR = BASE / "combined_candidate_list"
URL_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}
CLAIM_COLUMNS = [
    "discovery_claim_id", "source_channel", "source_claim_ids_json",
    "candidate_year_id", "candidate_contest_id", "contest", "full_name",
    "geo_name", "state_abb", "year", "month", "raw_url",
    "exact_normalized_url", "site_root_url", "registrable_domain", "url_type",
    "evidence_grade", "review_status", "acceptance", "identity_status",
    "temporal_status", "strict_custom_domain", "strict_exact_url",
]
SUMMARY_COLUMNS = [
    "candidate_domain_id", "candidate_year_id", "candidate_contest_id", "contest",
    "full_name", "geo_name", "state_abb", "year", "month", "registrable_domain",
    "site_root_url", "url_types_json", "exact_urls_json", "source_channels_json",
    "source_claim_count", "discovery_claim_ids_json", "has_strict_custom_domain",
    "has_strict_exact_url", "review_statuses_json", "evidence_grades_json",
]


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _truth(value: object) -> bool:
    return _text(value).casefold() in {"1", "true", "yes"}


def _id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(_text(value) for value in values)
    return prefix + "_" + hashlib.sha256(payload.encode()).hexdigest()[:24]


def _standard_url_type(value: object, exact_url: object, domain: object) -> str:
    url_type = _text(value)
    if url_type == "candidate_owned_custom_domain":
        return "custom_domain"
    if url_type == "shared_host_root":
        host = (urlsplit(_text(exact_url)).hostname or "").casefold().removeprefix("www.")
        registrable = _text(domain).casefold()
        if host and registrable and host != registrable and host.endswith(f".{registrable}"):
            return "candidate_subdomain"
    return url_type


def _atomic_csv(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False, lineterminator="\n")
    temporary.replace(path)


def build_claims(
    existing: pd.DataFrame,
    serper: pd.DataFrame,
    openai: pd.DataFrame,
    openai_retry: pd.DataFrame,
    archiveit: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    active = existing.loc[
        existing["url_type"].map(_text).isin(URL_TYPES)
        & existing["exact_normalized_url"].map(_text).ne("")
        & ~existing["review_status"].map(_text).str.casefold().isin({"rejected", "exclude"})
    ]
    for item in active.to_dict("records"):
        rows.append({
            "discovery_claim_id": item["claim_id"], "source_channel": "existing_unified_claim",
            "source_claim_ids_json": json.dumps([item["claim_id"]], separators=(",", ":")),
            **{key: item.get(key, "") for key in (
                "candidate_year_id", "candidate_contest_id", "full_name", "geo_name",
                "state_abb", "year", "month", "raw_url", "exact_normalized_url",
                "site_root_url", "registrable_domain", "url_type", "evidence_grade",
                "review_status", "acceptance",
            )},
            "contest": item.get("contest_id", ""),
            "identity_status": item.get("identity_match", ""),
            "temporal_status": item.get("temporal_match", ""),
            "strict_custom_domain": _truth(item.get("strict_custom_domain")),
            "strict_exact_url": _truth(item.get("strict_exact_url")),
        })
    for item in serper.to_dict("records"):
        item = dict(item)
        item["url_type"] = _standard_url_type(
            item.get("url_type"), item.get("exact_normalized_url"), item.get("registrable_domain")
        )
        rows.append({
            "discovery_claim_id": item["url_claim_id"], "source_channel": "serper_direct_result",
            "source_claim_ids_json": json.dumps([item["url_claim_id"]], separators=(",", ":")),
            **{key: item.get(key, "") for key in (
                "candidate_year_id", "candidate_contest_id", "contest", "full_name",
                "geo_name", "state_abb", "year", "month", "raw_url",
                "exact_normalized_url", "site_root_url", "registrable_domain", "url_type",
                "evidence_grade", "review_status", "acceptance",
            )},
            "identity_status": item.get("candidate_attribution_status", ""),
            "temporal_status": "not_assessed", "strict_custom_domain": False,
            "strict_exact_url": False,
        })
    for source, frame in (("openai_web_search", openai), ("openai_web_search_retry", openai_retry)):
        for item in frame.to_dict("records"):
            rows.append({
                "discovery_claim_id": item["target_id"], "source_channel": source,
                "source_claim_ids_json": item["campaign_claim_ids_json"],
                "candidate_year_id": item["candidate_year_id"],
                "candidate_contest_id": item["candidate_contest_id"], "contest": item["contest"],
                "full_name": item["full_name"], "geo_name": item["geo_name"],
                "state_abb": item["state_abb"], "year": item["year"], "month": item["month"],
                "raw_url": item["primary_exact_url"],
                "exact_normalized_url": item["primary_exact_url"],
                "site_root_url": item["site_root_url"], "registrable_domain": item["registrable_domain"],
                "url_type": item["url_type"], "evidence_grade": item["evidence_grade"],
                "review_status": item["review_status"], "acceptance": item["acceptance"],
                "identity_status": item["candidate_attribution_status"],
                "temporal_status": item["temporal_validation_status"],
                "strict_custom_domain": False, "strict_exact_url": False,
            })
    for item in archiveit.to_dict("records"):
        item = dict(item)
        item["url_type"] = _standard_url_type(
            item.get("url_type"), item.get("exact_normalized_url"), item.get("registrable_domain")
        )
        rows.append({
            "discovery_claim_id": item["claim_id"], "source_channel": "archive_it_cross_collection",
            "source_claim_ids_json": json.dumps([item["claim_id"]], separators=(",", ":")),
            **{key: item.get(key, "") for key in (
                "candidate_year_id", "candidate_contest_id", "contest", "full_name",
                "geo_name", "state_abb", "year", "month", "raw_url",
                "exact_normalized_url", "site_root_url", "registrable_domain", "url_type",
                "evidence_grade", "review_status", "acceptance",
            )},
            "identity_status": item.get("identity_match", ""),
            "temporal_status": item.get("temporal_match", ""),
            "strict_custom_domain": False, "strict_exact_url": False,
        })
    result = pd.DataFrame(rows).reindex(columns=CLAIM_COLUMNS)
    if result.discovery_claim_id.duplicated().any():
        raise ValueError("Combined candidate list has duplicate discovery claim IDs")
    if not result.url_type.isin(URL_TYPES).all():
        raise ValueError("Combined candidate list contains an ineligible URL type")
    return result.sort_values(
        ["candidate_year_id", "registrable_domain", "source_channel", "discovery_claim_id"],
        kind="stable",
    ).reset_index(drop=True)


def summarize(claims: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (candidate_id, domain), group in claims.groupby(
        ["candidate_year_id", "registrable_domain"], sort=True
    ):
        first = group.iloc[0]
        unique = lambda column: list(dict.fromkeys(group[column].map(_text)))
        rows.append({
            "candidate_domain_id": _id("candidate_domain", candidate_id, domain),
            **{key: first[key] for key in (
                "candidate_year_id", "candidate_contest_id", "contest", "full_name",
                "geo_name", "state_abb", "year", "month", "registrable_domain", "site_root_url",
            )},
            "url_types_json": json.dumps(unique("url_type"), separators=(",", ":")),
            "exact_urls_json": json.dumps(unique("exact_normalized_url"), separators=(",", ":")),
            "source_channels_json": json.dumps(unique("source_channel"), separators=(",", ":")),
            "source_claim_count": len(group),
            "discovery_claim_ids_json": json.dumps(unique("discovery_claim_id"), separators=(",", ":")),
            "has_strict_custom_domain": group.strict_custom_domain.map(_truth).any(),
            "has_strict_exact_url": group.strict_exact_url.map(_truth).any(),
            "review_statuses_json": json.dumps(unique("review_status"), separators=(",", ":")),
            "evidence_grades_json": json.dumps(unique("evidence_grade"), separators=(",", ":")),
        })
    return pd.DataFrame(rows).reindex(columns=SUMMARY_COLUMNS)


def run(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, object]:
    claims = build_claims(
        pd.read_csv(DEFAULT_EXISTING, keep_default_na=False, low_memory=False),
        pd.read_csv(DEFAULT_SERPER, keep_default_na=False, low_memory=False),
        pd.read_csv(DEFAULT_OPENAI, keep_default_na=False, low_memory=False),
        pd.read_csv(DEFAULT_OPENAI_RETRY, keep_default_na=False, low_memory=False),
        pd.read_csv(DEFAULT_ARCHIVEIT, keep_default_na=False, low_memory=False),
    )
    summary = summarize(claims)
    _atomic_csv(claims, output_dir / "candidate_url_claims.csv", CLAIM_COLUMNS)
    _atomic_csv(summary, output_dir / "candidate_domains.csv", SUMMARY_COLUMNS)
    audit = {
        "claim_rows": len(claims), "candidate_domain_rows": len(summary),
        "candidate_years": int(claims.candidate_year_id.nunique()),
        "source_channel_claims": claims.source_channel.value_counts().sort_index().astype(int).to_dict(),
        "source_channel_candidates": {
            source: int(group.candidate_year_id.nunique())
            for source, group in claims.groupby("source_channel", sort=True)
        },
        "strict_custom_candidate_years": int(
            claims.loc[claims.strict_custom_domain.map(_truth), "candidate_year_id"].nunique()
        ),
        "strict_exact_candidate_years": int(
            claims.loc[claims.strict_exact_url.map(_truth), "candidate_year_id"].nunique()
        ),
        "pending_or_legacy_candidate_years": int(claims.candidate_year_id.nunique()),
        "interpretation": "This is a discovery candidate list. Only rows already carrying strict flags count as strict coverage.",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "audit.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
