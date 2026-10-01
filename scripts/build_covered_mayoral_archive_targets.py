#!/usr/bin/env python3
"""Build one archive target per covered mayoral candidate-year and campaign domain."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROSTER = REPO_ROOT / "data/municipal_discovery/screening_2010_2021/current_coverage_roster.csv"
DEFAULT_CLAIMS = REPO_ROOT / "data/municipal_discovery/final/mayoral_url_claims.csv"
DEFAULT_DISPOSITIONS = (
    REPO_ROOT / "quality_reports/mayoral_url_discovery_2026-08-25/batch20_evaluation/batch_claim_dispositions.csv",
    REPO_ROOT / "quality_reports/mayoral_url_discovery_2026-08-25/batch20_round2_evaluation/batch_claim_dispositions.csv",
)
DEFAULT_OUTPUT = REPO_ROOT / "data/municipal_archive_acquisition/covered_151_20260826/source_targets.csv"
EXPECTED_CANDIDATE_YEARS = 151
EXPECTED_TARGETS = 153

TARGET_COLUMNS = [
    "target_id", "candidate_year_id", "candidate_contest_id", "contest", "full_name",
    "geo_name", "state_abb", "year", "month", "period", "registrable_domain",
    "site_root_url", "primary_exact_url", "url_type", "campaign_claim_count",
    "campaign_claim_ids_json", "campaign_urls_json", "evidence_urls_json",
]


class CoveredTargetError(RuntimeError):
    pass


def _truth(value: object) -> bool:
    return str(value).strip().casefold() in {"1", "true", "t", "yes", "y"}


def _text(value: object) -> str:
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return ""
    return str(value).strip()


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise CoveredTargetError(f"{label} is missing columns: {missing}")


def _json_values(values: pd.Series) -> str:
    unique = sorted({_text(value) for value in values if _text(value)})
    return json.dumps(unique, ensure_ascii=False, separators=(",", ":"))


def _preferred_site_root(group: pd.DataFrame) -> str:
    roots = sorted({_text(value) for value in group["site_root_url"] if _text(value)})
    if not roots:
        raise CoveredTargetError("claim group has no site root")
    domain = _text(group.iloc[0]["registrable_domain"]).casefold()

    def rank(url: str) -> tuple[int, int, int, str]:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").casefold()
        shared_subdomain = host not in {domain, f"www.{domain}"}
        return (0 if shared_subdomain else 1, 0 if parsed.scheme == "https" else 1, len(url), url)

    return min(roots, key=rank)


def build_targets(
    roster: pd.DataFrame,
    claims: pd.DataFrame,
    dispositions: list[pd.DataFrame],
    *,
    expected_candidate_years: int = EXPECTED_CANDIDATE_YEARS,
    expected_targets: int = EXPECTED_TARGETS,
) -> tuple[pd.DataFrame, dict[str, object]]:
    _require(
        roster,
        {"covered", "candidate_year_id", "candidate_contest_id", "contest", "full_name",
         "geo_name", "state_abb", "year", "month", "period"},
        "coverage roster",
    )
    _require(
        claims,
        {"claim_id", "candidate_year_id", "claim_origin", "strict_custom_domain",
         "exact_normalized_url", "site_root_url", "registrable_domain", "url_type",
         "source_profile_url"},
        "claim table",
    )
    covered = roster.loc[roster["covered"].map(_truth)].copy()
    if covered["candidate_year_id"].duplicated().any() or len(covered) != expected_candidate_years:
        raise CoveredTargetError(
            f"expected {expected_candidate_years} unique covered candidate-years; found {len(covered)}"
        )
    if covered["month"].map(_text).eq("").any():
        missing = covered.loc[covered["month"].map(_text).eq(""), "candidate_year_id"].tolist()
        raise CoveredTargetError(f"covered candidates have missing election months: {missing[:5]}")
    covered_ids = set(covered["candidate_year_id"].map(_text))

    claim_rows = claims.loc[
        claims["candidate_year_id"].map(_text).isin(covered_ids)
        & (claims["claim_origin"].eq("legacy_baseline") | claims["strict_custom_domain"].map(_truth))
    ].copy()
    claim_rows["evidence_url"] = claim_rows["source_profile_url"]
    claim_rows["coverage_source"] = claim_rows["claim_origin"]
    selected_columns = [
        "claim_id", "candidate_year_id", "exact_normalized_url", "site_root_url",
        "registrable_domain", "url_type", "source_profile_url", "evidence_url",
        "coverage_source",
    ]
    claim_rows = claim_rows[selected_columns]
    disposition_rows = 0
    for index, disposition in enumerate(dispositions):
        _require(
            disposition,
            {"claim_id", "candidate_year_id", "batch_disposition", "exact_normalized_url",
             "site_root_url", "registrable_domain", "url_type", "source_profile_url"},
            f"disposition {index}",
        )
        inferred = disposition.loc[
            disposition["batch_disposition"].eq("accepted_under_sampled_audit_rule")
        ].copy()
        disposition_rows += len(inferred)
        inferred["evidence_url"] = inferred["source_profile_url"]
        inferred["coverage_source"] = "sampled_audit_rule"
        claim_rows = pd.concat([claim_rows, inferred[selected_columns]], ignore_index=True)

    claim_rows = claim_rows.drop_duplicates(["candidate_year_id", "claim_id"], keep="last")
    for column in ["candidate_year_id", "registrable_domain", "site_root_url", "exact_normalized_url"]:
        if claim_rows[column].map(_text).eq("").any():
            raise CoveredTargetError(f"selected claims have missing {column}")
    missing_covered = sorted(covered_ids - set(claim_rows["candidate_year_id"].map(_text)))
    if missing_covered:
        raise CoveredTargetError(f"covered candidates lack URL claims: {missing_covered[:5]}")

    context = covered.set_index("candidate_year_id")
    rows: list[dict[str, object]] = []
    for (candidate_year_id, domain), group in claim_rows.groupby(
        ["candidate_year_id", "registrable_domain"], sort=True, dropna=False
    ):
        candidate_year_id = _text(candidate_year_id)
        domain = _text(domain).casefold()
        candidate = context.loc[candidate_year_id]
        claim_ids = _json_values(group["claim_id"])
        exact_urls = _json_values(group["exact_normalized_url"])
        evidence_urls = _json_values(group["evidence_url"])
        target_key = f"{candidate_year_id}\x1f{domain}"
        target_id = "covered_site_target_" + hashlib.sha256(target_key.encode("utf-8")).hexdigest()[:24]
        url_types = sorted({_text(value) for value in group["url_type"] if _text(value)})
        rows.append({
            "target_id": target_id,
            "candidate_year_id": candidate_year_id,
            "candidate_contest_id": _text(candidate["candidate_contest_id"]),
            "contest": _text(candidate["contest"]),
            "full_name": _text(candidate["full_name"]),
            "geo_name": _text(candidate["geo_name"]),
            "state_abb": _text(candidate["state_abb"]),
            "year": int(candidate["year"]),
            "month": int(float(candidate["month"])),
            "period": _text(candidate["period"]),
            "registrable_domain": domain,
            "site_root_url": _preferred_site_root(group),
            "primary_exact_url": sorted(json.loads(exact_urls), key=lambda value: (len(value), value))[0],
            "url_type": "|".join(url_types),
            "campaign_claim_count": int(group["claim_id"].nunique()),
            "campaign_claim_ids_json": claim_ids,
            "campaign_urls_json": exact_urls,
            "evidence_urls_json": evidence_urls,
        })
    targets = pd.DataFrame(rows).reindex(columns=TARGET_COLUMNS).sort_values("target_id").reset_index(drop=True)
    if len(targets) != expected_targets or targets["target_id"].duplicated().any():
        raise CoveredTargetError(f"expected {expected_targets} unique targets; found {len(targets)}")
    if targets["candidate_year_id"].nunique() != expected_candidate_years:
        raise CoveredTargetError("target candidate-year set differs from covered roster")
    shared = targets["url_type"].str.contains("subdomain|shared_host", case=False, regex=True)
    for row in targets.loc[shared].to_dict("records"):
        host = (urlsplit(row["site_root_url"]).hostname or "").casefold()
        if host in {row["registrable_domain"], f"www.{row['registrable_domain']}"}:
            raise CoveredTargetError("shared-host target lost its candidate-specific hostname")
    audit = {
        "contract_version": "covered-mayoral-archive-targets-v1",
        "covered_candidate_years": len(covered),
        "target_rows": len(targets),
        "unique_registrable_domains": int(targets["registrable_domain"].nunique()),
        "selected_claim_rows": len(claim_rows),
        "sampled_audit_rule_claim_rows": disposition_rows,
        "candidate_years_with_two_domains": int(targets.groupby("candidate_year_id").size().eq(2).sum()),
        "missing_covered_candidate_years": missing_covered,
        "missing_election_months": int(targets["month"].map(_text).eq("").sum()),
        "shared_host_targets": int(shared.sum()),
    }
    return targets, audit


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False, lineterminator="\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roster", type=Path, default=DEFAULT_ROSTER)
    parser.add_argument("--claims", type=Path, default=DEFAULT_CLAIMS)
    parser.add_argument("--disposition", action="append", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    dispositions = args.disposition or list(DEFAULT_DISPOSITIONS)
    targets, audit = build_targets(
        pd.read_csv(args.roster, keep_default_na=False, low_memory=False),
        pd.read_csv(args.claims, keep_default_na=False, low_memory=False),
        [pd.read_csv(path, keep_default_na=False, low_memory=False) for path in dispositions],
    )
    _atomic_csv(targets, args.output)
    audit["output"] = {
        "path": str(args.output.resolve().relative_to(REPO_ROOT)),
        "rows": len(targets),
        "bytes": args.output.stat().st_size,
        "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
    }
    audit_path = args.output.with_name("source_targets_audit.json")
    audit_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
