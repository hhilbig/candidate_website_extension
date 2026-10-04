"""Build stage 3 scrape targets from the checked stage 2 URL leads.

One target is one finalist-election x its discovered campaign domain, in the
input format of scripts/freeze_mayoral_wayback_scrape_batch.py. The scrape
method (window, page cap, URL variants) is that script's, unchanged.

Inputs: data/municipal_discovery/top100_coverage/linkage/discovered_url_leads.csv
        data/municipal_discovery/top100_coverage/outputs/candidate_coverage.csv
Output: data/municipal_archive_acquisition/<batch>/candidate_site_targets.csv
A later batch passes --exclude with earlier batches' target files, so no finalist is scraped twice.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd

BASE = Path("data/municipal_discovery/top100_coverage")
LEADS = BASE / "linkage/discovered_url_leads.csv"
COVERAGE = BASE / "outputs/candidate_coverage.csv"
ACQUISITION = Path("data/municipal_archive_acquisition")
PERIODS = ((2010, 2013), (2014, 2017), (2018, 2021), (2022, 2025), (2026, 2029))


def period(year: int) -> str:
    for start, end in PERIODS:
        if start <= year <= end:
            return f"{start}_{end}"
    raise ValueError(f"year outside periods: {year}")


def build_targets(leads: pd.DataFrame, coverage: pd.DataFrame) -> pd.DataFrame:
    leads = leads[leads["campaign_url"].ne("") & leads["final_status"].isin(["accepted", "outside_window"])]
    coverage = coverage.set_index("candidate_election_id")
    missing = sorted(set(leads["candidate_election_id"]) - set(coverage.index))
    if missing:
        raise ValueError(f"leads without a candidate record: {missing[:3]}")
    rows = []
    for lead in leads.to_dict("records"):
        record = coverage.loc[lead["candidate_election_id"]]
        url = lead["campaign_url"]
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        domain = lead["registrable_domain"].lower()
        if host not in {domain, f"www.{domain}"}:
            raise ValueError(f"campaign URL host is not the registrable domain: {url}")
        day = pd.Timestamp(lead["election_date"])
        evidence = [u for u in [lead["checked_capture_url"]] + lead["lead_sources"].split(" | ") if u]
        rows.append({
            "target_id": "stage3_" + hashlib.sha256(lead["candidate_election_id"].encode()).hexdigest()[:24],
            "candidate_year_id": record["candidate_year_id"] if isinstance(record["candidate_year_id"], str) and record["candidate_year_id"] else lead["candidate_election_id"],
            "candidate_contest_id": lead["candidate_election_id"],
            "contest": record["election_id"],
            "full_name": lead["candidate_name"],
            "geo_name": record["city_name"],
            "state_abb": record["state_abb"],
            "year": day.year,
            "month": day.month,
            "period": period(day.year),
            "registrable_domain": domain,
            "site_root_url": f"{parts.scheme}://{host}/",
            "primary_exact_url": url,
            "url_type": "custom_domain",
            "campaign_claim_count": 1,
            "campaign_claim_ids_json": json.dumps([lead["link_basis"] + ":" + lead["candidate_election_id"]]),
            "campaign_urls_json": json.dumps([url]),
            "evidence_urls_json": json.dumps(evidence),
            "url_discovery_status": lead["final_status"],
            "election_date": lead["election_date"],
        })
    return pd.DataFrame(rows).sort_values("target_id").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", default="mayoral_stage3_20261002")
    parser.add_argument("--exclude", type=Path, nargs="*", default=[])
    args = parser.parse_args()
    leads = pd.read_csv(LEADS, keep_default_na=False, dtype=str)
    done = set()
    for path in args.exclude:
        done |= set(pd.read_csv(path, dtype=str)["candidate_contest_id"])
    leads = leads[~leads["candidate_election_id"].isin(done)]
    coverage = pd.read_csv(COVERAGE, keep_default_na=False, dtype=str)
    targets = build_targets(leads, coverage)
    OUTPUT = ACQUISITION / args.batch / "candidate_site_targets.csv"
    if targets["target_id"].duplicated().any():
        raise ValueError("duplicate target id")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    targets.to_csv(OUTPUT, index=False)
    print(json.dumps({"excluded_finalists": len(done), "targets": len(targets), "status": targets["url_discovery_status"].value_counts().to_dict(),
                      "domains": int(targets["registrable_domain"].nunique())}))


if __name__ == "__main__":
    main()
