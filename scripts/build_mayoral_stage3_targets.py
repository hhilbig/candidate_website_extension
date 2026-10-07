"""Build stage 3 scrape targets from the checked stage 2 URL leads.

One target is one finalist-election x its discovered campaign domain, in the
input format of scripts/freeze_mayoral_wayback_scrape_batch.py. The scrape
method (window, page cap, URL variants) is that script's, unchanged.

Inputs: data/municipal_discovery/top100_coverage/linkage/discovered_url_leads.csv
        data/municipal_discovery/top100_coverage/outputs/candidate_coverage.csv
Output: data/municipal_archive_acquisition/<batch>/candidate_site_targets.csv
With --legacy, targets are instead the legacy URL leads (own custom domains only) of finalists
that have a URL lead but no downloaded text; their attribution is checked after the scrape.
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
        domain = lead["registrable_domain"].lower().split()[0]  # some agents add a note after the domain
        url_type = "custom_domain"
        if host not in {domain, f"www.{domain}"}:
            # A site on a shared host (for example a wordpress.com or weebly.com subdomain) is scraped by
            # its own host name, so the prefix query covers only that subdomain, not the whole host.
            if not host.endswith("." + domain):
                raise ValueError(f"campaign URL host is not the registrable domain: {url}")
            domain, url_type = host.removeprefix("www."), "shared_host_subdomain"
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
            "url_type": url_type,
            "campaign_claim_count": 1,
            "campaign_claim_ids_json": json.dumps([lead["link_basis"] + ":" + lead["candidate_election_id"]]),
            "campaign_urls_json": json.dumps([url]),
            "evidence_urls_json": json.dumps(evidence),
            "url_discovery_status": lead["final_status"],
            "election_date": lead["election_date"],
        })
    return pd.DataFrame(rows).sort_values("target_id").reset_index(drop=True)


LEGACY_URLS = Path("quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_complete/"
                   "candidate_urls_with_capture_reviews.csv")


def build_legacy_targets(coverage: pd.DataFrame, urls: pd.DataFrame) -> pd.DataFrame:
    import ast
    eligible = coverage[(coverage["eligible"] == "True") & (coverage["url_lead"] == "True")
                        & (coverage["downloaded_content"] != "True") & (coverage["url_lead_basis"] == "legacy_url_inventory")]
    urls = urls.set_index("candidate_url_id")
    rows = {}
    for record in eligible.to_dict("records"):
        for url_id in ast.literal_eval(record["candidate_url_ids"]):
            row = urls.loc[url_id]
            url, domain = row["exact_normalized_url"], row["registrable_domain"].lower()
            parts = urlsplit(url)
            host = (parts.hostname or "").lower()
            if row["url_type"] != "custom_domain" or host not in {domain, f"www.{domain}"}:
                continue
            key = (record["candidate_election_id"], domain)
            if key in rows:
                rows[key]["campaign_urls"].append(url)
                rows[key]["url_ids"].append(url_id)
                continue
            day = pd.Timestamp(record["election_date"])
            rows[key] = {"record": record, "domain": domain, "root": f"{parts.scheme}://{host}/", "campaign_urls": [url],
                         "url_ids": [url_id], "day": day}
    out = []
    for (candidate_election_id, domain), item in rows.items():
        record, day = item["record"], item["day"]
        out.append({
            "target_id": "stage3L_" + hashlib.sha256(f"{candidate_election_id}|{domain}".encode()).hexdigest()[:24],
            "candidate_year_id": record["candidate_year_id"], "candidate_contest_id": candidate_election_id,
            "contest": record["election_id"], "full_name": record["candidate_name"], "geo_name": record["city_name"],
            "state_abb": record["state_abb"], "year": day.year, "month": day.month, "period": period(day.year),
            "registrable_domain": domain, "site_root_url": item["root"], "primary_exact_url": item["campaign_urls"][0],
            "url_type": "custom_domain", "campaign_claim_count": len(item["url_ids"]),
            "campaign_claim_ids_json": json.dumps(item["url_ids"]), "campaign_urls_json": json.dumps(item["campaign_urls"]),
            "evidence_urls_json": "[]", "url_discovery_status": "legacy_unattributed", "election_date": record["election_date"]})
    return pd.DataFrame(out).sort_values("target_id").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", default="mayoral_stage3_20261002")
    parser.add_argument("--exclude", type=Path, nargs="*", default=[])
    parser.add_argument("--legacy", action="store_true")
    args = parser.parse_args()
    leads = pd.read_csv(LEADS, keep_default_na=False, dtype=str)
    done = set()
    for path in args.exclude:
        done |= set(pd.read_csv(path, dtype=str)["candidate_contest_id"])
    leads = leads[~leads["candidate_election_id"].isin(done)]
    coverage = pd.read_csv(COVERAGE, keep_default_na=False, dtype=str)
    if args.legacy:
        targets = build_legacy_targets(coverage, pd.read_csv(LEGACY_URLS, keep_default_na=False, dtype=str, low_memory=False))
    else:
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
