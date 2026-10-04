#!/usr/bin/env python3
"""Discover candidate campaign sites from archived Ballotpedia election pages."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_mayoral_url_discovery import (
    ArchiveRequestError, CDX_URL, EVIDENCE_COLUMNS, fetch_archived_profile,
    fetch_text_with_curl, write_csv_atomic,
)
from src.mayoral_discovery import (
    choose_near_election_snapshots, extract_ballotpedia_election_page_campaign_links,
    extract_ballotpedia_election_stage,
)

LOGGER = logging.getLogger(__name__)
STATE_NAMES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
    "CO":"Colorado","CT":"Connecticut","DE":"Delaware","DC":"District of Columbia",
    "FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois",
    "IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana",
    "ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota",
    "MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada",
    "NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York",
    "NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon",
    "PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota",
    "TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia",
    "WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
}
PAGE_STATUS_COLUMNS = [
    "contest", "geo_name", "state_abb", "year", "source_profile_url", "source_status",
    "source_snapshot_count", "source_snapshots_checked", "source_snapshot_timestamps_checked",
    "candidate_urls_found", "error", "source_election_stage",
    "source_election_stage_label", "source_election_date",
    "source_election_stage_confidence", "stage_extraction_status",
]

CITY_TOKEN_CASE = {
    "mcallen": "McAllen",
    "mckinney": "McKinney",
}
CITY_NAME_OVERRIDES = {
    "boise city": "Boise",
    "nashville-davidson county": "Nashville",
}


def ballotpedia_city_title(city: str) -> str:
    """Restore display casing in lower-cased ALGEL city names."""
    normalized = str(city).strip().casefold()
    if normalized in CITY_NAME_OVERRIDES:
        return CITY_NAME_OVERRIDES[normalized]
    words = []
    for word in str(city).split():
        parts = [
            CITY_TOKEN_CASE.get(part, part[:1].upper() + part[1:])
            for part in word.split("-")
        ]
        words.append("-".join(parts))
    return "_".join(words)


def election_page_url(city: str, state_abb: str, year: int) -> str:
    city_title = ballotpedia_city_title(city)
    state_title = STATE_NAMES[state_abb].replace(" ", "_")
    return f"https://ballotpedia.org/Mayoral_election_in_{city_title},_{state_title}_({int(year)})"


def query_page_snapshots(page_url: str, year: int, timeout: int, attempts: int) -> list[str]:
    query = urlencode([
        ("url", page_url), ("from", year), ("to", year + 1), ("output", "json"),
        ("filter", "statuscode:200"), ("filter", "mimetype:text/html"),
        ("fl", "timestamp,original,digest"), ("collapse", "digest"),
    ])
    payload = json.loads(fetch_text_with_curl(f"{CDX_URL}?{query}", timeout, attempts))
    return [row[0] for row in payload[1:] if row] if len(payload) > 1 else []


def discover_election_pages(
    universe: pd.DataFrame, output_dir: Path, min_year: int = 2015,
    timeout: int = 20, attempts: int = 1, delay_seconds: float = 1,
    snapshot_limit: int = 1, max_pages: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scoped = universe[universe["in_discovery_scope"] & universe["year"].ge(min_year)].copy()
    pages = scoped.groupby(["contest", "geo_name", "state_abb", "year"], as_index=False).agg(
        month=("month", "first")
    ).sort_values(["year", "state_abb", "geo_name"], ascending=[False, True, True])
    evidence_path = output_dir / "mayoral_url_evidence_election_pages.csv"
    status_path = output_dir / "mayoral_source_status_election_pages.csv"
    prior_evidence = pd.read_csv(evidence_path) if evidence_path.exists() else pd.DataFrame(columns=EVIDENCE_COLUMNS)
    prior_status = pd.read_csv(status_path, dtype={"contest":"string"}) if status_path.exists() else pd.DataFrame(columns=PAGE_STATUS_COLUMNS)
    for column in PAGE_STATUS_COLUMNS:
        if column not in prior_status:
            prior_status[column] = ""
    valid_contests = set(pages["contest"].astype(str))
    prior_year = pd.to_numeric(prior_status["year"], errors="coerce")
    prior_status = prior_status.loc[
        prior_year.lt(min_year) | prior_status["contest"].astype(str).isin(valid_contests)
    ].drop_duplicates("contest", keep="last").copy()
    stage_complete = (
        prior_status.source_status.ne("page_checked")
        | prior_status.stage_extraction_status.fillna("").ne("")
    )
    expected_url = prior_status.apply(
        lambda row: election_page_url(row.geo_name, row.state_abb, int(row.year)), axis=1
    )
    source_url_current = prior_status.source_profile_url.eq(expected_url)
    completed = set(prior_status.loc[
        prior_status.source_status.ne("error") & stage_complete & source_url_current, "contest"
    ].astype(str))
    pages = pages[~pages.contest.astype(str).isin(completed)]
    if max_pages is not None:
        pages = pages.head(max_pages)
    evidence_rows, status_rows = prior_evidence.to_dict("records"), prior_status.to_dict("records")
    for number, page in enumerate(pages.itertuples(index=False), start=1):
        candidates = scoped[scoped.contest.astype(str).eq(str(page.contest))].drop_duplicates("candidate_year_id")
        page_url = election_page_url(page.geo_name, page.state_abb, page.year)
        status = {"contest":page.contest,"geo_name":page.geo_name,"state_abb":page.state_abb,
                  "year":page.year,"source_profile_url":page_url,"source_status":"not_attempted",
                  "source_snapshot_count":0,"source_snapshots_checked":0,
                  "source_snapshot_timestamps_checked":"","candidate_urls_found":0,"error":"",
                  "source_election_stage":"", "source_election_stage_label":"",
                  "source_election_date":"", "source_election_stage_confidence":"",
                  "stage_extraction_status":"not_attempted"}
        LOGGER.info("Checking election page %s/%s: %s %s", number, len(pages), page.geo_name, page.year)
        try:
            timestamps = query_page_snapshots(page_url, int(page.year), timeout, attempts)
            status["source_snapshot_count"] = len(timestamps)
            chosen = choose_near_election_snapshots(timestamps, int(page.year), page.month, snapshot_limit)
            status["source_status"] = "no_archived_page" if not timestamps else "page_checked"
            found = set()
            lookup = candidates.set_index("full_name")
            for timestamp in chosen:
                time.sleep(delay_seconds)
                html = fetch_archived_profile(timestamp, page_url, timeout, attempts)
                status["source_snapshots_checked"] += 1
                status["source_snapshot_timestamps_checked"] = timestamp
                stage = extract_ballotpedia_election_stage(html, int(page.year), int(page.month))
                if stage:
                    status.update(stage)
                    status["stage_extraction_status"] = "date_matched_stage_found"
                for item in extract_ballotpedia_election_page_campaign_links(html, candidates.full_name.tolist()):
                    row = lookup.loc[item.pop("full_name")]
                    key = (row.candidate_year_id, item["evidence_url"])
                    if key in found:
                        continue
                    found.add(key)
                    evidence_rows.append({
                        "candidate_year_id":row.candidate_year_id,"ledb_candid":row.ledb_candid,
                        "full_name":row.name,"geo_name":row.geo_name,"state_abb":row.state_abb,
                        "year":row.year,"candidate_source":"American Local Government Elections Database",
                        "candidate_source_doi":"10.17605/OSF.IO/MV5E6",
                        "url_source":"Ballotpedia archived mayoral election page",
                        "source_profile_url":page_url,"source_snapshot_timestamp":timestamp,
                        "retrieved_on":date.today().isoformat(), **item,
                    })
            status["candidate_urls_found"] = len({candidate for candidate, _ in found})
            if chosen and status["stage_extraction_status"] == "not_attempted":
                status["stage_extraction_status"] = "no_unique_date_matched_stage"
        except (ArchiveRequestError, ValueError, json.JSONDecodeError) as error:
            status["source_status"] = "error"
            status["error"] = f"{type(error).__name__}: {error}"[:500]
        status_rows = [
            item for item in status_rows if str(item.get("contest")) != str(page.contest)
        ]
        status_rows.append(status)
        evidence_frame = pd.DataFrame(evidence_rows).drop_duplicates(
            ["candidate_year_id", "evidence_url", "source_snapshot_timestamp"],
            keep="last",
        )
        evidence_rows = evidence_frame.to_dict("records")
        write_csv_atomic(evidence_frame, evidence_path, EVIDENCE_COLUMNS)
        write_csv_atomic(pd.DataFrame(status_rows), status_path, PAGE_STATUS_COLUMNS)
        time.sleep(delay_seconds)
    return pd.DataFrame(evidence_rows).reindex(columns=EVIDENCE_COLUMNS), pd.DataFrame(status_rows).reindex(columns=PAGE_STATUS_COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/municipal_discovery/shard_0")
    parser.add_argument("--min-year", type=int, default=2015)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--delay-seconds", type=float, default=1)
    parser.add_argument("--snapshot-limit", type=int, default=1)
    parser.add_argument("--max-pages", type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    directory = Path(args.input_dir)
    evidence, status = discover_election_pages(
        pd.read_csv(directory / "mayoral_candidate_universe.csv", low_memory=False), directory,
        min_year=args.min_year, timeout=args.timeout, attempts=args.attempts,
        delay_seconds=args.delay_seconds, snapshot_limit=args.snapshot_limit, max_pages=args.max_pages,
    )
    print({"evidence_rows":len(evidence),"candidate_years":evidence.candidate_year_id.nunique() if len(evidence) else 0,"status_rows":len(status)})


if __name__ == "__main__":
    main()
