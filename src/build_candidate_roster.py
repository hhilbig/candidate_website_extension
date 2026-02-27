#!/usr/bin/env python3
"""
Build candidate rosters for House and Senate races.

Sources:
  - FEC bulk candidate files: candidate names, state, party, committee IDs
  - Multi-source waterfall for campaign website URLs:
    OpenFEC API → Wikidata

Usage:
    python -m src.build_candidate_roster --office house --year 2022
    python -m src.build_candidate_roster --office senate --years 2002-2024
"""

import argparse
import io
import logging
import os
import zipfile
from typing import Optional

import pandas as pd
import requests

from .name_utils import clean_name
from .url_sources import build_default_sources, run_waterfall
from .utils import load_config, setup_logging

logger = logging.getLogger(__name__)


# ── FEC bulk data ────────────────────────────────────────────────────

FEC_CANDIDATE_COLUMNS = [
    "cand_id", "cand_name", "cand_pty_affiliation", "cand_election_yr",
    "cand_office_st", "cand_office", "cand_office_district",
    "cand_ici", "cand_status", "cand_pcc", "cand_st1", "cand_st2",
    "cand_city", "cand_st", "cand_zip",
]

# FEC party codes for D and R
PARTY_MAP = {"DEM": "D", "REP": "R", "DFL": "D"}  # DFL = Minnesota Democrats

# FEC office codes
OFFICE_MAP = {"H": "house", "S": "senate", "P": "president"}


def download_fec_candidates(year: int, config: dict) -> Optional[pd.DataFrame]:
    """
    Download and parse FEC bulk candidate file for a given cycle.
    Caches downloaded files locally to avoid re-downloading.

    Args:
        year: Election cycle year (even years).
        config: Config dict with FEC URL template.

    Returns:
        DataFrame of candidates or None on failure.
    """
    # FEC uses 2-year cycle years; round up to even
    cycle = year if year % 2 == 0 else year + 1

    # Check local cache first
    cache_dir = os.path.join(config.get("output", {}).get("base_dir", "data"), "fec_cache")
    cache_path = os.path.join(cache_dir, f"cn{cycle}.csv")
    if os.path.exists(cache_path):
        logger.info(f"Loading FEC {cycle} from cache: {cache_path}")
        return pd.read_csv(cache_path, dtype=str)

    # FEC changed format around 2024; try both URL patterns
    urls_to_try = [
        f"https://www.fec.gov/files/bulk-downloads/{cycle}/cn{str(cycle)[-2:]}.zip",
        f"https://www.fec.gov/files/bulk-downloads/{cycle}/cn{cycle}.zip",
    ]

    for url in urls_to_try:
        try:
            logger.info(f"Downloading FEC candidate file: {url}")
            response = requests.get(url, timeout=60)
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    # Find the cn.txt file inside the zip
                    txt_files = [f for f in zf.namelist() if f.endswith(".txt")]
                    if not txt_files:
                        logger.warning(f"No .txt file in {url}")
                        continue

                    with zf.open(txt_files[0]) as f:
                        df = pd.read_csv(
                            f, sep="|", header=None,
                            names=FEC_CANDIDATE_COLUMNS[:15],
                            encoding="latin-1",
                            dtype=str,
                            on_bad_lines="skip",
                        )
                    logger.info(f"Loaded {len(df)} candidates from FEC {cycle}")

                    # Cache locally
                    os.makedirs(cache_dir, exist_ok=True)
                    df.to_csv(cache_path, index=False)
                    logger.info(f"Cached FEC {cycle} to {cache_path}")

                    return df

        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")

    logger.error(f"Could not download FEC candidate file for {cycle}")
    return None


def build_fec_roster(year: int, office: str, config: dict) -> pd.DataFrame:
    """
    Build candidate roster from FEC data for House or Senate.

    Args:
        year: Election year.
        office: "house" or "senate".
        config: Full config dict.

    Returns:
        DataFrame with columns: candidate, state, district, office, year, party,
        website_url, fec_raw_name, cand_pcc, cand_id
    """
    fec_office = "H" if office == "house" else "S"
    df = download_fec_candidates(year, config)

    if df is None:
        return pd.DataFrame()

    # Filter to office, D/R, active candidates
    df = df[df["cand_office"] == fec_office].copy()
    df = df[df["cand_pty_affiliation"].isin(PARTY_MAP.keys())].copy()
    df["party"] = df["cand_pty_affiliation"].map(PARTY_MAP)

    # Keep raw name for nickname extraction; parse clean name
    df["fec_raw_name"] = df["cand_name"].fillna("")
    df["candidate"] = df["cand_name"].apply(clean_name)
    df["state"] = df["cand_office_st"]
    df["district"] = df["cand_office_district"].fillna("")
    df["year"] = year
    df["office"] = office

    # Website URL: filled later by URL waterfall
    df["website_url"] = ""

    # Keep cand_pcc and cand_id for OpenFEC lookups
    df["cand_pcc"] = df["cand_pcc"].fillna("")
    df["cand_id"] = df["cand_id"].fillna("")

    cols = ["candidate", "state", "district", "office", "year", "party",
            "website_url", "fec_raw_name", "cand_pcc", "cand_id"]
    roster = df[cols].copy()
    roster = roster.drop_duplicates(subset=["candidate", "state", "district"])

    logger.info(f"FEC roster: {len(roster)} {office} candidates for {year}")
    return roster


# ── Main pipeline ────────────────────────────────────────────────────

def build_roster(office: str, year: int, config: dict) -> pd.DataFrame:
    """
    Build a complete candidate roster for an office and year.

    Strategy:
      - Build FEC roster (names, state, party, committee IDs)
      - Run multi-source URL waterfall (OpenFEC → Wikidata)
      - Drop helper columns and candidates with no URL found
    """
    if office not in ("house", "senate"):
        raise ValueError(f"Unsupported office: {office}. Use 'house' or 'senate'.")

    roster = build_fec_roster(year, office, config)
    if roster.empty:
        return roster

    # Run multi-source URL waterfall
    sources = build_default_sources()
    roster = run_waterfall(roster, config, sources)

    # Drop helper columns
    helper_cols = ["fec_raw_name", "cand_pcc", "cand_id"]
    roster = roster.drop(columns=[c for c in helper_cols if c in roster.columns])

    # Drop candidates with no website URL
    n_before = len(roster)
    roster = roster[roster["website_url"] != ""].copy()
    n_dropped = n_before - len(roster)
    if n_dropped > 0:
        logger.warning(f"Dropped {n_dropped}/{n_before} candidates with no website URL")

    return roster


def save_roster(roster: pd.DataFrame, office: str, year: int, config: dict):
    """Save roster to CSV."""
    out_dir = config.get("output", {}).get("roster_dir", "data/rosters")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"roster_{office}_{year}.csv")
    roster.to_csv(path, index=False)
    logger.info(f"Saved roster ({len(roster)} candidates) to {path}")


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build candidate rosters from FEC + multi-source URL lookup."
    )
    parser.add_argument("--office", type=str, required=True,
                        choices=["house", "senate"])
    parser.add_argument("--year", type=int, default=None,
                        help="Single election year")
    parser.add_argument("--years", type=str, default=None,
                        help="Year range, e.g., 2018-2024")
    parser.add_argument("--config", type=str, default="config/config.yaml")
    parser.add_argument("--log-level", type=str, default="INFO")

    args = parser.parse_args()
    setup_logging(args.log_level)
    config = load_config(args.config)

    # Determine years
    if args.year:
        years = [args.year]
    elif args.years:
        start, end = map(int, args.years.split("-"))
        years = list(range(start, end + 1, 2))  # House/Senate are even years only
    else:
        years = config.get("scope", {}).get(args.office, {}).get("years", [])

    if not years:
        parser.error("No years specified. Use --year, --years, or configure in config.yaml")

    for year in years:
        logger.info(f"Building roster for {args.office} {year}")
        roster = build_roster(args.office, year, config)
        if not roster.empty:
            save_roster(roster, args.office, year, config)
        else:
            logger.warning(f"Empty roster for {args.office} {year}")


if __name__ == "__main__":
    main()
