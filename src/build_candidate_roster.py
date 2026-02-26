#!/usr/bin/env python3
"""
Build candidate rosters for House, Senate, and Governor races.

Sources:
  - FEC bulk candidate files (House/Senate): website URLs from filings
  - Ballotpedia (Governor + supplemental): scraped via ScrapeGraphAI

Usage:
    python -m src.build_candidate_roster --office house --year 2022
    python -m src.build_candidate_roster --office governor --year 2022
    python -m src.build_candidate_roster --office senate --years 2002-2024
"""

import argparse
import io
import logging
import os
import re
import zipfile
from typing import Optional

import pandas as pd
import requests

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

    Args:
        year: Election cycle year (even years).
        config: Config dict with FEC URL template.

    Returns:
        DataFrame of candidates or None on failure.
    """
    # FEC uses 2-year cycle years; round up to even
    cycle = year if year % 2 == 0 else year + 1

    # FEC changed format around 2024; try both URL patterns
    # Pattern 1: https://www.fec.gov/files/bulk-downloads/2024/cn24.zip
    # Pattern 2: older cycles use full year
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
        DataFrame with columns: candidate, state, district, office, year, party, website_url
    """
    fec_office = "H" if office == "house" else "S"
    df = download_fec_candidates(year, config)

    if df is None:
        return pd.DataFrame()

    # Filter to office, D/R, active candidates
    df = df[df["cand_office"] == fec_office].copy()
    df = df[df["cand_pty_affiliation"].isin(PARTY_MAP.keys())].copy()
    df["party"] = df["cand_pty_affiliation"].map(PARTY_MAP)

    # Parse candidate name (FEC format: LASTNAME, FIRSTNAME MIDDLE)
    df["candidate"] = df["cand_name"].apply(_clean_name)
    df["state"] = df["cand_office_st"]
    df["district"] = df["cand_office_district"].fillna("")
    df["year"] = year
    df["office"] = office

    # Website URL: not directly in cn.txt; will need supplemental sources
    # For now, construct a placeholder that build_supplemental_urls can fill
    df["website_url"] = ""

    roster = df[["candidate", "state", "district", "office", "year", "party", "website_url"]].copy()
    roster = roster.drop_duplicates(subset=["candidate", "state", "district"])

    logger.info(f"FEC roster: {len(roster)} {office} candidates for {year}")
    return roster


def _clean_name(raw: str) -> str:
    """Convert FEC name format to readable name."""
    if pd.isna(raw):
        return ""
    # FEC: "LASTNAME, FIRSTNAME MIDDLE SUFFIX"
    parts = raw.split(",", 1)
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = parts[1].strip().title()
        return f"{first} {last}"
    return raw.strip().title()


# ── Ballotpedia (via ScrapeGraphAI) ─────────────────────────────────

def build_ballotpedia_roster(year: int, office: str, config: dict) -> pd.DataFrame:
    """
    Build candidate roster from Ballotpedia using ScrapeGraphAI.

    ScrapeGraphAI uses an LLM to intelligently extract structured candidate
    data from Ballotpedia pages, handling dynamic content and varying layouts.

    Args:
        year: Election year.
        office: "governor", "senate", or "house".
        config: Full config dict.

    Returns:
        DataFrame with roster columns.
    """
    try:
        from scrapegraphai.graphs import SmartScraperGraph
    except ImportError:
        logger.error(
            "scrapegraphai not installed. Install with: pip install scrapegraphai\n"
            "Then run: playwright install"
        )
        return pd.DataFrame()

    sg_config = config.get("scrapegraph", {})
    base_url = config.get("roster", {}).get("ballotpedia_base", "https://ballotpedia.org")

    # Build Ballotpedia URL for this office/year
    if office == "governor":
        url = f"{base_url}/Gubernatorial_elections,_{year}"
    elif office == "senate":
        url = f"{base_url}/United_States_Senate_elections,_{year}"
    else:
        url = f"{base_url}/United_States_House_of_Representatives_elections,_{year}"

    prompt = (
        f"Extract all {office} candidates for the {year} general election. "
        f"For each candidate, extract: full name, state, district (if applicable), "
        f"party (D or R only), and campaign website URL if listed. "
        f"Return as a JSON list of objects with keys: "
        f"candidate, state, district, party, website_url"
    )

    graph_config = {
        "llm": {
            "model": sg_config.get("llm_model", "openai/gpt-4o-mini"),
        },
        "headless": sg_config.get("headless", True),
        "verbose": sg_config.get("verbose", False),
    }

    try:
        logger.info(f"Scraping Ballotpedia for {office} {year} via ScrapeGraphAI")
        scraper = SmartScraperGraph(
            prompt=prompt,
            source=url,
            config=graph_config,
        )
        result = scraper.run()

        # Parse result into DataFrame
        if isinstance(result, dict) and "candidates" in result:
            candidates = result["candidates"]
        elif isinstance(result, list):
            candidates = result
        else:
            logger.warning(f"Unexpected ScrapeGraphAI result format: {type(result)}")
            return pd.DataFrame()

        df = pd.DataFrame(candidates)
        df["year"] = year
        df["office"] = office
        df["district"] = df.get("district", "")
        df["website_url"] = df.get("website_url", "")

        # Standardize columns
        for col in ["candidate", "state", "district", "office", "year", "party", "website_url"]:
            if col not in df.columns:
                df[col] = ""

        roster = df[["candidate", "state", "district", "office", "year", "party", "website_url"]].copy()
        logger.info(f"Ballotpedia roster: {len(roster)} {office} candidates for {year}")
        return roster

    except Exception as e:
        logger.error(f"ScrapeGraphAI failed for {office} {year}: {e}")
        return pd.DataFrame()


# ── Supplemental URL lookup ──────────────────────────────────────────

def fill_missing_urls_ballotpedia(roster: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    For candidates missing website URLs, attempt lookup via ScrapeGraphAI
    on their individual Ballotpedia pages.
    """
    try:
        from scrapegraphai.graphs import SmartScraperGraph
    except ImportError:
        logger.warning("scrapegraphai not available for URL supplementation")
        return roster

    sg_config = config.get("scrapegraph", {})
    base_url = config.get("roster", {}).get("ballotpedia_base", "https://ballotpedia.org")
    missing = roster[roster["website_url"] == ""].copy()

    if len(missing) == 0:
        logger.info("All candidates have website URLs")
        return roster

    logger.info(f"Looking up URLs for {len(missing)} candidates via Ballotpedia")

    graph_config = {
        "llm": {
            "model": sg_config.get("llm_model", "openai/gpt-4o-mini"),
        },
        "headless": sg_config.get("headless", True),
        "verbose": sg_config.get("verbose", False),
    }

    for idx, row in missing.iterrows():
        name = row["candidate"]
        # Ballotpedia URL format: First_Last
        bp_name = name.replace(" ", "_")
        url = f"{base_url}/{bp_name}"

        try:
            scraper = SmartScraperGraph(
                prompt=(
                    f"Find the official campaign website URL for {name}. "
                    f"Return just the URL as a string, or empty string if not found."
                ),
                source=url,
                config=graph_config,
            )
            result = scraper.run()

            if isinstance(result, str) and result.startswith("http"):
                roster.at[idx, "website_url"] = result
                logger.info(f"Found URL for {name}: {result}")
            elif isinstance(result, dict) and "url" in result:
                roster.at[idx, "website_url"] = result["url"]

        except Exception as e:
            logger.debug(f"Could not find URL for {name}: {e}")

    n_found = len(roster[roster["website_url"] != ""])
    logger.info(f"URLs found: {n_found}/{len(roster)}")
    return roster


# ── Main pipeline ────────────────────────────────────────────────────

def build_roster(office: str, year: int, config: dict,
                 supplement_urls: bool = True) -> pd.DataFrame:
    """
    Build a complete candidate roster for an office and year.

    Strategy:
      - House/Senate: start with FEC, supplement with Ballotpedia
      - Governor: Ballotpedia only (FEC doesn't cover state races)
    """
    if office in ("house", "senate"):
        roster = build_fec_roster(year, office, config)
        # Merge in Ballotpedia data for website URLs and missing candidates
        bp_roster = build_ballotpedia_roster(year, office, config)
        if not bp_roster.empty:
            roster = _merge_rosters(roster, bp_roster)
    elif office == "governor":
        roster = build_ballotpedia_roster(year, office, config)
    else:
        raise ValueError(f"Unknown office: {office}")

    if supplement_urls and not roster.empty:
        roster = fill_missing_urls_ballotpedia(roster, config)

    # Drop candidates with no website URL
    n_before = len(roster)
    roster = roster[roster["website_url"] != ""].copy()
    n_dropped = n_before - len(roster)
    if n_dropped > 0:
        logger.warning(f"Dropped {n_dropped}/{n_before} candidates with no website URL")

    return roster


def _merge_rosters(fec: pd.DataFrame, bp: pd.DataFrame) -> pd.DataFrame:
    """Merge FEC and Ballotpedia rosters, preferring FEC for overlaps."""
    if fec.empty:
        return bp
    if bp.empty:
        return fec

    # Update FEC entries with Ballotpedia URLs where missing
    merged = fec.copy()
    for idx, row in merged.iterrows():
        if row["website_url"] == "":
            match = bp[
                (bp["state"] == row["state"]) &
                (bp["party"] == row["party"]) &
                (bp["candidate"].str.contains(row["candidate"].split()[-1], case=False, na=False))
            ]
            if not match.empty and match.iloc[0]["website_url"]:
                merged.at[idx, "website_url"] = match.iloc[0]["website_url"]

    # Add Ballotpedia-only candidates not in FEC
    fec_states_parties = set(zip(merged["state"], merged["party"], merged["district"]))
    bp_only = bp[
        ~bp.apply(lambda r: (r["state"], r["party"], r["district"]) in fec_states_parties, axis=1)
    ]
    if not bp_only.empty:
        merged = pd.concat([merged, bp_only], ignore_index=True)

    return merged


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
        description="Build candidate rosters from FEC and Ballotpedia."
    )
    parser.add_argument("--office", type=str, required=True,
                        choices=["house", "senate", "governor"])
    parser.add_argument("--year", type=int, default=None,
                        help="Single election year")
    parser.add_argument("--years", type=str, default=None,
                        help="Year range, e.g., 2018-2024")
    parser.add_argument("--no-supplement", action="store_true",
                        help="Skip Ballotpedia URL supplementation")
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
        # For House/Senate, only even years; for Governor, could be odd too
        if args.office in ("house", "senate"):
            years = list(range(start, end + 1, 2))
        else:
            years = list(range(start, end + 1))
    else:
        # Use config defaults
        years = config.get("scope", {}).get(args.office, {}).get("years", [])

    if not years:
        parser.error("No years specified. Use --year, --years, or configure in config.yaml")

    for year in years:
        logger.info(f"Building roster for {args.office} {year}")
        roster = build_roster(args.office, year, config,
                              supplement_urls=not args.no_supplement)
        if not roster.empty:
            save_roster(roster, args.office, year, config)
        else:
            logger.warning(f"Empty roster for {args.office} {year}")


if __name__ == "__main__":
    main()
