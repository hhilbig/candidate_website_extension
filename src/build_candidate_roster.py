#!/usr/bin/env python3
"""
Build candidate rosters for House and Senate races.

Sources:
  - FEC bulk candidate files: candidate names, state, party
  - Ballotpedia: campaign website URLs (via BeautifulSoup scraping)

Usage:
    python -m src.build_candidate_roster --office house --year 2022
    python -m src.build_candidate_roster --office senate --years 2002-2024
"""

import argparse
import io
import logging
import os
import re
import time
import zipfile
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .utils import RateLimiter, load_config, setup_logging

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

# FEC nicknames: quoted strings preceded by whitespace (not mid-word apostrophes)
# Matches: CRUZ, RAFAEL EDWARD "TED" → TED
# Avoids: O'ROURKE (apostrophe is part of name, not a quote)
NICKNAME_PATTERN = re.compile(r'(?<=\s)["\']([A-Za-z]+)["\']')

# Suffixes to strip before constructing Ballotpedia URLs
NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}


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
        website_url, fec_raw_name
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
    df["candidate"] = df["cand_name"].apply(_clean_name)
    df["state"] = df["cand_office_st"]
    df["district"] = df["cand_office_district"].fillna("")
    df["year"] = year
    df["office"] = office

    # Website URL: filled later by Ballotpedia lookup
    df["website_url"] = ""

    cols = ["candidate", "state", "district", "office", "year", "party",
            "website_url", "fec_raw_name"]
    roster = df[cols].copy()
    roster = roster.drop_duplicates(subset=["candidate", "state", "district"])

    logger.info(f"FEC roster: {len(roster)} {office} candidates for {year}")
    return roster


def _clean_name(raw: str) -> str:
    """Convert FEC name format to readable name.

    Strips quoted nicknames and extra whitespace.
    'CRUZ, RAFAEL EDWARD "TED"' → 'Rafael Edward Cruz'
    """
    if pd.isna(raw):
        return ""
    # Remove quoted nicknames before parsing
    cleaned = re.sub(r'["\'][A-Za-z]+["\']', '', raw)
    # FEC: "LASTNAME, FIRSTNAME MIDDLE SUFFIX"
    parts = cleaned.split(",", 1)
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = " ".join(parts[1].split()).strip().title()
        return f"{first} {last}"
    return " ".join(cleaned.split()).strip().title()


def _extract_nickname(fec_name: str) -> Optional[str]:
    """
    Extract nickname from FEC name if present.

    FEC format: 'CRUZ, RAFAEL EDWARD "TED"' → "Ted"
    """
    match = NICKNAME_PATTERN.search(fec_name)
    if match:
        return match.group(1).strip().title()
    return None


def _name_to_ballotpedia_slug(name: str, state: str = "") -> list[str]:
    """
    Convert a cleaned candidate name to Ballotpedia URL slug(s) to try.

    Returns a list of slugs ordered by likelihood:
      1. Full name (e.g., "Michael_F_Bennet")
      2. First + Last only (e.g., "Michael_Bennet") — most common on Ballotpedia
      3. State disambiguation variants of both

    Strips suffixes (Jr, III, etc.) and handles apostrophes/hyphens.
    """
    # Remove suffixes (Jr, III, etc.)
    words = name.split()
    words = [w for w in words if w.lower().rstrip(".") not in NAME_SUFFIXES]

    slugs = []
    state_name = _state_abbrev_to_name(state) if state else None

    # First + Last only (most likely to match Ballotpedia)
    if len(words) >= 2:
        first_last = f"{words[0]}_{words[-1]}"
        slugs.append(first_last)
        if state_name:
            slugs.append(f"{first_last}_({state_name})")

    # Full name (may include middle initial/name)
    full = "_".join(words)
    if full not in slugs:
        slugs.append(full)
        if state_name:
            slugs.append(f"{full}_({state_name})")

    return slugs


# State abbreviation → full name mapping
_STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "AS": "American Samoa", "GU": "Guam", "MP": "Northern Mariana Islands",
    "PR": "Puerto Rico", "VI": "U.S. Virgin Islands",
}


def _state_abbrev_to_name(abbrev: str) -> Optional[str]:
    """Convert 2-letter state abbreviation to full name."""
    return _STATE_NAMES.get(abbrev.upper())


def _extract_campaign_website(page_url: str, session: requests.Session,
                               rate_limiter: RateLimiter) -> Optional[str]:
    """
    Fetch a Ballotpedia candidate page and extract the campaign website URL.

    Looks for links with text "Campaign website" or "Official website" in
    the candidate infobox.

    Returns:
        Campaign website URL string, or None if not found.
    """
    rate_limiter.wait()

    try:
        response = session.get(page_url, timeout=(15, 30))
        if response.status_code == 404:
            return None
        response.raise_for_status()
    except requests.RequestException as e:
        logger.debug(f"Failed to fetch {page_url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Collect all website links, prioritizing campaign over official
    campaign_url = None
    official_url = None

    for link in soup.find_all("a"):
        text = link.get_text(strip=True).lower()
        href = link.get("href", "")
        if not href.startswith("http"):
            continue
        if text in ("campaign website", "campaign site"):
            campaign_url = href
            break  # Best match, stop looking
        elif text == "official website" and official_url is None:
            official_url = href

    if campaign_url:
        return campaign_url
    if official_url:
        return official_url

    # Fallback: look in infobox table for "Website" row
    for td in soup.find_all("td"):
        if "website" in td.get_text(strip=True).lower():
            link = td.find_next("a", href=True)
            if link and link["href"].startswith("http"):
                return link["href"]

    return None


def _search_ballotpedia(name: str, state: str, session: requests.Session,
                         rate_limiter: RateLimiter) -> Optional[str]:
    """
    Search Ballotpedia's MediaWiki API for a candidate page.

    Returns:
        The page title of the best match, or None.
    """
    rate_limiter.wait()

    state_name = _state_abbrev_to_name(state) or state
    query = f"{name} {state_name}"

    try:
        response = session.get(
            "https://ballotpedia.org/wiki/api.php",
            params={
                "action": "query",
                "list": "search",
                "srsearch": query,
                "format": "json",
                "srlimit": 5,
            },
            timeout=(15, 30),
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.debug(f"Ballotpedia search failed for '{query}': {e}")
        return None

    results = data.get("query", {}).get("search", [])
    if not results:
        return None

    # Try to match by last name — only accept short titles that look like
    # person names (not bills, elections, or other long article titles)
    last_name = name.split()[-1].lower() if name.split() else ""
    for result in results:
        title = result.get("title", "")
        # Person pages are typically short (< 6 words) and contain the last name
        if last_name and last_name in title.lower() and len(title.split()) <= 5:
            return title

    return None


def fill_urls_from_ballotpedia(roster: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    For each candidate with an empty website_url, look up their Ballotpedia
    page and extract the campaign website URL.

    Uses direct name → URL construction first, then MediaWiki search as fallback.
    """
    bp_config = config.get("ballotpedia", {})
    rate_limit = bp_config.get("rate_limit_seconds", 1.0)
    max_retries = bp_config.get("max_retries", 2)
    user_agent = bp_config.get("user_agent", "CandidateWebsiteExtension/1.0 (Academic Research)")

    missing = roster[roster["website_url"] == ""].index
    if len(missing) == 0:
        logger.info("All candidates already have website URLs")
        return roster

    logger.info(f"Looking up Ballotpedia URLs for {len(missing)} candidates")

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    rate_limiter = RateLimiter(min_delay=rate_limit, backoff_factor=2, backoff_max=60)

    n_found = 0
    n_search_fallback = 0
    n_failed = 0

    for idx in missing:
        row = roster.loc[idx]
        name = row["candidate"]
        state = row["state"]
        fec_raw = row.get("fec_raw_name", "")

        # Build list of name variants to try
        slugs = _name_to_ballotpedia_slug(name, state)

        # If FEC name has a nickname, also try nickname + last name
        if fec_raw:
            nickname = _extract_nickname(fec_raw)
            if nickname:
                last_name = name.split()[-1] if name.split() else ""
                if last_name:
                    nick_name = f"{nickname} {last_name}"
                    slugs = _name_to_ballotpedia_slug(nick_name, state) + slugs

        # Try direct URL construction
        found = False
        for slug in slugs:
            url = f"https://ballotpedia.org/{slug}"
            website = _extract_campaign_website(url, session, rate_limiter)
            if website:
                roster.at[idx, "website_url"] = website
                logger.debug(f"Found URL for {name}: {website}")
                n_found += 1
                found = True
                break

        if found:
            continue

        # Fallback: MediaWiki search
        title = _search_ballotpedia(name, state, session, rate_limiter)
        if title:
            url = f"https://ballotpedia.org/{title.replace(' ', '_')}"
            website = _extract_campaign_website(url, session, rate_limiter)
            if website:
                roster.at[idx, "website_url"] = website
                logger.debug(f"Found URL for {name} (via search): {website}")
                n_found += 1
                n_search_fallback += 1
                continue

        n_failed += 1
        logger.debug(f"No campaign website found for {name} ({state})")

    session.close()

    logger.info(
        f"Ballotpedia URL lookup complete: {n_found} found "
        f"({n_search_fallback} via search fallback), {n_failed} not found"
    )
    return roster


# ── Main pipeline ────────────────────────────────────────────────────

def build_roster(office: str, year: int, config: dict) -> pd.DataFrame:
    """
    Build a complete candidate roster for an office and year.

    Strategy:
      - Build FEC roster (names, state, party, raw FEC name)
      - Look up campaign website URLs from Ballotpedia
      - Drop candidates with no URL found
    """
    if office not in ("house", "senate"):
        raise ValueError(f"Unsupported office: {office}. Use 'house' or 'senate'.")

    roster = build_fec_roster(year, office, config)
    if roster.empty:
        return roster

    # Look up campaign website URLs from Ballotpedia
    roster = fill_urls_from_ballotpedia(roster, config)

    # Drop the fec_raw_name helper column
    if "fec_raw_name" in roster.columns:
        roster = roster.drop(columns=["fec_raw_name"])

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
        description="Build candidate rosters from FEC + Ballotpedia."
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
